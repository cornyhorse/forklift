"""The OpenID Provider as the gateway talks to it: discovery, keys, the token and UserInfo
endpoints.

Every call goes to the issuer the deployment configured or to an endpoint the provider's own
discovery document names, never to a URL a user chose (so the webhooks' public-address rule does
not apply: providers often live on the installation's own network). Each call

- uses ``https://`` only (``http://`` too with FORKLIFT_OIDC_ALLOW_HTTP, for development) and
  verifies TLS against the host name, with the system's CA certificates or the ones in
  FORKLIFT_OIDC_CA_FILE;
- has 5 seconds to connect and 10 seconds for the whole exchange, so a provider that answers a
  byte at a time cannot hold a sign-in;
- reads at most 1 MiB of an answer, follows no redirects and uses no proxy.

The discovery document (whose ``issuer`` must equal the configured issuer exactly) and the key
set are cached in each process for an hour. A token signed with a key the cache does not hold
makes it fetch the key set again, at most once a minute: a key rotation at the provider is
picked up at once, and a stream of made-up key ids does not become a stream of requests.

Errors are :class:`ProviderError`: a message for the person signing in and a detail for the log,
neither with a token, a code or the client secret.
"""

from __future__ import annotations

import base64
import functools
import http.client
import json
import logging
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urlencode, urlsplit

from forklift_web import __version__
from forklift_web.deadline import DeadlineSocket
from forklift_web.oidc.config import OidcConfig

logger = logging.getLogger(__name__)

CONNECT_SECONDS = 5.0
TOTAL_SECONDS = 10.0
MAX_BYTES = 1024 * 1024
CACHE_SECONDS = 3600.0
KEYS_REFRESH_SECONDS = 60.0
DEFAULT_PORTS = {"https": 443, "http": 80}


class ProviderError(Exception):
    """The provider could not be used. ``message`` is for the person signing in, ``detail`` for
    the log and ``reason`` a code for the audit log; none contains a token, a code or a secret."""

    def __init__(self, message: str, *, detail: str = "", reason: str = "provider_unavailable"):
        super().__init__(message)
        self.message = message
        self.detail = detail or message
        self.reason = reason


@dataclass(frozen=True)
class Metadata:
    """What the gateway uses of the discovery document. ``algorithms`` are the ID token
    signature algorithms both the provider and FORKLIFT_OIDC_ALGORITHMS allow."""

    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    userinfo_endpoint: Optional[str]
    end_session_endpoint: Optional[str]
    algorithms: tuple


@functools.lru_cache(maxsize=8)
def _tls_context(ca_file: Optional[str]) -> ssl.SSLContext:
    return ssl.create_default_context(cafile=ca_file)


def with_query(url: str, params: dict) -> str:
    """``url`` with ``params`` added to its query (an endpoint may have a query of its own)."""
    return url + ("&" if urlsplit(url).query else "?") + urlencode(params)


class Provider:
    """The configured provider, with its discovery document and keys cached (thread-safe).
    ``clock`` is there for tests."""

    def __init__(self, config: OidcConfig, *, clock=time.monotonic):
        self.config = config
        self.clock = clock
        self._lock = threading.Lock()
        self._keys_lock = threading.Lock()
        self._metadata: Optional[Metadata] = None
        self._metadata_until = 0.0
        self._keys: list = []
        self._keys_fetched_at: Optional[float] = None

    @property
    def name(self) -> str:
        return self.config.provider_name

    # ----------------------------------------------------------------------- HTTP

    def _unavailable(self, host: str, problem: str, detail: str) -> ProviderError:
        return ProviderError(
            f"Signing in with {self.name} is not possible right now: {host} {problem}. Try again "
            "later; if it keeps failing, an admin finds the details in the gateway's log.",
            detail=detail,
        )

    def _target(self, url, what: str):
        schemes = ("https", "http") if self.config.allow_http else ("https",)
        parts = urlsplit(url) if isinstance(url, str) else None
        try:
            port = parts.port if parts is not None else None
        except ValueError:
            parts = None
        if parts is None or parts.scheme not in schemes or not parts.hostname or parts.username:
            raise ProviderError(
                f"Signing in with {self.name} is not set up correctly: the provider's {what} is "
                "a URL this installation cannot use. An admin finds the details in the gateway's "
                "log.",
                detail=f"The {what} URL {url!r} is not an "
                + " or ".join(f"{scheme}://" for scheme in schemes)
                + " URL with a host (and without a user name).",
                reason="provider_misconfigured",
            )
        return parts, port or DEFAULT_PORTS[parts.scheme]

    def request(self, method: str, url: str, *, what: str, body=None, headers=None):
        """(status, Content-Type, body) of one exchange with the provider."""
        parts, port = self._target(url, what)
        host = parts.hostname
        path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        if parts.scheme == "https":
            connection = http.client.HTTPSConnection(
                host, port, timeout=CONNECT_SECONDS, context=_tls_context(self.config.ca_file)
            )
        else:
            connection = http.client.HTTPConnection(host, port, timeout=CONNECT_SECONDS)
        sent = {
            "Accept": "application/json",
            "User-Agent": f"forklift-web/{__version__}",
            **(headers or {}),
        }
        deadline = time.monotonic() + TOTAL_SECONDS
        response = None
        try:
            connection.connect()
            connection.sock = DeadlineSocket(connection.sock, deadline)
            connection.request(method, path, body=body, headers=sent)
            response = connection.getresponse()
            length = response.getheader("Content-Length", "")
            if length.isdigit() and int(length) > MAX_BYTES:
                raise _TooLarge()
            data = response.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise _TooLarge()
            return response.status, response.getheader("Content-Type", ""), data
        except _TooLarge:
            raise self._unavailable(
                host,
                f"sent an answer larger than {MAX_BYTES // 1024} KiB",
                f"The {what} at {url} answered with more than {MAX_BYTES} bytes.",
            ) from None
        except (OSError, http.client.HTTPException) as error:
            raise self._unavailable(
                host, _problem(error), f"The {what} at {url} failed: {_describe(error)}"
            ) from None
        finally:
            if response is not None:
                response.close()
            connection.close()

    def _json(self, url: str, what: str, status: int, content_type: str, data: bytes) -> dict:
        host = urlsplit(url).hostname
        if 300 <= status < 400:
            raise self._unavailable(
                host,
                f"answered with a redirect ({status}), which is not followed",
                f"The {what} at {url} answered {status}; give the URL it redirects to.",
            )
        if status != 200:
            raise self._unavailable(
                host, f"answered {status}", f"The {what} at {url} answered {status}."
            )
        try:
            document = json.loads(data)
        except (UnicodeDecodeError, ValueError):
            document = None
        if not isinstance(document, dict):
            raise self._unavailable(
                host,
                "did not answer with a JSON object",
                f"The {what} at {url} did not answer with a JSON object (Content-Type "
                f"{content_type!r}).",
            )
        return document

    def get_json(self, url: str, what: str) -> dict:
        return self._json(url, what, *self.request("GET", url, what=what))

    # ----------------------------------------------------------------------- discovery

    def metadata(self) -> Metadata:
        """The discovery document's endpoints and algorithms (cached for an hour)."""
        with self._lock:
            if self._metadata is not None and self.clock() < self._metadata_until:
                return self._metadata
        found = self._discover()  # outside the lock: a slow provider must not queue sign-ins
        with self._lock:
            self._metadata, self._metadata_until = found, self.clock() + CACHE_SECONDS
        return found

    def _misconfigured(self, problem: str, detail: str) -> ProviderError:
        return ProviderError(
            f"Signing in with {self.name} is not set up correctly: {problem}. An admin finds "
            "the details in the gateway's log.",
            detail=detail,
            reason="provider_misconfigured",
        )

    def _discover(self) -> Metadata:
        config = self.config
        url = config.discovery_url
        document = self.get_json(url, "discovery document")
        issuer = document.get("issuer")
        if issuer != config.issuer:
            raise self._misconfigured(
                "the provider's discovery document names another issuer than this installation "
                "is configured for",
                f"The discovery document at {url} names the issuer {issuer!r}, but "
                f"FORKLIFT_OIDC_ISSUER is {config.issuer!r}; they must be equal character for "
                "character (a trailing slash counts).",
            )
        endpoints = {}
        for key in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
            value = document.get(key)
            if not isinstance(value, str):
                raise self._misconfigured(
                    f"the provider's discovery document has no {key}",
                    f"The discovery document at {url} has no {key}.",
                )
            self._target(value, key.replace("_", " "))
            endpoints[key] = value
        for key in ("userinfo_endpoint", "end_session_endpoint"):
            value = document.get(key)
            endpoints[key] = value if isinstance(value, str) and value else None
            if endpoints[key] is not None:
                self._target(value, key.replace("_", " "))
        if config.userinfo and endpoints["userinfo_endpoint"] is None:
            raise self._misconfigured(
                "the provider has no UserInfo endpoint",
                f"FORKLIFT_OIDC_USERINFO is true, but the discovery document at {url} names no "
                "userinfo_endpoint.",
            )
        self._supports(document, "response_types_supported", "code", "the authorization code flow")
        self._supports(document, "code_challenge_methods_supported", "S256", "PKCE with S256")
        self._supports(
            document,
            "token_endpoint_auth_methods_supported",
            config.token_auth_method,
            f"{config.token_auth_method} (FORKLIFT_OIDC_TOKEN_AUTH_METHOD)",
        )
        offered = document.get("id_token_signing_alg_values_supported")
        if isinstance(offered, list):
            algorithms = tuple(alg for alg in config.algorithms if alg in offered)
        else:
            algorithms = config.algorithms
        if not algorithms:
            raise self._misconfigured(
                "the provider signs ID tokens with algorithms this installation does not accept",
                f"The provider signs ID tokens with {', '.join(map(str, offered))}; "
                f"FORKLIFT_OIDC_ALGORITHMS allows {', '.join(config.algorithms)}.",
            )
        return Metadata(issuer=issuer, algorithms=algorithms, **endpoints)

    def _supports(self, document: dict, key: str, needed: str, what: str) -> None:
        """A list the provider may publish must, when it does, include ``needed``."""
        offered = document.get(key)
        if isinstance(offered, list) and needed not in offered:
            raise self._misconfigured(
                f"the provider does not offer {what}",
                f"The discovery document's {key} is {offered!r}, without {needed!r}.",
            )

    # ----------------------------------------------------------------------- keys

    def keys(self, *, refresh: bool = False) -> tuple:
        """(the provider's JWKs, whether this call fetched them). Cached for an hour; with
        ``refresh`` fetched again, unless the last fetch was less than a minute ago. One fetch
        at a time: the others wait for its result instead of asking again."""
        if not self._keys_lock.acquire(timeout=CONNECT_SECONDS + TOTAL_SECONDS):
            raise self._unavailable(
                urlsplit(self.config.issuer).hostname,
                "is slow to send its keys",
                "Waiting for another request's fetch of the key set took too long.",
            )
        try:
            fetched_at = self._keys_fetched_at
            if fetched_at is not None:
                age = self.clock() - fetched_at
                if age < (KEYS_REFRESH_SECONDS if refresh else CACHE_SECONDS):
                    return list(self._keys), False
            self._keys = self._fetch_keys()
            self._keys_fetched_at = self.clock()
            return list(self._keys), True
        finally:
            self._keys_lock.release()

    def _fetch_keys(self) -> list:
        url = self.metadata().jwks_uri
        document = self.get_json(url, "key set (jwks_uri)")
        keys = document.get("keys")
        if not isinstance(keys, list):
            raise self._misconfigured(
                "the provider's key set has no keys",
                f"The key set at {url} has no list of keys.",
            )
        return [key for key in keys if isinstance(key, dict)]

    # ----------------------------------------------------------------------- tokens

    def exchange(self, code: str, redirect_uri: str, verifier: str) -> dict:
        """The token endpoint's answer to the authorization code (with the PKCE verifier)."""
        config = self.config
        url = self.metadata().token_endpoint
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": verifier,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if config.token_auth_method == "client_secret_basic":
            # RFC 6749 §2.3.1: the id and the secret are form-encoded before Base64
            pair = f"{quote(config.client_id, safe='')}:{quote(config.client_secret, safe='')}"
            headers["Authorization"] = "Basic " + base64.b64encode(pair.encode()).decode()
        else:
            form.update(client_id=config.client_id, client_secret=config.client_secret)
        status, content_type, data = self.request(
            "POST", url, what="token endpoint", body=urlencode(form), headers=headers
        )
        if status in (400, 401):
            raise self._refused(url, status, data)
        answer = self._json(url, "token endpoint", status, content_type, data)
        if not isinstance(answer.get("id_token"), str):
            raise ProviderError(
                f"{self.name} completed the sign-in without an ID token, so it was refused.",
                detail=f"The token endpoint at {url} answered without an id_token (is the "
                "openid scope granted to this client?).",
                reason="token_exchange_failed",
            )
        return answer

    def _refused(self, url: str, status: int, data: bytes) -> ProviderError:
        """The token endpoint's error answer (RFC 6749 §5.2): its error code, which says what
        went wrong without any secret, and a shortened description for the log."""
        try:
            answer = json.loads(data)
        except (UnicodeDecodeError, ValueError):
            answer = None
        answer = answer if isinstance(answer, dict) else {}
        code = answer.get("error") if isinstance(answer.get("error"), str) else "no error code"
        code = code[:64]
        description = answer.get("error_description")
        description = description[:200] if isinstance(description, str) else ""
        return ProviderError(
            f"{self.name} refused to complete the sign-in ({code}). Start again; if it keeps "
            "failing, an admin finds the details in the gateway's log.",
            detail=f"The token endpoint at {url} answered {status}: {code}"
            + (f" ({description})" if description else "")
            + (
                ". invalid_client usually means a wrong FORKLIFT_OIDC_CLIENT_SECRET or "
                "FORKLIFT_OIDC_TOKEN_AUTH_METHOD."
                if code == "invalid_client"
                else "."
            ),
            reason="token_exchange_failed",
        )

    def userinfo(self, access_token) -> dict:
        """The UserInfo endpoint's claims (a JSON answer; signed answers are not supported)."""
        url = self.metadata().userinfo_endpoint
        if not isinstance(access_token, str) or not access_token:
            raise ProviderError(
                f"{self.name} completed the sign-in without an access token, so the UserInfo "
                "endpoint could not be asked.",
                detail="The token endpoint answered without an access_token, which the UserInfo "
                "endpoint needs (FORKLIFT_OIDC_USERINFO is true).",
                reason="userinfo_failed",
            )
        status, content_type, data = self.request(
            "GET",
            url,
            what="UserInfo endpoint",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if content_type.split(";")[0].strip().lower() == "application/jwt":
            raise self._misconfigured(
                "the provider signs its UserInfo answers, which this installation does not read",
                f"The UserInfo endpoint at {url} answered with a signed JWT; configure the client "
                "at the provider to answer UserInfo requests with JSON.",
            )
        return self._json(url, "UserInfo endpoint", status, content_type, data)


class _TooLarge(Exception):
    pass


def _problem(error: BaseException) -> str:
    """What went wrong, for the person signing in."""
    if isinstance(error, TimeoutError):
        return "did not answer in time"
    if isinstance(error, ssl.SSLCertVerificationError):
        return "presented a TLS certificate that could not be verified"
    if isinstance(error, ssl.SSLError):
        return "failed the TLS handshake"
    if isinstance(error, ConnectionRefusedError):
        return "refused the connection"
    if isinstance(error, http.client.HTTPException):
        return "did not answer with valid HTTP"
    return "could not be reached"


def _describe(error: BaseException) -> str:
    """What went wrong, for the log (the exception's own text names no secret)."""
    if isinstance(error, TimeoutError):
        return f"no complete answer within {TOTAL_SECONDS:g} seconds"
    if isinstance(error, ssl.SSLCertVerificationError):
        return (
            f"TLS certificate verification failed ({error.verify_message}); set "
            "FORKLIFT_OIDC_CA_FILE to the CA certificates of a private CA"
        )
    return f"{type(error).__name__}: {error}"


@functools.lru_cache(maxsize=8)
def get(config: OidcConfig) -> Provider:
    """The process's provider for ``config`` (one per configuration, so its caches are shared)."""
    return Provider(config)


def reset() -> None:
    """Forget every cached provider (tests, or after the settings changed)."""
    get.cache_clear()
    _tls_context.cache_clear()
