"""A fake OpenID Provider for the single sign-on tests: a real HTTP(S) server in a thread, with
real RSA and EC keys that sign real JWTs (PyJWT), serving discovery, the key set and the
authorization, token, UserInfo and end-session endpoints.

It checks what a real provider checks (the client id and secret, the redirect URI, PKCE S256,
single-use codes), records every request it gets, and has knobs for the misbehaving providers
the tests need: other claims or headers in the ID token, another signature, keys rotated, a
discovery document that names another issuer, answers that are slow, huge or not JSON.

``FakeProvider.log_in(authorization_url, person)`` plays the person signing in at the provider:
it calls the authorization endpoint as a browser would and returns where the provider sends the
browser back (the gateway's callback, with the code and the state).
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import http.server
import json
import secrets
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from django.test import Client
from webhook_support import certificates, server_context

from forklift_web.oidc import config as oidc_config
from forklift_web.oidc import provider as providers

CLIENT_ID = "forklift-tests"
CLIENT_SECRET = "the-tests-client-secret-" + "x" * 16
REALM = "/realms/forklift"
REMOVE = object()  # a token_claims value that removes the claim


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def challenge(verifier: str) -> str:
    return b64url(hashlib.sha256(verifier.encode("ascii")).digest())


def rsa_key(bits: int = 2048):
    return rsa.generate_private_key(public_exponent=65537, key_size=bits)


def ec_key():
    return ec.generate_private_key(ec.SECP256R1())


@functools.lru_cache(maxsize=None)
def shared_key(kind: str):
    """A key every fake provider starts with (making RSA keys takes a while)."""
    return rsa_key() if kind == "rsa" else ec_key()


def public_pem(key) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def hand_signed(header: dict, claims: dict, secret: bytes = b"", alg: str = "HS256") -> str:
    """A JWT built by hand: PyJWT refuses to make the forgeries the tests need (``none`` with a
    signature, or HMAC keyed with a public key)."""
    signing_input = (
        b64url(json.dumps({"alg": alg, **header}).encode())
        + "."
        + b64url(json.dumps(claims).encode())
    )
    if alg == "none":
        return signing_input + "."
    digest = {"HS256": hashlib.sha256, "HS384": hashlib.sha384}[alg]
    signature = hmac.new(secret, signing_input.encode(), digest).digest()
    return signing_input + "." + b64url(signature)


@dataclass
class Person:
    """Someone with an account at the provider: ``claims`` go into the ID token (and the
    UserInfo answer) next to the protocol's own."""

    subject: str
    claims: dict = field(default_factory=dict)


@dataclass
class Grant:
    client_id: str
    redirect_uri: str
    challenge: str
    nonce: str
    scope: str
    person: Person


@dataclass
class Seen:
    method: str
    path: str
    query: dict
    headers: dict  # lower-case names
    body: bytes


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def _dispatch(self, method: str):
        provider: FakeProvider = self.server.provider
        parts = urllib.parse.urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        seen = Seen(
            method,
            parts.path,
            dict(urllib.parse.parse_qsl(parts.query)),
            {name.lower(): value for name, value in self.headers.items()},
            body,
        )
        provider.requests.append(seen)
        try:
            provider.answer(self, seen)
        except OSError:  # the gateway gave up waiting (the slow and huge answers)
            self.close_connection = True

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")


class FakeProvider:
    """The provider at ``issuer`` (https://localhost:<port>/realms/forklift, or http:// with
    ``tls=False``). ``ca_file`` is the CA certificate its server certificate is signed by."""

    def __init__(self, directory: Path, *, tls: bool = True):
        self.tls = tls
        self.keys = {"rsa-1": (shared_key("rsa"), "RS256"), "ec-1": (shared_key("ec"), "ES256")}
        self.signing_kid = "rsa-1"
        self.published = ["rsa-1", "ec-1"]
        self.discovery_overrides: dict = {}
        self.token_claims: dict = {}  # claim -> value (REMOVE drops it), after the defaults
        self.token_header: dict = {}  # extra JOSE header entries (kid=REMOVE drops the kid)
        self.forge = None  # (claims, header) -> token, instead of signing properly
        self.token_answer: Optional[tuple] = None  # (status, JSON body) instead of tokens
        self.userinfo_answer: Optional[tuple] = None  # (status, content type, body)
        self.raw: dict = {}  # path -> (status, headers, body bytes) for any endpoint
        self.delay: dict = {}  # path -> seconds to wait before answering
        self.huge: dict = {}  # path -> "length" (says so in Content-Length) or "stream"
        self.expected_auth = "client_secret_basic"
        self.people: dict = {}
        self.grants: dict = {}
        self.access_tokens: dict = {}
        self.requests: list = []
        self.logouts: list = []
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.daemon_threads = True
        self.server.provider = self
        # TLS handshakes the gateway gives up on are what some tests are about: no tracebacks
        self.server.handle_error = lambda request, address: None
        self.ca_file = None
        if tls:
            ca_pem, cert_file, key_file = certificates(directory, host="localhost")
            self.ca_file = directory / "provider-ca.pem"
            self.ca_file.write_text(ca_pem)
            self.server.socket = server_context(cert_file, key_file).wrap_socket(
                self.server.socket, server_side=True, do_handshake_on_connect=False
            )
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self.thread.start()

    # ----------------------------------------------------------------------- set-up

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    @property
    def origin(self) -> str:
        return f"{'https' if self.tls else 'http'}://localhost:{self.port}"

    @property
    def issuer(self) -> str:
        return self.origin + REALM

    def url(self, path: str) -> str:
        return self.issuer + path

    def environ(self, **overrides) -> dict:
        """FORKLIFT_OIDC_* variables for this provider (``overrides`` without the prefix)."""
        values = {
            "ISSUER": self.issuer,
            "CLIENT_ID": CLIENT_ID,
            "CLIENT_SECRET": CLIENT_SECRET,
            "PROVIDER_NAME": "Fake IdP",
            "ADMIN_GROUPS": "forklift-admins",
            "AUTHOR_GROUPS": "forklift-authors",
            "OPERATOR_GROUPS": "forklift-operators",
            "VIEWER_GROUPS": "forklift-viewers",
        }
        if self.ca_file is not None:
            values["CA_FILE"] = str(self.ca_file)
        if not self.tls:
            values["ALLOW_HTTP"] = "true"
        values.update(overrides)
        return {
            f"FORKLIFT_OIDC_{key}": value for key, value in values.items() if value is not None
        }

    def config(self, **overrides) -> oidc_config.OidcConfig:
        return oidc_config.load(self.environ(**overrides))

    def person(self, subject: str = "", **claims) -> Person:
        """Someone who can sign in here (by default a viewer named after the subject)."""
        subject = subject or f"sub-{secrets.token_hex(6)}"
        defaults = {
            "preferred_username": f"user-{subject}",
            "email": f"{subject}@example.org",
            "email_verified": True,
            "given_name": "Ada",
            "family_name": "Lovelace",
            "groups": ["forklift-viewers"],
        }
        found = Person(subject, {**defaults, **claims})
        self.people[subject] = found
        return found

    def rotate(self) -> str:
        """Sign with a new RSA key from now on, publishing only it; returns its kid."""
        kid = f"rsa-{len(self.keys) + 1}"
        self.keys[kid] = (rsa_key(), "RS256")
        self.signing_kid, self.published = kid, [kid]
        return kid

    def seen(self, path: str) -> list:
        return [request for request in self.requests if request.path == REALM + path]

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    # ----------------------------------------------------------------------- documents

    def discovery(self) -> dict:
        document = {
            "issuer": self.issuer,
            "authorization_endpoint": self.url("/auth"),
            "token_endpoint": self.url("/token"),
            "jwks_uri": self.url("/certs"),
            "userinfo_endpoint": self.url("/userinfo"),
            "end_session_endpoint": self.url("/logout"),
            "response_types_supported": ["code", "id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256", "ES256", "PS256"],
            "code_challenge_methods_supported": ["plain", "S256"],
            "token_endpoint_auth_methods_supported": ["client_secret_basic", "client_secret_post"],
        }
        for key, value in self.discovery_overrides.items():
            if value is REMOVE:
                document.pop(key, None)
            else:
                document[key] = value
        return document

    def jwks(self) -> dict:
        keys = []
        for kid in self.published:
            private, alg = self.keys[kid]
            kind = jwt.algorithms.RSAAlgorithm if alg.startswith(("RS", "PS")) else None
            kind = kind or jwt.algorithms.ECAlgorithm
            public = kind.to_jwk(private.public_key(), as_dict=True)
            keys.append({**public, "kid": kid, "use": "sig", "alg": alg})
        return {"keys": keys}

    def id_token(self, grant: Grant) -> str:
        now = int(time.time())
        claims = {
            "iss": self.issuer,
            "sub": grant.person.subject,
            "aud": grant.client_id,
            "exp": now + 300,
            "iat": now,
            "auth_time": now,
            "nonce": grant.nonce,
            **grant.person.claims,
        }
        for key, value in self.token_claims.items():
            if value is REMOVE:
                claims.pop(key, None)
            else:
                claims[key] = value
        private, alg = self.keys[self.signing_kid]
        header = {"kid": self.signing_kid, **self.token_header}
        if header["kid"] is REMOVE:
            del header["kid"]
        if self.forge is not None:
            return self.forge(claims, header)
        return jwt.encode(claims, private, algorithm=header.pop("alg", alg), headers=header)

    # ----------------------------------------------------------------------- endpoints

    def answer(self, handler: _Handler, seen: Seen) -> None:
        path = seen.path[len(REALM) :] if seen.path.startswith(REALM) else seen.path
        if path in self.delay:
            time.sleep(self.delay[path])
        if path in self.huge:
            return self._huge(handler, self.huge[path])
        if path in self.raw:
            status, headers, body = self.raw[path]
            return self._send(handler, status, body, headers)
        route = {
            "/.well-known/openid-configuration": lambda: self._json(handler, self.discovery()),
            "/certs": lambda: self._json(handler, self.jwks()),
            "/auth": lambda: self._authorize(handler, seen),
            "/login": lambda: self._login_form(handler, seen),
            "/token": lambda: self._token(handler, seen),
            "/userinfo": lambda: self._userinfo(handler, seen),
            "/logout": lambda: self._logout(handler, seen),
        }.get(path)
        if route is None:
            return self._json(handler, {"error": "not_found"}, 404)
        route()

    def _send(self, handler, status: int, body: bytes, headers: dict) -> None:
        handler.send_response(status)
        for name, value in headers.items():
            handler.send_header(name, value)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _json(self, handler, document, status: int = 200) -> None:
        body = json.dumps(document).encode()
        self._send(handler, status, body, {"Content-Type": "application/json"})

    def _huge(self, handler, how: str) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        if how == "length":
            handler.send_header("Content-Length", str(64 * 1024 * 1024))
            handler.end_headers()
            return
        handler.send_header("Connection", "close")
        handler.end_headers()
        chunk = b" " * 65536
        try:
            for _ in range(32):  # 2 MiB, more than the gateway reads
                handler.wfile.write(chunk)
        except OSError:  # the gateway stopped reading
            pass
        handler.close_connection = True

    def _redirect(self, handler, location: str) -> None:
        self._send(handler, 302, b"", {"Location": location})

    def _authorize(self, handler, seen: Seen) -> None:
        query = seen.query
        problems = [
            name
            for name, ok in (
                ("response_type", query.get("response_type") == "code"),
                ("client_id", query.get("client_id") == CLIENT_ID),
                ("scope", "openid" in query.get("scope", "").split()),
                ("code_challenge_method", query.get("code_challenge_method") == "S256"),
                ("code_challenge", len(query.get("code_challenge", "")) == 43),
                ("state", bool(query.get("state"))),
                ("nonce", bool(query.get("nonce"))),
                (
                    "redirect_uri",
                    query.get("redirect_uri", "").startswith(("http://", "https://")),
                ),
            )
            if not ok
        ]
        if problems:
            return self._json(handler, {"error": "invalid_request", "bad": problems}, 400)
        login_as = query.get("login_as")
        if login_as is None:  # a browser: show the provider's sign-in form
            return self._login_form(handler, seen)
        return self._grant(handler, query, self.people[login_as])

    def _grant(self, handler, query: dict, person: Person) -> None:
        code = secrets.token_urlsafe(24)
        self.grants[code] = Grant(
            query["client_id"],
            query["redirect_uri"],
            query["code_challenge"],
            query["nonce"],
            query["scope"],
            person,
        )
        back = {"code": code, "state": query["state"]}
        self._redirect(handler, query["redirect_uri"] + "?" + urllib.parse.urlencode(back))

    def _login_form(self, handler, seen: Seen) -> None:
        """The provider's own sign-in page (for the browser test): pick a person, submit."""
        if seen.method == "POST":
            form = dict(urllib.parse.parse_qsl(seen.body.decode()))
            query = json.loads(base64.urlsafe_b64decode(form["request"]))
            return self._grant(handler, query, self.people[form["subject"]])
        request = base64.urlsafe_b64encode(json.dumps(seen.query).encode()).decode()
        options = "".join(
            f'<option value="{subject}">{subject}</option>' for subject in self.people
        )
        page = (
            "<!doctype html><title>Fake IdP</title><h1>Fake IdP sign-in</h1>"
            f'<form method="post" action="{REALM}/login">'
            f'<input type="hidden" name="request" value="{request}">'
            f'<label>Account <select name="subject">{options}</select></label>'
            "<button type=submit>Continue</button></form>"
        ).encode()
        self._send(handler, 200, page, {"Content-Type": "text/html; charset=utf-8"})

    def _client_authenticated(self, seen: Seen, form: dict) -> bool:
        if self.expected_auth == "client_secret_basic":
            header = seen.headers.get("authorization", "")
            if not header.startswith("Basic ") or "client_secret" in form:
                return False
            pair = base64.b64decode(header[6:]).decode()
            client, _, secret = pair.partition(":")
            return (urllib.parse.unquote(client), urllib.parse.unquote(secret)) == (
                CLIENT_ID,
                CLIENT_SECRET,
            )
        return "authorization" not in seen.headers and (
            form.get("client_id"),
            form.get("client_secret"),
        ) == (CLIENT_ID, CLIENT_SECRET)

    def _token(self, handler, seen: Seen) -> None:
        form = dict(urllib.parse.parse_qsl(seen.body.decode()))
        if not self._client_authenticated(seen, form):
            return self._json(handler, {"error": "invalid_client"}, 401)
        grant = self.grants.pop(form.get("code", ""), None)
        if (
            form.get("grant_type") != "authorization_code"
            or grant is None
            or grant.redirect_uri != form.get("redirect_uri")
            or challenge(form.get("code_verifier", "")) != grant.challenge
        ):
            return self._json(
                handler,
                {"error": "invalid_grant", "error_description": "Code not valid"},
                400,
            )
        if self.token_answer is not None:
            status, document = self.token_answer
            return self._json(handler, document, status)
        access = secrets.token_urlsafe(24)
        self.access_tokens[access] = grant.person
        self._json(
            handler,
            {
                "access_token": access,
                "token_type": "Bearer",
                "expires_in": 300,
                "id_token": self.id_token(grant),
                "scope": grant.scope,
            },
        )

    def _userinfo(self, handler, seen: Seen) -> None:
        person = self.access_tokens.get(seen.headers.get("authorization", "")[7:])
        if person is None:
            return self._json(handler, {"error": "invalid_token"}, 401)
        if self.userinfo_answer is not None:
            status, content_type, body = self.userinfo_answer
            return self._send(handler, status, body, {"Content-Type": content_type})
        self._json(handler, {"sub": person.subject, **person.claims})

    def _logout(self, handler, seen: Seen) -> None:
        self.logouts.append(seen.query)
        self._redirect(handler, seen.query.get("post_logout_redirect_uri", self.issuer))

    # ----------------------------------------------------------------------- the person's side

    def opener(self) -> urllib.request.OpenerDirector:
        """A client that trusts this provider's CA and does not follow redirects."""

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        handlers = [NoRedirect()]
        if self.ca_file is not None:
            context = ssl.create_default_context(cafile=str(self.ca_file))
            handlers.append(urllib.request.HTTPSHandler(context=context))
        return urllib.request.build_opener(*handlers)

    def log_in(self, authorization_url: str, person: Person) -> str:
        """Sign ``person`` in at the authorization URL the gateway sent the browser to; returns
        the URL the provider sends the browser back to."""
        assert authorization_url.startswith(self.url("/auth?")), authorization_url
        try:
            self.opener().open(f"{authorization_url}&login_as={person.subject}", timeout=10)
        except urllib.error.HTTPError as answer:
            assert answer.code == 302, answer.read()
            return answer.headers["Location"]
        raise AssertionError("The provider did not redirect back.")


# --------------------------------------------------------------------------- fixtures


@pytest.fixture
def idp(tmp_path):
    """A fake provider over TLS (its CA in FORKLIFT_OIDC_CA_FILE) for one test."""
    provider = FakeProvider(tmp_path)
    providers.reset()
    yield provider
    provider.stop()
    providers.reset()


@pytest.fixture
def sso(idp, settings):
    """Single sign-on configured for ``idp``; ``sso(**overrides)`` configures it again (keys of
    FakeProvider.environ, without FORKLIFT_OIDC_)."""

    def configure(**overrides):
        settings.FORKLIFT_OIDC = idp.config(**overrides)
        providers.reset()
        return settings.FORKLIFT_OIDC

    configure()
    return configure


def start(client: Client, next_url: str = "") -> str:
    """Start a single sign-on in ``client``; returns the provider URL it was sent to."""
    query = "?" + urllib.parse.urlencode({"next": next_url}) if next_url else ""
    response = client.get("/accounts/oidc/login/" + query)
    assert response.status_code == 302, response.content.decode()
    return response["Location"]


def finish(client: Client, back: str, **extra):
    """Follow the provider's redirect back to the gateway's callback."""
    parts = urllib.parse.urlsplit(back)
    assert parts.path == "/accounts/oidc/callback/", back
    return client.get(parts.path + "?" + parts.query, **extra)


def sign_in(client: Client, idp: FakeProvider, person: Person):
    """The whole round trip; returns the callback's response."""
    return finish(client, idp.log_in(start(client), person))
