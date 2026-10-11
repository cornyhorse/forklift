"""HTTP on the standard library only, and when to send a request again.

- Redirects are not followed: a 3xx comes back as it is, so a request never lands on a host
  nobody chose, and the token is never sent anywhere but the API.
- Only http:// and https:// URLs are opened (urllib would also open file:// and ftp://).
- Proxies come from the environment (``HTTPS_PROXY``, ``NO_PROXY``), as urllib does by default.
- Error messages name the host only for presigned URLs: their query strings are signatures.
"""

from __future__ import annotations

import contextlib
import email.utils
import http.client
import logging
import random
import re
import socket
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import IO, Callable, Dict, Iterator, Mapping, Optional, Tuple, Union
from urllib.parse import urlsplit

from .errors import TransportError

log = logging.getLogger("forklift_client")

ERROR_BODY_BYTES = 64 * 1024  # most bytes of an error body read
_S3_CODE = re.compile(rb"<Code>([^<]{1,100})</Code>")

Body = Union[bytes, IO[bytes], None]


@dataclass(frozen=True)
class RetryPolicy:
    """How often and how long to wait before sending a request again.

    ``attempts`` counts every try, the first included (1: never retry). Waits grow from
    ``initial_delay`` by doubling up to ``max_delay`` seconds, each drawn between half and all
    of its step (jitter, so clients that failed together do not come back together). A
    ``Retry-After`` the server sends is waited instead, unless it asks for more than
    ``max_retry_after`` seconds: then the error is raised (with ``retry_after`` set).
    """

    attempts: int = 4
    initial_delay: float = 0.5
    max_delay: float = 30.0
    max_retry_after: float = 120.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("RetryPolicy.attempts counts the first try too: it must be >= 1")
        if min(self.initial_delay, self.max_delay, self.max_retry_after) < 0:
            raise ValueError("RetryPolicy delays must not be negative")

    def delay(self, retry: int, rng: random.Random) -> float:
        """The wait before retry number ``retry`` (1 for the first retry)."""
        step = min(self.initial_delay * 2 ** (retry - 1), self.max_delay)
        return step / 2 + rng.uniform(0, step / 2)


NO_RETRIES = RetryPolicy(attempts=1)


@dataclass(frozen=True)
class Response:
    status: int
    headers: Dict[str, str]  # lower-case names
    body: bytes


def host_of(url: str) -> str:
    """``host`` or ``host:port`` of an http(s) URL; ValueError for anything else."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("not an http:// or https:// URL with a host")
    host = parts.hostname
    if ":" in host:
        host = f"[{host}]"
    return f"{host}:{parts.port}" if parts.port else host


def store_error_code(body: bytes) -> str:
    """The ``<Code>`` of an S3 error body ("" if it has none)."""
    match = _S3_CODE.search(body)
    return match.group(1).decode("utf-8", "replace") if match else ""


def retry_after(headers: Mapping[str, str], now: Optional[float] = None) -> Optional[float]:
    """Seconds the ``Retry-After`` header asks to wait (delta-seconds or an HTTP date)."""
    value = headers.get("retry-after", "").strip()
    if not value:
        return None
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:  # RFC 7231 dates are GMT; parsedate leaves "-0000" naive
        return None
    return max(0.0, when.timestamp() - (time.time() if now is None else now))


def _reason(error: BaseException) -> str:
    if isinstance(error, (socket.timeout, TimeoutError)):
        return "timed out"
    if isinstance(error, ssl.SSLError):
        return f"TLS error: {getattr(error, 'reason', None) or type(error).__name__}"
    name = type(error).__name__
    text = getattr(error, "strerror", None) or str(error)
    return f"{name}: {text}" if text and text != name else name


def _transport_error(what: str, host: str, error: BaseException) -> TransportError:
    """The TransportError for ``error``, raised while connecting or sending (URLError) or while
    waiting for or reading the answer (anything else)."""
    if isinstance(error, urllib.error.URLError) and not isinstance(error.reason, str):
        cause = error.reason
        if isinstance(cause, ssl.SSLError):
            return TransportError(what, host, _reason(cause), sent=False, retryable=False)
        not_sent = isinstance(cause, (ConnectionRefusedError, socket.gaierror))
        return TransportError(what, host, _reason(cause), sent=not not_sent)
    if isinstance(error, urllib.error.URLError):
        return TransportError(what, host, error.reason)
    return TransportError(what, host, _reason(error))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # urllib then raises HTTPError with the 3xx status


class Transport:
    """Sends one request at a time; thread-safe (every request opens its own connection)."""

    def __init__(self, *, timeout: float, user_agent: str, ca_file: Optional[str] = None):
        try:
            context = ssl.create_default_context(cafile=ca_file)
        except OSError as error:  # ssl.SSLError is an OSError too
            raise ValueError(
                f"ca_file {ca_file!r} is not a readable PEM bundle of CA certificates "
                f"({_reason(error)})"
            ) from None
        self._opener = urllib.request.build_opener(
            _NoRedirect(), urllib.request.HTTPSHandler(context=context)
        )
        self.timeout = timeout
        self.user_agent = user_agent

    def _open(self, method: str, url: str, headers: Mapping[str, str], body: Body, what: str):
        host = host_of(url)
        merged = {"User-Agent": self.user_agent, **headers}
        request = urllib.request.Request(url, data=body, headers=merged, method=method)
        try:
            return host, self._opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            return host, error  # an answer with an error status: the caller reads it
        except (OSError, http.client.HTTPException) as error:
            raise _transport_error(what, host, error) from None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Body = None,
        what: str,
        max_body: int = 64 * 1024 * 1024,
    ) -> Response:
        """Send one request and read its (bounded) answer, whatever its status."""
        host, response = self._open(method, url, headers, body, what)
        with response:
            ok = 200 <= response.status < 300
            limit = max_body if ok else ERROR_BODY_BYTES
            data = read(response, limit + 1, host, what)
            names = {key.lower(): value for key, value in response.headers.items()}
        if ok:
            if len(data) > limit:
                raise TransportError(what, host, f"the answer is larger than {max_body} bytes")
            check_length(what, host, names, len(data))
        return Response(response.status, names, data[:limit])

    @contextlib.contextmanager
    def stream(
        self, url: str, *, headers: Mapping[str, str], what: str
    ) -> Iterator[Tuple[str, int, Dict[str, str], IO[bytes]]]:
        """A GET whose body the caller reads: (host, status, headers, response)."""
        host, response = self._open("GET", url, headers, None, what)
        with response:
            names = {key.lower(): value for key, value in response.headers.items()}
            yield host, response.status, names, response


def read(response: IO[bytes], size: int, host: str, what: str) -> bytes:
    """``response.read(size)``; a connection error while reading is a TransportError."""
    try:
        return response.read(size)
    except (OSError, http.client.HTTPException) as error:
        raise _transport_error(what, host, error) from None


def check_length(what: str, host: str, headers: Mapping[str, str], received: int) -> None:
    """http.client returns a short body without complaint when the connection drops."""
    expected = headers.get("content-length", "").strip()
    if expected.isdigit() and received < int(expected):
        raise TransportError(
            what, host, f"the connection closed after {received} of {expected} bytes"
        )


def send_with_retries(
    send: Callable[[], Response],
    *,
    policy: RetryPolicy,
    replay_safe: bool,
    what: str,
    sleep: Callable[[float], None],
    rng: random.Random,
) -> Response:
    """``send()`` until it gets an answer worth returning or the attempts run out.

    Requests that are safe to send twice (``replay_safe``) are sent again after a connection
    error or timeout and after a 429, 502, 503 or 504 answer. Others only when the server
    certainly did not act on them: the connection was refused or the name did not resolve,
    or it answered 429, or 503 with ``Retry-After``. TLS failures are never retried. The last
    answer (or error) is returned (raised) when the attempts run out.
    """
    retry = 0
    while True:
        retry += 1
        try:
            response = send()
        except TransportError as error:
            allowed = error.retryable and (replay_safe or not error.sent)
            if not allowed or retry >= policy.attempts:
                raise
            delay = policy.delay(retry, rng)
            log.info("%s: %s; trying again in %.1f s", what, error.reason, delay)
        else:
            wait = retry_after(response.headers)
            status = response.status
            worth_it = status == 429 or (status == 503 and wait is not None)
            if replay_safe and status in (502, 503, 504):
                worth_it = True
            if not worth_it or retry >= policy.attempts:
                return response
            if wait is not None and wait > policy.max_retry_after:
                return response
            delay = policy.delay(retry, rng) if wait is None else wait
            log.info("%s: HTTP %d; trying again in %.1f s", what, status, delay)
        sleep(delay)
