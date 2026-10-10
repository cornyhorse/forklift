"""HTTP for the gateway's internal API and the object store, on the standard library only.

- Redirects are not followed: a 3xx is an error, so a request never lands on a host nobody
  checked.
- Only http:// and https:// URLs are opened (urllib would also open file:// and ftp://).
- Proxies come from the environment (HTTPS_PROXY, NO_PROXY), as urllib does by default.
- Errors name the host only: presigned URLs carry signatures and must never reach a log.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import re
import socket
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Iterator, Mapping
from urllib.parse import urlsplit

_DETAIL_BYTES = 64 * 1024
_S3_CODE = re.compile(rb"<Code>([^<]{1,100})</Code>")


class TransportError(Exception):
    """The request did not get an HTTP answer: connection refused, DNS, TLS, timeout, reset."""

    def __init__(self, host: str, reason: str):
        super().__init__(f"{host}: {reason}")
        self.host = host
        self.reason = reason


class HttpStatusError(Exception):
    """The server answered with a status that is not 2xx."""

    def __init__(self, host: str, status: int, detail: str = ""):
        text = f"{host} answered HTTP {status}"
        super().__init__(f"{text}: {detail}" if detail else text)
        self.host = host
        self.status = status
        self.detail = detail

    @property
    def transient(self) -> bool:
        return self.status in (408, 429) or self.status >= 500


def is_transient(error: Exception) -> bool:
    """Worth retrying: no answer at all, or a 408, 429 or 5xx."""
    return isinstance(error, TransportError) or (
        isinstance(error, HttpStatusError) and error.transient
    )


def host_of(url: str) -> str:
    """``host`` or ``host:port`` (lower case) of an http(s) URL; ValueError for anything else."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("not an http:// or https:// URL with a host")
    host = parts.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    return f"{host}:{parts.port}" if parts.port else host


def port_of(url: str) -> int:
    parts = urlsplit(url)
    return parts.port or (443 if parts.scheme == "https" else 80)


def error_detail(body: bytes) -> str:
    """A short reason from an error body: the gateway's ``detail`` or the store's ``<Code>``."""
    try:
        document = json.loads(body)
    except ValueError:
        match = _S3_CODE.search(body)
        return match.group(1).decode("utf-8", "replace") if match else ""
    if isinstance(document, dict) and "detail" in document:
        return str(document["detail"])[:500]
    return ""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # urllib then raises HTTPError with the 3xx status


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self):
        return json.loads(self.body)


class Download:
    """An open response whose body is read in chunks (errors become TransportError)."""

    def __init__(self, host: str, response: http.client.HTTPResponse):
        self.host = host
        self.status = response.status
        self.headers = {key.lower(): value for key, value in response.getheaders()}
        self._response = response

    def chunks(self, size: int) -> Iterator[bytes]:
        received = 0
        while True:
            try:
                chunk = self._response.read(size)
            except (OSError, http.client.HTTPException) as error:
                raise TransportError(self.host, _reason(error)) from None
            if not chunk:
                _check_length(self.host, self.headers, received)
                return
            received += len(chunk)
            yield chunk


def _check_length(host: str, headers: Mapping[str, str], received: int) -> None:
    """http.client returns a short body without complaint when the connection drops."""
    expected = headers.get("content-length", "").strip()
    if expected.isdigit() and received < int(expected):
        raise TransportError(
            host, f"the connection closed after {received} of {expected} bytes of the body"
        )


def _reason(error: BaseException) -> str:
    if isinstance(error, urllib.error.URLError) and not isinstance(error.reason, str):
        error = error.reason
    if isinstance(error, (socket.timeout, TimeoutError)):
        return "timed out"
    if isinstance(error, ssl.SSLError):
        return f"TLS error ({error.reason or type(error).__name__})"
    name = type(error).__name__
    text = getattr(error, "strerror", None) or str(error)
    return f"{name}: {text}" if text and text != name else name


class HttpClient:
    def __init__(self, *, timeout: float, user_agent: str, ca_file: Path | None = None):
        context = ssl.create_default_context(cafile=str(ca_file) if ca_file else None)
        self._opener = urllib.request.build_opener(
            _NoRedirect(), urllib.request.HTTPSHandler(context=context)
        )
        self.timeout = timeout
        self.user_agent = user_agent

    def _request(self, method, url, headers, body) -> tuple[str, urllib.request.Request]:
        host = host_of(url)
        merged = {"User-Agent": self.user_agent, **(headers or {})}
        return host, urllib.request.Request(url, data=body, headers=merged, method=method)

    def _open(self, host: str, request: urllib.request.Request):
        try:
            return self._opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            with error:
                body = error.read(_DETAIL_BYTES)
            raise HttpStatusError(host, error.code, error_detail(body)) from None
        except (OSError, http.client.HTTPException) as error:
            raise TransportError(host, _reason(error)) from None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes | IO[bytes] | None = None,
        max_response_bytes: int = 32 * 1024 * 1024,
    ) -> Response:
        """Send one request and read the whole (bounded) response body."""
        host, request = self._request(method, url, headers, body)
        with self._open(host, request) as response:
            try:
                data = response.read(max_response_bytes + 1)
            except (OSError, http.client.HTTPException) as error:
                raise TransportError(host, _reason(error)) from None
            if len(data) > max_response_bytes:
                raise TransportError(host, f"response larger than {max_response_bytes} bytes")
            headers_out = {key.lower(): value for key, value in response.getheaders()}
            _check_length(host, headers_out, len(data))
            return Response(response.status, headers_out, data)

    @contextlib.contextmanager
    def download(self, url: str, headers: Mapping[str, str] | None = None) -> Iterator[Download]:
        """A GET whose body the caller reads in chunks."""
        host, request = self._request("GET", url, headers, None)
        with self._open(host, request) as response:
            yield Download(host, response)
