"""Streamed inputs: one object behind a presigned URL, read without copying it (ADR 0006).

:class:`PresignedUrlSource` is the :class:`~forklift.engine.input_source.InputSource` that
``run_job`` builds for a ``presigned_url`` location:

* the main read is one forward-only HTTP stream (``GET`` with ``Range: bytes=0-``); when the
  connection drops it resumes where it stopped with ``Range: bytes=<offset>-``, sending the
  object's ETag as ``If-Match`` so that a changed object is never stitched onto the old one;
* header detection reads the start in small range requests (64 KiB each) instead of opening
  the whole object;
* only hosts in ``allowed_hosts`` are contacted, a redirect to another host is refused, and the
  URL (whose query string is a signature) never appears in messages or metadata:
  :attr:`PresignedUrlSource.name` is the URL without its query string.

Proxies configured in the environment (``HTTPS_PROXY``, ``NO_PROXY``) are honoured, so an
allow-listing egress proxy works as the design describes.
"""

from __future__ import annotations

import http.client
import io
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, BinaryIO, Callable, Iterable, Optional, Tuple

from ..engine.exceptions import INPUT_UNREADABLE, PERMISSION_DENIED, SPEC_INVALID
from ..engine.input_source import InputSource

#: Bytes per range request while the header is detected
HEAD_CHUNK_BYTES = 64 * 1024

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")
# Errors that mean "the connection is gone"; the read resumes from where it was
_DROPPED = (OSError, http.client.HTTPException)


class RemoteInputError(Exception):
    """A streamed input could not be read; ``error_code`` and ``retryable`` say how.

    Messages name the host and the HTTP status, never the URL's query string.
    """

    def __init__(self, message: str, code: str = INPUT_UNREADABLE, retryable: bool = False):
        super().__init__(message)
        self.error_code = code
        self.retryable = retryable


def redact_url(url: str) -> str:
    """``url`` without user information, query string and fragment (they can hold secrets)."""
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    return urllib.parse.urlunsplit((parts.scheme, host + port, parts.path, "", ""))


def _quoted(etag: str) -> str:
    """An ETag as ``If-Match`` needs it: quoted (stores often hand them out without quotes)."""
    if etag.startswith('"') or etag.startswith("W/"):
        return etag
    return f'"{etag}"'


def _host_and_port(scheme: str, netloc: str) -> Tuple[str, int]:
    parts = urllib.parse.urlsplit(f"{scheme}://{netloc}")
    default = 443 if scheme == "https" else 80
    return (parts.hostname or "").lower(), parts.port or default


def check_url(url: str, allowed_hosts: Iterable[str]) -> urllib.parse.SplitResult:
    """Refuse ``url`` unless it is http(s), has no user information and an allowed host.

    ``allowed_hosts`` holds host names (any port) or ``host:port`` entries.

    Raises:
        RemoteInputError: With code ``SPEC_INVALID``, naming the host but not the URL
    """
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise RemoteInputError(
            "input.location.url: must be an http:// or https:// URL with a host", SPEC_INVALID
        )
    if parts.username is not None or parts.password is not None:
        raise RemoteInputError(
            "input.location.url: must not contain user information (user:password@)",
            SPEC_INVALID,
        )
    host, port = _host_and_port(parts.scheme, parts.netloc)
    allowed = []
    for entry in allowed_hosts:
        entry_host, entry_port = _host_and_port(parts.scheme, entry.strip())
        has_port = urllib.parse.urlsplit(f"//{entry.strip()}").port is not None
        allowed.append(entry.strip())
        if entry_host == host and (not has_port or entry_port == port):
            return parts
    shown = ", ".join(repr(a) for a in allowed) if allowed else "none"
    raise RemoteInputError(
        f"input.location.url: host {host!r} is not one of the hosts this job may read from "
        f"(allowed_url_hosts: {shown}); a presigned URL is only followed to the object store "
        "the caller allows",
        SPEC_INVALID,
    )


class _SameHostRedirects(urllib.request.HTTPRedirectHandler):
    """Follow redirects on the same host only (a presigned URL must not lead elsewhere)."""

    def __init__(self, scheme: str, netloc: str):
        super().__init__()
        self._origin = _host_and_port(scheme, netloc)

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlsplit(urllib.parse.urljoin(req.full_url, newurl))
        if (target.scheme not in ("http", "https")) or _host_and_port(
            target.scheme, target.netloc
        ) != self._origin:
            raise RemoteInputError(
                f"The object store redirected the input to another host "
                f"({target.hostname!r}, HTTP {code}); redirects are only followed on the same "
                "host"
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class PresignedUrlSource(InputSource):
    """One object behind a presigned GET URL.

    Args:
        url: The presigned URL
        allowed_hosts: Hosts (or ``host:port``) the URL may point at
        size: Size of the object if known; learned from the first response otherwise
        etag: ETag of the object if known; learned from the first response otherwise
        timeout: Seconds to wait for the server on each request or read
        max_retries: Attempts in a row (without progress) before a dropped read gives up
        retry_delay: Seconds before the first retry; doubled for every further one
        sleep: Waits between retries (tests pass a function that does not wait)
        opener: ``urllib`` opener to use (tests); by default one that honours the proxy
            environment and refuses redirects to other hosts

    Raises:
        RemoteInputError: The URL is not allowed (code ``SPEC_INVALID``)
    """

    def __init__(
        self,
        url: str,
        *,
        allowed_hosts: Iterable[str],
        size: Optional[int] = None,
        etag: Optional[str] = None,
        timeout: float = 60.0,
        max_retries: int = 5,
        retry_delay: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        opener: Optional[urllib.request.OpenerDirector] = None,
    ):
        parts = check_url(url, allowed_hosts)
        self._url = url
        self.host = parts.hostname
        self.name = redact_url(url)
        self.size = size
        self.etag = _quoted(etag) if etag else None
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._sleep = sleep
        self._opener = opener or urllib.request.build_opener(
            _SameHostRedirects(parts.scheme, parts.netloc)
        )

    def __repr__(self) -> str:
        return f"PresignedUrlSource({self.name!r})"

    # ------------------------------------------------------------------ streams

    def open(self) -> BinaryIO:
        """The whole object as one forward-only stream that survives dropped connections."""
        return _ResumingStream(self)

    def open_head(self) -> BinaryIO:
        """The start of the object, fetched in ranges of :data:`HEAD_CHUNK_BYTES`."""
        return _RangeStream(self, HEAD_CHUNK_BYTES)

    # ------------------------------------------------------------------ requests

    def get(self, start: int, end: Optional[int] = None) -> Optional[Any]:
        """``GET`` bytes ``start``..``end`` (inclusive; to the end when None).

        Returns:
            The response, positioned at byte ``start``, or None when there is nothing at or
            after ``start`` (the object is shorter)

        Raises:
            RemoteInputError: For a refused, missing, changed or unrangeable object
            OSError: The server could not be reached (the caller retries)
        """
        headers = {"Range": f"bytes={start}-{'' if end is None else end}"}
        headers["Accept-Encoding"] = "identity"
        if self.etag:
            headers["If-Match"] = self.etag
        request = urllib.request.Request(self._url, headers=headers, method="GET")
        try:
            response = self._opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            error.close()
            return self._refused(error.code, start)
        status = response.status
        if status == 200 and start > 0:
            response.close()
            raise RemoteInputError(
                f"The object store at {self.host!r} ignored the Range request (HTTP 200 for "
                f"bytes {start}-), so the input cannot be resumed after a dropped connection",
                retryable=True,
            )
        self._learn(response, start)
        return response

    def _refused(self, status: int, start: int) -> None:
        if status == 416:
            # Nothing at or after ``start``: the end of the object (or an empty object)
            if self.size is None and start == 0:
                self.size = 0
            return None
        if status == 412:
            raise RemoteInputError(
                f"The input changed while it was being read (HTTP 412 from {self.host!r}: its "
                "ETag no longer matches); run the job again on the new object"
            )
        if status in (401, 403):
            raise RemoteInputError(
                f"The object store at {self.host!r} refused the presigned URL (HTTP {status}); "
                "it may have expired or have been signed for another object",
                PERMISSION_DENIED,
            )
        if status == 404:
            raise RemoteInputError(
                f"The input object does not exist (HTTP 404 from {self.host!r})"
            )
        if status == 429 or status >= 500:
            raise OSError(f"HTTP {status} from {self.host!r}")  # transient: retried
        raise RemoteInputError(f"The object store at {self.host!r} answered HTTP {status}")

    def _learn(self, response: Any, start: int) -> None:
        """Record size and ETag from a response; refuse one that is not what was asked for."""
        etag = response.headers.get("ETag")
        if etag and not self.etag:
            self.etag = etag
        total: Optional[int] = None
        if response.status == 206:
            match = _CONTENT_RANGE.match(response.headers.get("Content-Range", ""))
            if not match or int(match.group(1)) != start:
                response.close()
                raise RemoteInputError(
                    f"The object store at {self.host!r} answered a Range request for bytes "
                    f"{start}- with a different range"
                )
            total = None if match.group(3) == "*" else int(match.group(3))
        else:
            length = response.headers.get("Content-Length")
            total = int(length) if length and length.isdigit() else None
        if total is not None:
            if self.size is not None and total != self.size:
                response.close()
                raise RemoteInputError(
                    f"The input is {total} bytes, not the {self.size} bytes the job expected; "
                    "it may have changed"
                )
            self.size = total

    def wait_before_retry(self, attempt: int) -> None:
        self._sleep(self.retry_delay * (2 ** (attempt - 1)))


class _ResumingStream(io.RawIOBase):
    """Forward-only stream of a :class:`PresignedUrlSource` that resumes after a drop."""

    def __init__(self, source: PresignedUrlSource):
        super().__init__()
        self._source = source
        self._response: Optional[Any] = None
        self._position = 0
        self._failures = 0
        self._finished = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        while not self._finished:
            if self._response is None:
                try:
                    self._response = self._source.get(self._position)
                except _DROPPED as error:
                    self._dropped(error)
                    continue
                if self._response is None:  # nothing left
                    self._finished = True
                    break
            try:
                size = self._response.readinto(buffer)
            except _DROPPED as error:
                self._dropped(error)
                continue
            if size:
                self._position += size
                self._failures = 0
                return size
            if self._source.size is not None and self._position < self._source.size:
                # The body ended early: the connection was closed under us
                self._dropped(EOFError("the response ended early"))
                continue
            self._finished = True
        self._close_response()
        return 0

    def _dropped(self, error: BaseException) -> None:
        self._close_response()
        self._failures += 1
        if self._failures > self._source.max_retries:
            raise RemoteInputError(
                f"Reading the input from {self._source.host!r} failed {self._failures} times in "
                f"a row at byte {self._position} ({type(error).__name__}); giving up",
                retryable=True,
            ) from None
        self._source.wait_before_retry(self._failures)

    def _close_response(self) -> None:
        if self._response is not None:
            response, self._response = self._response, None
            try:
                response.close()
            except Exception:
                pass

    def close(self) -> None:
        self._close_response()
        super().close()


class _RangeStream(io.RawIOBase):
    """The start of a :class:`PresignedUrlSource`, fetched one small range at a time."""

    def __init__(self, source: PresignedUrlSource, chunk_size: int):
        super().__init__()
        self._source = source
        self._chunk_size = chunk_size
        self._position = 0
        self._pending = b""
        self._finished = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        if not self._pending and not self._finished:
            self._pending = self._fetch()
        size = min(len(buffer), len(self._pending))
        buffer[:size] = self._pending[:size]
        self._pending = self._pending[size:]
        self._position += size
        return size

    def _fetch(self) -> bytes:
        end = self._position + self._chunk_size - 1
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._source.get(self._position, end)
                if response is None:
                    self._finished = True
                    return b""
                with response:
                    data = response.read(self._chunk_size)
                size = self._source.size  # known now (from the response)
                expected = self._chunk_size if size is None else size - self._position
                if len(data) < min(self._chunk_size, expected):
                    if size is None:  # no way to tell a short object from a cut response
                        self._finished = True
                        return data
                    raise http.client.IncompleteRead(data, expected - len(data))
                break
            except _DROPPED as error:
                if attempt > self._source.max_retries:
                    raise RemoteInputError(
                        f"Reading the start of the input from {self._source.host!r} failed "
                        f"{attempt} times in a row ({type(error).__name__}); giving up",
                        retryable=True,
                    ) from None
                self._source.wait_before_retry(attempt)
        if self._source.size is not None and self._position + len(data) >= self._source.size:
            self._finished = True
        return data
