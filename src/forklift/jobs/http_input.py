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
  :attr:`PresignedUrlSource.name` is the URL without its query string;
* with a ``refresh`` callable, a URL that is about to expire is replaced before the next request
  (SigV4 URLs say when: ``X-Amz-Date`` plus ``X-Amz-Expires``), and a request the store refuses
  with HTTP 403 (an expired or invalid signature), or with HTTP 400 ``ExpiredToken`` (the
  temporary credentials the URL was signed with have ended), is sent once more with a fresh URL.
  A fresh URL must name the same object (scheme, host, port and path); the ETag pin keeps
  guaranteeing that the bytes are the same.

Proxies configured in the environment (``HTTPS_PROXY``, ``NO_PROXY``) are honoured, so an
allow-listing egress proxy works as the design describes.
"""

from __future__ import annotations

import http.client
import io
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, BinaryIO, Callable, Iterable, List, Optional, Tuple

from ..engine.exceptions import INPUT_UNREADABLE, PERMISSION_DENIED, SPEC_INVALID
from ..engine.input_source import InputSource

logger = logging.getLogger(__name__)

#: Bytes per range request while the header is detected
HEAD_CHUNK_BYTES = 64 * 1024
#: A URL is replaced this many seconds before it expires (or after half its lifetime, if sooner)
REFRESH_MARGIN_SECONDS = 300.0
#: Fresh URLs one input may ask for, before requests and after refusals together
MAX_REFRESHES = 100

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")
# S3's answer (HTTP 400) to a URL signed with temporary credentials that have ended
_EXPIRED_TOKEN = re.compile(rb"<Code>\s*(ExpiredToken|TokenRefreshRequired)\s*</Code>")
_ERROR_BODY_BYTES = 4096
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


class _NoFreshUrl(RemoteInputError):
    """``refresh`` gave no URL; ``reason`` says why."""

    def __init__(self, reason: str, retryable: bool = False):
        super().__init__(
            f"The input URL expired and a fresh one could not be obtained: {reason}",
            PERMISSION_DENIED,
            retryable,
        )
        self.reason = reason


def url_expiry(url: str) -> Optional[Tuple[float, float]]:
    """``(expires_at, lifetime)`` of a SigV4 presigned URL, in seconds (``expires_at`` since the
    epoch), from its ``X-Amz-Date`` and ``X-Amz-Expires``; None when the URL does not say."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    date, expires = query.get("X-Amz-Date", [""])[0], query.get("X-Amz-Expires", [""])[0]
    try:
        signed = datetime.strptime(date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    if not expires.isdigit():
        return None
    return signed.timestamp() + int(expires), float(expires)


def _url_refused(error: urllib.error.HTTPError) -> bool:
    """HTTP 403, or S3's HTTP 400 ``ExpiredToken``: refusals that a fresh URL may get past."""
    if error.code == 403:
        return True
    if error.code != 400:
        return False
    try:
        body = error.read(_ERROR_BODY_BYTES)
    except _DROPPED:
        return False
    return _EXPIRED_TOKEN.search(body) is not None


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


def _object_of(parts: urllib.parse.SplitResult) -> Tuple[str, Tuple[str, int], str]:
    """What names the object a URL opens: scheme, host and port, and path."""
    return parts.scheme, _host_and_port(parts.scheme, parts.netloc), parts.path


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
        refresh: Returns a fresh presigned URL for the same object; called when the URL is
            about to expire or was refused (see the module docstring). None: never replaced
        refresh_margin: Seconds before expiry from which the URL is replaced (at most half its
            lifetime, so that a fresh URL is not replaced at once)
        max_refreshes: Fresh URLs to ask for at most (at most one per request in any case)
        clock: Seconds since the epoch, compared with the URL's expiry (tests replace it)

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
        refresh: Optional[Callable[[], str]] = None,
        refresh_margin: float = REFRESH_MARGIN_SECONDS,
        max_refreshes: int = MAX_REFRESHES,
        clock: Callable[[], float] = time.time,
    ):
        parts = check_url(url, allowed_hosts)
        self._url = url
        self._urls = [url]
        self._object = _object_of(parts)
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
        self._refresh = refresh
        self.refresh_margin = refresh_margin
        self.max_refreshes = max_refreshes
        self.refreshes = 0
        self._clock = clock

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
            RemoteInputError: For a refused, missing, changed or unrangeable object, or a URL
                that expired and could not be replaced
            OSError: The server could not be reached (the caller retries)
        """
        headers = {"Range": f"bytes={start}-{'' if end is None else end}"}
        headers["Accept-Encoding"] = "identity"
        if self.etag:
            headers["If-Match"] = self.etag
        fresh = self._refresh_if_expiring()
        while True:
            request = urllib.request.Request(self._url, headers=headers, method="GET")
            try:
                response = self._opener.open(request, timeout=self.timeout)
                break
            except urllib.error.HTTPError as error:
                url_refused = _url_refused(error)
                error.close()
                if not url_refused or self._refresh is None or fresh:
                    return self._refused(error.code, start, url_refused, fresh)
                self._use(self._fresh_url())
                fresh = True  # at most one fresh URL per request
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

    def _refused(self, status: int, start: int, url_refused: bool, fresh: bool) -> None:
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
        if status == 401 or url_refused:
            again = " although it had just been refreshed" if fresh else ""
            raise RemoteInputError(
                f"The object store at {self.host!r} refused the presigned URL (HTTP {status})"
                f"{again}; it may have expired or have been signed for another object or with "
                "credentials that have ended",
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

    # ------------------------------------------------------------------ fresh URLs

    def secrets(self) -> List[str]:
        """Every URL this source was given, and their query strings: none may be shown."""
        return [part for url in self._urls for part in (url, urllib.parse.urlsplit(url).query)]

    def _refresh_if_expiring(self) -> bool:
        """Replace the URL if it expires soon; True when it was replaced."""
        if self._refresh is None or self.refreshes >= self.max_refreshes:
            return False
        expiry = url_expiry(self._url)
        if expiry is None or self._clock() < expiry[0] - min(self.refresh_margin, expiry[1] / 2):
            return False
        try:
            self._use(self._fresh_url())
        except _NoFreshUrl as error:
            # It has not expired yet (or the store will say so): go on with it
            logger.warning(
                "The input URL expires soon and a fresh one could not be obtained (%s); the "
                "current one is used until the store refuses it",
                error.reason,
            )
            return False
        return True

    def _fresh_url(self) -> str:
        """A fresh URL from ``refresh``; _NoFreshUrl when there is none."""
        if self.refreshes >= self.max_refreshes:
            raise _NoFreshUrl(
                f"the limit of {self.max_refreshes} fresh URLs for one input was reached"
            )
        self.refreshes += 1
        try:
            url = self._refresh()
        except Exception as error:
            reason = str(error) or type(error).__name__
            raise _NoFreshUrl(reason, bool(getattr(error, "retryable", False))) from None
        if not isinstance(url, str):
            raise _NoFreshUrl(f"the refresh callback returned {type(url).__name__}, not a URL")
        self._urls.append(url)
        return url

    def _use(self, url: str) -> None:
        """Read from ``url`` from now on; it must name the same object as the job's URL."""
        try:
            parts = urllib.parse.urlsplit(url)
            same = _object_of(parts) == self._object
        except ValueError:  # a port that is not a number
            same = False
        if not same or parts.username is not None or parts.password is not None:
            raise RemoteInputError(
                f"The fresh input URL does not name the job's input ({self.name!r}): a fresh "
                "URL must keep the scheme, host, port and path and carry no user information, "
                "so it was not used"
            )
        self._url = url


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
