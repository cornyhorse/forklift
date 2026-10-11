"""Uploading a file straight to the object store through an upload's presigned URLs.

1. ``POST /uploads`` with the file's name and size answers with one PUT URL, or (for files
   above the gateway's ``multipart_threshold_bytes``) the part size and the first part URLs.
2. The bytes go up in one PUT, or part by part: one PUT each, part URLs asked for in batches
   as they are needed (``POST /uploads/{id}/parts``), each part's ETag kept.
3. ``POST /uploads/{id}/complete`` (with the parts' ETags) makes the gateway check with a HEAD
   that the object has the declared size.

Every PUT sends ``Content-Length`` and ``Content-MD5``, so the store itself refuses a body that
was corrupted on the way. Bodies are streamed: from the file itself when it can seek, otherwise a
part at a time through a temporary file (in memory up to 8 MiB), so a failed PUT can be sent
again. A PUT is retried after a connection error, a timeout, 408, 429 or 5xx (and a part, which
gets a fresh URL for every retry, after a 403 too: its URL may have expired). The bytes read must
be exactly the size declared: a file that changes while it is uploaded is a VerificationError.
An upload that fails is deleted (a multipart upload is aborted), so nothing half-uploaded stays.
"""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import math
import mimetypes
import os
import tempfile
from typing import IO, TYPE_CHECKING, Callable, Dict, List, Mapping, Optional, Union

from ._transport import host_of, store_error_code
from .errors import (
    ForkliftError,
    StoreError,
    TransportError,
    UnexpectedResponse,
    VerificationError,
)

if TYPE_CHECKING:  # pragma: no cover - imports for type checkers only
    from .client import ForkliftClient
    from .models import Upload, UploadTicket

log = logging.getLogger("forklift_client")

READ_CHUNK = 1024 * 1024  # most bytes read from the file at once
SPOOL_IN_MEMORY = 8 * 1024 * 1024  # a part of a stream that cannot seek: in memory up to this
PART_URLS_PER_REQUEST = 100


class _Window:
    """``length`` bytes of a seekable binary file from ``offset``, as a request body."""

    def __init__(self, file: IO[bytes], offset: int, length: int, what: str):
        self._file, self._offset, self.length, self._what = file, offset, length, what
        self._left = 0

    def rewind(self) -> None:
        self._file.seek(self._offset)
        self._left = self.length

    def read(self, size: int = -1) -> bytes:
        if self._left <= 0:
            return b""
        size = READ_CHUNK if size is None or size < 0 else min(size, READ_CHUNK)
        chunk = self._file.read(min(size, self._left))
        if not chunk:
            done = self.length - self._left
            raise VerificationError(
                f"{self._what}: the file ended after {done} of the {self.length} bytes of this "
                "part (did it change while it was uploaded?)"
            )
        self._left -= len(chunk)
        return chunk

    def md5(self) -> str:
        """The base64 MD5 of the window (``Content-MD5``)."""
        digest = hashlib.md5(usedforsecurity=False)
        self.rewind()
        for chunk in iter(self.read, b""):
            digest.update(chunk)
        return base64.b64encode(digest.digest()).decode("ascii")


class _Source:
    """The bytes to upload: ``size`` bytes of ``stream`` from ``start``."""

    def __init__(self, stream: IO[bytes], seekable: bool, start: int, size: int, what: str):
        self.stream = stream
        self.seekable = seekable
        self.start = start
        self.size = size
        self.what = what
        self._spool: Optional[IO[bytes]] = None

    def window(self, offset: int, length: int, what: str) -> _Window:
        if self.seekable:
            return _Window(self.stream, self.start + offset, length, what)
        if self._spool is not None:
            self._spool.close()
        self._spool = spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_IN_MEMORY)
        copied = 0
        while copied < length:
            chunk = self.stream.read(min(READ_CHUNK, length - copied))
            if not chunk:
                raise VerificationError(
                    f"{self.what}: the stream ended after {offset + copied} of the {self.size} "
                    "bytes declared"
                )
            spool.write(chunk)
            copied += len(chunk)
        return _Window(spool, 0, length, what)

    def check_end(self) -> None:
        """The source holds no more than ``size`` bytes (it did not grow while uploading)."""
        if self.seekable:
            end = self.stream.seek(0, io.SEEK_END)
            extra = end - self.start - self.size
        else:
            extra = len(self.stream.read(1))
        if extra:
            raise VerificationError(
                f"{self.what}: the file has more than the {self.size} bytes declared (did it "
                "change while it was uploaded?)"
            )

    def close(self) -> None:
        if self._spool is not None:
            self._spool.close()


def _measure(stream: IO[bytes], size: Optional[int]) -> tuple:
    """(seekable, start, size) of a binary stream, read from its current position."""
    if isinstance(stream, io.TextIOBase):
        raise TypeError("open the file in binary mode ('rb'): uploads send bytes")
    try:
        seekable = bool(stream.seekable())
    except (AttributeError, ValueError, OSError):
        seekable = False
    if not seekable:
        if size is None:
            raise ValueError("size is required for a stream that cannot seek")
        return False, 0, size
    start = stream.tell()
    actual = stream.seek(0, io.SEEK_END) - start
    stream.seek(start)
    if size is not None and size != actual:
        raise ValueError(f"size is {size}, but the file has {actual} bytes from its position")
    return True, start, actual


def _content_type(headers: Mapping[str, str], fallback: str) -> Dict[str, str]:
    if any(name.lower() == "content-type" for name in headers):
        return {}
    return {"Content-Type": fallback or "application/octet-stream"}


def _put(
    client: "ForkliftClient",
    url: str,
    window: _Window,
    headers: Mapping[str, str],
    what: str,
    fresh_url: Optional[Callable[[], str]] = None,
) -> Dict[str, str]:
    """PUT ``window`` to ``url`` with retries; returns the store's answer's headers.

    ``fresh_url`` (parts) gives a new URL for each retry, which then also follows a 403."""
    policy, attempt = client.retry, 0
    while True:
        attempt += 1
        window.rewind()
        try:
            response = client._transport.request(
                "PUT", url, headers=headers, body=window, what=what
            )
        except TransportError as error:
            if not error.retryable or attempt >= policy.attempts:
                raise
            reason = error.reason
        else:
            if 200 <= response.status < 300:
                return response.headers
            failure = StoreError(
                what, host_of(url), response.status, store_error_code(response.body)
            )
            renewable = response.status == 403 and fresh_url is not None
            if not (failure.transient or renewable) or attempt >= policy.attempts:
                raise failure
            reason = f"HTTP {response.status}"
        delay = policy.delay(attempt, client._rng)
        log.info("%s: %s; trying again in %.1f s", what, reason, delay)
        client._sleep(delay)
        if fresh_url is not None:
            url = fresh_url()


def _put_single(client: "ForkliftClient", ticket: "UploadTicket", source: _Source) -> None:
    window = source.window(0, source.size, source.what)
    headers = {
        **ticket.headers,
        "Content-Length": str(source.size),
        "Content-MD5": window.md5(),
        **_content_type(ticket.headers, ticket.upload.content_type),
    }
    _put(client, ticket.url, window, headers, source.what)  # type: ignore[arg-type]


def _put_parts(
    client: "ForkliftClient", ticket: "UploadTicket", source: _Source
) -> List[Dict[str, object]]:
    upload_id, part_size, count = ticket.upload.id, ticket.part_size, ticket.part_count
    if not part_size or count != math.ceil(source.size / part_size):
        raise UnexpectedResponse(
            f"{source.what}: the gateway answered a multipart upload of {count} parts of "
            f"{part_size} bytes, which does not fit {source.size} bytes"
        )
    urls = {part.part_number: part.url for part in ticket.parts}
    parts: List[Dict[str, object]] = []
    for number in range(1, count + 1):
        if number not in urls:
            last = min(count, number + PART_URLS_PER_REQUEST - 1)
            fresh = client.upload_part_urls(upload_id, range(number, last + 1))
            urls.update({part.part_number: part.url for part in fresh})
        what = f"{source.what} (part {number} of {count})"
        url = urls.pop(number, None)
        if url is None:
            raise UnexpectedResponse(f"{what}: the gateway gave no URL for the part")
        offset = (number - 1) * part_size
        window = source.window(offset, min(part_size, source.size - offset), what)
        headers = {"Content-Length": str(window.length), "Content-MD5": window.md5()}

        def fresh_url(number: int = number) -> str:
            (part,) = client.upload_part_urls(upload_id, [number])
            return part.url

        answer = _put(client, url, window, headers, what, fresh_url)
        etag = answer.get("etag", "")
        if not etag:
            raise StoreError(what, host_of(url), 200, "no ETag in the answer")
        parts.append({"part_number": number, "etag": etag})
        log.debug("%s: uploaded", what)
    return parts


def _discard(client: "ForkliftClient", upload_id: str, what: str) -> None:
    try:
        client.delete_upload(upload_id)
    except ForkliftError as error:
        log.warning("%s failed, and deleting upload %s failed too: %s", what, upload_id, error)


def upload(
    client: "ForkliftClient",
    source: Union[str, "os.PathLike[str]", IO[bytes]],
    *,
    filename: Optional[str],
    size: Optional[int],
    content_type: Optional[str],
    classification: Optional[str],
    sha256: str,
) -> "Upload":
    if isinstance(source, (str, os.PathLike)):
        stream: IO[bytes] = open(source, "rb")
        owned = True
        name = os.path.basename(os.fspath(source))
    else:
        stream, owned = source, False
        given = getattr(source, "name", None)
        name = os.path.basename(given) if isinstance(given, str) else ""
    try:
        filename = filename or name
        if not filename:
            raise ValueError("filename is required for a file object without a name")
        if content_type is None:
            content_type = mimetypes.guess_type(filename)[0] or ""
        seekable, start, size = _measure(stream, size)
        what = f"Uploading {filename}"
        data = _Source(stream, seekable, start, size, what)
        ticket = client.create_upload(
            filename,
            size,
            content_type=content_type,
            sha256=sha256,
            classification=classification,
        )
        upload_id = ticket.upload.id
        try:
            parts = None
            if ticket.url:
                _put_single(client, ticket, data)
            else:
                parts = _put_parts(client, ticket, data)
            data.check_end()
            done = client.complete_upload(upload_id, parts)
        except Exception:
            _discard(client, upload_id, what)
            raise
        finally:
            data.close()
        log.info("%s: upload %s complete (%d bytes)", what, upload_id, size)
        return done
    finally:
        if owned:
            stream.close()
