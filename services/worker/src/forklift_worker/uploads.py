"""Upload artifacts through the presigned URLs the gateway hands out.

An artifact goes up with one PUT, or, when the gateway answered presign with a multipart upload
(files above its ``multipart_threshold_bytes``), part by part. Each PUT sends ``Content-MD5``, so
the store itself rejects a body that was corrupted on the way (S3 and compatible stores answer
``BadDigest``). Transient failures are retried: a single PUT from the start of the file, a part
on its own, with a fresh URL (it may have expired; stores answer 403 then). A part is read from
the file in chunks of at most ``READ_CHUNK`` bytes, once for its MD5 and once as it is sent, so
memory stays bounded whatever the part size. The worker never aborts a multipart upload: the
gateway aborts the ones still pending when the attempt ends.
"""

from __future__ import annotations

import base64
import hashlib
import math
import os
import time
from typing import Any, Callable

from .gateway import MultipartTarget, PartUrls, UploadTarget
from .results import ArtifactFile, ResultInvalid
from .retry import Backoff, Interrupted, Wait, retry
from .transport import HttpClient, HttpStatusError, TransportError, is_transient

READ_CHUNK = 1024 * 1024  # most bytes of an artifact read at once
PARTS_PER_REQUEST = 100  # part URLs asked for at a time
URL_USE_FRACTION = 0.75  # a presigned URL starts requests only in this first part of its life


class UploadError(Exception):
    def __init__(self, message: str, retryable: bool):
        super().__init__(message)
        self.message = message
        self.retryable = retryable


def usable(received_at: float, expires_in: float | None, now: float) -> bool:
    """Whether a URL received at ``received_at`` (monotonic seconds) that stays valid
    ``expires_in`` seconds (None: unknown) is young enough to start a request with."""
    return expires_in is None or now - received_at < expires_in * URL_USE_FRACTION


def parts_sha256(etags) -> str:
    """How complete reports the parts of a multipart artifact, so the report does not grow with
    them: the sha256 (hex) of their ETags, without quotes, in part order, each followed by a
    newline. The gateway computes it again from the parts the store holds."""
    digest = hashlib.sha256()
    for etag in etags:
        digest.update(etag.strip('"').encode("utf-8") + b"\n")
    return digest.hexdigest()


def _failure(error: Exception, name: str, what: str, attempts: int, retried) -> UploadError:
    """The UploadError for the last error of uploading ``what`` (the artifact ``name`` or one of
    its parts); ``retried(error)`` says whether that error was retried."""
    if isinstance(error, TransportError):
        return UploadError(
            f"Uploading {what} to {error.host} failed after {attempts} attempts "
            f"({error.reason}).",
            retryable=True,
        )
    if isinstance(error, HttpStatusError):
        detail = f": {error.detail}" if error.detail else ""
        tried = f" after {attempts} attempts" if retried(error) else ""
        return UploadError(
            f"The object store at {error.host} refused the upload of {what} "
            f"(HTTP {error.status}{detail}){tried}.",
            retryable=error.transient,
        )
    return UploadError(f"The artifact {name} cannot be uploaded: {error}.", retryable=False)


# --------------------------------------------------------------------------- one PUT


def _put(http: HttpClient, target: UploadTarget, artifact: ArtifactFile, stopped) -> None:
    if stopped():
        raise Interrupted()
    headers = {
        **target.headers,
        "Content-Length": str(artifact.bytes),
        "Content-MD5": artifact.md5_base64,
    }
    with os.fdopen(artifact.open(), "rb") as body:
        http.request(target.method, target.url, headers=headers, body=body)


def upload_artifact(
    http: HttpClient,
    target: UploadTarget,
    artifact: ArtifactFile,
    *,
    stopped: Callable[[], bool],
    wait: Wait,
    attempts: int = 5,
    backoff: Backoff | None = None,
) -> None:
    """PUT ``artifact`` to ``target``; raises UploadError or Interrupted."""
    try:
        retry(
            lambda: _put(http, target, artifact, stopped),
            attempts=attempts,
            retryable=is_transient,
            backoff=backoff or Backoff(1.0, 15.0),
            wait=wait,
        )
    except (TransportError, HttpStatusError, ResultInvalid) as error:
        what = f"the artifact {artifact.name}"
        raise _failure(error, artifact.name, what, attempts, is_transient) from None


# --------------------------------------------------------------------------- in parts


class _PartBody:
    """One part of an open artifact as a request body, read with pread in bounded chunks."""

    def __init__(self, descriptor: int, offset: int, length: int):
        self._descriptor, self._offset, self._left = descriptor, offset, length

    def read(self, size: int = -1) -> bytes:
        size = READ_CHUNK if size < 0 else min(size, READ_CHUNK)
        chunk = os.pread(self._descriptor, min(size, self._left), self._offset)
        self._offset += len(chunk)
        self._left -= len(chunk)
        return chunk


def _part_md5(descriptor: int, offset: int, length: int, name: str) -> str:
    digest, done = hashlib.md5(usedforsecurity=False), 0
    while done < length:
        chunk = os.pread(descriptor, min(READ_CHUNK, length - done), offset + done)
        if not chunk:
            raise ResultInvalid(f"the artifact {name!r} changed after it was hashed")
        digest.update(chunk)
        done += len(chunk)
    return base64.b64encode(digest.digest()).decode("ascii")


class _PartUrls:
    """The part URLs at hand: a batch at a time, fetched again when the batch has no URL for a
    part, when its URLs grew old, or when a part failed with its URL."""

    def __init__(self, target: MultipartTarget, fetch: Callable[[list[int]], PartUrls], clock):
        self._count, self._fetch, self._clock = target.part_count, fetch, clock
        self._take(target.parts)

    def _take(self, batch: PartUrls) -> None:
        self._urls = dict(batch.urls)
        self._received_at, self._expires_in = batch.received_at, batch.expires_in

    def url(self, number: int) -> str:
        if number not in self._urls or not usable(
            self._received_at, self._expires_in, self._clock()
        ):
            last = min(number + PARTS_PER_REQUEST - 1, self._count)
            self._take(self._fetch(list(range(number, last + 1))))
        return self._urls[number]

    def discard(self, number: int) -> None:
        self._urls.pop(number, None)


def _put_part(http: HttpClient, urls: _PartUrls, descriptor: int, number: int, part, stopped):
    """PUT one part (``part``: offset, length, MD5); returns the ETag the store answered."""
    if stopped():
        raise Interrupted()
    offset, length, md5 = part
    url = urls.url(number)
    headers = {"Content-Length": str(length), "Content-MD5": md5}
    try:
        response = http.request(
            "PUT", url, headers=headers, body=_PartBody(descriptor, offset, length)
        )
    except (TransportError, HttpStatusError):
        urls.discard(number)  # the next try signs it again
        raise
    etag = response.headers.get("etag", "")
    if not etag.strip('"'):
        raise ResultInvalid(f"the store answered part {number} without an ETag")
    return etag


def _part_retryable(error: Exception) -> bool:
    return is_transient(error) or (isinstance(error, HttpStatusError) and error.status == 403)


def upload_multipart(
    http: HttpClient,
    target: MultipartTarget,
    artifact: ArtifactFile,
    *,
    fresh_urls: Callable[[list[int]], PartUrls],
    stopped: Callable[[], bool],
    wait: Wait,
    on_progress: Callable[[int], None] = lambda uploaded: None,
    attempts: int = 5,
    backoff: Backoff | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> list[dict[str, Any]]:
    """PUT ``artifact`` part by part; returns each part's number and ETag (complete reports
    their count and ``parts_sha256``).

    ``fresh_urls(part_numbers)`` asks the gateway for URLs (its errors pass through);
    ``on_progress`` hears the bytes uploaded after each part. Raises UploadError or Interrupted
    (also between parts, once ``stopped()``).
    """
    if math.ceil(artifact.bytes / target.part_size) != target.part_count:
        raise UploadError(
            f"The gateway's multipart upload of the artifact {artifact.name} has "
            f"{target.part_count} parts of {target.part_size} bytes, which does not fit its "
            f"{artifact.bytes} bytes.",
            retryable=False,
        )
    urls = _PartUrls(target, fresh_urls, clock)
    delays = backoff or Backoff(1.0, 15.0)
    parts: list[dict[str, Any]] = []
    try:
        descriptor = artifact.open()
    except ResultInvalid as error:
        what = f"the artifact {artifact.name}"
        raise _failure(error, artifact.name, what, attempts, _part_retryable) from None
    try:
        for number in range(1, target.part_count + 1):
            offset = (number - 1) * target.part_size
            length = min(target.part_size, artifact.bytes - offset)
            what = f"part {number} of {target.part_count} of the artifact {artifact.name}"
            delays.reset()  # each part gets the whole backoff
            try:
                part = (offset, length, _part_md5(descriptor, offset, length, artifact.name))
                etag = retry(
                    lambda: _put_part(http, urls, descriptor, number, part, stopped),
                    attempts=attempts,
                    retryable=_part_retryable,
                    backoff=delays,
                    wait=wait,
                )
            except (TransportError, HttpStatusError, ResultInvalid) as error:
                raise _failure(error, artifact.name, what, attempts, _part_retryable) from None
            parts.append({"part_number": number, "etag": etag})
            on_progress(offset + length)
    finally:
        os.close(descriptor)
    return parts
