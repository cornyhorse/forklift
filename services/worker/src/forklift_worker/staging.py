"""Stage an input into scratch through its presigned GET, checking its size and checksum.

The download must match what the gateway said when it leased the job: ``Content-Length`` and
the byte count equal the location's ``size``, the ``ETag`` header equals its ``etag``, and when the
ETag is a plain MD5 (an object uploaded in one part) the bytes must hash to it. A multipart ETag
(``<md5>-<parts>``) cannot be recomputed without the part size, so for those the size and the
ETag header are what is checked. Transient failures restart the download from the beginning.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from pathlib import Path
from typing import Callable

from .retry import Backoff, Interrupted, Wait, retry
from .spec import StagedInput, format_bytes
from .transport import HttpClient, HttpStatusError, TransportError

CHUNK_BYTES = 1024 * 1024
_MD5 = re.compile(r"^[0-9a-f]{32}$")


class StagingError(Exception):
    def __init__(self, code: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


def _normal_etag(etag: str) -> str:
    tag = etag.strip()
    if tag.startswith("W/"):
        tag = tag[2:]
    return tag.strip('"').lower()


def _status_error(error: HttpStatusError) -> StagingError:
    detail = f": {error.detail}" if error.detail else ""
    if error.status == 403:
        message = (
            f"The object store at {error.host} refused the input's presigned URL (HTTP 403"
            f"{detail}); it may have expired before the job started."
        )
    elif error.status == 404:
        message = f"The input no longer exists in the object store at {error.host} (HTTP 404)."
    else:
        message = (
            f"Downloading the input from {error.host} failed with HTTP {error.status}{detail}."
        )
    return StagingError("INPUT_UNREADABLE", message, retryable=error.transient)


def _download(
    http: HttpClient,
    item: StagedInput,
    destination: Path,
    stopped: Callable[[], bool],
    on_progress: Callable[[int], None],
) -> None:
    expected_tag = _normal_etag(item.etag) if item.etag else None
    digest = (
        hashlib.md5(usedforsecurity=False) if expected_tag and _MD5.match(expected_tag) else None
    )
    received = 0
    try:
        with http.download(item.url) as response:
            length = response.headers.get("content-length")
            if length is not None and length.strip() != str(item.size):
                raise StagingError(
                    "INPUT_UNREADABLE",
                    f"The object store reports the input as {length.strip()} bytes, but the job "
                    f"expects {item.size}: the object changed after the job was queued.",
                )
            tag = response.headers.get("etag")
            if expected_tag and tag and _normal_etag(tag) != expected_tag:
                raise StagingError(
                    "INPUT_UNREADABLE",
                    "The input's ETag in the object store differs from the one the job was "
                    "queued with: the object changed after the job was queued.",
                )
            descriptor = os.open(
                destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600
            )
            with os.fdopen(descriptor, "wb") as output:
                for chunk in response.chunks(CHUNK_BYTES):
                    if stopped():
                        raise Interrupted()
                    received += len(chunk)
                    if received > item.size:
                        raise StagingError(
                            "INPUT_UNREADABLE",
                            f"The object store sent more than the {item.size} bytes the job "
                            "expects: the object changed after the job was queued.",
                        )
                    output.write(chunk)
                    if digest:
                        digest.update(chunk)
                    on_progress(received)
    except TransportError as error:
        raise StagingError(
            "INPUT_UNREADABLE",
            f"Downloading the input from {error.host} failed ({error.reason}).",
            retryable=True,
        ) from None
    except HttpStatusError as error:
        raise _status_error(error) from None
    except OSError as error:
        raise StagingError(
            "INTERNAL",
            f"Writing the staged input into scratch failed ({error.strerror or error}).",
        ) from None
    if received != item.size:
        raise StagingError(
            "INPUT_UNREADABLE",
            f"The download from {item.host} ended after {received} of {item.size} bytes.",
            retryable=True,
        )
    if digest and digest.hexdigest() != expected_tag:
        raise StagingError(
            "INPUT_UNREADABLE",
            f"The input downloaded from {item.host} does not match its checksum (the MD5 in "
            "its ETag): it was corrupted on the way.",
            retryable=True,
        )


def stage_input(
    http: HttpClient,
    item: StagedInput,
    workdir: Path,
    *,
    stopped: Callable[[], bool],
    wait: Wait,
    attempts: int = 3,
    backoff: Backoff | None = None,
    on_progress: Callable[[int], None] = lambda received: None,
) -> Path:
    """Download ``item`` to ``workdir / item.path``; raises StagingError or Interrupted."""
    destination = workdir / item.path
    destination.parent.mkdir(mode=0o700, exist_ok=True)
    free = shutil.disk_usage(destination.parent).free
    if item.size > free:
        raise StagingError(
            "LIMIT_EXCEEDED",
            f"The input is {format_bytes(item.size)}, more than the {format_bytes(free)} free "
            "in this worker's scratch space.",
            retryable=True,
        )
    retry(
        lambda: _download(http, item, destination, stopped, on_progress),
        attempts=attempts,
        retryable=lambda error: isinstance(error, StagingError) and error.retryable,
        backoff=backoff or Backoff(1.0, 10.0),
        wait=wait,
    )
    return destination
