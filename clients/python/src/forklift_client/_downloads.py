"""Downloading artifacts from the object store through presigned GETs, checked as they arrive.

``GET /artifacts/{id}/download`` gives a short-lived presigned URL (and an audit entry); the
bytes come straight from the store, without the API token. Their count must be the artifact's
``bytes`` and, when the worker reported one, their sha256 its ``sha256``.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import uuid
from pathlib import Path, PurePosixPath
from typing import IO, TYPE_CHECKING, Iterator, Union

from ._transport import ERROR_BODY_BYTES, check_length, read, store_error_code
from .errors import StoreError, TransportError, VerificationError
from .models import Artifact

if TYPE_CHECKING:  # pragma: no cover - imports for type checkers only
    from .client import ForkliftClient

log = logging.getLogger("forklift_client")

READ_CHUNK = 1024 * 1024


class ArtifactStream:
    """An artifact's bytes as they arrive from the store.

    ``read(size)`` returns up to ``size`` bytes (b"" at the end), iterating yields chunks of up
    to 1 MiB. When the end is read, the byte count and sha256 are checked
    (:class:`~forklift_client.errors.VerificationError`); ``verified`` is then True.
    """

    def __init__(self, response: IO[bytes], host: str, what: str, artifact: Artifact, headers):
        self._response = response
        self._host = host
        self._what = what
        self._headers = headers
        self.artifact = artifact
        self.received = 0
        self.verified = False
        self._digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return b"".join(iter(lambda: self.read(READ_CHUNK), b""))
        if self.verified or size == 0:
            return b""
        chunk = read(self._response, size, self._host, self._what)
        if chunk:
            self.received += len(chunk)
            if self.received > self.artifact.bytes:
                raise VerificationError(
                    f"{self._what}: the store sent more than the artifact's {self.artifact.bytes} "
                    "bytes"
                )
            self._digest.update(chunk)
            return chunk
        self._finish()
        return b""

    def _finish(self) -> None:
        check_length(self._what, self._host, self._headers, self.received)
        if self.received != self.artifact.bytes:
            raise VerificationError(
                f"{self._what}: received {self.received} bytes, the artifact has "
                f"{self.artifact.bytes}"
            )
        expected = self.artifact.sha256.lower()
        if expected and self._digest.hexdigest() != expected:
            raise VerificationError(f"{self._what}: the bytes do not have the artifact's sha256")
        self.verified = True

    def readable(self) -> bool:
        return True

    def __iter__(self) -> Iterator[bytes]:
        return iter(lambda: self.read(READ_CHUNK), b"")


def _artifact(client: "ForkliftClient", artifact: Union[Artifact, str, uuid.UUID]) -> Artifact:
    return artifact if isinstance(artifact, Artifact) else client.get_artifact(artifact)


@contextlib.contextmanager
def open_stream(
    client: "ForkliftClient", artifact: Union[Artifact, str, uuid.UUID]
) -> Iterator[ArtifactStream]:
    found = _artifact(client, artifact)
    link = client.get_download_url(found)
    what = f"Downloading {found.name} of job {found.job_id}"
    with client._transport.stream(link.url, headers={}, what=what) as (
        host,
        status,
        headers,
        body,
    ):
        if status != 200:
            raise StoreError(
                what, host, status, store_error_code(read(body, ERROR_BODY_BYTES, host, what))
            )
        yield ArtifactStream(body, host, what, found, headers)


def _file_name(artifact: Artifact) -> str:
    name = PurePosixPath(artifact.name).name
    return name if name not in ("", ".", "..") else artifact.id


def _remove(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def download(
    client: "ForkliftClient",
    artifact: Union[Artifact, str, uuid.UUID],
    destination: Union[str, "os.PathLike[str]"],
) -> Path:
    found = _artifact(client, artifact)
    target = Path(destination)
    if target.is_dir():
        target = target / _file_name(found)
    partial = target.with_name(f".{target.name}.{uuid.uuid4().hex[:12]}.part")
    attempt = 0
    while True:
        attempt += 1
        try:
            with open_stream(client, found) as stream, open(partial, "wb") as out:
                for chunk in stream:
                    out.write(chunk)
            os.replace(partial, target)
            log.info("Downloaded %s of job %s to %s", found.name, found.job_id, target)
            return target
        except (TransportError, StoreError) as error:
            _remove(partial)
            if isinstance(error, TransportError):
                retryable = error.retryable
            else:  # a fresh URL is asked for each attempt, so an expired one (403) is retried
                retryable = error.transient or error.status == 403
            if not retryable or attempt >= client.retry.attempts:
                raise
            delay = client.retry.delay(attempt, client._rng)
            log.info("%s; trying again in %.1f s", error, delay)
            client._sleep(delay)
        except BaseException:
            _remove(partial)
            raise
