"""Upload artifacts through the presigned PUTs the gateway hands out.

Each PUT sends ``Content-MD5``, so the store itself rejects a body that was corrupted on the way
(S3 and compatible stores answer ``BadDigest``). Transient failures are retried from the start
of the file.
"""

from __future__ import annotations

import os
from typing import Callable

from .gateway import UploadTarget
from .results import ArtifactFile, ResultInvalid
from .retry import Backoff, Interrupted, Wait, retry
from .transport import HttpClient, HttpStatusError, TransportError, is_transient


class UploadError(Exception):
    def __init__(self, message: str, retryable: bool):
        super().__init__(message)
        self.message = message
        self.retryable = retryable


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
    except TransportError as error:
        raise UploadError(
            f"Uploading the artifact {artifact.name} to {error.host} failed after {attempts} "
            f"attempts ({error.reason}).",
            retryable=True,
        ) from None
    except HttpStatusError as error:
        detail = f": {error.detail}" if error.detail else ""
        tried = f" after {attempts} attempts" if error.transient else ""
        raise UploadError(
            f"The object store at {error.host} refused the upload of the artifact "
            f"{artifact.name} (HTTP {error.status}{detail}){tried}.",
            retryable=error.transient,
        ) from None
    except ResultInvalid as error:
        raise UploadError(
            f"The artifact {artifact.name} cannot be uploaded: {error}.", retryable=False
        ) from None
