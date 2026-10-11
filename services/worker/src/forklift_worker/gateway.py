"""Client for the gateway's internal API (``/internal/v1``): lease, heartbeat, presign, parts,
complete, input-url.

Every request carries ``Authorization: Bearer <worker token>``. The token file is read for each
request, so a rotated token is picked up without a restart, and the token is never logged.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote

from . import __version__
from .transport import HttpClient, HttpStatusError, Response, TransportError

SPEC_VERSIONS = (1,)


class GatewayError(Exception):
    """A call to the gateway failed."""


class GatewayUnavailable(GatewayError):
    """No answer, or a 408, 429 or 5xx: worth retrying."""


class GatewayRejected(GatewayError):
    """The gateway refused the request or answered something this worker cannot use."""


class GatewayAuthError(GatewayRejected):
    """The worker token is missing, unreadable or refused (HTTP 401 or 403)."""


class LeaseLost(GatewayError):
    """The job's lease is no longer this worker's (HTTP 409, or 404 for a job that is gone)."""


@dataclass(frozen=True)
class Lease:
    job_id: str
    attempt: int
    lease_seconds: float
    stage_max_bytes: int
    spec: dict[str, Any]


@dataclass(frozen=True)
class HeartbeatReply:
    lease_seconds: float | None
    cancel: bool


@dataclass(frozen=True)
class UploadTarget:
    name: str
    key: str
    url: str
    method: str
    headers: dict[str, str]
    expires_in: float | None = None  # seconds the URL stays valid (None: the gateway did not say)
    received_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class PartUrls:
    """Presigned URLs for parts of a multipart upload, by part number."""

    urls: dict[int, str]
    expires_in: float | None = None
    received_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class MultipartTarget:
    """A multipart upload the gateway started for one artifact: every part but the last has
    ``part_size`` bytes; ``parts`` are the URLs of the first ones (POST /parts gives more)."""

    name: str
    key: str
    upload_id: str
    part_size: int
    part_count: int
    parts: PartUrls


def _malformed(what: str, problem: str) -> GatewayRejected:
    return GatewayRejected(
        f"The gateway's answer to {what} is malformed ({problem}); the gateway and this worker "
        f"(forklift-worker {__version__}) may not speak the same internal API version."
    )


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _expires_in(document: dict[str, Any], what: str) -> float | None:
    expires_in = document.get("expires_in")
    if expires_in is not None and (not _number(expires_in) or expires_in <= 0):
        raise _malformed(what, "expires_in is not a positive number")
    return None if expires_in is None else float(expires_in)


def _part_urls(document: dict[str, Any], what: str) -> PartUrls:
    parts = document.get("parts")
    if not isinstance(parts, list):
        raise _malformed(what, "parts is not a list")
    urls: dict[int, str] = {}
    for part in parts:
        number = part.get("part_number") if isinstance(part, dict) else None
        url = part.get("url") if isinstance(part, dict) else None
        if not _integer(number) or number < 1 or not isinstance(url, str) or not url:
            raise _malformed(what, "a part lacks part_number or url")
        urls[number] = url
    return PartUrls(urls, _expires_in(document, what))


def _multipart_target(upload: dict[str, Any]) -> MultipartTarget:
    what = "a presign request"
    name, key, upload_id = upload.get("name"), upload.get("key"), upload.get("upload_id")
    if not all(isinstance(value, str) and value for value in (name, key, upload_id)):
        raise _malformed(what, "a multipart upload lacks name, key or upload_id")
    part_size, part_count = upload.get("part_size"), upload.get("part_count")
    if not (_integer(part_size) and part_size > 0 and _integer(part_count) and part_count > 0):
        raise _malformed(what, "a multipart upload's part_size or part_count is not positive")
    return MultipartTarget(name, key, upload_id, part_size, part_count, _part_urls(upload, what))


class GatewayClient:
    def __init__(
        self,
        http: HttpClient,
        base_url: str,
        token_file: Path,
        *,
        worker_id: str,
        lanes: Sequence[str],
        engine_version: str,
    ):
        self.http = http
        self.base_url = base_url
        self.token_file = token_file
        self.worker_id = worker_id
        self.lanes = list(lanes)
        self.engine_version = engine_version

    def _token(self) -> str:
        try:
            token = self.token_file.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise GatewayAuthError(
                f"The worker token file {self.token_file} cannot be read "
                f"({type(error).__name__}: {error.strerror or error})."
            ) from None
        if not token:
            raise GatewayAuthError(f"The worker token file {self.token_file} is empty.")
        return token

    def _post(self, path: str, payload: Mapping[str, Any], *, job_id: str | None) -> Response:
        url = self.base_url + path
        headers = {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = json.dumps(payload).encode("utf-8")
        try:
            return self.http.request("POST", url, headers=headers, body=body)
        except TransportError as error:
            raise GatewayUnavailable(
                f"The gateway at {error.host} could not be reached ({error.reason})."
            ) from None
        except HttpStatusError as error:
            raise self._status_error(path, job_id, error) from None

    def _status_error(self, path: str, job_id: str | None, error: HttpStatusError) -> Exception:
        detail = f": {error.detail}" if error.detail else ""
        status = error.status
        if status in (401, 403):
            return GatewayAuthError(
                f"The gateway refused the worker token (HTTP {status}{detail}). Check that "
                f"{self.token_file} holds a current worker token; an admin creates and revokes "
                "them on the gateway's worker tokens screen."
            )
        if status == 409 and job_id is not None:
            return LeaseLost(
                f"The gateway says job {job_id} is no longer leased to this worker "
                f"(HTTP 409{detail})."
            )
        if status == 404 and job_id is not None:
            return LeaseLost(f"The gateway no longer knows job {job_id} (HTTP 404{detail}).")
        if status == 404:
            return GatewayRejected(
                f"The gateway has no internal API at {self.base_url}{path} (HTTP 404). --gateway "
                "must point at the gateway's internal port, not its public one."
            )
        if error.transient:
            return GatewayUnavailable(f"The gateway answered HTTP {status}{detail}.")
        return GatewayRejected(f"The gateway refused {path} (HTTP {status}{detail}).")

    @staticmethod
    def _json(response: Response, what: str) -> dict[str, Any]:
        try:
            document = response.json()
        except ValueError:
            raise _malformed(what, "not JSON") from None
        if not isinstance(document, dict):
            raise _malformed(what, "not a JSON object")
        return document

    def lease(self) -> Lease | None:
        """The next job for this worker's lanes, or None when there is nothing to do (204)."""
        response = self._post(
            "/leases",
            {
                "worker_id": self.worker_id,
                "lanes": self.lanes,
                "spec_versions": list(SPEC_VERSIONS),
                "engine_version": self.engine_version,
                "worker_version": __version__,
            },
            job_id=None,
        )
        if response.status == 204:
            return None
        document = self._json(response, "a lease request")
        job_id = document.get("job_id")
        if not isinstance(job_id, str) or not job_id or len(job_id) > 200:
            raise _malformed("a lease request", "job_id is not a non-empty string")
        attempt = document.get("attempt")
        if not _integer(attempt) or attempt < 1:
            raise _malformed("a lease request", "attempt is not a whole number from 1")
        lease_seconds = document.get("lease_seconds")
        if not _number(lease_seconds) or lease_seconds <= 0:
            raise _malformed("a lease request", "lease_seconds is not a positive number")
        stage_max_bytes = document.get("stage_max_bytes")
        if not _integer(stage_max_bytes) or stage_max_bytes < 0:
            raise _malformed("a lease request", "stage_max_bytes is not a whole number from 0")
        spec = document.get("spec")
        if not isinstance(spec, dict):
            raise _malformed("a lease request", "spec is not a JSON object")
        return Lease(job_id, attempt, float(lease_seconds), stage_max_bytes, spec)

    def _job_path(self, job_id: str, action: str) -> str:
        return f"/jobs/{quote(job_id, safe='')}/{action}"

    def heartbeat(self, job_id: str, attempt: int, progress: Mapping[str, Any]) -> HeartbeatReply:
        response = self._post(
            self._job_path(job_id, "heartbeat"),
            {"attempt": attempt, "progress": dict(progress)},
            job_id=job_id,
        )
        document = self._json(response, "a heartbeat")
        lease_seconds = document.get("lease_seconds")
        if lease_seconds is not None and (not _number(lease_seconds) or lease_seconds <= 0):
            raise _malformed("a heartbeat", "lease_seconds is not a positive number")
        cancel = document.get("cancel", False)
        if not isinstance(cancel, bool):
            raise _malformed("a heartbeat", "cancel is not true or false")
        return HeartbeatReply(None if lease_seconds is None else float(lease_seconds), cancel)

    def presign(
        self, job_id: str, attempt: int, files: Sequence[Mapping[str, Any]]
    ) -> dict[str, UploadTarget | MultipartTarget]:
        """How to upload ``files`` ([{name, bytes}]), by name: a PUT URL, or (for large files)
        a multipart upload."""
        response = self._post(
            self._job_path(job_id, "presign"),
            {"attempt": attempt, "files": [dict(item) for item in files], "multipart": True},
            job_id=job_id,
        )
        uploads = self._json(response, "a presign request").get("uploads")
        if not isinstance(uploads, list):
            raise _malformed("a presign request", "uploads is not a list")
        targets: dict[str, UploadTarget | MultipartTarget] = {}
        for upload in uploads:
            if not isinstance(upload, dict):
                raise _malformed("a presign request", "an upload is not a JSON object")
            if upload.get("upload_id") is not None:
                multipart = _multipart_target(upload)
                targets[multipart.name] = multipart
                continue
            name, key, url = upload.get("name"), upload.get("key"), upload.get("url")
            method = upload.get("method", "PUT")
            headers = upload.get("headers") or {}
            if not all(isinstance(value, str) and value for value in (name, key, url)):
                raise _malformed("a presign request", "an upload lacks name, key or url")
            if method != "PUT":
                raise _malformed("a presign request", f"upload method {method!r} is not PUT")
            if not isinstance(headers, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in headers.items()
            ):
                raise _malformed("a presign request", "upload headers are not strings")
            targets[name] = UploadTarget(
                name, key, url, method, dict(headers), _expires_in(upload, "a presign request")
            )
        missing = {str(item["name"]) for item in files} - set(targets)
        if missing:
            raise _malformed(
                "a presign request", f"no upload URL for {', '.join(sorted(missing))}"
            )
        return targets

    def part_urls(
        self, job_id: str, attempt: int, name: str, upload_id: str, part_numbers: Sequence[int]
    ) -> PartUrls:
        """Fresh URLs for parts of the multipart upload of the artifact ``name``."""
        response = self._post(
            self._job_path(job_id, "parts"),
            {
                "attempt": attempt,
                "name": name,
                "upload_id": upload_id,
                "part_numbers": list(part_numbers),
            },
            job_id=job_id,
        )
        what = "a part URL request"
        parts = _part_urls(self._json(response, what), what)
        missing = set(part_numbers) - set(parts.urls)
        if missing:
            raise _malformed(what, f"no URL for part {min(missing)}")
        return parts

    def complete(
        self,
        job_id: str,
        attempt: int,
        result: Mapping[str, Any],
        artifacts: Sequence[Mapping[str, Any]],
    ) -> None:
        self._post(
            self._job_path(job_id, "complete"),
            {
                "attempt": attempt,
                "result": dict(result),
                "artifacts": [dict(a) for a in artifacts],
            },
            job_id=job_id,
        )

    def input_url(self, job_id: str, attempt: int) -> str:
        """A fresh presigned URL for the job's streamed input (``POST /jobs/{id}/input-url``)."""
        response = self._post(
            self._job_path(job_id, "input-url"), {"attempt": attempt}, job_id=job_id
        )
        location = self._json(response, "an input-url request").get("location")
        if not isinstance(location, dict) or location.get("type") != "presigned_url":
            raise _malformed("an input-url request", "location is not a presigned_url location")
        url = location.get("url")
        if not isinstance(url, str) or not url:
            raise _malformed("an input-url request", "the location has no url")
        return url
