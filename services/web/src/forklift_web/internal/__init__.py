"""The internal API, /internal/v1: what workers call (and nothing else can).

Served only on the internal port (forklift_web.wsgi routes by the port a connection arrived
on, and the public URL configuration does not include it) and authenticated only by worker
tokens (``Authorization: Bearer fkw_...``); API tokens and sessions are refused.

- ``POST /leases`` -> 200 with a job, or 204 when there is nothing to do
- ``POST /jobs/{id}/heartbeat`` -> lease extended, ``cancel`` flag; 409 if the lease is lost
- ``POST /jobs/{id}/presign`` -> PUT URLs under ``jobs/<id>/attempt-<n>/``
- ``POST /jobs/{id}/complete`` -> result and artifacts recorded
- ``POST /jobs/{id}/input-url`` -> a fresh presigned input URL for a streamed input
"""

import uuid
from typing import Any, Optional

from ninja import Field, NinjaAPI, Schema, Status
from ninja.errors import AuthenticationError
from ninja.security import HttpBearer

from forklift_web import __version__
from forklift_web.errors import ServiceError
from forklift_web.services import queue, workers


class WorkerTokenAuth(HttpBearer):
    def authenticate(self, request, token: str):
        return workers.authenticate_worker_token(
            token,
            request_id=getattr(request, "request_id", ""),
            ip=getattr(request, "client_ip", None),
        )


class ErrorOut(Schema):
    detail: str
    code: str


ERRORS = {frozenset({400, 401, 404, 409, 502}): ErrorOut}


class LeaseIn(Schema):
    worker_id: str = Field(description="Names this worker process (stable across leases)")
    lanes: list[str] = Field(description="interactive, batch and/or sql")
    spec_versions: list[int] = Field(description="Job contract versions the worker runs")
    engine_version: str = ""
    worker_version: str = ""


class LeaseOut(Schema):
    job_id: uuid.UUID
    attempt: int
    lease_seconds: int
    stage_max_bytes: int
    spec: dict[str, Any] = Field(
        description="The JobSpec: inputs in the store are presigned_url locations, outputs "
        "a file directory; sql locations carry their connection string"
    )


class HeartbeatIn(Schema):
    attempt: int
    progress: dict[str, Any] = Field(default_factory=dict)


class HeartbeatOut(Schema):
    lease_seconds: int
    cancel: bool


class OutputFile(Schema):
    name: str = Field(description="Path under the attempt's prefix, e.g. data.parquet")
    bytes: int


class PresignIn(Schema):
    attempt: int
    files: list[OutputFile]


class UploadUrl(Schema):
    name: str
    key: str
    url: str
    method: str
    headers: dict[str, str]


class PresignOut(Schema):
    uploads: list[UploadUrl]


class ReportedArtifact(Schema):
    kind: str
    name: str
    key: str
    bytes: int
    sha256: Optional[str] = None
    rows: Optional[int] = None


class CompleteIn(Schema):
    attempt: int
    result: dict[str, Any] = Field(description="The JobResult")
    artifacts: list[ReportedArtifact] = Field(default_factory=list)


class CompleteOut(Schema):
    job_id: uuid.UUID
    status: str


class InputUrlIn(Schema):
    attempt: int


class InputUrlOut(Schema):
    location: dict[str, Any]


internal_api = NinjaAPI(
    title="Forklift internal API",
    version="1",
    description=f"Worker endpoints of the forklift gateway (forklift-web {__version__}).",
    urls_namespace="internal-v1",
    auth=WorkerTokenAuth(),
    docs_url=None,
)


@internal_api.post("/leases", response={200: LeaseOut, 204: None, **ERRORS}, tags=["queue"])
def lease(request, payload: LeaseIn):
    leased = queue.lease(request.auth, **payload.model_dump())
    if leased is None:
        return Status(204, None)
    return {
        "job_id": leased.job.id,
        "attempt": leased.job.attempt,
        "lease_seconds": leased.lease_seconds,
        "stage_max_bytes": leased.stage_max_bytes,
        "spec": leased.spec,
    }


@internal_api.post(
    "/jobs/{job_id}/heartbeat", response={200: HeartbeatOut, **ERRORS}, tags=["queue"]
)
def heartbeat(request, job_id: uuid.UUID, payload: HeartbeatIn):
    return queue.heartbeat(
        request.auth, job_id, attempt=payload.attempt, progress=payload.progress
    )


@internal_api.post("/jobs/{job_id}/presign", response={200: PresignOut, **ERRORS}, tags=["queue"])
def presign(request, job_id: uuid.UUID, payload: PresignIn):
    files = [entry.model_dump() for entry in payload.files]
    return {
        "uploads": queue.presign_outputs(
            request.auth, job_id, attempt=payload.attempt, files=files
        )
    }


@internal_api.post(
    "/jobs/{job_id}/complete", response={200: CompleteOut, **ERRORS}, tags=["queue"]
)
def complete(request, job_id: uuid.UUID, payload: CompleteIn):
    job = queue.complete(
        request.auth,
        job_id,
        attempt=payload.attempt,
        result=payload.result,
        artifacts=[entry.model_dump() for entry in payload.artifacts],
    )
    return {"job_id": job.id, "status": job.status}


@internal_api.post(
    "/jobs/{job_id}/input-url", response={200: InputUrlOut, **ERRORS}, tags=["queue"]
)
def input_url(request, job_id: uuid.UUID, payload: InputUrlIn):
    return {"location": queue.refresh_input(request.auth, job_id, attempt=payload.attempt)}


@internal_api.exception_handler(ServiceError)
def _service_error(request, error: ServiceError):
    return internal_api.create_response(
        request, {"detail": error.message, "code": error.code}, status=error.status
    )


@internal_api.exception_handler(AuthenticationError)
def _not_authenticated(request, error: AuthenticationError):
    return internal_api.create_response(
        request,
        {
            "detail": "Send a worker token (Authorization: Bearer fkw_...); unknown, revoked "
            "and expired tokens and API tokens are refused.",
            "code": "not_authenticated",
        },
        status=401,
    )
