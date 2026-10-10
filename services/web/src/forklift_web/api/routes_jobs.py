"""/api/v1/uploads, /api/v1/jobs and /api/v1/artifacts."""

import uuid
from typing import Optional

from django.http import HttpResponseRedirect
from ninja import Header, Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import (
    ArtifactOut,
    DownloadOut,
    JobDetailOut,
    JobEventOut,
    JobIn,
    JobOut,
    PartsIn,
    PartUrlOut,
    UploadCompleteIn,
    UploadIn,
    UploadOut,
    UploadTicketOut,
)
from forklift_web.services import artifacts, jobs, retention, uploads

uploads_router = Router(tags=["uploads"])
jobs_router = Router(tags=["jobs"])
artifacts_router = Router(tags=["artifacts"])


# --------------------------------------------------------------------------- uploads


@uploads_router.post(
    "",
    response=responses({201: UploadTicketOut}),
    summary="Start an upload: a presigned PUT, or a multipart upload's part URLs",
)
def create_upload(request, payload: UploadIn):
    return Status(201, uploads.create_upload(request.auth, **payload.model_dump()))


@uploads_router.get("", response=responses({200: list[UploadOut]}), summary="My uploads")
@paginate
def list_uploads(request):
    return uploads.list_uploads(request.auth)


@uploads_router.get("/{upload_id}", response=responses({200: UploadOut}), summary="An upload")
def get_upload(request, upload_id: uuid.UUID):
    return uploads.get_upload(request.auth, upload_id)


@uploads_router.post(
    "/{upload_id}/complete",
    response=responses({200: UploadOut}),
    summary="Complete an upload (checked with a HEAD of the object)",
)
def complete_upload(request, upload_id: uuid.UUID, payload: Optional[UploadCompleteIn] = None):
    parts = payload.model_dump()["parts"] if payload is not None else None
    return uploads.complete_upload(request.auth, upload_id, parts=parts)


@uploads_router.post(
    "/{upload_id}/parts",
    response=responses({200: list[PartUrlOut]}),
    summary="Fresh URLs for parts of a multipart upload",
)
def part_urls(request, upload_id: uuid.UUID, payload: PartsIn):
    return uploads.part_urls(request.auth, upload_id, payload.part_numbers)


@uploads_router.delete(
    "/{upload_id}", response=responses({204: None}), summary="Delete an upload's object"
)
def delete_upload(request, upload_id: uuid.UUID):
    uploads.delete_upload(request.auth, upload_id)
    return Status(204, None)


# --------------------------------------------------------------------------- jobs


@jobs_router.post(
    "",
    response=responses({200: JobDetailOut, 201: JobDetailOut}),
    summary="Enqueue a job (201; 200 when the Idempotency-Key was already used)",
)
def create_job(
    request, payload: JobIn, idempotency_key: str = Header("", alias="Idempotency-Key")
):
    fields = payload.model_dump()
    fields["schema"] = fields.pop("schema_")
    job, created = jobs.create_job(
        request.auth, jobs.JobRequest(**fields), idempotency_key=idempotency_key
    )
    return Status(201 if created else 200, job)


@jobs_router.get("", response=responses({200: list[JobOut]}), summary="Jobs, newest first")
@paginate
def list_jobs(
    request,
    status: Optional[str] = None,
    kind: Optional[str] = None,
    dataset_id: Optional[uuid.UUID] = None,
    mine: bool = False,
):
    return jobs.list_jobs(request.auth, status=status, kind=kind, dataset_id=dataset_id, mine=mine)


@jobs_router.get("/{job_id}", response=responses({200: JobDetailOut}), summary="A job")
def get_job(request, job_id: uuid.UUID):
    return jobs.get_job(request.auth, job_id)


@jobs_router.get(
    "/{job_id}/events",
    response=responses({200: list[JobEventOut]}),
    summary="A job's events (after: only events with a larger id)",
)
@paginate
def list_events(request, job_id: uuid.UUID, after: Optional[int] = None):
    return jobs.list_events(request.auth, job_id, after=after)


@jobs_router.post(
    "/{job_id}/cancel",
    response=responses({200: JobDetailOut}),
    summary="Cancel a job (a running one stops at its next heartbeat)",
)
def cancel_job(request, job_id: uuid.UUID):
    return jobs.cancel_job(request.auth, job_id)


@jobs_router.get(
    "/{job_id}/artifacts", response=responses({200: list[ArtifactOut]}), summary="A job's files"
)
def list_artifacts(request, job_id: uuid.UUID):
    found = list(artifacts.list_artifacts(request.auth, job_id))
    policies = retention.Policies.load()
    for artifact in found:
        artifact.expires_at = retention.artifact_expires_at(artifact, policies)
    return found


# --------------------------------------------------------------------------- artifacts


@artifacts_router.get("/{artifact_id}", response=responses({200: ArtifactOut}), summary="A file")
def get_artifact(request, artifact_id: uuid.UUID):
    artifact = artifacts.get_artifact(request.auth, artifact_id)
    artifact.expires_at = retention.artifact_expires_at(artifact)
    return artifact


@artifacts_router.get(
    "/{artifact_id}/download",
    response=responses({200: DownloadOut, 302: None}),
    summary="A presigned download URL (audited); redirect=true answers with a redirect to it",
)
def download_artifact(request, artifact_id: uuid.UUID, redirect: bool = False):
    download = artifacts.download(request.auth, artifact_id)
    if redirect:
        return HttpResponseRedirect(download.url)
    return download
