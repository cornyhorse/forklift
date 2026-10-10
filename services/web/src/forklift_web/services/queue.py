"""The worker side of the job queue (the internal API, ``/internal/v1``).

- :func:`lease` hands the oldest queued job of the worker's lanes and spec versions to one
  worker: ``SELECT ... FOR UPDATE SKIP LOCKED`` on PostgreSQL, so two workers never get the same
  job. It returns the rendered spec (presigned input URLs, connection strings).
- :func:`heartbeat` extends the lease, records progress and answers whether the job should stop.
- :func:`presign_outputs` signs PUT URLs for the attempt's outputs (``jobs/<id>/attempt-<n>/``).
- :func:`complete` records the result and the artifacts and, for a dataset with an s3
  destination, publishes the outputs (data first, ``manifest.json`` last).
- :func:`requeue_expired_leases` returns jobs whose lease ran out to the queue (or fails them
  after ``max_attempts``, or cancels them when cancellation was requested); every lease call
  runs it first, and so does ``forklift-web requeue_expired_leases``.

A worker holds a lease while the job is running with the attempt it was given and the token it
leased with; any other call about the job answers 409.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from django.db import transaction
from django.utils import timezone

from forklift_web import contracts, storage
from forklift_web.core.choices import (
    TERMINAL_STATUSES,
    ArtifactKind,
    ConnectionKind,
    EventType,
    JobStatus,
    Lane,
)
from forklift_web.core.models import Artifact, Job, JobEvent
from forklift_web.errors import Conflict, InvalidRequest, NotFound, StoreUnavailable
from forklift_web.services import connections, installation, specs, workers
from forklift_web.services.workers import WorkerPrincipal

logger = logging.getLogger(__name__)

MAX_RENDER_FAILURES = 20  # jobs failed in one lease call before giving up on this call
MAX_FILES = 100
MAX_RESULT_BYTES = 1024 * 1024
_NAME = re.compile(r"^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$")
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,199}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PROGRESS_KEYS = 20


@dataclass(frozen=True)
class Lease:
    job: Job
    spec: dict
    lease_seconds: int
    stage_max_bytes: int


@dataclass(frozen=True)
class HeartbeatReply:
    lease_seconds: int
    cancel: bool


def _event(job: Job, type_: str, **payload) -> None:
    JobEvent.objects.create(job=job, type=type_, attempt=job.attempt, payload=payload)


def _finish(job: Job, status: str, code: str = "", message: str = "") -> None:
    job.status = status
    job.error_code = code
    job.error_message = message
    job.finished_at = timezone.now()
    job.lease_expires_at = None
    job.save()
    _event(job, EventType.STATE, status=status, error_code=code or None)


# --------------------------------------------------------------------------- expiry


def requeue_expired_leases(now=None) -> dict:
    """Return expired leases to the queue; returns how many jobs were requeued, failed and
    cancelled."""
    now = now or timezone.now()
    counts = {"requeued": 0, "failed": 0, "cancelled": 0}
    with transaction.atomic():
        expired = Job.objects.select_for_update(skip_locked=True).filter(
            status=JobStatus.RUNNING, lease_expires_at__lt=now
        )
        for job in expired:
            worker = job.lease_worker.worker_id if job.lease_worker_id else "unknown"
            if job.cancel_requested_at is not None:
                _finish(
                    job,
                    JobStatus.CANCELLED,
                    "CANCELLED",
                    "Cancelled; the worker stopped sending heartbeats before it confirmed.",
                )
                counts["cancelled"] += 1
            elif job.attempt >= job.max_attempts:
                _finish(
                    job,
                    JobStatus.FAILED,
                    "LEASE_EXPIRED",
                    f"The lease of attempt {job.attempt} of {job.max_attempts} expired (worker "
                    f"{worker} stopped sending heartbeats); no attempts are left.",
                )
                counts["failed"] += 1
            else:
                job.status = JobStatus.QUEUED
                job.lease_expires_at = None
                job.save()
                _event(
                    job,
                    EventType.STATE,
                    status=JobStatus.QUEUED,
                    reason="lease_expired",
                    worker=worker,
                )
                counts["requeued"] += 1
    if any(counts.values()):
        logger.info("Expired leases handled", extra=counts)
    return counts


# --------------------------------------------------------------------------- lease


def _check_lease_request(worker_id: str, lanes: list, spec_versions: list) -> None:
    if not _WORKER_ID.match(worker_id or ""):
        raise InvalidRequest(
            "worker_id must be 1 to 200 letters, digits and . _ : @ -, starting with a letter "
            "or digit."
        )
    if not lanes or any(lane not in Lane.values for lane in lanes):
        raise InvalidRequest(f"lanes must name one or more of: {', '.join(Lane.values)}.")
    if not spec_versions or any(
        isinstance(version, bool) or not isinstance(version, int) for version in spec_versions
    ):
        raise InvalidRequest("spec_versions must list the contract versions the worker accepts.")


def lease(
    principal: WorkerPrincipal,
    *,
    worker_id: str,
    lanes: list,
    spec_versions: list,
    engine_version: str = "",
    worker_version: str = "",
) -> Optional[Lease]:
    """The next job for this worker, or None when there is nothing to do."""
    _check_lease_request(worker_id, lanes, spec_versions)
    worker = workers.seen(
        principal,
        worker_id=worker_id,
        lanes=sorted(set(lanes)),
        spec_versions=sorted(set(spec_versions)),
        engine_version=engine_version[:100],
        worker_version=worker_version[:100],
    )
    versions = [v for v in spec_versions if v in specs.SUPPORTED_SPEC_VERSIONS]
    if not versions:
        return None
    requeue_expired_leases()
    settings = installation.current()
    for _ in range(MAX_RENDER_FAILURES):
        with transaction.atomic():
            job = (
                Job.objects.select_for_update(skip_locked=True)
                .filter(status=JobStatus.QUEUED, lane__in=lanes, spec_version__in=versions)
                .order_by("created_at", "id")
                .first()
            )
            if job is None:
                return None
            now = timezone.now()
            job.status = JobStatus.RUNNING
            job.attempt += 1
            job.lease_token = principal.token
            job.lease_worker = worker
            job.lease_expires_at = now + timedelta(seconds=settings["lease_seconds"])
            job.started_at = job.started_at or now
            try:
                spec = specs.render(job, settings)
            except specs.SpecUnavailable as error:
                _finish(job, JobStatus.FAILED, error.code, error.message)
                logger.warning(
                    "Job failed while leasing", extra={"job_id": str(job.id), "code": error.code}
                )
                continue
            job.save()
            _event(job, EventType.STATE, status=JobStatus.RUNNING, worker=worker.worker_id)
            return Lease(
                job=job,
                spec=spec,
                lease_seconds=settings["lease_seconds"],
                stage_max_bytes=settings["stage_max_bytes"],
            )
    return None


# --------------------------------------------------------------------------- the leased job


def _leased(principal: WorkerPrincipal, job_id, attempt: int) -> Job:
    """The job, locked, if this worker holds its lease for ``attempt``; else Conflict."""
    job = Job.objects.select_for_update().filter(pk=job_id).first()
    if job is None:
        raise NotFound(f"There is no job with id {job_id}.")
    if (
        job.status != JobStatus.RUNNING
        or job.attempt != attempt
        or job.lease_token_id != principal.token.pk
    ):
        raise Conflict(
            f"The lease of job {job.id} attempt {attempt} is no longer this worker's (the job "
            f"is {job.status}, attempt {job.attempt}); stop working on it.",
            code="lease_lost",
        )
    return job


def _clean_progress(progress) -> dict:
    if not isinstance(progress, dict):
        raise InvalidRequest("progress must be an object of counters.")
    clean = {}
    for key, value in list(progress.items())[:_PROGRESS_KEYS]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidRequest(f"progress.{key} must be a number.")
        clean[str(key)[:64]] = value
    return clean


def heartbeat(principal: WorkerPrincipal, job_id, *, attempt: int, progress) -> HeartbeatReply:
    progress = _clean_progress(progress or {})
    lease_seconds = installation.get("lease_seconds")
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
        job.lease_expires_at = timezone.now() + timedelta(seconds=lease_seconds)
        if progress:
            job.progress = progress
            _event(job, EventType.PROGRESS, **progress)
        job.save(update_fields=["lease_expires_at", "progress"])
    return HeartbeatReply(lease_seconds=lease_seconds, cancel=job.cancel_requested_at is not None)


def _check_name(name) -> str:
    if (
        not isinstance(name, str)
        or len(name) > 512
        or not _NAME.match(name)
        or any(part in {".", ".."} for part in name.split("/"))
    ):
        raise InvalidRequest(
            f"Output name {name!r} must be a relative path of letters, digits, '.', '_' and '-' "
            "segments (no '.' or '..' segments), at most 512 characters."
        )
    return name


def presign_outputs(principal: WorkerPrincipal, job_id, *, attempt: int, files: list) -> list:
    """PUT URLs for the attempt's output files (signed for the worker's endpoint)."""
    if not files or len(files) > MAX_FILES:
        raise InvalidRequest(f"files must list 1 to {MAX_FILES} outputs.")
    settings = installation.current()
    for entry in files:
        _check_name(entry["name"])
        size = entry["bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise InvalidRequest(f"bytes of {entry['name']!r} must be a size in bytes.")
        if size > settings["output_max_bytes"]:
            raise InvalidRequest(
                f"{entry['name']!r} is {size} bytes; outputs may be at most "
                f"{settings['output_max_bytes']} bytes (installation setting output_max_bytes)."
            )
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
    bucket = storage.store()
    uploads = []
    for entry in files:
        key = job.attempt_prefix + entry["name"]
        uploads.append(
            {
                "name": entry["name"],
                "key": key,
                "url": bucket.presign_put(
                    key, expires=settings["output_url_seconds"], audience=storage.Audience.WORKER
                ),
                "method": "PUT",
                "headers": {},
            }
        )
    return uploads


def refresh_input(principal: WorkerPrincipal, job_id, *, attempt: int) -> dict:
    """A fresh input location for a streamed input whose URL is about to expire (ADR 0006)."""
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
    try:
        spec = specs.render(job, installation.current())
    except specs.SpecUnavailable as error:
        raise Conflict(error.message, code=error.code.lower()) from None
    return spec["input"]["location"]


def _check_result(job: Job, result) -> dict:
    if not isinstance(result, dict):
        raise InvalidRequest("result must be a JobResult object.")
    try:
        contracts.validate(contracts.JOBRESULT, result)
    except contracts.ContractViolation as error:
        raise InvalidRequest(str(error), code="result_invalid") from None
    if result["job_id"] != str(job.id):
        raise InvalidRequest(f"result.job_id must be {job.id}, the job being completed.")
    if len(json.dumps(result)) > MAX_RESULT_BYTES:
        raise InvalidRequest(f"The result is larger than {MAX_RESULT_BYTES} bytes.")
    return result


def _check_artifacts(job: Job, artifacts: list) -> list:
    if len(artifacts) > MAX_FILES:
        raise InvalidRequest(f"A job may report at most {MAX_FILES} artifacts.")
    checked, seen = [], set()
    bucket = storage.store()
    for entry in artifacts:
        name = _check_name(entry["name"])
        if entry["kind"] not in ArtifactKind.values:
            raise InvalidRequest(
                f"Artifact {name!r} has kind {entry['kind']!r}; kinds: "
                f"{', '.join(ArtifactKind.values)}."
            )
        key = job.attempt_prefix + name
        if entry["key"] != key or key in seen:
            raise InvalidRequest(
                f"Artifact {name!r} must be reported once, at key {key!r} (this attempt's "
                "prefix plus its name)."
            )
        seen.add(key)
        sha256 = entry.get("sha256") or ""
        if sha256 and not _SHA256.match(sha256):
            raise InvalidRequest(f"sha256 of artifact {name!r} must be 64 lower-case hex digits.")
        info = bucket.head(key)
        if info is None or info.size != entry["bytes"]:
            found = "nothing" if info is None else f"{info.size} bytes"
            raise InvalidRequest(
                f"Artifact {name!r} was reported with {entry['bytes']} bytes, but the store "
                f"has {found} at {key!r}; upload it before completing."
            )
        checked.append({**entry, "sha256": sha256})
    return checked


def complete(
    principal: WorkerPrincipal, job_id, *, attempt: int, result: dict, artifacts: list
) -> Job:
    """Record the attempt's result and artifacts. Repeating the same completion (a retry after
    a lost response) answers with the job as it is."""
    with transaction.atomic():
        job = Job.objects.select_for_update().filter(pk=job_id).first()
        if (
            job is not None
            and job.status in TERMINAL_STATUSES
            and job.attempt == attempt
            and job.lease_token_id == principal.token.pk
            and job.result is not None
        ):
            return job
        job = _leased(principal, job_id, attempt)
        result = _check_result(job, result)
        checked = _check_artifacts(job, artifacts)
        for entry in checked:
            Artifact.objects.create(
                job=job,
                attempt=attempt,
                kind=entry["kind"],
                name=entry["name"],
                key=entry["key"],
                bytes=entry["bytes"],
                rows=entry.get("rows"),
                sha256=entry["sha256"],
            )
        job.result = result
        error = result.get("error") or {}
        _finish(job, result["status"], error.get("code", ""), error.get("message", ""))
    if job.status == JobStatus.SUCCEEDED:
        job = _publish(job)
    return job


# --------------------------------------------------------------------------- publishing


def _publish(job: Job) -> Job:
    """Copy a successful run's outputs to its dataset's s3 destination, manifest last; returns
    the job as it is afterwards (failed with TARGET_WRITE_FAILED when the copy failed)."""
    dataset = job.dataset
    if dataset is None or dataset.destination_connection is None:
        return job
    destination = dataset.destination_connection
    if destination.kind != ConnectionKind.S3:
        return job
    target = connections.bucket_of(destination)
    base = "/".join(
        part for part in (target.prefix, dataset.destination_prefix, str(job.id)) if part
    )
    artifacts = sorted(
        job.artifacts.filter(attempt=job.attempt),
        key=lambda artifact: (artifact.kind == ArtifactKind.MANIFEST, artifact.name),
    )
    try:
        for artifact in artifacts:
            target.copy_from(storage.store(), artifact.key, f"{base}/{artifact.name}")
    except StoreUnavailable as error:
        with transaction.atomic():
            job = Job.objects.select_for_update().get(pk=job.pk)
            job.status = JobStatus.FAILED
            job.error_code = "TARGET_WRITE_FAILED"
            job.error_message = (
                f"The run succeeded but publishing to {destination.name!r} failed: "
                f"{error.message} The outputs are kept as job artifacts."
            )
            job.save()
            _event(job, EventType.STATE, status=JobStatus.FAILED, error_code=job.error_code)
        return job
    _event(job, EventType.LOG, published_to=destination.name, prefix=base, files=len(artifacts))
    return job
