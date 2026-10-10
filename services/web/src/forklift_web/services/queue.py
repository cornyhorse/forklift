"""The worker side of the job queue (the internal API, ``/internal/v1``).

- :func:`lease` hands the oldest queued job of the worker's lanes and spec versions to one
  worker: ``SELECT ... FOR UPDATE SKIP LOCKED`` on PostgreSQL, so two workers never get the same
  job. It returns the rendered spec (presigned input URLs, connection strings).
- :func:`heartbeat` extends the lease, records progress and answers whether the job should stop.
- :func:`refresh_input` signs a streamed input again when the engine's URL is about to expire
  or was refused.
- :func:`presign_outputs` signs PUT URLs for the attempt's outputs (``jobs/<id>/attempt-<n>/``),
  or starts a multipart upload for an output above ``multipart_threshold_bytes``;
  :func:`part_urls` signs URLs for more of its parts, or fresh ones.
- :func:`complete` records the result and the artifacts (completing multipart uploads once every
  check passed) and, for a dataset with an s3 destination, publishes the outputs (data first,
  ``manifest.json`` last); a publish that fails for any reason fails the job.
- :func:`requeue_expired_leases` returns jobs whose lease ran out to the queue (or fails them
  after ``max_attempts``, or cancels them when cancellation was requested); every lease call
  runs it first, and so does ``forklift-web requeue_expired_leases``.

When an attempt ends (completed with any status, or its lease expired), the multipart uploads
still pending under its prefix are aborted; workers never abort their own.

A worker holds a lease while the job is running with the attempt it was given and the token it
leased with; any other call about the job answers 409.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from django.db import transaction
from django.utils import timezone

from forklift_web import contracts, secret_backend, storage
from forklift_web.core.choices import (
    TERMINAL_STATUSES,
    ArtifactKind,
    ConnectionKind,
    EventType,
    InputFormat,
    JobStatus,
    Lane,
)
from forklift_web.core.models import Artifact, Job, JobEvent
from forklift_web.errors import Conflict, Gone, InvalidRequest, NotFound, StoreUnavailable
from forklift_web.services import connections, installation, specs, uploads, webhooks, workers
from forklift_web.services.workers import WorkerPrincipal

logger = logging.getLogger(__name__)

MAX_RENDER_FAILURES = 20  # jobs failed in one lease call before giving up on this call
MAX_FILES = 100
MAX_RESULT_BYTES = 1024 * 1024
_NAME = re.compile(r"^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$")
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,199}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PROGRESS_KEYS = 20
MAX_SINGLE_PUT_BYTES = 5 * installation.GIB  # S3's limit for one PUT
FIRST_PARTS = 100  # part URLs per multipart output in a presign answer; POST /parts gives more
_UPLOAD_ID_MAX = 1024


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


def _finish(
    job: Job, status: str, code: str = "", message: str = "", *, notify: bool = True
) -> None:
    job.status = status
    job.error_code = code
    job.error_message = message
    job.finished_at = timezone.now()
    job.lease_expires_at = None
    job.save()
    _event(job, EventType.STATE, status=status, error_code=code or None)
    if notify:
        webhooks.job_finished(job)


# --------------------------------------------------------------------------- expiry


def requeue_expired_leases(now=None) -> dict:
    """Return expired leases to the queue; returns how many jobs were requeued, failed and
    cancelled."""
    now = now or timezone.now()
    counts = {"requeued": 0, "failed": 0, "cancelled": 0}
    ended = []
    with transaction.atomic():
        expired = Job.objects.select_for_update(skip_locked=True).filter(
            status=JobStatus.RUNNING, lease_expires_at__lt=now
        )
        for job in expired:
            ended.append(job.attempt_prefix)
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
    _abort_pending_outputs(ended)
    if any(counts.values()):
        logger.info("Expired leases handled", extra=counts)
    return counts


def _abort_pending_outputs(prefixes: list) -> None:
    """Abort the multipart uploads still pending under the prefixes of attempts that ended.
    Best effort: the retention sweeper aborts what this misses."""
    bucket = storage.store()
    for prefix in prefixes:
        try:
            for pending in bucket.list_multipart_uploads(prefix):
                bucket.abort_multipart(pending.key, pending.upload_id)
        except StoreUnavailable as error:
            logger.warning(
                "Pending output uploads were not aborted; the sweeper will abort them",
                extra={"prefix": prefix, "reason": error.message},
            )


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


def _check_output(entry: dict, settings: dict, multipart: bool) -> None:
    name = _check_name(entry["name"])
    size = entry["bytes"]
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise InvalidRequest(f"bytes of {name!r} must be a size in bytes.")
    if size > settings["output_max_bytes"]:
        raise InvalidRequest(
            f"{name!r} is {size} bytes; outputs may be at most "
            f"{settings['output_max_bytes']} bytes (installation setting output_max_bytes)."
        )
    if size > MAX_SINGLE_PUT_BYTES and not multipart:
        raise InvalidRequest(
            f"{name!r} is {size} bytes; one PUT holds at most {MAX_SINGLE_PUT_BYTES} bytes, and "
            "this worker does not upload in parts (multipart: true): upgrade it."
        )


def _part_urls(bucket: storage.Bucket, key: str, upload_id: str, numbers, expires: int) -> list:
    return [
        {
            "part_number": number,
            "url": bucket.presign_part(
                key, upload_id, number, expires=expires, audience=storage.Audience.WORKER
            ),
        }
        for number in numbers
    ]


def _output_upload(
    bucket: storage.Bucket, key: str, size: int, settings: dict, multipart: bool
) -> dict:
    """How one output goes up: a PUT URL, or a multipart upload and its first part URLs."""
    expires = settings["output_url_seconds"]
    upload = {"key": key, "method": "PUT", "headers": {}, "expires_in": expires}
    if not multipart or size <= settings["multipart_threshold_bytes"]:
        url = bucket.presign_put(key, expires=expires, audience=storage.Audience.WORKER)
        return {**upload, "url": url}
    part_size = uploads.part_size(size, settings)
    count = math.ceil(size / part_size)
    upload_id = bucket.create_multipart(key)
    first = range(1, min(count, FIRST_PARTS) + 1)
    return {
        **upload,
        "url": None,
        "upload_id": upload_id,
        "part_size": part_size,
        "part_count": count,
        "parts": _part_urls(bucket, key, upload_id, first, expires),
    }


def presign_outputs(
    principal: WorkerPrincipal, job_id, *, attempt: int, files: list, multipart: bool = False
) -> list:
    """How to upload the attempt's output files (signed for the worker's endpoint): a PUT URL
    each, or, for a file above multipart_threshold_bytes when the worker uploads in parts
    (``multipart``), a multipart upload with the URLs of its first parts."""
    if not files or len(files) > MAX_FILES:
        raise InvalidRequest(f"files must list 1 to {MAX_FILES} outputs.")
    settings = installation.current()
    for entry in files:
        _check_output(entry, settings, multipart)
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
    bucket = storage.store()
    return [
        {
            "name": entry["name"],
            **_output_upload(
                bucket, job.attempt_prefix + entry["name"], entry["bytes"], settings, multipart
            ),
        }
        for entry in files
    ]


def _check_upload_id(upload_id) -> str:
    if not isinstance(upload_id, str) or not upload_id or len(upload_id) > _UPLOAD_ID_MAX:
        raise InvalidRequest(
            f"upload_id must be the id presign answered with (1 to {_UPLOAD_ID_MAX} characters)."
        )
    return upload_id


def part_urls(
    principal: WorkerPrincipal,
    job_id,
    *,
    attempt: int,
    name: str,
    upload_id: str,
    part_numbers: list,
) -> dict:
    """Fresh URLs for parts of an output's multipart upload: parts presign gave no URL for, or
    ones whose URLs expired (at most PARTS_PER_RESPONSE at a time). For an upload id that is not
    pending at the output's key, the store refuses the URLs."""
    _check_name(name)
    _check_upload_id(upload_id)
    numbers = sorted(set(part_numbers))
    if (
        not numbers
        or len(numbers) > uploads.PARTS_PER_RESPONSE
        or numbers[0] < 1
        or numbers[-1] > uploads.MAX_PARTS
    ):
        raise InvalidRequest(
            f"part_numbers must name 1 to {uploads.PARTS_PER_RESPONSE} parts between 1 and "
            f"{uploads.MAX_PARTS}."
        )
    expires = installation.get("output_url_seconds")
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
    key = job.attempt_prefix + name
    return {
        "parts": _part_urls(storage.store(), key, upload_id, numbers, expires),
        "expires_in": expires,
    }


def refresh_input(principal: WorkerPrincipal, job_id, *, attempt: int) -> dict:
    """A fresh location for a streamed input whose URL is about to expire or was refused
    (ADR 0006): the input signed again, for as long as at lease time. Only the input is
    rendered, so an output connection that cannot be used now does not stand in the way.

    Only CSV inputs in a store can be streamed: for any other input there is no URL to
    refresh (400). An input that is gone answers 410, not 409, which workers read as a lost
    lease.
    """
    with transaction.atomic():
        job = _leased(principal, job_id, attempt)
    spec_input = job.spec["input"]
    if (
        spec_input["location"].get("type") not in specs.STORE_INPUTS
        or spec_input["format"] != InputFormat.CSV
    ):
        raise InvalidRequest(
            f"The input of job {job.id} is not streamed (only CSV inputs in a store are), so it "
            "has no URL to refresh.",
            code="not_streamed",
        )
    try:
        return specs.input_location(job, installation.current())
    except specs.SpecUnavailable as error:
        raise Gone(error.message, code=error.code.lower()) from None


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


def parts_sha256(etags) -> str:
    """What a worker reports for the parts of a multipart artifact instead of listing them: the
    sha256 (hex) of the parts' ETags, without quotes, in part order, each followed by a newline.
    The gateway computes it again from the parts the store holds."""
    digest = hashlib.sha256()
    for etag in etags:
        digest.update(etag.strip('"').encode("utf-8") + b"\n")
    return digest.hexdigest()


def _check_reported_parts(name: str, entry: dict) -> None:
    upload_id, count, digest = (
        entry.get(key) for key in ("upload_id", "part_count", "parts_sha256")
    )
    if upload_id is None and count is None and digest is None:
        return
    if (
        not isinstance(upload_id, str)
        or not 0 < len(upload_id) <= _UPLOAD_ID_MAX
        or isinstance(count, bool)
        or not isinstance(count, int)
        or not 0 < count <= uploads.MAX_PARTS
        or not isinstance(digest, str)
        or not _SHA256.match(digest)
    ):
        raise InvalidRequest(
            f"Artifact {name!r} was uploaded in parts: report its upload_id, its part_count (1 to "
            f"{uploads.MAX_PARTS}) and parts_sha256 (the sha256 of its parts' ETags)."
        )


def _verified_parts(bucket: storage.Bucket, entry: dict) -> Optional[list]:
    """The parts to complete a multipart artifact with, once the store's match the report: parts
    1 to part_count all there, their ETags giving parts_sha256, all but the last of one size, the
    last no larger, together the artifact's size. None when the upload is no longer pending
    because an earlier try of this completion completed it (its object is checked instead)."""
    name, count = entry["name"], entry["part_count"]
    stored = bucket.list_parts(entry["key"], entry["upload_id"])
    if stored is None:
        return None
    held = {part.number: part for part in stored}
    missing = [number for number in range(1, count + 1) if number not in held]
    if missing:
        raise InvalidRequest(
            f"Part {missing[0]} of artifact {name!r} is not in its multipart upload; upload every "
            "part before completing."
        )
    parts = [held[number] for number in range(1, count + 1)]
    if parts_sha256(part.etag for part in parts) != entry["parts_sha256"]:
        raise InvalidRequest(
            f"The parts the store holds for artifact {name!r} are not the ones reported: their "
            "ETags do not give its parts_sha256 (a part was uploaded again, or reported wrong)."
        )
    sizes = [part.size for part in parts]
    if sum(sizes) != entry["bytes"]:
        raise InvalidRequest(
            f"The {count} parts of artifact {name!r} hold {sum(sizes)} bytes in all, but it was "
            f"reported with {entry['bytes']} bytes; upload every part before completing."
        )
    if any(size != sizes[0] for size in sizes[:-1]) or sizes[-1] > sizes[0]:
        raise InvalidRequest(
            f"The parts of artifact {name!r} are not cut evenly: every part but the last must "
            "have the part size presign gave, and the last one no more."
        )
    return [{"part_number": part.number, "etag": part.etag} for part in parts]


def _check_size(bucket: storage.Bucket, entry: dict) -> None:
    info = bucket.head(entry["key"])
    if info is None or info.size != entry["bytes"]:
        found = "nothing" if info is None else f"{info.size} bytes"
        raise InvalidRequest(
            f"Artifact {entry['name']!r} was reported with {entry['bytes']} bytes, but the "
            f"store has {found} at {entry['key']!r}; upload it before completing."
        )


def _check_artifacts(job: Job, artifacts: list) -> tuple:
    """The reported artifacts, checked, and the multipart uploads to complete for them ((entry,
    parts) pairs). Every check that can refuse the report runs here, before any upload is
    completed: names, kinds and keys, the parts of each pending multipart upload against the
    store's, and the size of every other object (with a HEAD)."""
    if len(artifacts) > MAX_FILES:
        raise InvalidRequest(f"A job may report at most {MAX_FILES} artifacts.")
    checked, seen = [], set()
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
        _check_reported_parts(name, entry)
        checked.append({**entry, "sha256": sha256})
    bucket = storage.store()
    pending = []
    for entry in checked:
        parts = None if entry.get("upload_id") is None else _verified_parts(bucket, entry)
        if parts is None:
            _check_size(bucket, entry)
        else:
            pending.append((entry, parts))
    return checked, pending


def _complete_uploads(pending: list, completed: list) -> None:
    """Complete the checked multipart uploads (adding each key to ``completed`` once its object
    exists), then check the size of every object they made."""
    bucket = storage.store()
    for entry, parts in pending:
        bucket.complete_multipart(entry["key"], entry["upload_id"], parts)
        completed.append(entry["key"])
    for entry, _ in pending:
        _check_size(bucket, entry)


def _remove(keys: list) -> None:
    """Delete objects a refused completion made: no artifact records them, so nothing else
    would. Best effort."""
    bucket = storage.store()
    for key in keys:
        try:
            bucket.delete(key)
        except StoreUnavailable as error:
            logger.warning(
                "An object of a refused completion was not deleted",
                extra={"key": key, "reason": error.message},
            )


def complete(
    principal: WorkerPrincipal, job_id, *, attempt: int, result: dict, artifacts: list
) -> Job:
    """Record the attempt's result and artifacts, then publish a successful run to its s3
    destination. Repeating the same completion (a retry after a lost response) answers with the
    job as it is; it does not resume a publish the gateway stopped in the middle of, which
    leaves the job succeeded but not (wholly) published."""
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
        checked, pending = _check_artifacts(job, artifacts)
        completed: list = []
        try:
            _complete_uploads(pending, completed)
            destination = _record(job, attempt, result, checked)
        except StoreUnavailable:
            raise  # the worker tries again; its retry finds these uploads completed
        except Exception:
            _remove(completed)
            raise
    _abort_pending_outputs([job.attempt_prefix])
    if destination is not None:
        job = _publish(job, destination)
    return job


def _record(job: Job, attempt: int, result: dict, checked: list):
    """Record the artifacts and finish the job; returns the s3 destination to publish to, if
    any (a run that is published has finished, for its webhooks, once publishing has)."""
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
    destination = _s3_destination(job) if result["status"] == JobStatus.SUCCEEDED else None
    _finish(
        job,
        result["status"],
        error.get("code", ""),
        error.get("message", ""),
        notify=destination is None,
    )
    return destination


# --------------------------------------------------------------------------- publishing


def _s3_destination(job: Job):
    """The s3 connection a successful run of ``job`` is published to (None: nothing is)."""
    destination = job.dataset.destination_connection if job.dataset is not None else None
    if destination is None or destination.kind != ConnectionKind.S3:
        return None
    return destination


def _publish(job: Job, destination) -> Job:
    """Copy a successful run's outputs to its dataset's s3 ``destination``, manifest last;
    returns the job as it is afterwards. Whatever stops the copy (the store, the destination's
    secrets, anything else) fails the job with TARGET_WRITE_FAILED and keeps the artifacts.
    Either way, its webhooks hear about the job now."""
    artifacts = sorted(
        job.artifacts.filter(attempt=job.attempt),
        key=lambda artifact: (artifact.kind == ArtifactKind.MANIFEST, artifact.name),
    )
    try:
        target = connections.bucket_of(destination)
        base = "/".join(
            part for part in (target.prefix, job.dataset.destination_prefix, str(job.id)) if part
        )
        for artifact in artifacts:
            target.copy_from(storage.store(), artifact.key, f"{base}/{artifact.name}")
    except Exception as error:  # the job must end failed and announced, whatever this was
        return _publish_failed(job, destination, error)
    with transaction.atomic():
        _event(
            job, EventType.LOG, published_to=destination.name, prefix=base, files=len(artifacts)
        )
        webhooks.job_finished(job)
    return job


def _publish_failed(job: Job, destination, error: Exception) -> Job:
    if isinstance(error, StoreUnavailable):
        reason = error.message
    elif isinstance(error, secret_backend.SecretError):
        reason = str(error)
    else:  # its text could hold anything: only its type is passed on
        reason = f"An unexpected {type(error).__name__} stopped it."
    logger.warning(
        "Publishing a run failed",
        extra={
            "job_id": str(job.id),
            "connection_id": str(destination.pk),
            "error": type(error).__name__,
        },
    )
    with transaction.atomic():
        job = Job.objects.select_for_update().get(pk=job.pk)
        job.status = JobStatus.FAILED
        job.error_code = "TARGET_WRITE_FAILED"
        job.error_message = (
            f"The run succeeded but publishing to {destination.name!r} failed: {reason} The "
            "outputs are kept as job artifacts."
        )
        job.save()
        _event(job, EventType.STATE, status=JobStatus.FAILED, error_code=job.error_code)
        webhooks.job_finished(job)
    return job
