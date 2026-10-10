"""Jobs as people see them: enqueue (with idempotency keys), follow, cancel.

A job reads an upload or a dataset's source, applies a schema (a stored version, the dataset's,
or an inline draft) and writes artifacts. Its kind picks the lane: ``run`` goes to ``batch``,
``preview`` / ``validate_schema`` / ``generate_schema`` to ``interactive``, and anything that
reads or writes a database to ``sql``. Lane limits (an installation setting) can only be
lowered per job. The worker side of the queue is in :mod:`forklift_web.services.queue`.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional

from django.db import IntegrityError, transaction
from django.utils import timezone

from forklift_web.core.choices import (
    TERMINAL_STATUSES,
    Classification,
    ConnectionKind,
    EventType,
    InputFormat,
    JobKind,
    JobStatus,
    Lane,
)
from forklift_web.core.models import Dataset, Job, JobEvent
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, datasets, installation, schemas, specs, uploads

INTERACTIVE_KINDS = {JobKind.PREVIEW, JobKind.VALIDATE_SCHEMA, JobKind.GENERATE_SCHEMA}
NEEDS_SCHEMA = {JobKind.RUN, JobKind.VALIDATE_SCHEMA}
LIMIT_NAMES = ("max_input_bytes", "max_seconds", "max_rows")


@dataclass
class JobRequest:
    """What to run. Give ``dataset_id`` (its source, schema and destination), or ``upload_id``
    with a ``format``. The schema is the dataset's, ``schema_version_id``'s, or ``schema``
    (an inline draft; for a dataset, only for kinds other than ``run``)."""

    kind: str
    dataset_id: Optional[uuid.UUID] = None
    upload_id: Optional[uuid.UUID] = None
    format: Optional[str] = None
    input_options: dict = field(default_factory=dict)
    schema_version_id: Optional[uuid.UUID] = None
    schema: Optional[dict] = None
    compression: Optional[str] = None
    options: dict = field(default_factory=dict)
    limits: dict = field(default_factory=dict)
    classification: Optional[str] = None

    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(asdict(self), sort_keys=True, default=str).encode()
        ).hexdigest()


@dataclass
class _Plan:
    """A job request resolved into what the job record stores."""

    classification: str
    input_format: str
    input_location: dict
    input_size: Optional[int]
    input_options: dict
    schema: Optional[dict]
    schema_version: object
    output_location: Optional[dict]
    compression: str
    options: dict
    dataset: Optional[Dataset] = None
    upload: object = None


def _event(job: Job, type_: str, **payload) -> None:
    JobEvent.objects.create(job=job, type=type_, attempt=job.attempt, payload=payload)


def _schema(actor: Actor, request: JobRequest):
    if request.schema is not None and request.schema_version_id is not None:
        raise InvalidRequest(
            "Give either schema (an inline draft) or schema_version_id, not both."
        )
    if request.schema_version_id is not None:
        version = schemas.get_version_by_id(actor, request.schema_version_id)
        return version.document, version
    if request.schema is not None:
        schemas.check_document(request.schema)
        return request.schema, None
    return None, None


def _plan_dataset(actor: Actor, request: JobRequest) -> _Plan:
    dataset = datasets.get_dataset(actor, request.dataset_id)
    check(actor, Action.DATASET_RUN)
    if request.format is not None and request.format != dataset.input_format:
        raise InvalidRequest(
            f"Dataset {dataset.name!r} reads {dataset.input_format}; leave format out."
        )
    if request.classification is not None:
        raise InvalidRequest(
            f"Jobs of dataset {dataset.name!r} have its classification "
            f"({dataset.classification}); leave classification out."
        )
    schema, version = _schema(actor, request)
    if (schema is not None) and request.kind == JobKind.RUN:
        raise InvalidRequest(
            f"A run of dataset {dataset.name!r} uses the dataset's schema version; to run "
            "another schema, change the dataset (or validate the draft first)."
        )
    if schema is None and request.kind != JobKind.GENERATE_SCHEMA:
        schema, version = dataset.schema_version.document, dataset.schema_version
    source = dataset.source_connection
    upload = None
    if source is None:
        if request.upload_id is None:
            raise InvalidRequest(
                f"Dataset {dataset.name!r} reads uploads: give the upload_id of the file to run."
            )
        upload = uploads.usable_upload(actor, request.upload_id)
        location, size = {"type": specs.UPLOAD, "upload_id": str(upload.id)}, upload.size
    elif request.upload_id is not None:
        raise InvalidRequest(
            f"Dataset {dataset.name!r} reads from connection {source.name!r}; leave upload_id "
            "out."
        )
    elif source.kind == ConnectionKind.SQL:
        location, size = {"type": specs.SQL, "connection_id": str(source.id)}, None
    else:
        key = "/".join(part for part in (source.config.get("prefix"), dataset.source_path) if part)
        location, size = {"type": specs.OBJECT, "connection_id": str(source.id), "key": key}, None
    output = None
    destination = dataset.destination_connection
    # An s3 destination is published after a successful run (queue.complete); a table is
    # written by the engine, so only runs carry it in their spec.
    if (
        request.kind == JobKind.RUN
        and destination is not None
        and destination.kind == ConnectionKind.SQL
    ):
        output = {
            "type": specs.SQL_TABLE,
            "connection_id": str(destination.id),
            **dataset.destination_options,
        }
    classification = dataset.classification
    if upload is not None:
        classification = uploads.classification_for(upload, classification)
    return _Plan(
        classification=classification,
        input_format=dataset.input_format,
        input_location=location,
        input_size=size,
        input_options={**dataset.input_options, **request.input_options},
        schema=schema,
        schema_version=version,
        output_location=output,
        compression=request.compression or dataset.compression,
        options={**dataset.options, **request.options},
        dataset=dataset,
        upload=upload,
    )


def _plan_upload(actor: Actor, request: JobRequest, settings: dict) -> _Plan:
    if request.upload_id is None:
        raise InvalidRequest("A job needs an input: give upload_id (or dataset_id).")
    upload = uploads.usable_upload(actor, request.upload_id)
    fmt = request.format or InputFormat.CSV
    if fmt not in InputFormat.values or fmt == InputFormat.SQL:
        raise InvalidRequest(
            f"format {fmt!r} cannot be read from an upload (formats: csv, excel, fwf; sql "
            "inputs come from a dataset's sql connection)."
        )
    classification = request.classification or settings["default_classification"]
    if classification not in Classification.values:
        raise InvalidRequest(
            f"Unknown classification {classification!r}; classifications: "
            f"{', '.join(Classification.values)}."
        )
    schema, version = _schema(actor, request)
    return _Plan(
        classification=uploads.classification_for(upload, classification),
        input_format=fmt,
        input_location={"type": specs.UPLOAD, "upload_id": str(upload.id)},
        input_size=upload.size,
        input_options=request.input_options,
        schema=schema,
        schema_version=version,
        output_location=None,
        compression=request.compression or "snappy",
        options=request.options,
        upload=upload,
    )


def _lane(kind: str, plan: _Plan) -> str:
    if (
        plan.input_format == InputFormat.SQL
        or (plan.output_location or {}).get("type") == specs.SQL_TABLE
    ):
        return Lane.SQL
    return Lane.INTERACTIVE if kind in INTERACTIVE_KINDS else Lane.BATCH


def _limits(lane: str, requested: dict, settings: dict) -> dict:
    unknown = sorted(set(requested) - set(LIMIT_NAMES))
    if unknown:
        raise InvalidRequest(
            f"Unknown limits: {', '.join(unknown)} (limits: {', '.join(LIMIT_NAMES)})."
        )
    limits = dict(settings["lane_limits"][lane])
    for name, value in requested.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise InvalidRequest(f"limits.{name} must be a positive number.")
        ceiling = limits.get(name)
        limits[name] = value if ceiling is None else min(value, ceiling)
    return limits


def _check_input_size(plan: _Plan, limits: dict, settings: dict) -> None:
    if plan.input_size is None:
        return
    largest = limits.get("max_input_bytes")
    if largest is not None and plan.input_size > largest:
        raise InvalidRequest(
            f"The input is {plan.input_size} bytes; this job may read at most {largest} "
            "(limits.max_input_bytes)."
        )
    if plan.input_format != InputFormat.CSV and plan.input_size > settings["stage_max_bytes"]:
        raise InvalidRequest(
            f"The {plan.input_format} input is {plan.input_size} bytes. Only CSV inputs can be "
            "streamed to the engine; other formats are staged into the worker's scratch "
            f"directory, which takes inputs up to stage_max_bytes ({settings['stage_max_bytes']} "
            "bytes)."
        )


def create_job(actor: Actor, request: JobRequest, *, idempotency_key: str = "") -> tuple:
    """Enqueue a job; returns (job, created). With an ``idempotency_key`` already used by this
    user for the same request, returns the existing job and False; for a different request,
    raises Conflict."""
    if request.kind not in JobKind.values:
        raise InvalidRequest(
            f"Unknown job kind {request.kind!r}; kinds: {', '.join(JobKind.values)}."
        )
    check(actor, Action.JOB_PREVIEW if request.kind == JobKind.PREVIEW else Action.JOB_RUN)
    if len(idempotency_key) > 255:
        raise InvalidRequest("An Idempotency-Key is at most 255 characters.")
    fingerprint = request.fingerprint()
    if idempotency_key:
        existing = Job.objects.filter(requested_by=actor.user, idempotency_key=idempotency_key)
        replay = _replay(existing.first(), fingerprint, idempotency_key)
        if replay is not None:
            return replay, False
    settings = installation.current()
    plan = (
        _plan_dataset(actor, request)
        if request.dataset_id is not None
        else _plan_upload(actor, request, settings)
    )
    if request.kind == JobKind.PREVIEW:
        check(actor, Action.JOB_PREVIEW, plan.classification)
    if request.kind in NEEDS_SCHEMA and plan.schema is None:
        raise InvalidRequest(f"A {request.kind} job needs a schema (schema_version_id or schema).")
    if request.kind == JobKind.GENERATE_SCHEMA and plan.schema is not None:
        raise InvalidRequest("A generate_schema job infers the schema; give none.")
    lane = _lane(request.kind, plan)
    limits = _limits(lane, request.limits, settings)
    _check_input_size(plan, limits, settings)
    job = Job(
        kind=request.kind,
        lane=lane,
        classification=plan.classification,
        dataset=plan.dataset,
        upload=plan.upload,
        schema_version=plan.schema_version,
        requested_by=actor.user,
        requested_with_token=actor.token,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        max_attempts=settings["max_attempts"],
    )
    job.spec = specs.template(
        str(job.id),
        request.kind,
        input_format=plan.input_format,
        input_location=plan.input_location,
        input_options=plan.input_options,
        schema=plan.schema,
        output_location=plan.output_location,
        compression=plan.compression,
        options=plan.options,
        limits=limits,
    )
    try:
        specs.render(job, settings, dry_run=True)
    except specs.SpecUnavailable as error:
        raise InvalidRequest(error.message, code=error.code.lower()) from None
    try:
        with transaction.atomic():
            job.save()
            _event(job, EventType.STATE, status=JobStatus.QUEUED, lane=lane)
    except IntegrityError:
        # The same key was used concurrently; the first request won.
        existing = Job.objects.get(requested_by=actor.user, idempotency_key=idempotency_key)
        return _replay(existing, fingerprint, idempotency_key), False
    return job, True


def _replay(existing: Optional[Job], fingerprint: str, key: str) -> Optional[Job]:
    if existing is None:
        return None
    if existing.request_fingerprint != fingerprint:
        raise Conflict(
            f"The Idempotency-Key {key!r} was already used for a different request (job "
            f"{existing.id}); use a new key for a new request.",
            code="idempotency_key_reused",
        )
    return existing


def run_dataset(actor: Actor, dataset_id, *, upload_id=None, idempotency_key: str = "") -> tuple:
    """Run a dataset (``upload_id`` for datasets that read uploads); returns (job, created)."""
    return create_job(
        actor,
        JobRequest(kind=JobKind.RUN, dataset_id=dataset_id, upload_id=upload_id),
        idempotency_key=idempotency_key,
    )


def wait_for(job: Job, seconds: float, *, poll: float = 0.25) -> Job:
    """Wait up to ``seconds`` for ``job`` to finish; returns it as it is then."""
    deadline = time.monotonic() + seconds
    while job.status not in TERMINAL_STATUSES and time.monotonic() < deadline:
        _sleep(poll)
        job.refresh_from_db()
    return job


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def validate_schema(
    actor: Actor,
    *,
    upload_id=None,
    dataset_id=None,
    schema: Optional[dict] = None,
    schema_version_id=None,
    format: Optional[str] = None,
    input_options: Optional[dict] = None,
    options: Optional[dict] = None,
    wait_seconds: Optional[float] = None,
) -> tuple:
    """Enqueue an interactive validate_schema job and wait briefly for its result; returns
    (job, finished). The installation setting validate_wait_seconds caps the wait."""
    check(actor, Action.SCHEMA_VALIDATE)
    job, _ = create_job(
        actor,
        JobRequest(
            kind=JobKind.VALIDATE_SCHEMA,
            upload_id=upload_id,
            dataset_id=dataset_id,
            schema=schema,
            schema_version_id=schema_version_id,
            format=format,
            input_options=input_options or {},
            options=options or {},
        ),
    )
    limit = installation.get("validate_wait_seconds")
    job = wait_for(job, limit if wait_seconds is None else min(wait_seconds, limit))
    return job, job.status in TERMINAL_STATUSES


def list_jobs(
    actor: Actor,
    *,
    status: Optional[str] = None,
    kind: Optional[str] = None,
    dataset_id=None,
    mine: bool = False,
):
    check(actor, Action.JOB_VIEW)
    found = Job.objects.select_related("dataset", "requested_by", "lease_worker")
    if status:
        found = found.filter(status=status)
    if kind:
        found = found.filter(kind=kind)
    if dataset_id is not None:
        found = found.filter(dataset_id=dataset_id)
    if mine:
        found = found.filter(requested_by=actor.user)
    return found


def get_job(actor: Actor, job_id) -> Job:
    job = list_jobs(actor).filter(pk=job_id).first()
    if job is None:
        raise NotFound(f"There is no job with id {job_id}.")
    return job


def list_events(actor: Actor, job_id, *, after: Optional[int] = None):
    """The job's events in order; ``after`` (an event id) returns only newer ones."""
    job = get_job(actor, job_id)
    events = job.events.all()
    return events.filter(id__gt=after) if after is not None else events


def cancel_job(actor: Actor, job_id) -> Job:
    """Cancel a queued job at once; ask a running one to stop (its worker hears it on the next
    heartbeat and reports the job cancelled)."""
    check(actor, Action.JOB_CANCEL)
    with transaction.atomic():
        job = Job.objects.select_for_update().filter(pk=job_id).first()
        if job is None:
            raise NotFound(f"There is no job with id {job_id}.")
        check(actor, Action.JOB_CANCEL, job)
        if job.status in TERMINAL_STATUSES:
            raise Conflict(f"Job {job.id} already finished ({job.status}).")
        now = timezone.now()
        if job.cancel_requested_at is None:
            job.cancel_requested_at = now
            job.cancel_requested_by = actor.user
        if job.status == JobStatus.QUEUED:
            job.status = JobStatus.CANCELLED
            job.finished_at = now
            job.error_code = "CANCELLED"
            job.error_message = f"Cancelled by {actor.label} before a worker started it."
            _event(job, EventType.STATE, status=JobStatus.CANCELLED)
        else:
            _event(job, EventType.STATE, status=job.status, cancel_requested=True)
        job.save()
        audit.record(actor, "job.cancel", job, {"status": job.status})
    return job
