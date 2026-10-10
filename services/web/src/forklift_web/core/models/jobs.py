"""Uploads, the job queue, job events, artifacts and workers."""

from __future__ import annotations

import uuid

from django.db import models
from django.db.models import Q
from django.utils import timezone

from forklift_web.core.choices import (
    ArtifactKind,
    Classification,
    EventType,
    JobKind,
    JobStatus,
    Lane,
    UploadStatus,
)
from forklift_web.core.models.accounts import ApiToken, User, WorkerToken
from forklift_web.core.models.catalog import Dataset, SchemaVersion
from forklift_web.core.models.fields import OrderedJSONField


class Upload(models.Model):
    """A file put into the store at ``key`` through a presigned PUT (or multipart upload).

    Created before the upload (``pending``), completed once a HEAD shows the object with the
    declared size. ``declared_sha256`` is what the client said; the gateway never reads the
    object, so it cannot check it (the worker can).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    key = models.CharField(max_length=1024, unique=True)
    filename = models.CharField(max_length=255)
    content_type = models.CharField(max_length=255, blank=True, default="")
    size = models.BigIntegerField()
    declared_sha256 = models.CharField(max_length=64, blank=True, default="")
    classification = models.CharField(
        max_length=16, choices=Classification.choices, default=Classification.INTERNAL
    )
    status = models.CharField(
        max_length=16, choices=UploadStatus.choices, default=UploadStatus.PENDING
    )
    uploaded_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="uploads"
    )
    created_at = models.DateTimeField(default=timezone.now)
    expires_at = models.DateTimeField()
    completed_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)
    etag = models.CharField(max_length=200, blank=True, default="")
    multipart_upload_id = models.CharField(max_length=1024, blank=True, default="")
    part_size = models.BigIntegerField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["status", "created_at"])]

    @property
    def is_multipart(self) -> bool:
        return bool(self.multipart_upload_id)

    def __str__(self) -> str:
        return f"{self.filename} ({self.id})"


class Worker(models.Model):
    """A worker process as it last described itself when leasing."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    worker_id = models.CharField(max_length=200, unique=True)
    token = models.ForeignKey(
        WorkerToken, on_delete=models.SET_NULL, null=True, blank=True, related_name="workers"
    )
    lanes = models.JSONField(default=list)
    spec_versions = models.JSONField(default=list)
    engine_version = models.CharField(max_length=100, blank=True, default="")
    worker_version = models.CharField(max_length=100, blank=True, default="")
    first_seen_at = models.DateTimeField(default=timezone.now)
    last_seen_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["worker_id"]

    def __str__(self) -> str:
        return self.worker_id


class Job(models.Model):
    """One unit of engine work and its state machine (design section 5.3).

    ``spec`` is the job's JobSpec with placeholders where the lease fills in what must not be
    stored: presigned URLs for inputs in the store and connection strings (with secrets) for
    SQL sources and targets. ``attempt`` counts leases; outputs of attempt *n* live under
    ``jobs/<id>/attempt-<n>/``. The lease is (``lease_token``, ``attempt``): a worker holds it
    while the job is running with that attempt and that token.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    kind = models.CharField(max_length=32, choices=JobKind.choices)
    lane = models.CharField(max_length=16, choices=Lane.choices)
    status = models.CharField(max_length=16, choices=JobStatus.choices, default=JobStatus.QUEUED)
    classification = models.CharField(max_length=16, choices=Classification.choices)
    spec_version = models.PositiveSmallIntegerField(default=1)
    spec = OrderedJSONField()  # holds the schema, whose key order is meaningful
    result = models.JSONField(null=True, blank=True)
    error_code = models.CharField(max_length=64, blank=True, default="")
    error_message = models.TextField(blank=True, default="")
    dataset = models.ForeignKey(
        Dataset, on_delete=models.PROTECT, null=True, blank=True, related_name="jobs"
    )
    upload = models.ForeignKey(
        Upload, on_delete=models.PROTECT, null=True, blank=True, related_name="jobs"
    )
    schema_version = models.ForeignKey(
        SchemaVersion, on_delete=models.PROTECT, null=True, blank=True, related_name="jobs"
    )
    requested_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="jobs"
    )
    requested_with_token = models.ForeignKey(
        ApiToken, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    # A job a schedule enqueued has no requester (the system asked); scheduled_for is the
    # slot it runs for and stays when the schedule is deleted.
    schedule = models.ForeignKey(
        "core.Schedule", on_delete=models.SET_NULL, null=True, blank=True, related_name="jobs"
    )
    scheduled_for = models.DateTimeField(null=True, blank=True)
    idempotency_key = models.CharField(max_length=255, blank=True, default="")
    request_fingerprint = models.CharField(max_length=64, blank=True, default="")
    attempt = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=3)
    lease_token = models.ForeignKey(
        WorkerToken, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    lease_worker = models.ForeignKey(
        Worker, on_delete=models.SET_NULL, null=True, blank=True, related_name="jobs"
    )
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    cancel_requested_at = models.DateTimeField(null=True, blank=True)
    cancel_requested_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    progress = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["requested_by", "idempotency_key"],
                condition=~Q(idempotency_key=""),
                name="job_idempotency_key",
            ),
            # Scheduled jobs have no requester, which the constraint above cannot see (NULLs
            # are distinct); their keys name the schedule and the slot, so one slot is one job.
            models.UniqueConstraint(
                fields=["idempotency_key"],
                condition=Q(schedule__isnull=False),
                name="job_schedule_slot",
            ),
        ]
        indexes = [
            models.Index(
                fields=["lane", "created_at"],
                condition=Q(status="queued"),
                name="job_queue",
            ),
            models.Index(
                fields=["lease_expires_at"],
                condition=Q(status="running"),
                name="job_running_leases",
            ),
            models.Index(fields=["status", "created_at"]),
        ]

    @property
    def attempt_prefix(self) -> str:
        return f"jobs/{self.id}/attempt-{self.attempt}/"

    def __str__(self) -> str:
        return f"{self.kind} job {self.id}"


class JobEvent(models.Model):
    """State changes, progress and log lines of a job; never cell values."""

    id = models.BigAutoField(primary_key=True)
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="events")
    created_at = models.DateTimeField(default=timezone.now)
    type = models.CharField(max_length=16, choices=EventType.choices)
    attempt = models.PositiveIntegerField(default=0)
    payload = models.JSONField(default=dict)

    class Meta:
        ordering = ["id"]
        indexes = [models.Index(fields=["job", "id"])]


class Artifact(models.Model):
    """A file a job attempt produced, in the store at ``key`` (``jobs/<job>/attempt-<n>/...``).

    Downloads go through the permission policy and short-lived presigned GETs, and are audited.
    ``deleted_at`` is set when retention removed the object; the record stays for history.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="artifacts")
    attempt = models.PositiveIntegerField()
    kind = models.CharField(max_length=16, choices=ArtifactKind.choices)
    name = models.CharField(max_length=512)
    key = models.CharField(max_length=1024, unique=True)
    bytes = models.BigIntegerField()
    rows = models.BigIntegerField(null=True, blank=True)
    sha256 = models.CharField(max_length=64, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["job", "name"]

    def __str__(self) -> str:
        return f"{self.kind} {self.name} of job {self.job_id}"
