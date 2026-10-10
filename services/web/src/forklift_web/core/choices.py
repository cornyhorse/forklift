"""The fixed vocabularies of the data model (roles, classifications, job kinds, ...)."""

from __future__ import annotations

from django.db import models


class Role(models.TextChoices):
    """Design section 5.2; each role can do everything the one before it can."""

    VIEWER = "viewer", "Viewer"
    OPERATOR = "operator", "Operator"
    AUTHOR = "author", "Author"
    ADMIN = "admin", "Admin"


ROLE_ORDER = [Role.VIEWER, Role.OPERATOR, Role.AUTHOR, Role.ADMIN]


class Classification(models.TextChoices):
    PUBLIC = "public", "Public"
    INTERNAL = "internal", "Internal"
    SENSITIVE = "sensitive", "Sensitive"


CLASSIFICATION_ORDER = [Classification.PUBLIC, Classification.INTERNAL, Classification.SENSITIVE]


def most_restrictive(*classifications: str) -> str:
    """The most restrictive of the given classifications (public < internal < sensitive)."""
    return max(classifications, key=CLASSIFICATION_ORDER.index)


class ConnectionKind(models.TextChoices):
    S3 = "s3", "S3-compatible store"
    LOCALFS = "localfs", "Local file system"
    SQL = "sql", "SQL database"


class SqlDialect(models.TextChoices):
    POSTGRESQL = "postgresql", "PostgreSQL"
    MYSQL = "mysql", "MySQL"
    SQLSERVER = "sqlserver", "SQL Server"
    ORACLE = "oracle", "Oracle"


class InputFormat(models.TextChoices):
    CSV = "csv", "CSV"
    EXCEL = "excel", "Excel"
    FWF = "fwf", "Fixed width"
    SQL = "sql", "SQL"


class JobKind(models.TextChoices):
    RUN = "run", "Run"
    PREVIEW = "preview", "Preview"
    VALIDATE_SCHEMA = "validate_schema", "Validate schema"
    GENERATE_SCHEMA = "generate_schema", "Generate schema"


class Lane(models.TextChoices):
    INTERACTIVE = "interactive", "Interactive"
    BATCH = "batch", "Batch"
    SQL = "sql", "SQL"


class JobStatus(models.TextChoices):
    QUEUED = "queued", "Queued"
    RUNNING = "running", "Running"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"


TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


class EventType(models.TextChoices):
    STATE = "state", "State change"
    PROGRESS = "progress", "Progress"
    LOG = "log", "Log"


class ArtifactKind(models.TextChoices):
    DATA = "data", "Data"
    BAD_ROWS = "bad_rows", "Bad rows"
    MANIFEST = "manifest", "Manifest"
    METADATA = "metadata", "Metadata"
    PREVIEW = "preview", "Preview"
    SCHEMA = "schema", "Schema"
    REPORT = "report", "Report"


# Artifacts that contain rows of the input: on `sensitive` data they need "view raw rows".
RAW_ROW_KINDS = {ArtifactKind.DATA, ArtifactKind.BAD_ROWS, ArtifactKind.PREVIEW}


class UploadStatus(models.TextChoices):
    PENDING = "pending", "Waiting for the upload"
    COMPLETE = "complete", "Complete"
    EXPIRED = "expired", "Expired before completion"
    DELETED = "deleted", "Deleted"


class SignInThrottleKind(models.TextChoices):
    """What failed sign-ins are counted by: the normalised username, or the client address."""

    USER = "user", "Username"
    IP = "ip", "Client address"


class RetentionScope(models.TextChoices):
    INSTALLATION = "installation", "Installation"
    CLASSIFICATION = "classification", "Classification"
    DATASET = "dataset", "Dataset"


class RetentionKind(models.TextChoices):
    """What a retention policy sets a lifetime for (design section 5.7)."""

    UPLOADS = "uploads", "Uploads"
    DATA = "data", "Data (Parquet outputs)"
    BAD_ROWS = "bad_rows", "Bad rows"
    PREVIEWS = "previews", "Previews"
    METADATA = "metadata", "Manifests, metadata, schemas and reports"
    JOB_RECORDS = "job_records", "Job records"


ARTIFACT_RETENTION_KIND = {
    ArtifactKind.DATA: RetentionKind.DATA,
    ArtifactKind.BAD_ROWS: RetentionKind.BAD_ROWS,
    ArtifactKind.PREVIEW: RetentionKind.PREVIEWS,
    ArtifactKind.MANIFEST: RetentionKind.METADATA,
    ArtifactKind.METADATA: RetentionKind.METADATA,
    ArtifactKind.SCHEMA: RetentionKind.METADATA,
    ArtifactKind.REPORT: RetentionKind.METADATA,
}


class ScheduleOutcome(models.TextChoices):
    """What the dispatcher did with a schedule's last slot."""

    QUEUED = "queued", "Queued a run"
    SKIPPED_OVERLAP = "skipped_overlap", "Skipped: the previous run had not finished"
    MISSED = "missed", "Missed: too late to catch up"
    FAILED_TO_ENQUEUE = "failed_to_enqueue", "Could not queue a run"


class WebhookEvent(models.TextChoices):
    """What a webhook delivery reports; ``webhook.test`` only answers a test request."""

    JOB_SUCCEEDED = "job.succeeded", "Job succeeded"
    JOB_FAILED = "job.failed", "Job failed"
    JOB_CANCELLED = "job.cancelled", "Job cancelled"
    TEST = "webhook.test", "Test"


JOB_EVENTS = [WebhookEvent.JOB_SUCCEEDED, WebhookEvent.JOB_FAILED, WebhookEvent.JOB_CANCELLED]
EVENT_OF_STATUS = {
    JobStatus.SUCCEEDED: WebhookEvent.JOB_SUCCEEDED,
    JobStatus.FAILED: WebhookEvent.JOB_FAILED,
    JobStatus.CANCELLED: WebhookEvent.JOB_CANCELLED,
}


class WebhookScope(models.TextChoices):
    """Which jobs a webhook hears about."""

    DATASET = "dataset", "Every job of one dataset"
    OWN_JOBS = "own_jobs", "Jobs I requested (also through my API tokens)"
    ALL_JOBS = "all_jobs", "Every job (admins)"


class WebhookDisabledReason(models.TextChoices):
    OWNER = "owner", "Disabled by its owner"
    ADMIN = "admin", "Disabled by an admin"
    FAILURES = "failures", "Disabled after repeated failed deliveries"


class DeliveryStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    DELIVERED = "delivered", "Delivered"
    FAILED = "failed", "Failed"
    SKIPPED = "skipped", "Skipped"
