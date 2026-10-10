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
