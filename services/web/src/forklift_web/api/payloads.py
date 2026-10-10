"""Request and response bodies of /api/v1 (pydantic models, published in the OpenAPI document).

Secrets never appear in responses: connections list the names of the secrets that are set
(``secret_fields``), tokens show their prefix, and a new token's value is returned exactly once.
"""

import uuid
from datetime import datetime
from typing import Any, Literal, Optional

from ninja import Field, Schema

Role = Literal["viewer", "operator", "author", "admin"]
ClassificationName = Literal["public", "internal", "sensitive"]
JobKindName = Literal["run", "preview", "validate_schema", "generate_schema"]


class ErrorOut(Schema):
    detail: str = Field(description="What went wrong, and what to do about it")
    code: str = Field(description="A stable machine-readable code")


# --------------------------------------------------------------------------- users and tokens


class UserOut(Schema):
    id: int
    username: str
    email: str
    first_name: str
    last_name: str
    role: Role
    can_view_raw_rows: bool
    is_service_account: bool
    is_active: bool
    date_joined: datetime
    last_login: Optional[datetime]


class RoleOut(Schema):
    role: Role
    label: str
    scopes: list[str]


class UserIn(Schema):
    username: str
    role: Role
    email: str = ""
    first_name: str = ""
    last_name: str = ""
    can_view_raw_rows: bool = False
    is_service_account: bool = False
    password: Optional[str] = Field(None, description="Leave out for service accounts")


class UserPatch(Schema):
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[Role] = None
    can_view_raw_rows: Optional[bool] = None
    is_active: Optional[bool] = None


class PasswordIn(Schema):
    password: str


class TokenOut(Schema):
    id: uuid.UUID
    name: str
    prefix: str = Field(description="The first characters of the token, to recognise it")
    scopes: list[str]
    owner_id: int
    owner_username: str
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    revoked_at: Optional[datetime]

    @staticmethod
    def resolve_owner_username(token) -> str:
        return token.owner.username


class TokenCreatedOut(TokenOut):
    token: str = Field(description="The token itself: shown only now, store it safely")


class TokenIn(Schema):
    name: str
    scopes: list[str] = Field(description="Scopes within your role (see GET /me)")
    expires_at: Optional[datetime] = None


class AdminTokenIn(TokenIn):
    owner_id: int


class MeOut(Schema):
    user: UserOut
    scopes: list[str] = Field(description="Effective scopes of this request")
    role_scopes: list[str] = Field(description="Every scope the role grants")
    token: Optional[TokenOut] = Field(description="The API token of this request, if any")


class WorkerTokenOut(Schema):
    id: uuid.UUID
    name: str
    prefix: str
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    revoked_at: Optional[datetime]


class WorkerTokenCreatedOut(WorkerTokenOut):
    token: str = Field(description="The token itself: shown only now")


class WorkerTokenIn(Schema):
    name: str
    expires_at: Optional[datetime] = None


class WorkerOut(Schema):
    id: uuid.UUID
    worker_id: str
    token_id: Optional[uuid.UUID]
    lanes: list[str]
    spec_versions: list[int]
    engine_version: str
    worker_version: str
    first_seen_at: datetime
    last_seen_at: datetime


# --------------------------------------------------------------------------- connections


class ConnectionOut(Schema):
    id: uuid.UUID
    name: str
    kind: Literal["s3", "localfs", "sql"]
    description: str
    config: dict[str, Any]
    secret_fields: list[str] = Field(description="Names of the secrets that are set")
    allowed_roles: list[str]
    created_at: datetime
    updated_at: datetime
    last_tested_at: Optional[datetime]
    last_test_ok: Optional[bool]
    last_test_message: str


class ConnectionIn(Schema):
    name: str
    kind: Literal["s3", "localfs", "sql"]
    config: dict[str, Any]
    secrets: dict[str, str] = Field(
        default_factory=dict,
        description="s3: access_key_id, secret_access_key (session_token); sql: password",
    )
    description: str = ""
    allowed_roles: Optional[list[Role]] = None


class ConnectionPatch(Schema):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict[str, Any]] = None
    secrets: Optional[dict[str, Optional[str]]] = Field(
        None, description="Secrets to set; null removes one; secrets not named keep their value"
    )
    allowed_roles: Optional[list[Role]] = None


class ConnectionTestOut(Schema):
    ok: Optional[bool] = Field(description="null when the gateway cannot tell")
    message: str


# --------------------------------------------------------------------------- schemas


class SchemaOut(Schema):
    id: uuid.UUID
    name: str
    description: str
    latest_version: Optional[int]
    created_at: datetime
    updated_at: datetime


class SchemaIn(Schema):
    name: str
    document: dict[str, Any]
    description: str = ""
    notes: str = Field("", description="Notes for version 1")


class SchemaPatch(Schema):
    name: Optional[str] = None
    description: Optional[str] = None


class SchemaVersionOut(Schema):
    id: uuid.UUID
    schema_id: uuid.UUID
    number: int
    document: dict[str, Any]
    sha256: str
    author_id: Optional[int]
    created_at: datetime
    notes: str


class SchemaVersionIn(Schema):
    document: dict[str, Any]
    notes: str = ""


class ValidateIn(Schema):
    upload_id: Optional[uuid.UUID] = None
    dataset_id: Optional[uuid.UUID] = None
    schema_: Optional[dict[str, Any]] = Field(None, alias="schema")
    schema_version_id: Optional[uuid.UUID] = None
    format: Optional[str] = None
    input_options: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    wait_seconds: Optional[float] = Field(
        None, ge=0, description="At most the installation setting validate_wait_seconds"
    )


# --------------------------------------------------------------------------- datasets


class DatasetOut(Schema):
    id: uuid.UUID
    name: str
    description: str
    classification: ClassificationName
    input_format: str
    input_options: dict[str, Any]
    source_connection_id: Optional[uuid.UUID]
    source_path: str
    schema_version_id: uuid.UUID
    schema_id: uuid.UUID
    schema_version: int = Field(description="The number of the schema version")
    destination_connection_id: Optional[uuid.UUID]
    destination_prefix: str
    destination_options: dict[str, Any]
    compression: str
    options: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def resolve_schema_id(dataset) -> uuid.UUID:
        return dataset.schema_version.schema_id

    @staticmethod
    def resolve_schema_version(dataset) -> int:
        return dataset.schema_version.number


class DatasetIn(Schema):
    name: str
    schema_version_id: uuid.UUID
    classification: ClassificationName = "internal"
    description: str = ""
    input_format: str = "csv"
    input_options: dict[str, Any] = Field(default_factory=dict)
    source_connection_id: Optional[uuid.UUID] = None
    source_path: str = ""
    destination_connection_id: Optional[uuid.UUID] = None
    destination_prefix: str = ""
    destination_options: dict[str, Any] = Field(default_factory=dict)
    compression: str = "snappy"
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetPatch(Schema):
    """Fields to change; ``source_connection_id`` and ``destination_connection_id`` may be set
    to null to remove the connection."""

    name: Optional[str] = None
    description: Optional[str] = None
    classification: Optional[ClassificationName] = None
    input_format: Optional[str] = None
    input_options: Optional[dict[str, Any]] = None
    source_connection_id: Optional[uuid.UUID] = None
    source_path: Optional[str] = None
    schema_version_id: Optional[uuid.UUID] = None
    destination_connection_id: Optional[uuid.UUID] = None
    destination_prefix: Optional[str] = None
    destination_options: Optional[dict[str, Any]] = None
    compression: Optional[str] = None
    options: Optional[dict[str, Any]] = None


class DatasetRunIn(Schema):
    upload_id: Optional[uuid.UUID] = Field(None, description="For datasets that read uploads")


# --------------------------------------------------------------------------- uploads


class UploadOut(Schema):
    id: uuid.UUID
    filename: str
    content_type: str
    size: int
    declared_sha256: str
    classification: ClassificationName
    status: Literal["pending", "complete", "expired", "deleted"]
    uploaded_by_id: Optional[int]
    created_at: datetime
    completed_at: Optional[datetime]
    deleted_at: Optional[datetime]
    multipart: bool

    @staticmethod
    def resolve_multipart(upload) -> bool:
        return upload.is_multipart


class UploadIn(Schema):
    filename: str
    size: int = Field(description="Size of the file in bytes")
    content_type: str = ""
    sha256: str = Field("", description="Optional; recorded as declared, not checked")
    classification: Optional[ClassificationName] = None


class PartUrlOut(Schema):
    part_number: int
    url: str


class UploadTicketOut(Schema):
    upload: UploadOut
    method: Literal["PUT"]
    url: Optional[str] = Field(description="PUT the whole file here (single uploads)")
    headers: dict[str, str]
    expires_at: datetime
    part_size: Optional[int] = Field(description="Multipart uploads: bytes per part")
    part_count: Optional[int]
    parts: list[PartUrlOut] = Field(
        description="Multipart uploads: URLs for the first parts (POST /uploads/{id}/parts for "
        "more or fresh ones)"
    )


class PartIn(Schema):
    part_number: int
    etag: str


class UploadCompleteIn(Schema):
    parts: Optional[list[PartIn]] = Field(None, description="Multipart uploads only")


class PartsIn(Schema):
    part_numbers: list[int]


# --------------------------------------------------------------------------- jobs


class JobIn(Schema):
    kind: JobKindName
    dataset_id: Optional[uuid.UUID] = None
    upload_id: Optional[uuid.UUID] = None
    format: Optional[str] = None
    input_options: dict[str, Any] = Field(default_factory=dict)
    schema_version_id: Optional[uuid.UUID] = None
    schema_: Optional[dict[str, Any]] = Field(None, alias="schema")
    compression: Optional[str] = None
    options: dict[str, Any] = Field(default_factory=dict)
    limits: dict[str, int | float] = Field(default_factory=dict)
    classification: Optional[ClassificationName] = None


class JobError(Schema):
    code: str
    message: str


class JobOut(Schema):
    id: uuid.UUID
    kind: JobKindName
    lane: str
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    classification: ClassificationName
    dataset_id: Optional[uuid.UUID]
    upload_id: Optional[uuid.UUID]
    schema_version_id: Optional[uuid.UUID]
    requested_by_id: Optional[int]
    attempt: int
    max_attempts: int
    cancel_requested: bool
    progress: dict[str, Any]
    result: Optional[dict[str, Any]]
    error: Optional[JobError]
    worker: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]

    @staticmethod
    def resolve_cancel_requested(job) -> bool:
        return job.cancel_requested_at is not None

    @staticmethod
    def resolve_error(job):
        return {"code": job.error_code, "message": job.error_message} if job.error_code else None

    @staticmethod
    def resolve_worker(job):
        return job.lease_worker.worker_id if job.lease_worker_id else None


class JobDetailOut(JobOut):
    spec: dict[str, Any] = Field(
        description="The job's spec as stored: inputs in the store and database connections "
        "are placeholders that are filled in only for the worker that leases the job"
    )


class JobEventOut(Schema):
    id: int
    created_at: datetime
    type: str
    attempt: int
    payload: dict[str, Any]


# --------------------------------------------------------------------------- artifacts


class ArtifactOut(Schema):
    id: uuid.UUID
    job_id: uuid.UUID
    attempt: int
    kind: str
    name: str
    bytes: int
    rows: Optional[int]
    sha256: str
    created_at: datetime
    deleted_at: Optional[datetime]
    expires_at: Optional[datetime] = Field(None, description="When retention deletes it")


class DownloadOut(Schema):
    url: str = Field(description="A presigned GET, valid until expires_at")
    expires_at: datetime
    filename: str


# --------------------------------------------------------------------------- administration


class AuditOut(Schema):
    id: int
    created_at: datetime
    actor_id: Optional[int]
    actor_label: str
    token_prefix: str
    action: str
    object_type: str
    object_id: str
    object_repr: str
    details: dict[str, Any]
    request_id: str
    ip: Optional[str]


class RetentionPolicyOut(Schema):
    id: int
    scope: Literal["installation", "classification", "dataset"]
    classification: str
    dataset_id: Optional[uuid.UUID]
    days: dict[str, Optional[int]]
    updated_at: datetime


class RetentionOut(Schema):
    policies: list[RetentionPolicyOut]
    warnings: list[str]
    kinds: list[str]


class RetentionIn(Schema):
    days: dict[str, Optional[int]] = Field(
        description="Retention kind -> days to keep, or null to keep until deleted; kinds left "
        "out inherit from the next, less specific level"
    )


class SweepIn(Schema):
    dry_run: bool = True


class SweepOut(Schema):
    dry_run: bool
    expired_uploads: int
    uploads: int
    artifacts: int
    jobs: int
    errors: list[str]


class SettingOut(Schema):
    value: Any
    default: Any
    description: str
