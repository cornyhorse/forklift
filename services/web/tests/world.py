"""A small installation with one of everything, for tests that need objects to act on.

``World`` builds users of every role, a schema, datasets, a connection, uploads, jobs with
artifacts, tokens and retention policies through the ORM (objects in the store only where a
test asks for them, to keep the role matrix fast).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from conftest import put_url
from django.utils import timezone
from webhook_support import make_webhook

from forklift_web import secret_backend, storage
from forklift_web.core.choices import (
    ArtifactKind,
    Classification,
    JobKind,
    JobStatus,
    Lane,
    RetentionScope,
    Role,
    UploadStatus,
)
from forklift_web.core.models import (
    ApiToken,
    Artifact,
    Connection,
    Dataset,
    Job,
    RetentionPolicy,
    Schedule,
    Schema,
    SchemaVersion,
    Upload,
    User,
    Webhook,
    WebhookDelivery,
    Worker,
    WorkerToken,
)
from forklift_web.services import specs, tokens

PASSWORD = "correct horse battery staple"
SCHEMA_DOCUMENT = {"type": "object", "properties": {"id": {"type": "integer"}}}


def make_user(role: str, *, raw_rows: bool = False, name: str = "") -> User:
    user = User(
        username=name or f"{role}-{uuid.uuid4().hex[:8]}", role=role, can_view_raw_rows=raw_rows
    )
    user.set_password(PASSWORD)
    user.save()
    return user


def make_upload(
    owner: User,
    *,
    classification: str = Classification.INTERNAL,
    status: str = UploadStatus.COMPLETE,
    size: int = 24,
    key: str = "",
) -> Upload:
    upload = Upload(
        filename="people.csv",
        size=size,
        classification=classification,
        status=status,
        uploaded_by=owner,
        expires_at=timezone.now() + timedelta(hours=1),
        completed_at=timezone.now() if status == UploadStatus.COMPLETE else None,
        etag="etag-1",
    )
    upload.key = key or f"uploads/{upload.id}/people.csv"
    upload.save()
    return upload


def make_schema(author: User, name: str = "") -> SchemaVersion:
    schema = Schema.objects.create(
        name=name or f"schema-{uuid.uuid4().hex[:8]}", created_by=author
    )
    return SchemaVersion.objects.create(
        schema=schema, number=1, document=SCHEMA_DOCUMENT, sha256="0" * 64, author=author
    )


def make_job(
    requester: User,
    upload: Upload,
    *,
    status: str = JobStatus.QUEUED,
    kind: str = JobKind.RUN,
    classification: str = "",
    dataset=None,
    lane: str = "",
) -> Job:
    job = Job(
        kind=kind,
        lane=lane or (Lane.BATCH if kind == JobKind.RUN else Lane.INTERACTIVE),
        status=status,
        classification=classification or upload.classification,
        upload=upload,
        dataset=dataset,
        requested_by=requester,
        finished_at=timezone.now() if status in {JobStatus.SUCCEEDED, JobStatus.FAILED} else None,
    )
    job.spec = specs.template(
        str(job.id),
        kind,
        input_format="csv",
        input_location={"type": specs.UPLOAD, "upload_id": str(upload.id)},
        input_options={},
        schema=SCHEMA_DOCUMENT if kind != JobKind.GENERATE_SCHEMA else None,
        output_location=None,
        compression="snappy",
        options={},
        limits={"max_seconds": 600},
    )
    job.save()
    return job


def make_artifact(job: Job, kind: str = ArtifactKind.DATA, name: str = "") -> Artifact:
    name = name or {"data": "data.parquet", "bad_rows": "bad_rows.parquet"}.get(
        kind, f"{kind}.json"
    )
    return Artifact.objects.create(
        job=job,
        attempt=1,
        kind=kind,
        name=name,
        key=f"jobs/{job.id}/attempt-1/{name}",
        bytes=10,
        rows=1,
        sha256="a" * 64,
    )


def s3_connection(name: str = "", **config) -> Connection:
    conf = storage.store()
    access, secret = settings_credentials()
    return Connection.objects.create(
        name=name or f"store-{uuid.uuid4().hex[:8]}",
        kind="s3",
        config={
            "bucket": conf.bucket,
            "endpoint_url": endpoint(),
            "prefix": "",
            "region": "us-east-1",
            "addressing_style": "path",
            **config,
        },
        secret_ciphertext=secret_backend.backend().encrypt(
            {"access_key_id": access, "secret_access_key": secret}
        ),
        secret_fields=["access_key_id", "secret_access_key"],
    )


def endpoint() -> str:
    from django.conf import settings

    return settings.FORKLIFT_STORE["endpoint_url"]


def settings_credentials():
    from django.conf import settings

    return settings.FORKLIFT_STORE["credentials"]["upload"]


def api_token(owner: User, scopes, **fields):
    token, raw = tokens.create(
        ApiToken,
        tokens.API_TOKEN_PREFIX,
        owner=owner,
        name="token",
        scopes=sorted(scopes),
        **fields,
    )
    return token, raw


@dataclass
class World:
    viewer: User
    operator: User
    author: User
    admin: User
    version: SchemaVersion
    connection: Connection
    spare_connection: Connection
    dataset: Dataset
    spare_dataset: Dataset
    upload: Upload
    spare_upload: Upload
    queued_job: Job
    finished_job: Job
    artifact: Artifact
    worker_token: WorkerToken
    worker: Worker
    tokens: dict = field(default_factory=dict)
    raw_tokens: dict = field(default_factory=dict)
    extra: dict = field(default_factory=dict)

    @classmethod
    def build(cls) -> "World":
        viewer, operator = make_user(Role.VIEWER), make_user(Role.OPERATOR)
        author, admin = make_user(Role.AUTHOR), make_user(Role.ADMIN)
        version = make_schema(author)
        connection = s3_connection()
        spare_connection = s3_connection()
        dataset = Dataset.objects.create(
            name=f"ds-{uuid.uuid4().hex[:8]}", schema_version=version, created_by=author
        )
        spare_dataset = Dataset.objects.create(
            name=f"spare-{uuid.uuid4().hex[:8]}", schema_version=version, created_by=author
        )
        upload, spare_upload = make_upload(operator), make_upload(operator)
        queued_job = make_job(operator, upload)
        finished_job = make_job(operator, upload, status=JobStatus.SUCCEEDED, dataset=dataset)
        artifact = make_artifact(finished_job)
        worker_token, _ = tokens.create(WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="pool")
        worker = Worker.objects.create(
            worker_id="worker-1", token=worker_token, lanes=["batch"], spec_versions=[1]
        )
        RetentionPolicy.objects.create(scope=RetentionScope.INSTALLATION, days={"previews": 7})
        RetentionPolicy.objects.create(
            scope=RetentionScope.CLASSIFICATION,
            classification=Classification.PUBLIC,
            days={"data": 30},
        )
        RetentionPolicy.objects.create(
            scope=RetentionScope.DATASET, dataset=spare_dataset, days={"data": 1}
        )
        world = cls(
            viewer,
            operator,
            author,
            admin,
            version,
            connection,
            spare_connection,
            dataset,
            spare_dataset,
            upload,
            spare_upload,
            queued_job,
            finished_job,
            artifact,
            worker_token,
            worker,
        )
        for user in (viewer, operator, author, admin):
            token, raw = api_token(user, ["jobs:read", "tokens:read"])
            world.tokens[user.role] = token
            world.raw_tokens[user.role] = raw
        return world

    # Objects with a counterpart in the store, built only when a test needs them

    def pending_upload(self) -> Upload:
        upload = make_upload(self.operator, status=UploadStatus.PENDING, size=5)
        put_url(
            storage.store().presign_put(upload.key, expires=60, audience=storage.Audience.GATEWAY),
            b"a,b\n\n",
        )
        return upload

    def multipart_upload(self) -> Upload:
        upload = make_upload(self.operator, status=UploadStatus.PENDING, size=12 * 1024 * 1024)
        upload.part_size = 8 * 1024 * 1024
        upload.multipart_upload_id = storage.store().create_multipart(upload.key)
        upload.save()
        return upload

    # Objects only some tests need

    def schedule(self) -> Schedule:
        """A daily schedule of a dataset that reads an object of ``connection`` (made the first
        time it is asked for)."""
        if "schedule" not in self.extra:
            self.extra["schedule"] = make_schedule(self.author, self.version, self.connection)
        return self.extra["schedule"]

    def webhook(self) -> Webhook:
        """The operator's webhook, with one failed delivery of ``finished_job`` (made the first
        time it is asked for)."""
        if "webhook" not in self.extra:
            webhook = make_webhook(self.operator)
            WebhookDelivery.objects.create(
                webhook=webhook,
                job=self.finished_job,
                event="job.succeeded",
                payload="{}",
                status="failed",
                attempts=7,
            )
            self.extra["webhook"] = webhook
        return self.extra["webhook"]


def make_schedule(author: User, version: SchemaVersion, connection: Connection, **fields):
    """A schedule (``fields`` override its own) of a new dataset reading from ``connection``."""
    dataset = Dataset.objects.create(
        name=f"scheduled-{uuid.uuid4().hex[:8]}",
        schema_version=version,
        source_connection=connection,
        source_path="exports/people.csv",
        created_by=author,
    )
    values = {
        "cron": "0 2 * * *",
        "timezone": "UTC",
        "next_run_at": timezone.now() + timedelta(hours=1),
        "created_by": author,
        **fields,
    }
    return Schedule.objects.create(dataset=dataset, **values)


def job_result(
    job_id: str, artifacts: list = (), *, status: str = "succeeded", error=None
) -> dict:
    """A JobResult (contract v1) as the engine reports it."""
    return {
        "spec_version": 1,
        "job_id": job_id,
        "status": status,
        "counts": {"total_rows": 2, "valid_rows": 1, "invalid_rows": 1, "truncated_rows": 0},
        "schema_extensions": [],
        "validation_summary": {"TYPE_MISMATCH:id": 1},
        "warnings": [],
        "artifacts": [
            {
                "kind": a["kind"],
                "path": "out/" + a["name"],
                "rows": a.get("rows"),
                "bytes": a["bytes"],
                "sha256": a.get("sha256"),
            }
            for a in artifacts
        ],
        "error": error,
    }
