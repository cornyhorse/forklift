"""Enqueueing jobs: requests, lanes, limits, idempotency keys, dataset runs, validation, cancel."""

from __future__ import annotations

import pytest
from world import SCHEMA_DOCUMENT, World, make_job, make_upload, s3_connection

from forklift_web.core.choices import JobStatus, Lane, Role
from forklift_web.core.models import AuditLog, Dataset, Job, JobEvent
from forklift_web.errors import Conflict, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Actor
from forklift_web.services import connections, installation, jobs, queue, specs, workers
from forklift_web.services.jobs import JobRequest

pytestmark = pytest.mark.django_db


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def operator(world):
    return Actor.for_user(world.operator)


def run(upload, **fields) -> JobRequest:
    return JobRequest(
        **{"kind": "run", "upload_id": upload.pk, "schema": SCHEMA_DOCUMENT, **fields}
    )


def test_ad_hoc_jobs_get_lanes_limits_and_a_spec(world, operator):
    job, created = jobs.create_job(operator, run(world.upload, limits={"max_seconds": 60}))
    assert created and (job.status, job.lane, job.attempt) == (JobStatus.QUEUED, Lane.BATCH, 0)
    assert job.spec["input"] == {
        "format": "csv",
        "location": {"type": specs.UPLOAD, "upload_id": str(world.upload.pk)},
        "options": {},
    }
    assert job.spec["limits"] == {"max_seconds": 60}  # lowered from the lane's one day
    assert job.spec["output"] == {"location": specs.OUTPUT_DIRECTORY, "compression": "snappy"}
    assert job.max_attempts == 3 and job.requested_by == world.operator
    assert JobEvent.objects.get(job=job).payload == {"status": "queued", "lane": "batch"}
    preview, _ = jobs.create_job(operator, JobRequest(kind="preview", upload_id=world.upload.pk))
    assert preview.lane == Lane.INTERACTIVE and preview.spec["schema"] is None
    assert preview.spec["limits"] == {"max_input_bytes": 256 * 1024**2, "max_seconds": 120}
    generated, _ = jobs.create_job(
        operator, JobRequest(kind="generate_schema", upload_id=world.upload.pk, format="excel")
    )
    assert generated.spec["input"]["format"] == "excel"


def test_stored_schema_versions_and_drafts(world, operator):
    stored, _ = jobs.create_job(
        operator,
        JobRequest(kind="run", upload_id=world.upload.pk, schema_version_id=world.version.pk),
    )
    assert stored.schema_version == world.version
    assert stored.spec["schema"] == world.version.document
    with pytest.raises(InvalidRequest, match="either schema .* or schema_version_id"):
        jobs.create_job(operator, run(world.upload, schema_version_id=world.version.pk))
    with pytest.raises(InvalidRequest, match="must be a JSON object"):
        jobs.create_job(operator, run(world.upload, schema=[1]))
    with pytest.raises(InvalidRequest, match="A run job needs a schema"):
        jobs.create_job(operator, run(world.upload, schema=None))
    with pytest.raises(InvalidRequest, match="infers the schema; give none"):
        jobs.create_job(operator, run(world.upload, kind="generate_schema"))


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"kind": "explode"}, "Unknown job kind 'explode'"),
        ({"upload_id": None}, "needs an input: give upload_id"),
        ({"format": "parquet"}, "format 'parquet' cannot be read from an upload"),
        ({"format": "sql"}, "format 'sql' cannot be read from an upload"),
        ({"classification": "secret"}, "Unknown classification 'secret'"),
        ({"limits": {"max_cpu": 1}}, "Unknown limits: max_cpu"),
        ({"limits": {"max_rows": 0}}, "limits.max_rows must be a positive number"),
        ({"limits": {"max_rows": True}}, "limits.max_rows must be a positive number"),
        ({"limits": {"max_input_bytes": 10}}, "may read at most 10"),
        ({"input_options": {"delimiter": ";;"}}, "input/options/delimiter: fails 'maxLength'"),
        ({"compression": "rar"}, "output/compression: fails 'enum'"),
    ],
)
def test_job_request_validation(world, operator, fields, message):
    with pytest.raises(InvalidRequest, match=message):
        jobs.create_job(operator, run(world.upload, **fields))


def test_contract_violations_carry_their_code(world, operator):
    with pytest.raises(InvalidRequest) as raised:
        jobs.create_job(operator, run(world.upload, options={"preview_rows": 0}))
    assert raised.value.code == "spec_invalid"
    assert "options/preview_rows: fails 'minimum'" in raised.value.message


def test_inputs_that_cannot_be_streamed_must_fit_the_stage(world, operator, admin_actor):
    installation.update(admin_actor, {"stage_max_bytes": 10})
    jobs.create_job(operator, run(world.upload))  # CSV is streamed
    with pytest.raises(InvalidRequest, match="Only CSV inputs can be streamed"):
        jobs.create_job(operator, run(world.upload, format="excel"))


def test_classification_of_ad_hoc_jobs(world, operator, admin_actor):
    sensitive = make_upload(world.operator, classification="sensitive")
    job, _ = jobs.create_job(operator, run(sensitive, classification="public"))
    assert job.classification == "sensitive"  # never less restricted than its input
    public = make_upload(world.operator, classification="public")
    assert jobs.create_job(operator, run(public))[0].classification == "internal"
    installation.update(admin_actor, {"default_classification": "public"})
    assert jobs.create_job(operator, run(public))[0].classification == "public"


def test_idempotency_keys(world, operator):
    first, created = jobs.create_job(operator, run(world.upload), idempotency_key="nightly-1")
    again, created_again = jobs.create_job(
        operator, run(world.upload), idempotency_key="nightly-1"
    )
    assert created and not created_again and again.pk == first.pk
    with pytest.raises(Conflict) as raised:
        jobs.create_job(
            operator, run(world.upload, options={"batch_size": 5}), idempotency_key="nightly-1"
        )
    assert raised.value.code == "idempotency_key_reused" and str(first.pk) in raised.value.message
    other = Actor.for_user(world.admin)  # keys are per user
    assert jobs.create_job(other, run(world.upload), idempotency_key="nightly-1")[1]
    with pytest.raises(InvalidRequest, match="at most 255 characters"):
        jobs.create_job(operator, run(world.upload), idempotency_key="k" * 256)


def test_concurrent_requests_with_one_key_create_one_job(world, operator, monkeypatch):
    request = run(world.upload)
    real_render = specs.render

    def render_after_a_rival_saved(job, settings, *, dry_run=False):
        if dry_run and not Job.objects.filter(idempotency_key="race").exists():
            rival = make_job(world.operator, world.upload)
            Job.objects.filter(pk=rival.pk).update(
                idempotency_key="race", request_fingerprint=request.fingerprint()
            )
        return real_render(job, settings, dry_run=dry_run)

    monkeypatch.setattr(specs, "render", render_after_a_rival_saved)
    job, created = jobs.create_job(operator, request, idempotency_key="race")
    assert not created and Job.objects.filter(idempotency_key="race").count() == 1
    assert job.pk == Job.objects.get(idempotency_key="race").pk


def test_api_create_job_and_replay(world, as_user):
    caller = as_user(world.operator)
    body = {
        "kind": "run",
        "upload_id": str(world.upload.pk),
        "schema": SCHEMA_DOCUMENT,
        "limits": {"max_rows": 1000},
    }
    created = caller.post("/api/v1/jobs", body, HTTP_IDEMPOTENCY_KEY="k1")
    assert created.status_code == 201
    assert created.json()["spec"]["limits"]["max_rows"] == 1000
    replay = caller.post("/api/v1/jobs", body, HTTP_IDEMPOTENCY_KEY="k1")
    assert replay.status_code == 200 and replay.json()["id"] == created.json()["id"]
    conflict = caller.post("/api/v1/jobs", {**body, "limits": {}}, HTTP_IDEMPOTENCY_KEY="k1")
    assert conflict.status_code == 409


# --------------------------------------------------------------------------- dataset runs


def test_dataset_runs(world, operator, as_user):
    job, created = jobs.run_dataset(operator, world.dataset.pk, upload_id=world.upload.pk)
    assert created and job.dataset == world.dataset and job.schema_version == world.version
    assert job.classification == world.dataset.classification
    response = as_user(world.operator).post(
        f"/api/v1/datasets/{world.dataset.pk}/run",
        {"upload_id": str(world.upload.pk)},
        HTTP_IDEMPOTENCY_KEY="d1",
    )
    assert response.status_code == 201
    again = as_user(world.operator).post(
        f"/api/v1/datasets/{world.dataset.pk}/run",
        {"upload_id": str(world.upload.pk)},
        HTTP_IDEMPOTENCY_KEY="d1",
    )
    assert again.status_code == 200
    with pytest.raises(InvalidRequest, match="reads uploads: give the upload_id"):
        jobs.run_dataset(operator, world.dataset.pk)


def test_dataset_job_rules(world, operator):
    dataset = world.dataset
    with pytest.raises(InvalidRequest, match="reads csv; leave format out"):
        jobs.create_job(
            operator,
            JobRequest(
                kind="run", dataset_id=dataset.pk, upload_id=world.upload.pk, format="excel"
            ),
        )
    with pytest.raises(InvalidRequest, match="have its classification"):
        jobs.create_job(
            operator,
            JobRequest(
                kind="run",
                dataset_id=dataset.pk,
                upload_id=world.upload.pk,
                classification="public",
            ),
        )
    with pytest.raises(InvalidRequest, match="uses the dataset's schema version"):
        jobs.create_job(
            operator,
            JobRequest(kind="run", dataset_id=dataset.pk, upload_id=world.upload.pk, schema={}),
        )
    draft, _ = jobs.create_job(
        operator,
        JobRequest(
            kind="validate_schema",
            dataset_id=dataset.pk,
            upload_id=world.upload.pk,
            schema={"type": "object", "title": "draft"},
            input_options={"delimiter": ";"},
        ),
    )
    assert draft.spec["schema"]["title"] == "draft" and draft.schema_version is None
    assert draft.spec["input"]["options"] == {"delimiter": ";"}
    inferred, _ = jobs.create_job(
        operator,
        JobRequest(kind="generate_schema", dataset_id=dataset.pk, upload_id=world.upload.pk),
    )
    assert inferred.spec["schema"] is None
    with pytest.raises(NotFound):
        jobs.run_dataset(
            operator, "00000000-0000-0000-0000-000000000000", upload_id=world.upload.pk
        )


def test_datasets_reading_connections(world, operator, admin_actor):
    store = s3_connection(prefix="incoming")
    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=store, source_path="people.csv", input_options={"delimiter": ";"}
    )
    job, _ = jobs.run_dataset(operator, world.dataset.pk)
    assert job.spec["input"]["location"] == {
        "type": specs.OBJECT,
        "connection_id": str(store.pk),
        "key": "incoming/people.csv",
    }
    assert job.lane == Lane.BATCH and job.upload is None
    with pytest.raises(InvalidRequest, match="leave upload_id out"):
        jobs.run_dataset(operator, world.dataset.pk, upload_id=world.upload.pk)

    db = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "p"},
    )
    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=db,
        source_path="",
        input_format="sql",
        input_options={},
        destination_connection=db,
        destination_options={"table": "out", "mode": "append"},
    )
    job, _ = jobs.run_dataset(operator, world.dataset.pk)
    assert job.lane == Lane.SQL
    assert job.spec["input"]["location"] == {"type": specs.SQL, "connection_id": str(db.pk)}
    assert job.spec["output"]["location"] == {
        "type": specs.SQL_TABLE,
        "connection_id": str(db.pk),
        "table": "out",
        "mode": "append",
    }
    assert job.spec["output"]["artifacts"] == specs.OUTPUT_DIRECTORY
    assert "connection_string" not in str(job.spec)
    preview, _ = jobs.create_job(operator, JobRequest(kind="preview", dataset_id=world.dataset.pk))
    assert preview.spec["output"]["location"] == specs.OUTPUT_DIRECTORY  # never the table

    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=None,
        input_format="csv",
        destination_connection=store,
        destination_options={},
    )
    job, _ = jobs.run_dataset(operator, world.dataset.pk, upload_id=world.upload.pk)
    assert job.lane == Lane.BATCH  # an s3 destination is published after the run
    assert job.spec["output"]["location"] == specs.OUTPUT_DIRECTORY


def test_sql_lane_for_a_table_destination_with_a_csv_source(world, operator, admin_actor):
    db = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        config={"dialect": "mysql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "p"},
    )
    Dataset.objects.filter(pk=world.dataset.pk).update(
        destination_connection=db, destination_options={"table": "out", "mode": "append"}
    )
    job, _ = jobs.run_dataset(operator, world.dataset.pk, upload_id=world.upload.pk)
    assert job.lane == Lane.SQL


# --------------------------------------------------------------------------- following jobs


def test_listing_and_events(world, operator):
    viewer = Actor.for_user(world.viewer)
    assert set(jobs.list_jobs(viewer)) == {world.queued_job, world.finished_job}
    assert list(jobs.list_jobs(viewer, status="succeeded")) == [world.finished_job]
    assert list(jobs.list_jobs(viewer, kind="preview")) == []
    assert list(jobs.list_jobs(viewer, dataset_id=world.dataset.pk)) == [world.finished_job]
    assert list(jobs.list_jobs(viewer, mine=True)) == []
    assert len(jobs.list_jobs(operator, mine=True)) == 2
    with pytest.raises(NotFound):
        jobs.get_job(viewer, "00000000-0000-0000-0000-000000000000")
    job, _ = jobs.create_job(operator, run(world.upload))
    jobs.cancel_job(operator, job.pk)
    events = list(jobs.list_events(viewer, job.pk))
    assert [e.payload["status"] for e in events] == ["queued", "cancelled"]
    assert list(jobs.list_events(viewer, job.pk, after=events[0].id)) == events[1:]


def test_cancelling(world, operator, worker_token):
    job, _ = jobs.create_job(operator, run(world.upload))
    cancelled = jobs.cancel_job(operator, job.pk)
    assert (cancelled.status, cancelled.error_code) == (JobStatus.CANCELLED, "CANCELLED")
    assert AuditLog.objects.get(action="job.cancel").details == {"status": "cancelled"}
    with pytest.raises(Conflict, match="already finished \\(cancelled\\)"):
        jobs.cancel_job(operator, job.pk)
    with pytest.raises(NotFound):
        jobs.cancel_job(operator, "00000000-0000-0000-0000-000000000000")
    with pytest.raises(PermissionDenied):
        jobs.cancel_job(Actor.for_user(world.author), world.queued_job.pk)

    token, _ = worker_token
    leased = queue.lease(
        workers.WorkerPrincipal(token), worker_id="w", lanes=["batch"], spec_versions=[1]
    )
    assert leased.job == world.queued_job
    first = jobs.cancel_job(operator, world.queued_job.pk)
    assert first.status == JobStatus.RUNNING and first.cancel_requested_at is not None
    second = jobs.cancel_job(Actor.for_user(world.admin), world.queued_job.pk)
    assert second.cancel_requested_at == first.cancel_requested_at
    assert second.cancel_requested_by == world.operator


def test_validate_schema_waits_briefly(world, operator, monkeypatch, worker_token):
    job, finished = jobs.validate_schema(
        operator, upload_id=world.upload.pk, schema=SCHEMA_DOCUMENT, wait_seconds=0
    )
    assert not finished and job.kind == "validate_schema" and job.lane == Lane.INTERACTIVE
    token, _ = worker_token
    principal = workers.WorkerPrincipal(token)

    def worker_finishes_it(seconds):
        leased = queue.lease(principal, worker_id="w", lanes=["interactive"], spec_versions=[1])
        Job.objects.filter(pk=leased.job.pk).update(status=JobStatus.SUCCEEDED)

    monkeypatch.setattr(jobs, "_sleep", worker_finishes_it)
    Job.objects.filter(kind="validate_schema").update(status=JobStatus.CANCELLED)
    job, finished = jobs.validate_schema(
        operator, upload_id=world.upload.pk, schema=SCHEMA_DOCUMENT
    )
    assert finished and job.status == JobStatus.SUCCEEDED


def test_validate_schema_api_answers_202_while_running(world, as_user, monkeypatch):
    monkeypatch.setattr(jobs, "_sleep", lambda seconds: None)
    response = as_user(world.operator).post(
        "/api/v1/schemas/validate",
        {"upload_id": str(world.upload.pk), "schema": SCHEMA_DOCUMENT, "wait_seconds": 0.01},
    )
    assert response.status_code == 202 and response.json()["status"] == "queued"


def test_viewers_cannot_run(world):
    with pytest.raises(PermissionDenied):
        jobs.create_job(Actor.for_user(world.viewer), run(world.upload))
    assert Role.VIEWER == world.viewer.role


def test_waiting_for_a_job_polls_until_the_deadline(world):
    job = jobs.wait_for(world.queued_job, 0.02, poll=0.005)
    assert job.status == JobStatus.QUEUED
    assert jobs.wait_for(world.finished_job, 10).status == JobStatus.SUCCEEDED  # no wait at all


def test_a_dataset_job_is_as_restricted_as_its_upload(world, operator):
    sensitive = make_upload(world.operator, classification="sensitive")
    job, _ = jobs.run_dataset(operator, world.dataset.pk, upload_id=sensitive.pk)
    assert world.dataset.classification == "internal" and job.classification == "sensitive"
