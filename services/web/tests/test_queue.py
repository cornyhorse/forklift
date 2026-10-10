"""The worker side of the queue: leases (SKIP LOCKED), heartbeats, expiry, presigning, completion
and publishing; and two workers never lease the same job."""

from __future__ import annotations

import hashlib
import threading
from datetime import timedelta

import pytest
from conftest import put_url
from django.db import connection, transaction
from django.utils import timezone
from world import World, job_result, make_job, make_upload, make_user, s3_connection

from forklift_web import secret_backend, storage
from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.models import Artifact, Dataset, Job, JobEvent, Worker
from forklift_web.errors import Conflict, Gone, InvalidRequest, NotFound, StoreUnavailable
from forklift_web.services import installation, queue, tokens
from forklift_web.services.workers import WorkerPrincipal

pytestmark = pytest.mark.django_db


@pytest.fixture
def principal(worker_token):
    token, _ = worker_token
    return WorkerPrincipal(token=token)


def lease(principal, lanes=("batch",), worker_id="w-1", **fields):
    return queue.lease(
        principal, worker_id=worker_id, lanes=list(lanes), spec_versions=[1], **fields
    )


def upload_output(job: Job, name: str, data: bytes, kind: str) -> dict:
    key = f"jobs/{job.id}/attempt-{job.attempt}/{name}"
    put_url(storage.store().presign_put(key, expires=60, audience=storage.Audience.GATEWAY), data)
    return {
        "kind": kind,
        "name": name,
        "key": key,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "rows": 1,
    }


# --------------------------------------------------------------------------- leasing


def test_lease_hands_out_the_oldest_job_of_the_lanes(principal):
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner)
    older = make_job(owner, upload)
    newer = make_job(owner, upload)
    preview = make_job(owner, upload, kind="preview")
    assert lease(principal, lanes=["sql"]) is None
    first = lease(principal, engine_version="0.2.0", worker_version="0.1.0")
    assert first.job == older and first.job.attempt == 1
    assert (first.lease_seconds, first.stage_max_bytes) == (60, 2 * 1024**3)
    assert first.spec["job_id"] == str(older.id)
    running = Job.objects.get(pk=older.pk)
    assert running.status == JobStatus.RUNNING and running.lease_token == principal.token
    assert running.lease_expires_at > timezone.now() and running.started_at is not None
    assert lease(principal).job == newer
    assert lease(principal, lanes=["batch", "interactive"]).job == preview
    assert lease(principal, lanes=["batch", "interactive"]) is None
    worker = Worker.objects.get(worker_id="w-1")  # as it described itself last
    assert (worker.engine_version, worker.lanes) == ("", ["batch", "interactive"])
    assert worker.last_seen_at >= worker.first_seen_at and worker.token == principal.token
    assert principal.token.__class__.objects.get(pk=principal.token.pk).last_used_at


def test_lease_only_hands_out_spec_versions_the_worker_runs(principal):
    owner = make_user(Role.OPERATOR)
    make_job(owner, make_upload(owner))
    assert queue.lease(principal, worker_id="old", lanes=["batch"], spec_versions=[0]) is None
    Job.objects.update(spec_version=2)
    assert queue.lease(principal, worker_id="new", lanes=["batch"], spec_versions=[1, 2]) is None


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"worker_id": ""}, "worker_id must be"),
        ({"worker_id": "-x"}, "worker_id must be"),
        ({"lanes": []}, "lanes must name"),
        ({"lanes": ["gpu"]}, "lanes must name"),
        ({"spec_versions": []}, "spec_versions must list"),
        ({"spec_versions": [True]}, "spec_versions must list"),
        ({"spec_versions": ["1"]}, "spec_versions must list"),
    ],
)
def test_lease_request_validation(principal, fields, message):
    with pytest.raises(InvalidRequest, match=message):
        queue.lease(
            principal, **{"worker_id": "w", "lanes": ["batch"], "spec_versions": [1], **fields}
        )


def test_jobs_whose_spec_cannot_be_rendered_fail_and_the_next_one_is_leased(principal):
    owner = make_user(Role.OPERATOR)
    gone = make_upload(owner, status="deleted")
    broken = make_job(owner, gone)
    good = make_job(owner, make_upload(owner))
    leased = lease(principal)
    assert leased.job == good
    failed = Job.objects.get(pk=broken.pk)
    assert (failed.status, failed.error_code) == (JobStatus.FAILED, "INPUT_UNREADABLE")
    assert "no longer available" in failed.error_message


def test_a_lease_call_gives_up_after_many_broken_jobs(principal, monkeypatch):
    owner = make_user(Role.OPERATOR)
    gone = make_upload(owner, status="deleted")
    monkeypatch.setattr(queue, "MAX_RENDER_FAILURES", 2)
    for _ in range(3):
        make_job(owner, gone)
    assert lease(principal) is None
    assert Job.objects.filter(status=JobStatus.FAILED).count() == 2
    assert Job.objects.filter(status=JobStatus.QUEUED).count() == 1


# --------------------------------------------------------------------------- heartbeats


def test_heartbeats_extend_the_lease_record_progress_and_carry_cancel(principal):
    world = World.build()
    leased = lease(principal)
    job = leased.job
    Job.objects.filter(pk=job.pk).update(lease_expires_at=timezone.now() + timedelta(seconds=5))
    reply = queue.heartbeat(
        principal, job.pk, attempt=1, progress={"rows_read": 10, "bytes_read": 2.5}
    )
    assert reply == queue.HeartbeatReply(lease_seconds=60, cancel=False)
    stored = Job.objects.get(pk=job.pk)
    assert stored.progress == {"rows_read": 10, "bytes_read": 2.5}
    assert stored.lease_expires_at > timezone.now() + timedelta(seconds=30)
    assert JobEvent.objects.filter(job=job, type="progress").count() == 1
    queue.heartbeat(principal, job.pk, attempt=1, progress={})  # no progress: no event
    assert JobEvent.objects.filter(job=job, type="progress").count() == 1
    Job.objects.filter(pk=job.pk).update(cancel_requested_at=timezone.now())
    assert queue.heartbeat(principal, job.pk, attempt=1, progress=None).cancel is True
    with pytest.raises(InvalidRequest, match="progress.rows must be a number"):
        queue.heartbeat(principal, job.pk, attempt=1, progress={"rows": "many"})
    with pytest.raises(InvalidRequest, match="progress must be an object"):
        queue.heartbeat(principal, job.pk, attempt=1, progress=[1])
    assert world.queued_job.pk == job.pk


def test_calls_about_a_lease_the_worker_lost_answer_409(principal, worker_token):
    World.build()
    leased = lease(principal)
    job = leased.job
    with pytest.raises(Conflict) as raised:
        queue.heartbeat(principal, job.pk, attempt=2, progress={})
    assert raised.value.code == "lease_lost" and "attempt 2" in raised.value.message
    other_token, _ = tokens.create(type(principal.token), tokens.WORKER_TOKEN_PREFIX, name="b")
    with pytest.raises(Conflict):
        queue.heartbeat(WorkerPrincipal(other_token), job.pk, attempt=1, progress={})
    with pytest.raises(NotFound):
        queue.heartbeat(principal, "00000000-0000-0000-0000-000000000000", attempt=1, progress={})


# --------------------------------------------------------------------------- expiry


def test_expired_leases_return_to_the_queue_until_attempts_run_out(principal, admin_actor):
    installation.update(admin_actor, {"max_attempts": 2})
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    Job.objects.filter(pk=job.pk).update(max_attempts=2)
    assert lease(principal).job.attempt == 1
    Job.objects.filter(pk=job.pk).update(lease_expires_at=timezone.now() - timedelta(seconds=1))
    assert queue.requeue_expired_leases() == {"requeued": 1, "failed": 0, "cancelled": 0}
    requeued = Job.objects.get(pk=job.pk)
    assert requeued.status == JobStatus.QUEUED and requeued.lease_expires_at is None
    assert JobEvent.objects.filter(job=job, payload__reason="lease_expired").exists()
    with pytest.raises(Conflict):  # the first attempt's worker has lost it
        queue.heartbeat(principal, job.pk, attempt=1, progress={})

    second = lease(principal, worker_id="w-2")  # the requeue happens inside the lease call too
    assert second.job.attempt == 2 and second.spec["job_id"] == str(job.pk)
    Job.objects.filter(pk=job.pk).update(lease_expires_at=timezone.now() - timedelta(seconds=1))
    assert lease(principal) is None  # expired on its last attempt: failed, not requeued
    failed = Job.objects.get(pk=job.pk)
    assert (failed.status, failed.error_code) == (JobStatus.FAILED, "LEASE_EXPIRED")
    assert "attempt 2 of 2" in failed.error_message and "w-2" in failed.error_message
    assert queue.requeue_expired_leases() == {"requeued": 0, "failed": 0, "cancelled": 0}


def test_an_expired_lease_of_a_cancelled_job_cancels_it(principal):
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    lease(principal)
    Job.objects.filter(pk=job.pk).update(
        lease_expires_at=timezone.now() - timedelta(seconds=1),
        cancel_requested_at=timezone.now(),
        lease_worker=None,
    )
    assert queue.requeue_expired_leases()["cancelled"] == 1
    cancelled = Job.objects.get(pk=job.pk)
    assert (cancelled.status, cancelled.error_code) == (JobStatus.CANCELLED, "CANCELLED")


def test_lease_expiry_with_an_unknown_worker(principal):
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    lease(principal)
    Job.objects.filter(pk=job.pk).update(
        lease_expires_at=timezone.now() - timedelta(seconds=1), lease_worker=None, max_attempts=1
    )
    queue.requeue_expired_leases()
    assert "worker unknown" in Job.objects.get(pk=job.pk).error_message


# --------------------------------------------------------------------------- outputs


def test_presign_outputs(principal, settings):
    World.build()
    job = lease(principal).job
    uploads = queue.presign_outputs(
        principal,
        job.pk,
        attempt=1,
        files=[
            {"name": "data.parquet", "bytes": 10},
            {"name": "parts/part-0.parquet", "bytes": 0},
        ],
    )
    assert [u["key"] for u in uploads] == [
        f"jobs/{job.pk}/attempt-1/data.parquet",
        f"jobs/{job.pk}/attempt-1/parts/part-0.parquet",
    ]
    assert all(u["method"] == "PUT" and u["headers"] == {} for u in uploads)
    put_url(uploads[0]["url"], b"0123456789")


def test_output_urls_are_signed_for_the_worker_endpoint(principal, settings):
    World.build()
    job = lease(principal).job
    settings.FORKLIFT_STORE = {
        **settings.FORKLIFT_STORE,
        "worker_endpoint_url": "http://rustfs.internal:9000",
    }
    storage.reset_store()
    try:
        [upload] = queue.presign_outputs(
            principal, job.pk, attempt=1, files=[{"name": "data.parquet", "bytes": 1}]
        )
        assert upload["url"].startswith("http://rustfs.internal:9000/")
    finally:
        storage.reset_store()


@pytest.mark.parametrize(
    "files,message",
    [
        ([], "files must list 1 to 100"),
        ([{"name": f"f{i}", "bytes": 1} for i in range(101)], "files must list 1 to 100"),
        ([{"name": "../x", "bytes": 1}], "must be a relative path"),
        ([{"name": "/abs", "bytes": 1}], "must be a relative path"),
        ([{"name": "a/./b", "bytes": 1}], "must be a relative path"),
        ([{"name": "a b", "bytes": 1}], "must be a relative path"),
        ([{"name": "x" * 513, "bytes": 1}], "must be a relative path"),
        ([{"name": 5, "bytes": 1}], "must be a relative path"),
        ([{"name": "a", "bytes": -1}], "must be a size in bytes"),
        ([{"name": "a", "bytes": True}], "must be a size in bytes"),
        ([{"name": "a", "bytes": 6 * 1024**3}], "at most 5368709120 bytes"),
    ],
)
def test_presign_validation(principal, files, message):
    World.build()
    job = lease(principal).job
    with pytest.raises(InvalidRequest, match=message):
        queue.presign_outputs(principal, job.pk, attempt=1, files=files)


def test_presign_needs_the_lease(principal):
    World.build()
    job = lease(principal).job
    with pytest.raises(Conflict):
        queue.presign_outputs(principal, job.pk, attempt=3, files=[{"name": "a", "bytes": 1}])


def test_refresh_input(principal):
    world = World.build()
    job = lease(principal).job
    location = queue.refresh_input(principal, job.pk, attempt=1)
    assert location["type"] == "presigned_url" and location["size"] == world.upload.size
    world.upload.status = "deleted"
    world.upload.save()
    with pytest.raises(Gone) as raised:  # not 409: workers read that as a lost lease
        queue.refresh_input(principal, job.pk, attempt=1)
    assert raised.value.code == "input_unreadable"


# --------------------------------------------------------------------------- completion


def test_complete_records_result_and_artifacts(principal):
    World.build()
    job = lease(principal).job
    reported = [
        upload_output(job, "data.parquet", b"PAR1", "data"),
        upload_output(job, "manifest.json", b"{}", "manifest"),
    ]
    reported[1]["sha256"] = None
    done = queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), reported), artifacts=reported
    )
    assert done.status == JobStatus.SUCCEEDED and done.error_code == ""
    assert done.lease_expires_at is None and done.finished_at is not None
    assert done.result["counts"]["valid_rows"] == 1
    artifacts = {a.name: a for a in Artifact.objects.filter(job=job)}
    assert artifacts["data.parquet"].bytes == 4 and artifacts["manifest.json"].sha256 == ""
    again = queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), reported), artifacts=reported
    )
    assert again.pk == job.pk and Artifact.objects.filter(job=job).count() == 2
    with pytest.raises(Conflict):
        queue.complete(principal, job.pk, attempt=2, result=job_result(str(job.pk)), artifacts=[])


@pytest.mark.parametrize(
    "status,error,expected",
    [
        (
            "failed",
            {
                "code": "BAD_ROWS_THRESHOLD_EXCEEDED",
                "message": "12 of 20 rows were bad",
                "retryable": False,
            },
            JobStatus.FAILED,
        ),
        (
            "cancelled",
            {"code": "CANCELLED", "message": "Cancelled", "retryable": False},
            JobStatus.CANCELLED,
        ),
    ],
)
def test_failed_and_cancelled_results(principal, status, error, expected):
    World.build()
    job = lease(principal).job
    bad_rows = [upload_output(job, "bad_rows.parquet", b"PAR1", "bad_rows")]
    done = queue.complete(
        principal,
        job.pk,
        attempt=1,
        result=job_result(str(job.pk), bad_rows, status=status, error=error),
        artifacts=bad_rows,
    )
    assert (done.status, done.error_code, done.error_message) == (
        expected,
        error["code"],
        error["message"],
    )
    assert Artifact.objects.filter(job=job, kind="bad_rows").exists()  # kept on failure


def test_complete_validation(principal):
    World.build()
    job = lease(principal).job
    good = upload_output(job, "data.parquet", b"PAR1", "data")
    result = job_result(str(job.pk), [good])
    cases = [
        ({"result": "x", "artifacts": []}, "result must be a JobResult"),
        ({"result": {**result, "status": "great"}, "artifacts": []}, "status: fails 'enum'"),
        ({"result": {**result, "job_id": "other"}, "artifacts": []}, "result.job_id must be"),
        ({"result": result, "artifacts": [{**good, "kind": "video"}]}, "has kind 'video'"),
        (
            {"result": result, "artifacts": [{**good, "key": "jobs/x/attempt-1/data.parquet"}]},
            "must be reported once, at key",
        ),
        ({"result": result, "artifacts": [good, good]}, "must be reported once"),
        ({"result": result, "artifacts": [{**good, "sha256": "XYZ"}]}, "64 lower-case hex"),
        ({"result": result, "artifacts": [{**good, "bytes": 5}]}, "the store has 4 bytes"),
        (
            {
                "result": result,
                "artifacts": [
                    {
                        **good,
                        "name": "missing.parquet",
                        "key": good["key"].replace("data", "missing"),
                    }
                ],
            },
            "the store has nothing",
        ),
        ({"result": result, "artifacts": [good] * 101}, "at most 100 artifacts"),
    ]
    for call, message in cases:
        with pytest.raises(InvalidRequest, match=message):
            queue.complete(principal, job.pk, attempt=1, **call)
    assert Job.objects.get(pk=job.pk).status == JobStatus.RUNNING


def test_oversized_results_are_refused(principal, monkeypatch):
    World.build()
    job = lease(principal).job
    monkeypatch.setattr(queue, "MAX_RESULT_BYTES", 100)
    with pytest.raises(InvalidRequest, match="larger than 100 bytes"):
        queue.complete(principal, job.pk, attempt=1, result=job_result(str(job.pk)), artifacts=[])


# --------------------------------------------------------------------------- publishing


def published_dataset(world: World, **connection_config):
    destination = s3_connection(prefix="published", **connection_config)
    Dataset.objects.filter(pk=world.dataset.pk).update(
        destination_connection=destination, destination_prefix="people"
    )
    job = make_job(world.operator, world.upload, dataset=world.dataset)
    Job.objects.filter(pk=world.queued_job.pk).delete()
    return job


def test_successful_runs_are_published_manifest_last(principal, s3, monkeypatch):
    world = World.build()
    job = published_dataset(world)
    leased = lease(principal).job
    assert leased == job
    outputs = [
        upload_output(leased, "manifest.json", b"{}", "manifest"),
        upload_output(leased, "data.parquet", b"PAR1", "data"),
        upload_output(leased, "metadata.json", b"{}", "metadata"),
    ]
    copied = []
    real_copy = storage.Bucket.copy_from

    def recording_copy(self, source, source_key, key):
        copied.append(key)
        return real_copy(self, source, source_key, key)

    monkeypatch.setattr(storage.Bucket, "copy_from", recording_copy)
    done = queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), outputs), artifacts=outputs
    )
    assert done.status == JobStatus.SUCCEEDED
    base = f"published/people/{job.pk}"
    assert copied == [f"{base}/data.parquet", f"{base}/metadata.json", f"{base}/manifest.json"]
    head = s3.head_object(Bucket=storage.store().bucket, Key=f"{base}/data.parquet")
    assert head["ContentLength"] == 4
    log = JobEvent.objects.get(job=job, type="log").payload
    assert log == {
        "published_to": Dataset.objects.get(pk=world.dataset.pk).destination_connection.name,
        "prefix": base,
        "files": 3,
    }


def test_a_failed_publish_fails_the_job_and_keeps_the_artifacts(principal):
    world = World.build()
    job = published_dataset(world, bucket="forklift-web-no-such-bucket")
    leased = lease(principal).job
    outputs = [upload_output(leased, "data.parquet", b"PAR1", "data")]
    done = queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), outputs), artifacts=outputs
    )
    assert (done.status, done.error_code) == (JobStatus.FAILED, "TARGET_WRITE_FAILED")
    assert "publishing to" in done.error_message and "kept as job artifacts" in done.error_message
    assert Artifact.objects.filter(job=job).count() == 1


@pytest.mark.parametrize(
    "failure, reason",
    [
        (
            secret_backend.SecretError(
                "A connection secret could not be decrypted with any key in FORKLIFT_SECRETS_KEYS."
            ),
            "A connection secret could not be decrypted",
        ),
        (RuntimeError("boom, with a password=hunter2"), "An unexpected RuntimeError stopped it"),
    ],
)
def test_any_failure_to_publish_fails_the_job_and_announces_it(
    principal, monkeypatch, caplog, failure, reason
):
    world = World.build()
    job = published_dataset(world)
    leased = lease(principal).job
    outputs = [upload_output(leased, "data.parquet", b"PAR1", "data")]

    def fail(connection):
        raise failure

    monkeypatch.setattr(queue.connections, "bucket_of", fail)
    announced = []
    monkeypatch.setattr(queue.webhooks, "job_finished", lambda job: announced.append(job.status))
    report = {"result": job_result(str(job.pk), outputs), "artifacts": outputs}
    done = queue.complete(principal, job.pk, attempt=1, **report)
    assert (done.status, done.error_code) == (JobStatus.FAILED, "TARGET_WRITE_FAILED")
    assert reason in done.error_message and "kept as job artifacts" in done.error_message
    assert announced == [JobStatus.FAILED]
    assert Artifact.objects.filter(job=job).count() == 1
    assert "hunter2" not in caplog.text and "hunter2" not in done.error_message
    again = queue.complete(principal, job.pk, attempt=1, **report)  # the worker's retry
    assert again.status == JobStatus.FAILED and announced == [JobStatus.FAILED]


def test_runs_without_an_s3_destination_are_not_published(principal, admin_actor, monkeypatch):
    world = World.build()
    from forklift_web.services import connections

    db = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "p"},
    )
    Dataset.objects.filter(pk=world.dataset.pk).update(
        destination_connection=db, destination_options={"table": "t", "mode": "append"}
    )
    job = make_job(world.operator, world.upload, dataset=world.dataset)
    Job.objects.filter(pk=job.pk).update(lane="sql")
    monkeypatch.setattr(storage.Bucket, "copy_from", lambda *a: pytest.fail("copied"))
    leased = queue.lease(principal, worker_id="sql-1", lanes=["sql"], spec_versions=[1])
    assert leased.job == job
    done = queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk)), artifacts=[]
    )
    assert done.status == JobStatus.SUCCEEDED
    plain = lease(principal).job  # no dataset at all
    assert (
        queue.complete(
            principal, plain.pk, attempt=1, result=job_result(str(plain.pk)), artifacts=[]
        ).status
        == JobStatus.SUCCEEDED
    )


def test_store_errors_while_checking_artifacts_surface(principal, monkeypatch):
    World.build()
    job = lease(principal).job

    def unavailable(self, key):
        raise StoreUnavailable("The object store refused or failed HEAD of x (timeout).")

    monkeypatch.setattr(storage.Bucket, "head", unavailable)
    with pytest.raises(StoreUnavailable):
        queue.complete(
            principal,
            job.pk,
            attempt=1,
            result=job_result(str(job.pk)),
            artifacts=[
                {"kind": "data", "name": "d", "key": f"jobs/{job.pk}/attempt-1/d", "bytes": 1}
            ],
        )


# --------------------------------------------------------------------------- concurrency


def _in_thread(target, results, index, *args):
    try:
        results[index] = target(*args)
    except BaseException as error:  # reported by the test
        results[index] = error
    finally:
        connection.close()


@pytest.mark.django_db(transaction=True)
def test_two_workers_never_lease_the_same_job(worker_token):
    token, _ = worker_token
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner)
    created = {make_job(owner, upload).pk for _ in range(24)}
    start = threading.Barrier(4)

    def drain(worker_id):
        principal = WorkerPrincipal(token=token)
        start.wait()
        leased = []
        while True:
            got = queue.lease(principal, worker_id=worker_id, lanes=["batch"], spec_versions=[1])
            if got is None:
                return leased
            leased.append((got.job.pk, got.job.attempt))

    results = [None] * 4
    threads = [
        threading.Thread(target=_in_thread, args=(drain, results, i, f"w-{i}")) for i in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not any(isinstance(result, BaseException) for result in results), results
    leased = [pk for result in results for pk, _ in result]
    assert len(leased) == len(set(leased)) == 24 and set(leased) == created
    assert {attempt for result in results for _, attempt in result} == {1}
    assert Job.objects.filter(status=JobStatus.RUNNING).count() == 24


@pytest.mark.django_db(transaction=True)
def test_a_job_locked_by_one_lease_is_skipped_by_another(worker_token):
    token, _ = worker_token
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner)
    first, second = make_job(owner, upload), make_job(owner, upload)
    locked, release = threading.Event(), threading.Event()

    def hold_the_first_job():
        with transaction.atomic():
            Job.objects.select_for_update().get(pk=first.pk)
            locked.set()
            release.wait(timeout=30)

    results = [None]
    holder = threading.Thread(target=_in_thread, args=(lambda: hold_the_first_job(), results, 0))
    holder.start()
    try:
        assert locked.wait(timeout=30)
        got = queue.lease(
            WorkerPrincipal(token=token), worker_id="w", lanes=["batch"], spec_versions=[1]
        )
        assert (
            got.job.pk == second.pk
        )  # SKIP LOCKED: the locked row is passed over, not waited for
    finally:
        release.set()
        holder.join()
    assert Job.objects.get(pk=first.pk).status == JobStatus.QUEUED
