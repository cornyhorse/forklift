"""Publishing a successful run to its dataset's s3 destination: complete leaves the job
publishing, and a publisher copies the outputs under a lease, resumes what a dead publisher
left, backs off after store errors and ends the job succeeded or failed, announced once."""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
import uuid
from datetime import timedelta
from io import StringIO

import pytest
from conftest import put_url
from django.core.management import CommandError, call_command
from django.db import DatabaseError, connection
from django.urls import reverse
from django.utils import timezone
from world import World, job_result, make_job, s3_connection

from forklift_web import secret_backend, storage
from forklift_web.core.choices import JobStatus
from forklift_web.core.management.commands import publish as publish_command
from forklift_web.core.models import Dataset, InstallationSetting, Job, JobEvent
from forklift_web.errors import Conflict, StoreUnavailable
from forklift_web.policy import Actor
from forklift_web.services import installation, jobs, publishing, queue, retention, webhooks
from forklift_web.services.workers import WorkerPrincipal

pytestmark = pytest.mark.django_db

OUTPUTS = (("manifest.json", b"{}", "manifest"), ("data.parquet", b"PAR1", "data"))


@pytest.fixture
def principal(worker_token):
    token, _ = worker_token
    return WorkerPrincipal(token=token)


@pytest.fixture
def announced(monkeypatch):
    """(job id, status) of every job its webhooks are told about."""
    calls, real = [], webhooks.job_finished

    def record(job):
        calls.append((job.pk, job.status))
        return real(job)

    monkeypatch.setattr(webhooks, "job_finished", record)
    return calls


@pytest.fixture
def copies(monkeypatch):
    """The destination keys copied to (the real copy runs)."""
    copied, real = [], storage.Bucket.copy_from

    def record(self, source, source_key, key, **kwargs):
        copied.append(key)
        return real(self, source, source_key, key, **kwargs)

    monkeypatch.setattr(storage.Bucket, "copy_from", record)
    return copied


def setting(**values) -> None:
    for key, value in values.items():
        InstallationSetting.objects.update_or_create(key=key, defaults={"value": value})


def lease(principal):
    return queue.lease(principal, worker_id="w-1", lanes=["batch"], spec_versions=[1]).job


def upload_output(job: Job, name: str, data: bytes, kind: str) -> dict:
    key = job.attempt_prefix + name
    put_url(storage.store().presign_put(key, expires=60, audience=storage.Audience.GATEWAY), data)
    return {
        "kind": kind,
        "name": name,
        "key": key,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "rows": 1,
    }


def destined_job(**connection_config) -> Job:
    """A queued run of a dataset that publishes to an s3 destination (the only queued job)."""
    world = World.build()
    destination = s3_connection(prefix="published", **connection_config)
    Dataset.objects.filter(pk=world.dataset.pk).update(
        destination_connection=destination, destination_prefix="people"
    )
    Job.objects.filter(pk=world.queued_job.pk).delete()
    return make_job(world.operator, world.upload, dataset=world.dataset)


def complete_run(principal, job: Job) -> Job:
    leased = lease(principal)
    assert leased == job
    outputs = [upload_output(leased, name, data, kind) for name, data, kind in OUTPUTS]
    return queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), outputs), artifacts=outputs
    )


@pytest.fixture
def run(principal):
    """A run its worker completed, waiting for the publisher."""
    job = destined_job()
    done = complete_run(principal, job)
    assert done.status == JobStatus.PUBLISHING
    return done


def later(seconds: float):
    return timezone.now() + timedelta(seconds=seconds)


def fresh(job: Job) -> Job:
    return Job.objects.get(pk=job.pk)


# --------------------------------------------------------------------------- complete


def test_complete_leaves_a_destined_run_publishing_and_copies_nothing(
    principal, monkeypatch, announced
):
    monkeypatch.setattr(storage.Bucket, "copy_from", lambda *a, **k: pytest.fail("copied"))
    job = destined_job()
    done = complete_run(principal, job)
    stored = fresh(job)
    assert (stored.status, stored.finished_at, stored.lease_expires_at) == (
        JobStatus.PUBLISHING,
        None,
        None,
    )
    assert stored.publish_attempts == 0 and stored.publish_lease_token is None
    assert announced == [] and stored.artifacts.count() == 2
    state = JobEvent.objects.filter(job=job, type="state").last().payload
    assert state == {
        "status": "publishing",
        "destination": stored.dataset.destination_connection.name,
    }
    events = JobEvent.objects.filter(job=job).count()
    again = queue.complete(  # the worker's retry: answered, nothing started
        principal, job.pk, attempt=1, result=done.result, artifacts=[]
    )
    assert again.status == JobStatus.PUBLISHING and fresh(job).publish_attempts == 0
    assert JobEvent.objects.filter(job=job).count() == events


def test_only_runs_are_published(principal, announced):
    job = destined_job()
    Job.objects.filter(pk=job.pk).update(kind="preview")
    leased = lease(principal)
    done = queue.complete(
        principal, leased.pk, attempt=1, result=job_result(str(job.pk)), artifacts=[]
    )
    assert done.status == JobStatus.SUCCEEDED and announced == [(job.pk, JobStatus.SUCCEEDED)]


# --------------------------------------------------------------------------- a pass


def test_a_pass_publishes_manifest_last_and_announces_once(run, s3, copies, announced):
    heartbeats = []
    counts = publishing.publish_due(heartbeat=lambda: heartbeats.append(1))
    assert counts == {"published": 1, "retrying": 0, "failed": 0, "lost": 0}
    base = f"published/people/{run.pk}"
    assert copies == [f"{base}/data.parquet", f"{base}/manifest.json"]
    head = s3.head_object(Bucket=storage.store().bucket, Key=f"{base}/data.parquet")
    assert head["ContentLength"] == 4
    job = fresh(run)
    assert job.status == JobStatus.SUCCEEDED and job.finished_at is not None
    assert (job.publish_attempts, job.publish_lease_token, job.publish_error) == (1, None, "")
    log = JobEvent.objects.get(job=job, type="log").payload
    assert log == {
        "published_to": job.dataset.destination_connection.name,
        "prefix": base,
        "files": 2,
    }
    assert announced == [(job.pk, JobStatus.SUCCEEDED)] and len(heartbeats) == 2
    assert publishing.publish_due() == {"published": 0, "retrying": 0, "failed": 0, "lost": 0}
    assert announced == [(job.pk, JobStatus.SUCCEEDED)]


def test_a_publisher_that_died_is_resumed_once_its_lease_expired(run, copies, announced):
    claimed = publishing._claim(timezone.now(), installation.current(), [])
    assert claimed is not None and fresh(run).publish_attempts == 1  # ... and it dies here
    assert publishing.publish_due() == {"published": 0, "retrying": 0, "failed": 0, "lost": 0}
    counts = publishing.publish_due(now=later(301))
    assert counts["published"] == 1 and len(copies) == 2
    job = fresh(run)
    assert job.status == JobStatus.SUCCEEDED and job.publish_attempts == 2
    assert announced == [(job.pk, JobStatus.SUCCEEDED)]


def test_a_lease_is_counted_from_its_claim_not_from_the_start_of_a_long_pass(run):
    # A pass that started an hour ago (it was copying other jobs) still leases for a full term
    publishing._claim(later(-3600), installation.current(), [])
    expires = fresh(run).publish_lease_expires_at
    assert expires >= later(installation.current()["publish_lease_seconds"] - 5)


def test_a_store_error_backs_off_and_a_later_pass_publishes(run, monkeypatch, announced):
    real, calls = storage.Bucket.copy_from, []

    def flaky(self, source, source_key, key, **kwargs):
        calls.append(key)
        if len(calls) == 1:
            raise StoreUnavailable("The object store refused or failed copying to x (SlowDown).")
        return real(self, source, source_key, key, **kwargs)

    monkeypatch.setattr(storage.Bucket, "copy_from", flaky)
    started = timezone.now()
    assert publishing.publish_due()["retrying"] == 1
    job = fresh(run)
    assert job.status == JobStatus.PUBLISHING and job.publish_lease_token is None
    assert job.publish_error == "The object store refused or failed copying to x (SlowDown)."
    assert started + timedelta(seconds=59) < job.publish_lease_expires_at < later(61)
    retry = JobEvent.objects.filter(job=job, type="log").last().payload
    assert retry == {"publish_attempt": 1, "retry_in_seconds": 60}
    assert publishing.publish_due()["retrying"] == 0  # not before the backoff is over
    assert publishing.publish_due(now=later(61))["published"] == 1
    job = fresh(run)
    assert (job.status, job.publish_attempts, job.publish_error) == (JobStatus.SUCCEEDED, 2, "")
    assert announced == [(job.pk, JobStatus.SUCCEEDED)]


def test_backoffs_grow(run, monkeypatch):
    def unavailable(*args, **kwargs):
        raise StoreUnavailable("The object store refused or failed copying to x (timeout).")

    monkeypatch.setattr(storage.Bucket, "copy_from", unavailable)
    setting(publish_max_attempts=20)
    when, delays = timezone.now(), []
    for _ in range(5):
        publishing.publish_due(now=when)
        when = fresh(run).publish_lease_expires_at
        delays.append(JobEvent.objects.filter(job=run, type="log").last().payload)
    assert [d["retry_in_seconds"] for d in delays] == [60, 300, 1800, 7200, 7200]


def test_a_job_out_of_attempts_fails_and_is_announced(run, monkeypatch, announced):
    def unavailable(*args, **kwargs):
        raise StoreUnavailable("The object store refused or failed copying to x (timeout).")

    monkeypatch.setattr(storage.Bucket, "copy_from", unavailable)
    setting(publish_max_attempts=2)
    assert publishing.publish_due()["retrying"] == 1
    assert publishing.publish_due(now=later(61))["failed"] == 1
    job = fresh(run)
    assert (job.status, job.error_code) == (JobStatus.FAILED, "TARGET_WRITE_FAILED")
    name = job.dataset.destination_connection.name
    assert job.error_message == (
        f"The run succeeded but publishing to {name!r} did not finish after 2 attempts: The "
        "object store refused or failed copying to x (timeout). The outputs are kept as job "
        "artifacts."
    )
    assert job.finished_at is not None and job.publish_lease_token is None
    assert announced == [(job.pk, JobStatus.FAILED)] and job.artifacts.count() == 2


@pytest.mark.parametrize("token", [True, False])
def test_a_dead_publisher_on_the_last_attempt_fails_the_job(run, announced, token):
    setting(publish_max_attempts=1)
    publishing._claim(timezone.now(), installation.current(), [])  # ... and it dies here
    if not token:  # its attempt ended with a store error, and an admin lowered the limit
        Job.objects.filter(pk=run.pk).update(publish_lease_token=None, publish_error="Slow.")
    assert publishing.publish_due(now=later(301))["failed"] == 1
    job = fresh(run)
    reason = publishing.STOPPED if token else "Slow."
    assert f"did not finish after 1 attempt: {reason}" in job.error_message
    assert announced == [(job.pk, JobStatus.FAILED)]


@pytest.mark.parametrize(
    "failure, reason",
    [
        (
            secret_backend.SecretError(
                "A connection secret could not be decrypted with any key in FORKLIFT_SECRETS_KEYS."
            ),
            "A connection secret could not be decrypted",
        ),
        (RuntimeError("boom, with a password=hunter2"), "An unexpected RuntimeError stopped it."),
    ],
)
def test_other_errors_fail_the_job_at_once(run, monkeypatch, caplog, announced, failure, reason):
    def fail(connection):
        raise failure

    monkeypatch.setattr(publishing.connections, "bucket_of", fail)
    assert publishing.publish_due()["failed"] == 1
    job = fresh(run)
    assert (job.status, job.error_code, job.publish_attempts) == (
        JobStatus.FAILED,
        "TARGET_WRITE_FAILED",
        1,
    )
    assert reason in job.error_message and "kept as job artifacts" in job.error_message
    assert announced == [(job.pk, JobStatus.FAILED)]
    assert "hunter2" not in caplog.text and "hunter2" not in job.error_message


def test_a_run_whose_dataset_lost_its_destination_fails(run, announced):
    Dataset.objects.filter(pk=run.dataset_id).update(destination_connection=None)
    assert publishing.publish_due()["failed"] == 1
    job = fresh(run)
    assert job.error_message == (
        "The run succeeded but publishing failed: Its dataset has no s3 destination now. The "
        "outputs are kept as job artifacts."
    )
    assert announced == [(job.pk, JobStatus.FAILED)]


def test_a_missing_destination_bucket_is_a_store_error(principal, announced):
    setting(publish_max_attempts=1)
    complete_run(principal, destined_job(bucket="forklift-web-no-such-bucket"))
    assert publishing.publish_due()["failed"] == 1
    [(_, status)] = announced
    assert status == JobStatus.FAILED


# --------------------------------------------------------------------------- the lease


@pytest.fixture
def renew_every_tick(monkeypatch):
    """Every call to keep the lease renews it, and a long copy keeps it every few ms."""
    ticks = iter(range(10**6))
    monkeypatch.setattr(publishing, "_monotonic", lambda: next(ticks) * 1000.0)
    monkeypatch.setattr(publishing, "TICK_SECONDS", 0.01)


def slow_copy(monkeypatch, seconds: float, progress_calls: list):
    """copy_from takes ``seconds``, reporting progress as it goes (stopping when it raises)."""

    def copy(self, source, source_key, key, *, progress=None):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            progress(1)
            progress_calls.append(key)
            time.sleep(0.005)

    monkeypatch.setattr(storage.Bucket, "copy_from", copy)


def test_the_lease_is_renewed_and_the_heartbeat_touched_during_a_long_copy(
    run, monkeypatch, renew_every_tick
):
    calls: list = []
    slow_copy(monkeypatch, 0.2, calls)
    leases = []  # the lease as each heartbeat finds it (each keep renews after its heartbeat)

    def heartbeat():
        leases.append(fresh(run).publish_lease_expires_at)

    assert publishing.publish_due(heartbeat=heartbeat)["published"] == 1
    assert len(leases) > 6, "a few heartbeats per copy, not just between the copies"
    assert len(set(leases)) == len(leases) and leases == sorted(leases), "renewed each time"


def test_a_publisher_that_lost_its_lease_stops_without_changing_the_job(
    run, monkeypatch, renew_every_tick, announced
):
    calls: list = []
    slow_copy(monkeypatch, 5, calls)
    other = uuid.uuid4()

    def heartbeat():  # on the second call, mid-copy, another publisher has taken the job
        heartbeat.calls += 1
        if heartbeat.calls == 2:
            Job.objects.filter(pk=run.pk).update(publish_lease_token=other)

    heartbeat.calls = 0
    started = time.monotonic()
    assert publishing.publish_due(heartbeat=heartbeat)["lost"] == 1
    assert time.monotonic() - started < 4, "the copy stopped at once"
    job = fresh(run)
    assert (job.status, job.publish_lease_token, job.publish_attempts) == (
        JobStatus.PUBLISHING,
        other,
        1,
    )
    assert announced == [] and len(set(calls)) == 1  # the manifest was never started


# --------------------------------------------------------------------------- concurrency


def _in_thread(target, results, index):
    try:
        results[index] = target()
    except BaseException as error:  # reported by the test
        results[index] = error
    finally:
        connection.close()


@pytest.mark.django_db(transaction=True)
def test_two_publishers_publish_a_job_once(principal, monkeypatch, announced):
    complete_run(principal, destined_job())
    real, copied = storage.Bucket.copy_from, []

    def slow(self, source, source_key, key, **kwargs):
        copied.append(key)
        time.sleep(0.2)  # the other publisher looks while this one copies
        return real(self, source, source_key, key, **kwargs)

    monkeypatch.setattr(storage.Bucket, "copy_from", slow)
    start = threading.Barrier(2)

    def publisher():
        start.wait()
        return publishing.publish_due()

    results = [None, None]
    threads = [threading.Thread(target=_in_thread, args=(publisher, results, i)) for i in (0, 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not any(isinstance(result, BaseException) for result in results), results
    assert sum(result["published"] for result in results) == 1
    assert len(copied) == 2 and len(announced) == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("ending", ["copied", "store_error", "unexpected"])
def test_an_ending_after_another_publisher_took_over_changes_nothing(
    principal, monkeypatch, announced, ending
):
    job = complete_run(principal, destined_job())
    other = uuid.uuid4()

    def taken_over(self, source, source_key, key, **kwargs):
        try:  # this runs on the copy's thread, with a database connection of its own
            Job.objects.filter(pk=job.pk).update(publish_lease_token=other)
        finally:
            connection.close()
        if ending == "store_error":
            raise StoreUnavailable("The object store refused or failed copying to x (timeout).")
        if ending == "unexpected":
            raise RuntimeError("boom")

    monkeypatch.setattr(storage.Bucket, "copy_from", taken_over)
    monkeypatch.setattr(publishing, "_monotonic", lambda: 0.0)  # never due for renewal
    assert publishing.publish_due()["lost"] == 1
    stored = fresh(job)
    assert (stored.status, stored.publish_lease_token, stored.publish_error) == (
        JobStatus.PUBLISHING,
        other,
        "",
    )
    assert announced == []


# --------------------------------------------------------------------------- the store client


def test_a_progress_callback_that_raises_stops_a_copy(s3):
    """What the publisher relies on: an exception from the callback aborts a multipart copy
    (no object, no pending upload) and comes out of copy_from as it was raised."""
    bucket = storage.store()
    put_url(
        bucket.presign_put("jobs/big", expires=60, audience=storage.Audience.GATEWAY),
        os.urandom(9 * 1024 * 1024),  # above boto3's 8 MiB: copied in parts
    )

    class Stop(Exception):
        pass

    def stop(transferred):
        raise Stop()

    with pytest.raises(Stop):
        bucket.copy_from(bucket, "jobs/big", "published/big", progress=stop)
    assert bucket.head("published/big") is None
    assert bucket.list_multipart_uploads("published/") == []


# --------------------------------------------------------------------------- elsewhere


def test_a_publishing_job_cannot_be_cancelled(run, admin_actor, as_user, admin):
    with pytest.raises(Conflict, match="is publishing its outputs to its destination"):
        jobs.cancel_job(admin_actor, run.pk)
    answer = as_user(admin).post(f"/api/v1/jobs/{run.pk}/cancel")
    assert answer.status_code == 409 and "cannot be cancelled" in answer.json()["detail"]
    assert fresh(run).status == JobStatus.PUBLISHING and fresh(run).cancel_requested_at is None


def test_retention_keeps_the_artifacts_of_a_publishing_job(run, admin_actor):
    retention.set_policy(admin_actor, "installation", days={"data": 0, "metadata": 0})
    system = Actor.for_system("sweep_retention")
    kept = run.artifacts.filter(deleted_at=None)
    retention.sweep(system, now=later(60))
    assert kept.count() == 2, "a publisher still copies them"
    publishing.publish_due()
    retention.sweep(system, now=later(60))
    assert kept.count() == 0


def test_the_job_page_and_the_api_show_publishing(run, monkeypatch, as_user, admin):
    caller = as_user(admin)
    page = caller.get(reverse("ui:job", kwargs={"job_id": run.pk})).content.decode()
    name = run.dataset.destination_connection.name
    assert '<span class="badge publishing">Publishing</span>' in page
    assert f"Waiting for the publisher to copy the outputs to <strong>{name}</strong>" in page
    assert 'hx-trigger="every 2s"' in page, "the page keeps polling until the job ends"
    assert "Cancel the job" not in page
    shown = caller.get(f"/api/v1/jobs/{run.pk}").json()
    assert (shown["status"], shown["publish_attempts"], shown["publish_error"]) == (
        "publishing",
        0,
        None,
    )

    def unavailable(*args, **kwargs):
        raise StoreUnavailable("The object store refused or failed copying to x (SlowDown).")

    monkeypatch.setattr(storage.Bucket, "copy_from", unavailable)
    publishing.publish_due()
    live = caller.get(reverse("ui:job-live", kwargs={"job_id": run.pk})).content.decode()
    assert f"Publishing to <strong>{name}</strong> (attempt 1)" in live
    assert "The last attempt failed and is tried again later: The object store refused" in live
    shown = caller.get(f"/api/v1/jobs/{run.pk}").json()
    assert shown["publish_attempts"] == 1 and shown["publish_error"].endswith("(SlowDown).")
    assert caller.get("/api/v1/jobs?status=publishing").json()["items"][0]["id"] == str(run.pk)


def test_lists_and_overviews_show_publishing_jobs(run, as_user, admin):
    operator = as_user(run.requested_by)
    assert "still queued, running or publishing" in operator.get("/").content.decode()
    listed = operator.get(reverse("ui:jobs") + "?status=publishing").content.decode()
    assert str(run.pk) in listed and "Publishing" in listed
    overview = as_user(admin).get(reverse("ui:admin")).content.decode()
    assert "<strong>1</strong> publishing" in overview and str(run.pk) in overview


# --------------------------------------------------------------------------- forklift-web publish


def command(*args) -> tuple:
    out, err = StringIO(), StringIO()
    call_command("publish", *args, stdout=out, stderr=err)
    return out.getvalue(), err.getvalue()


def test_a_pass_told_to_stop_claims_no_further_run(principal, run, copies):
    second = complete_run(principal, make_job(run.requested_by, run.upload, dataset=run.dataset))
    checks = []

    def stop():
        checks.append(len(copies))
        return len(checks) > 1  # stop is asked for once the first run is being published

    counts = publishing.publish_due(stop=stop)

    assert counts == {"published": 1, "retrying": 0, "failed": 0, "lost": 0}
    statuses = {fresh(run).status, fresh(second).status}
    assert statuses == {JobStatus.SUCCEEDED, JobStatus.PUBLISHING}  # the other one waits
    assert publishing.publish_due(stop=lambda: True) == {
        "published": 0,
        "retrying": 0,
        "failed": 0,
        "lost": 0,
    }


def test_one_pass_of_the_command(run):
    out, err = command()
    assert out == "Published 1, retrying 0, failed 0, lost to another publisher 0.\n"
    assert err == "" and fresh(run).status == JobStatus.SUCCEEDED


def test_the_command_loops_and_touches_its_heartbeat(monkeypatch, tmp_path):
    heartbeat = tmp_path / "publisher.alive"
    sleeps, touched = [], []

    def sleep(stop, seconds):
        sleeps.append(seconds)
        touched.append(heartbeat.exists())

    monkeypatch.setattr(publish_command, "_sleep", sleep)
    out, _ = command("--every", "5", "--iterations", "3", "--heartbeat", str(heartbeat))
    assert out.count("Published 0") == 3
    assert sleeps == [5.0, 5.0] and touched == [True, True] and heartbeat.exists()


def test_the_loop_waits_between_passes():
    out, _ = command("--every", "0.001", "--iterations", "2")
    assert out.count("Published 0") == 2


def test_the_heartbeat_is_touched_while_a_pass_copies(run, monkeypatch, tmp_path):
    heartbeat = tmp_path / "publisher.alive"
    real = publishing.publish_due

    def pass_that_watches(**kwargs):
        kwargs["heartbeat"]()  # what a long copy does every few seconds
        assert heartbeat.exists(), "touched during the pass, before it ended"
        return real(**kwargs)

    monkeypatch.setattr(publishing, "publish_due", pass_that_watches)
    command("--heartbeat", str(heartbeat))


def test_a_failing_pass_does_not_stop_the_loop(monkeypatch, tmp_path, caplog):
    calls = []

    def flaky(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise DatabaseError("the database went away")
        return {"published": 0, "retrying": 0, "failed": 0, "lost": 0}

    monkeypatch.setattr(publishing, "publish_due", flaky)
    monkeypatch.setattr(publish_command, "_sleep", lambda stop, seconds: None)
    heartbeat = tmp_path / "publisher.alive"
    with caplog.at_level(logging.ERROR, logger="forklift_web.core.management.commands.publish"):
        with pytest.raises(CommandError, match="1 publishing passes failed; the log has why."):
            command("--every", "1", "--iterations", "2", "--heartbeat", str(heartbeat))
    assert len(calls) == 2 and heartbeat.exists()  # the second pass went on, and touched it
    assert "A publishing pass failed" in caplog.text


def test_a_heartbeat_that_cannot_be_touched_does_not_stop_publishing(run, tmp_path, caplog):
    heartbeat = tmp_path / "missing" / "publisher.alive"
    with caplog.at_level(logging.WARNING, logger="forklift_web.core.management.commands.publish"):
        out, _ = command("--heartbeat", str(heartbeat))
    assert out.startswith("Published 1") and fresh(run).status == JobStatus.SUCCEEDED
    assert "The heartbeat file cannot be touched" in caplog.text
