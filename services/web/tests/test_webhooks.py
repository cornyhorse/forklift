"""Webhooks: the signature and the payload, the outbox on every terminal transition of a job,
which webhooks hear about which jobs, the delivery loop (retries, backoff, disabling, the
permission check at send time, claims), managing webhooks, retention and the commands."""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
from datetime import timedelta
from io import StringIO

import pytest
from conftest import put_url
from django.core.management import CommandError, call_command
from django.db import connection, transaction
from django.utils import timezone
from webhook_support import (
    ALL_EVENTS,
    SECRET,
    FakeClient,
    Receiver,
    StubResolver,
    failing,
    make_webhook,
)
from world import World, job_result, make_job, make_upload, make_user

from forklift_web import policy, secret_backend, storage, webhook_client
from forklift_web.core.choices import (
    DeliveryStatus,
    JobKind,
    JobStatus,
    RetentionScope,
    Role,
    WebhookScope,
)
from forklift_web.core.models import (
    ApiToken,
    AuditLog,
    Dataset,
    Job,
    RetentionPolicy,
    Schedule,
    Webhook,
    WebhookDelivery,
)
from forklift_web.errors import Conflict, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Action, Actor
from forklift_web.services import installation, jobs, queue, retention, tokens, webhooks
from forklift_web.services.workers import WorkerPrincipal
from forklift_web.webhook_client import Outcome

pytestmark = pytest.mark.django_db


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def principal(worker_token):
    token, _ = worker_token
    return WorkerPrincipal(token=token)


@pytest.fixture
def receiver():
    servers = []

    def start(status=200) -> Receiver:
        servers.append(Receiver(status))
        return servers[-1]

    yield start
    for server in servers:
        server.stop()


def finish(job: Job, status: str = JobStatus.SUCCEEDED, **fields) -> int:
    """Finish ``job`` as the queue would, and run the outbox."""
    now = timezone.now()
    values = {"status": status, "attempt": 1, "started_at": now, "finished_at": now, **fields}
    for name, value in values.items():
        setattr(job, name, value)
    with transaction.atomic():
        job.save()
        return webhooks.job_finished(job)


def events_of(job: Job) -> list:
    return sorted(WebhookDelivery.objects.filter(job=job).values_list("event", flat=True))


def payload_of(delivery: WebhookDelivery) -> dict:
    return json.loads(delivery.payload)


def hidden(actor, obj):
    """An object rule that hides everything (a stricter policy than today's)."""
    raise PermissionDenied("Hidden by a stricter policy.", code="hidden")


def verify(secret: str, header: str, body: bytes, now: float, tolerance: int = 300) -> bool:
    """A receiver's check, as docs/platform/webhooks.md describes it (not the gateway's code)."""
    fields = dict(part.split("=", 1) for part in header.split(","))
    timestamp = int(fields["t"])
    expected = hmac.new(
        secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return abs(now - timestamp) <= tolerance and hmac.compare_digest(expected, fields["v1"])


# --------------------------------------------------------------------------- signing, payloads


def test_the_signature_is_hmac_sha256_of_the_timestamp_and_the_body():
    header = webhooks.signature("fkwh_test-secret", 1700000000, b'{"event":"webhook.test"}')
    assert header == (
        "t=1700000000,v1=3c22bcf0929bf0d9f474b649065ed199120f9ca85fda4fd2d7b98ecdcd60831a"
    )
    body = b'{"a":1}'
    assert verify("fkwh_x", webhooks.signature("fkwh_x", 1000, body), body, now=1000)
    assert not verify("fkwh_y", webhooks.signature("fkwh_x", 1000, body), body, now=1000)
    assert not verify("fkwh_x", webhooks.signature("fkwh_x", 1000, body), b'{"a":2}', now=1000)
    assert not verify("fkwh_x", webhooks.signature("fkwh_x", 1000, body), body, now=1301)


def test_a_finished_job_reaches_the_receiver_signed(world, settings, receiver):
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["receiver.test"]
    settings.FORKLIFT_PUBLIC_URL = "https://forklift.example.org"
    server = receiver(202)
    webhook, secret = webhooks.create_webhook(
        Actor.for_user(world.operator),
        name="ci",
        url=f"http://receiver.test:{server.port}/in?key=abc",
        events=["job.failed", "job.succeeded"],
    )
    job = make_job(world.operator, world.upload, dataset=world.dataset)
    job.result = job_result(str(job.id), status="succeeded")
    assert finish(job) == 1
    client = webhook_client.Client(resolver=StubResolver({"receiver.test": ["127.0.0.1"]}))
    before = time.time()
    assert webhooks.deliver_due(client=client) == {
        "delivered": 1,
        "retrying": 0,
        "failed": 0,
        "skipped": 0,
    }
    delivery = WebhookDelivery.objects.get(job=job)
    [request] = server.requests
    assert request.path == "/in?key=abc" and request.body == delivery.payload.encode()
    assert verify(secret, request.headers["forklift-signature"], request.body, time.time())
    assert int(request.headers["forklift-signature"].split(",")[0][2:]) >= int(before)
    assert request.headers["forklift-event"] == "job.succeeded"
    assert request.headers["forklift-delivery"] == str(delivery.id)
    assert request.headers["user-agent"] == "forklift-webhooks/0.1.0"
    assert request.headers["content-type"] == "application/json"
    assert (delivery.status, delivery.attempts, delivery.last_status_code) == ("delivered", 1, 202)
    assert delivery.delivered_at is not None and delivery.next_attempt_at is None
    assert payload_of(delivery) == {
        "id": str(delivery.id),
        "event": "job.succeeded",
        "created_at": delivery.created_at.isoformat()[:-6] + "Z",
        "webhook": {"id": str(webhook.id), "name": "ci"},
        "job": {
            "id": str(job.id),
            "kind": "run",
            "status": "succeeded",
            "dataset": {"id": str(world.dataset.id), "name": world.dataset.name},
            "schedule_id": None,
            "classification": "internal",
            "attempt": 1,
            "created_at": job.created_at.isoformat()[:-6] + "Z",
            "started_at": job.started_at.isoformat()[:-6] + "Z",
            "finished_at": job.finished_at.isoformat()[:-6] + "Z",
            "counts": {"total_rows": 2, "valid_rows": 1, "invalid_rows": 1, "truncated_rows": 0},
            "error": None,
            "url": f"https://forklift.example.org/api/v1/jobs/{job.id}",
            "artifacts_url": f"https://forklift.example.org/api/v1/jobs/{job.id}/artifacts",
        },
    }


def test_the_payload_of_a_failure_names_its_error_and_never_the_data(world, settings):
    settings.FORKLIFT_PUBLIC_URL = ""
    make_webhook(world.operator)
    schedule = Schedule.objects.create(dataset=world.dataset, cron="0 * * * *")
    job = make_job(world.operator, world.upload)
    job.schedule = schedule
    job.result = job_result(str(job.id), status="failed")
    finish(
        job,
        JobStatus.FAILED,
        error_code="BAD_ROWS_THRESHOLD_EXCEEDED",
        error_message="2 of 2 rows were rejected; " + "x" * 3000,
    )
    payload = payload_of(WebhookDelivery.objects.get(job=job))
    summary = payload["job"]
    assert summary["error"] == {
        "code": "BAD_ROWS_THRESHOLD_EXCEEDED",
        "message": ("2 of 2 rows were rejected; " + "x" * 3000)[:2000],
    }
    assert summary["schedule_id"] == str(schedule.id) and summary["dataset"] is None
    assert summary["url"] == f"/api/v1/jobs/{job.id}"  # no FORKLIFT_PUBLIC_URL: paths only
    text = WebhookDelivery.objects.get(job=job).payload
    for absent in ("spec", "upload", "validation_summary", "TYPE_MISMATCH", str(world.upload.id)):
        assert absent not in text


def test_a_job_without_a_result_has_no_counts(world):
    make_webhook(world.operator)
    job = make_job(world.operator, world.upload)
    finish(job, JobStatus.CANCELLED, error_code="CANCELLED", started_at=None)
    summary = payload_of(WebhookDelivery.objects.get(job=job))["job"]
    assert summary["counts"] == {} and summary["started_at"] is None
    assert summary["error"]["code"] == "CANCELLED"


def test_the_models_defaults_and_names(world):
    webhook = Webhook.objects.create(
        owner=world.viewer, name="plain", url="https://hooks.example.org/", scope="own_jobs"
    )
    assert (webhook.kinds, webhook.events, str(webhook)) == (["run"], [], "plain")
    delivery = WebhookDelivery.objects.create(webhook=webhook, event="webhook.test", payload="{}")
    assert str(delivery) == f"webhook.test delivery {delivery.id}"
    assert make_webhook(world.viewer, url="https://h.example.org/a?b=c").endpoint == (
        "https://h.example.org/a"
    )


# --------------------------------------------------------------------------- the outbox


def lease(principal):
    return queue.lease(principal, worker_id="w-1", lanes=["batch"], spec_versions=[1])


@pytest.mark.parametrize(
    "status,error,event",
    [
        ("succeeded", None, "job.succeeded"),
        ("failed", {"code": "SCHEMA_INVALID", "message": "bad", "retryable": False}, "job.failed"),
        (
            "cancelled",
            {"code": "CANCELLED", "message": "stop", "retryable": False},
            "job.cancelled",
        ),
    ],
)
def test_completing_a_job_queues_its_outcome(world, principal, status, error, event):
    make_webhook(world.operator)
    job = lease(principal).job
    assert WebhookDelivery.objects.count() == 0
    queue.complete(
        principal,
        job.pk,
        attempt=1,
        result=job_result(str(job.pk), status=status, error=error),
        artifacts=[],
    )
    assert events_of(job) == [event]
    # A repeated completion (a retry after a lost response) queues nothing more
    queue.complete(
        principal, job.pk, attempt=1, result=job_result(str(job.pk), status=status), artifacts=[]
    )
    assert events_of(job) == [event]


def _published_job(world, **config):
    from world import s3_connection

    destination = s3_connection(prefix="published", **config)
    Dataset.objects.filter(pk=world.dataset.pk).update(destination_connection=destination)
    Job.objects.filter(pk=world.queued_job.pk).delete()
    return make_job(world.operator, world.upload, dataset=world.dataset)


def test_a_published_run_is_announced_once_publishing_succeeded(world, principal, monkeypatch):
    make_webhook(world.operator)
    job = _published_job(world)
    lease(principal)
    statuses = []
    real_publish = queue._publish

    def publish(job, destination):
        statuses.append(events_of(job))  # nothing is queued before publishing ends
        return real_publish(job, destination)

    monkeypatch.setattr(queue, "_publish", publish)
    queue.complete(principal, job.pk, attempt=1, result=job_result(str(job.pk)), artifacts=[])
    assert statuses == [[]]
    assert events_of(job) == ["job.succeeded"]


def test_a_run_whose_publishing_failed_is_announced_as_failed(world, principal):
    make_webhook(world.operator)
    job = _published_job(world, bucket="forklift-web-no-such-bucket")
    leased = lease(principal).job
    key = f"{leased.attempt_prefix}data.parquet"
    put_url(
        storage.store().presign_put(key, expires=60, audience=storage.Audience.GATEWAY), b"PAR1"
    )
    output = {"kind": "data", "name": "data.parquet", "key": key, "bytes": 4, "rows": 1}
    done = queue.complete(
        principal,
        job.pk,
        attempt=1,
        result=job_result(str(job.pk), [output]),
        artifacts=[output],
    )
    assert done.error_code == "TARGET_WRITE_FAILED"
    [delivery] = WebhookDelivery.objects.filter(job=job)
    assert delivery.event == "job.failed"
    assert payload_of(delivery)["job"]["error"]["code"] == "TARGET_WRITE_FAILED"


def test_a_job_that_fails_while_it_is_leased_is_announced(world, principal):
    make_webhook(world.operator)
    gone = make_job(world.operator, make_upload(world.operator, status="deleted"))
    Job.objects.filter(pk=world.queued_job.pk).delete()
    assert lease(principal) is None
    assert events_of(gone) == ["job.failed"]


def test_expired_leases_on_the_last_attempt_are_announced(world, principal):
    make_webhook(world.operator)
    past = timezone.now() - timedelta(seconds=5)
    failed = make_job(world.operator, world.upload)
    cancelled = make_job(world.operator, world.upload)
    requeued = make_job(world.operator, world.upload)
    Job.objects.filter(pk__in=[failed.pk, cancelled.pk, requeued.pk]).update(
        status=JobStatus.RUNNING, lease_expires_at=past, attempt=3
    )
    Job.objects.filter(pk=cancelled.pk).update(cancel_requested_at=past)
    Job.objects.filter(pk=requeued.pk).update(attempt=1)
    assert queue.requeue_expired_leases() == {"requeued": 1, "failed": 1, "cancelled": 1}
    assert events_of(failed) == ["job.failed"]
    assert events_of(cancelled) == ["job.cancelled"]
    assert events_of(requeued) == []


def test_cancelling_a_queued_job_is_announced_and_a_running_one_waits(world):
    make_webhook(world.operator)
    actor = Actor.for_user(world.operator)
    jobs.cancel_job(actor, world.queued_job.pk)
    assert events_of(world.queued_job) == ["job.cancelled"]
    running = make_job(world.operator, world.upload, status=JobStatus.RUNNING)
    jobs.cancel_job(actor, running.pk)
    assert events_of(running) == []  # the worker reports it cancelled at its next heartbeat


def test_the_outbox_is_written_in_the_transaction_that_finishes_the_job(world, monkeypatch):
    make_webhook(world.operator)

    def broken(job):
        raise RuntimeError("the outbox failed")

    monkeypatch.setattr(webhooks, "job_finished", broken)
    with pytest.raises(RuntimeError):
        jobs.cancel_job(Actor.for_user(world.operator), world.queued_job.pk)
    assert Job.objects.get(pk=world.queued_job.pk).status == JobStatus.QUEUED
    job = make_job(world.operator, world.upload)
    Job.objects.filter(pk=job.pk).update(
        status=JobStatus.RUNNING, attempt=3, lease_expires_at=timezone.now() - timedelta(1)
    )
    with pytest.raises(RuntimeError):
        queue.requeue_expired_leases()
    assert Job.objects.get(pk=job.pk).status == JobStatus.RUNNING


# --------------------------------------------------------------------------- who hears about what


def test_each_scope_hears_about_its_jobs(world):
    other_dataset = world.spare_dataset
    dataset_hook = make_webhook(world.viewer, scope=WebhookScope.DATASET, dataset=world.dataset)
    viewer_own = make_webhook(world.viewer)
    operator_own = make_webhook(world.operator)
    everything = make_webhook(world.admin, scope=WebhookScope.ALL_JOBS)
    of_dataset = make_job(world.operator, world.upload, dataset=world.dataset)
    of_other = make_job(world.operator, world.upload, dataset=other_dataset)
    token, _ = tokens.create(ApiToken, tokens.API_TOKEN_PREFIX, owner=world.operator, name="t")
    by_token = make_job(world.operator, world.upload)
    by_token.requested_with_token = token
    scheduled = make_job(world.operator, world.upload, dataset=world.dataset)
    scheduled.requested_by = None
    heard = {}
    for job in (of_dataset, of_other, by_token, scheduled):
        finish(job)
        heard[job.pk] = set(
            WebhookDelivery.objects.filter(job=job).values_list("webhook_id", flat=True)
        )
    assert heard[of_dataset.pk] == {dataset_hook.pk, operator_own.pk, everything.pk}
    assert heard[of_other.pk] == {operator_own.pk, everything.pk}
    assert heard[by_token.pk] == {operator_own.pk, everything.pk}
    assert heard[scheduled.pk] == {dataset_hook.pk, everything.pk}  # nobody requested it
    assert viewer_own.pk not in set().union(*heard.values())


def test_events_and_kinds_narrow_what_a_webhook_hears(world):
    failures = make_webhook(world.operator, events=["job.failed"])
    previews = make_webhook(world.operator, kinds=["preview", "run"])
    run = make_job(world.operator, world.upload)
    preview = make_job(world.operator, world.upload, kind=JobKind.PREVIEW)
    finish(run)
    finish(preview, JobStatus.FAILED, error_code="X")
    assert set(WebhookDelivery.objects.filter(job=run).values_list("webhook", flat=True)) == {
        previews.pk
    }
    assert set(WebhookDelivery.objects.filter(job=preview).values_list("webhook", flat=True)) == {
        previews.pk
    }
    assert failures.deliveries.count() == 0


def test_disabled_webhooks_and_deactivated_owners_hear_nothing(world, monkeypatch):
    make_webhook(world.operator, active=False)
    make_webhook(world.viewer, scope=WebhookScope.DATASET, dataset=world.dataset)
    world.viewer.is_active = False
    world.viewer.save()
    demoted = make_user(Role.VIEWER)
    make_webhook(demoted, scope=WebhookScope.ALL_JOBS)  # an admin's, before they were demoted
    job = make_job(world.operator, world.upload, dataset=world.dataset)
    assert finish(job) == 0
    monkeypatch.setitem(policy.OBJECT_RULES, Action.JOB_VIEW, hidden)
    make_webhook(world.operator)
    assert finish(make_job(world.operator, world.upload)) == 0  # the policy hides the job


# --------------------------------------------------------------------------- delivering


def test_failed_attempts_are_retried_with_backoff_until_the_delivery_fails(world):
    webhook = make_webhook(world.operator)
    job = make_job(world.operator, world.upload)
    finish(job)
    delivery = WebhookDelivery.objects.get(job=job)
    client = FakeClient(failing(503))
    moment = delivery.next_attempt_at
    for attempt, wait in enumerate(webhooks.BACKOFF_SECONDS, start=1):
        assert webhooks.deliver_due(now=moment, client=client)["retrying"] == 1
        delivery.refresh_from_db()
        webhook.refresh_from_db()
        assert (delivery.status, delivery.attempts) == ("pending", attempt)
        assert delivery.next_attempt_at == moment + timedelta(seconds=wait)
        assert webhook.backoff_until == delivery.next_attempt_at
        assert (delivery.last_status_code, delivery.last_error) == (
            503,
            "The receiver answered 503.",
        )
        assert delivery.last_attempt_at == moment
        too_early = moment + timedelta(seconds=wait - 1)
        assert webhooks.deliver_due(now=too_early, client=client)["retrying"] == 0
        moment += timedelta(seconds=wait)
    assert webhooks.deliver_due(now=moment, client=client)["failed"] == 1
    delivery.refresh_from_db()
    assert (delivery.status, delivery.attempts, delivery.next_attempt_at) == ("failed", 7, None)
    assert len(client.requests) == 7
    assert {headers["Forklift-Delivery"] for _, _, headers in client.requests} == {
        str(delivery.id)
    }  # the same id on every retry, so receivers can deduplicate
    webhook.refresh_from_db()
    assert webhook.consecutive_failures == 7 and webhook.active  # 10 attempts disable it
    assert webhook.backoff_until == moment + timedelta(seconds=webhooks.BACKOFF_SECONDS[-1])


def test_a_receiver_that_is_down_costs_one_attempt_per_step_however_much_waits(world):
    webhook = make_webhook(world.operator)
    for _ in range(5):
        finish(make_job(world.operator, world.upload))
    client = FakeClient(failing())
    start = timezone.now()
    assert webhooks.deliver_due(now=start, client=client)["retrying"] == 1
    held = webhook.deliveries.filter(attempts=0)
    assert held.count() == 4  # the others wait with the failed one
    finish(make_job(world.operator, world.upload))  # and so does a new one
    assert webhooks.deliver_due(now=start + timedelta(seconds=59), client=client)["retrying"] == 0
    # Each step one more attempt, and the waits grow with the webhook's failures in a row,
    # whichever delivery is tried: 1 minute, then 5, then 30
    moment = start
    for wait in (60, 300, 1800):
        moment += timedelta(seconds=wait)
        assert webhooks.deliver_due(now=moment, client=client)["retrying"] == 1
    assert len(client.requests) == 4
    webhook.refresh_from_db()
    assert webhook.consecutive_failures == 4
    assert webhook.backoff_until == moment + timedelta(seconds=2 * 3600)
    # Once an attempt gets through, everything that waited goes out at once
    moment = webhook.backoff_until
    counts = webhooks.deliver_due(now=moment, client=FakeClient())
    assert counts["delivered"] == 6
    webhook.refresh_from_db()
    assert (webhook.consecutive_failures, webhook.backoff_until) == (0, None)


def test_a_pass_takes_the_webhooks_in_turns(world):
    busy = make_webhook(world.operator, name="busy")
    quiet = make_webhook(world.viewer, scope=WebhookScope.DATASET, dataset=world.dataset)
    for _ in range(3):
        finish(make_job(world.operator, world.upload))
    finish(make_job(world.author, world.upload, dataset=world.dataset))
    client = FakeClient()
    assert webhooks.deliver_due(client=client, limit=2)["delivered"] == 2
    sent = {
        WebhookDelivery.objects.get(id=headers["Forklift-Delivery"]).webhook_id
        for _, _, headers in client.requests
    }
    assert sent == {busy.pk, quiet.pk}  # the busy one's backlog did not go first
    assert webhooks.deliver_due(client=client)["delivered"] == 2  # rounds go on in a pass
    assert not WebhookDelivery.objects.filter(status=DeliveryStatus.PENDING).exists()


def test_a_success_starts_the_failure_count_anew(world):
    later = timezone.now() - timedelta(minutes=1)  # a backoff that has just ended
    webhook = make_webhook(world.operator, consecutive_failures=5, backoff_until=later)
    finish(make_job(world.operator, world.upload))
    assert webhooks.deliver_due(client=FakeClient())["delivered"] == 1
    webhook.refresh_from_db()
    assert (webhook.consecutive_failures, webhook.backoff_until) == (0, None)


def test_a_webhook_is_disabled_after_too_many_failed_attempts(world, admin_actor):
    installation.update(admin_actor, {"webhook_disable_after_failures": 2})
    webhook = make_webhook(world.operator)
    for _ in range(2):
        finish(make_job(world.operator, world.upload))
    client = FakeClient(failing())
    moment = timezone.now()
    assert webhooks.deliver_due(now=moment, client=client)["retrying"] == 1
    moment += timedelta(minutes=1)
    assert webhooks.deliver_due(now=moment, client=client)["retrying"] == 1
    webhook.refresh_from_db()
    assert (webhook.active, webhook.disabled_reason, webhook.consecutive_failures) == (
        False,
        "failures",
        2,
    )
    entry = AuditLog.objects.get(action="webhook.disable")
    assert entry.actor_label == "system:dispatch"
    assert entry.details == {"reason": "failures", "consecutive_failures": 2}
    assert finish(make_job(world.operator, world.upload)) == 0
    # What still waits is skipped once the backoff ends
    later = moment + timedelta(hours=1)
    assert webhooks.deliver_due(now=later, client=client) == {
        "delivered": 0,
        "retrying": 0,
        "failed": 0,
        "skipped": 2,
    }
    assert set(webhook.deliveries.values_list("last_error", flat=True)) == {
        "The webhook is disabled."
    }
    on = webhooks.update_webhook(Actor.for_user(world.operator), webhook.pk, active=True)
    assert (on.consecutive_failures, on.backoff_until) == (0, None)


def test_a_webhook_disabled_while_its_delivery_was_out_is_not_disabled_again(world):
    webhook = make_webhook(world.operator)
    finish(make_job(world.operator, world.upload))
    installation.update(Actor.for_system("test"), {"webhook_disable_after_failures": 1})

    class DisablingClient(FakeClient):
        def post(self, url, body, headers):
            Webhook.objects.filter(pk=webhook.pk).update(active=False, disabled_reason="owner")
            return failing()

    WebhookDelivery.objects.update(attempts=len(webhooks.BACKOFF_SECONDS))
    assert webhooks.deliver_due(client=DisablingClient())["failed"] == 1
    webhook.refresh_from_db()
    assert (webhook.disabled_reason, webhook.consecutive_failures) == ("owner", 1)
    assert not AuditLog.objects.filter(action="webhook.disable").exists()


def test_the_owners_access_is_checked_again_before_sending(world, monkeypatch):
    own = make_webhook(world.operator, name="own")
    everything = make_webhook(world.admin, scope=WebhookScope.ALL_JOBS, name="all")
    of_dataset = make_webhook(
        world.author, scope=WebhookScope.DATASET, dataset=world.dataset, name="dataset"
    )
    finish(make_job(world.operator, world.upload, dataset=world.dataset))
    assert WebhookDelivery.objects.count() == 3
    world.operator.is_active = False
    world.operator.save()
    world.admin.role = Role.AUTHOR
    world.admin.save()
    monkeypatch.setitem(policy.OBJECT_RULES, Action.DATASET_VIEW, hidden)
    client = FakeClient()
    assert webhooks.deliver_due(client=client)["skipped"] == 3
    assert client.requests == []
    reasons = {
        delivery.webhook_id: delivery.last_error
        for delivery in WebhookDelivery.objects.filter(status=DeliveryStatus.SKIPPED)
    }
    assert reasons == {
        own.pk: "The webhook's owner is deactivated.",
        everything.pk: "The webhook's owner is no longer an admin, which a webhook for every job "
        "needs.",
        of_dataset.pk: "The webhook's owner may no longer see this job.",
    }


def test_a_secret_that_cannot_be_decrypted_fails_the_attempt(world):
    make_webhook(world.operator)
    Webhook.objects.update(secret_ciphertext="damaged")
    finish(make_job(world.operator, world.upload))
    client = FakeClient()
    assert webhooks.deliver_due(client=client)["retrying"] == 1
    assert client.requests == []
    assert "could not be decrypted with FORKLIFT_SECRETS_KEYS" in (
        WebhookDelivery.objects.get().last_error
    )


def test_a_webhook_deleted_while_its_delivery_was_out(world):
    webhook = make_webhook(world.operator)
    finish(make_job(world.operator, world.upload))

    class DeletingClient(FakeClient):
        def post(self, url, body, headers):
            Webhook.objects.filter(pk=webhook.pk).delete()
            return Outcome(True, 200)

    assert webhooks.deliver_due(client=DeletingClient())["skipped"] == 1
    assert not WebhookDelivery.objects.exists()


def test_a_pass_stops_at_its_limit_and_its_time_budget(world):
    make_webhook(world.operator)
    for _ in range(3):
        finish(make_job(world.operator, world.upload))
    client = FakeClient()
    assert webhooks.deliver_due(client=client, budget_seconds=0)["delivered"] == 0
    assert webhooks.deliver_due(client=client, limit=2)["delivered"] == 2
    assert webhooks.deliver_due(client=client)["delivered"] == 1
    assert webhooks.deliver_due(client=client) == dict.fromkeys(
        ("delivered", "retrying", "failed", "skipped"), 0
    )


def test_the_default_client_goes_through_the_guard(world):
    make_webhook(world.operator)  # https://192.0.2.10/: a documentation address
    finish(make_job(world.operator, world.upload))
    assert webhooks.deliver_due()["retrying"] == 1
    assert "not a publicly routable address" in WebhookDelivery.objects.get().last_error


def test_a_claimed_delivery_is_not_due_again_while_it_is_being_sent(world):
    make_webhook(world.operator)
    finish(make_job(world.operator, world.upload))
    now = timezone.now()
    claimed = webhooks._claim(now, set())
    assert claimed.next_attempt_at == now + timedelta(seconds=webhooks.CLAIM_SECONDS)
    assert webhooks._claim(now, set()) is None
    assert webhooks._claim(now + timedelta(seconds=webhooks.CLAIM_SECONDS), set()) == claimed


def _in_thread(target, results, index):
    try:
        results[index] = target()
    except BaseException as error:  # reported by the test
        results[index] = error
    finally:
        connection.close()


@pytest.mark.django_db(transaction=True)
def test_a_delivery_another_dispatcher_holds_is_skipped():
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner)
    make_webhook(owner)
    first, second = make_job(owner, upload), make_job(owner, upload)
    finish(first)
    finish(second)
    held = WebhookDelivery.objects.get(job=first)
    WebhookDelivery.objects.filter(pk=held.pk).update(
        next_attempt_at=timezone.now() - timedelta(minutes=1)
    )
    locked, release = threading.Event(), threading.Event()

    def hold():
        with transaction.atomic():
            WebhookDelivery.objects.select_for_update().get(pk=held.pk)
            locked.set()
            release.wait(timeout=30)

    results = [None]
    holder = threading.Thread(target=_in_thread, args=(hold, results, 0))
    holder.start()
    try:
        assert locked.wait(timeout=30)
        client = FakeClient()
        assert webhooks.deliver_due(client=client)["delivered"] == 1  # SKIP LOCKED, no waiting
        assert [headers["Forklift-Delivery"] for _, _, headers in client.requests] == [
            str(WebhookDelivery.objects.get(job=second).id)
        ]
    finally:
        release.set()
        holder.join()
    assert results == [None]
    assert WebhookDelivery.objects.get(pk=held.pk).status == DeliveryStatus.PENDING


# --------------------------------------------------------------------------- managing webhooks


def create(actor, **fields):
    values = {"name": "ci", "url": "https://hooks.example.org/forklift", "events": ALL_EVENTS}
    return webhooks.create_webhook(actor, **{**values, **fields})


def test_creating_a_webhook_shows_its_secret_once_and_keeps_it_encrypted(world):
    actor = Actor.for_user(world.viewer)
    webhook, secret = create(actor, url="https://hooks.example.org/in?key=private")
    assert secret.startswith("fkwh_") and len(secret) == 48
    assert webhook.secret_prefix == secret[:12] and webhook.owner == world.viewer
    assert secret not in webhook.secret_ciphertext
    assert secret_backend.backend().decrypt(webhook.secret_ciphertext) == {"secret": secret}
    assert (webhook.scope, webhook.kinds, webhook.active) == ("own_jobs", ["run"], True)
    assert webhook.events == sorted(ALL_EVENTS)
    entry = AuditLog.objects.get(action="webhook.create")
    assert entry.details["host"] == "hooks.example.org"
    assert "private" not in json.dumps(entry.details)  # a query string may hold a credential
    other, other_secret = create(actor)
    assert other_secret != secret


@pytest.mark.parametrize(
    "fields,error,message",
    [
        ({"name": " "}, InvalidRequest, "needs a name"),
        ({"name": "x" * 101}, InvalidRequest, "needs a name"),
        ({"url": "http://hooks.example.org/"}, InvalidRequest, "must start with https://"),
        ({"url": "https://127.0.0.1/"}, InvalidRequest, "not a publicly routable address"),
        ({"events": []}, InvalidRequest, "events must list one or more of"),
        ({"events": ["job.started"]}, InvalidRequest, "events must list one or more of"),
        ({"events": ["webhook.test"]}, InvalidRequest, "events must list one or more of"),
        ({"events": "job.failed"}, InvalidRequest, "events must list one or more of"),
        ({"kinds": ["everything"]}, InvalidRequest, "kinds must list one or more of"),
        ({"scope": "mine"}, InvalidRequest, "Unknown webhook scope"),
        ({"scope": "dataset"}, InvalidRequest, "needs dataset_id"),
        ({"dataset_id": "x"}, InvalidRequest, "only for webhooks with scope 'dataset'"),
        ({"scope": "all_jobs"}, PermissionDenied, "Only admins may have a webhook for every job"),
    ],
)
def test_webhooks_are_validated(world, fields, error, message):
    with pytest.raises(error, match=message):
        create(Actor.for_user(world.author), **fields)
    assert not Webhook.objects.exists()


def test_scopes_that_need_a_dataset_or_an_admin(world):
    _, _ = create(Actor.for_user(world.admin), scope=WebhookScope.ALL_JOBS)
    webhook, _ = create(
        Actor.for_user(world.viewer), scope=WebhookScope.DATASET, dataset_id=world.dataset.pk
    )
    assert webhook.dataset == world.dataset
    with pytest.raises(NotFound, match="no dataset with id"):
        create(Actor.for_user(world.viewer), scope="dataset", dataset_id=world.version.pk)
    with pytest.raises(InvalidRequest) as caught:
        create(Actor.for_user(world.viewer), url="https://u:p@hooks.example.org/")
    assert caught.value.code == "url_refused"


def test_a_user_has_a_limited_number_of_webhooks(world, admin_actor):
    assert installation.get("webhook_max_per_owner") == 25
    installation.update(admin_actor, {"webhook_max_per_owner": 1})
    create(Actor.for_user(world.viewer))
    with pytest.raises(Conflict, match="You have 1 webhooks, the most one user may have"):
        create(Actor.for_user(world.viewer))
    create(Actor.for_user(world.operator))  # counted per owner


def test_changing_a_webhook(world):
    actor = Actor.for_user(world.viewer)
    webhook, _ = create(actor, scope="dataset", dataset_id=world.dataset.pk)
    changed = webhooks.update_webhook(
        actor,
        webhook.pk,
        name=" alerts ",
        url="https://other.example.org/in?key=private",
        events=["job.failed"],
        kinds=["run", "preview"],
        scope="own_jobs",
    )
    assert (changed.name, changed.url, changed.events, changed.kinds) == (
        "alerts",
        "https://other.example.org/in?key=private",
        ["job.failed"],
        ["preview", "run"],
    )
    assert (changed.scope, changed.dataset_id) == ("own_jobs", None)  # the dataset went with it
    details = AuditLog.objects.get(action="webhook.update").details["changed"]
    assert details["url"] == {"from": "hooks.example.org", "to": "other.example.org"}
    assert details["scope"] == {"from": "dataset", "to": "own_jobs"}
    assert "private" not in json.dumps(details)
    webhooks.update_webhook(actor, webhook.pk, name="alerts")  # nothing changes
    assert AuditLog.objects.filter(action="webhook.update").count() == 1
    again = webhooks.update_webhook(
        actor, webhook.pk, scope="dataset", dataset_id=world.spare_dataset.pk
    )
    assert again.dataset == world.spare_dataset
    with pytest.raises(InvalidRequest, match="needs dataset_id"):
        webhooks.update_webhook(actor, webhook.pk, dataset_id=None)


def test_disabling_and_enabling_a_webhook(world):
    actor = Actor.for_user(world.viewer)
    webhook, _ = create(actor)
    off = webhooks.update_webhook(actor, webhook.pk, active=False)
    assert (off.active, off.disabled_reason) == (False, "owner")
    Webhook.objects.filter(pk=webhook.pk).update(
        consecutive_failures=20, disabled_reason="failures"
    )
    on = webhooks.update_webhook(actor, webhook.pk, active=True)
    assert (on.active, on.disabled_reason, on.consecutive_failures) == (True, "", 0)


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"owner_id": 1}, "cannot be changed: owner_id"),
        ({"active": "yes"}, "active must be true or false"),
        ({"name": ""}, "needs a name"),
        ({"url": "https://[::1]/"}, "not a publicly routable"),
        ({"events": []}, "events must list"),
        ({"kinds": []}, "kinds must list"),
        ({"scope": "all_jobs"}, "Only admins"),
    ],
)
def test_changes_are_validated(world, changes, message):
    actor = Actor.for_user(world.viewer)
    webhook, _ = create(actor)
    with pytest.raises((InvalidRequest, PermissionDenied), match=message):
        webhooks.update_webhook(actor, webhook.pk, **changes)


def test_only_the_owner_manages_a_webhook(world):
    webhook, _ = create(Actor.for_user(world.viewer))
    for user in (world.operator, world.admin):
        actor = Actor.for_user(user)
        for call in (
            lambda: webhooks.get_webhook(actor, webhook.pk),
            lambda: webhooks.update_webhook(actor, webhook.pk, name="mine"),
            lambda: webhooks.delete_webhook(actor, webhook.pk),
            lambda: webhooks.rotate_secret(actor, webhook.pk),
            lambda: webhooks.send_test(actor, webhook.pk),
            lambda: webhooks.list_deliveries(actor, webhook.pk),
        ):
            with pytest.raises(PermissionDenied, match="belongs to another user") as caught:
                call()
            assert caught.value.code == "not_owner"
        assert list(webhooks.list_webhooks(actor)) == []
    with pytest.raises(NotFound, match="There is no webhook with id"):
        webhooks.get_webhook(Actor.for_user(world.viewer), world.version.pk)
    token, _ = tokens.create(
        ApiToken,
        tokens.API_TOKEN_PREFIX,
        owner=world.viewer,
        name="read",
        scopes=["webhooks:read"],
    )
    reader = Actor.for_user(world.viewer, token=token)
    assert webhooks.get_webhook(reader, webhook.pk) == webhook
    with pytest.raises(PermissionDenied, match="webhooks:write"):
        webhooks.delete_webhook(reader, webhook.pk)


def test_deleting_a_webhook_deletes_its_log(world):
    actor = Actor.for_user(world.operator)
    webhook, _ = create(actor)
    finish(make_job(world.operator, world.upload))
    webhooks.delete_webhook(actor, webhook.pk)
    assert not Webhook.objects.exists() and not WebhookDelivery.objects.exists()
    assert AuditLog.objects.get(action="webhook.delete").details == {"host": "hooks.example.org"}


def test_rotating_the_secret_signs_every_later_delivery_with_the_new_one(world):
    actor = Actor.for_user(world.operator)
    webhook, old = create(actor)
    finish(make_job(world.operator, world.upload))
    rotated, new = webhooks.rotate_secret(actor, webhook.pk)
    assert new != old and new.startswith("fkwh_") and rotated.secret_prefix == new[:12]
    client = FakeClient()
    webhooks.deliver_due(client=client)
    [(_, body, headers)] = client.requests
    assert verify(new, headers["Forklift-Signature"], body, time.time())
    assert not verify(old, headers["Forklift-Signature"], body, time.time())
    assert AuditLog.objects.get(action="webhook.rotate_secret").details == {"prefix": new[:12]}


def test_a_test_event_is_queued_and_sent_once(world):
    actor = Actor.for_user(world.viewer)
    webhook, secret = create(actor)
    later = timezone.now() + timedelta(hours=2)
    Webhook.objects.filter(pk=webhook.pk).update(
        consecutive_failures=3, backoff_until=later, active=False
    )
    queued = webhooks.send_test(actor, webhook.pk)
    assert (queued.status, queued.event, queued.job_id, queued.attempts) == (
        "pending",
        "webhook.test",
        None,
        0,
    )
    with pytest.raises(Conflict, match="still waiting for the dispatcher") as caught:
        webhooks.send_test(actor, webhook.pk)  # one test at a time
    assert caught.value.code == "test_pending"
    client = FakeClient(Outcome(True, 200))
    # Sent although the webhook is disabled and backing off: its owner is trying it
    assert webhooks.deliver_due(client=client)["delivered"] == 1
    [(url, body, headers)] = client.requests
    assert url == webhook.url and verify(secret, headers["Forklift-Signature"], body, time.time())
    payload = json.loads(body)
    assert payload["event"] == "webhook.test" and payload["job"] is None
    assert payload["webhook"] == {"id": str(webhook.id), "name": "ci"}
    sent = webhook.deliveries.get(pk=queued.pk)
    assert (sent.status, sent.attempts, sent.last_status_code) == ("delivered", 1, 200)
    webhook.refresh_from_db()
    assert (webhook.consecutive_failures, webhook.backoff_until) == (0, None)  # it answers
    Webhook.objects.filter(pk=webhook.pk).update(consecutive_failures=3, backoff_until=later)
    failed = webhooks.send_test(actor, webhook.pk)
    assert webhooks.deliver_due(client=FakeClient(failing(404)))["failed"] == 1
    failed.refresh_from_db()
    assert (failed.status, failed.last_status_code, failed.next_attempt_at) == (
        "failed",
        404,
        None,
    )
    webhook.refresh_from_db()
    assert (webhook.consecutive_failures, webhook.backoff_until) == (3, later)  # not counted
    assert AuditLog.objects.filter(action="webhook.test").count() == 2


def test_a_test_event_of_a_deactivated_owner_is_skipped(world):
    webhook = make_webhook(world.viewer)
    webhooks.send_test(Actor.for_user(world.viewer), webhook.pk)
    world.viewer.is_active = False
    world.viewer.save()
    assert webhooks.deliver_due(client=FakeClient())["skipped"] == 1


def test_the_default_test_client_goes_through_the_guard(world):
    webhook = make_webhook(world.viewer)
    delivery = webhooks.send_test(Actor.for_user(world.viewer), webhook.pk)
    assert webhooks.deliver_due()["failed"] == 1
    delivery.refresh_from_db()
    assert delivery.status == "failed" and "not a publicly routable address" in delivery.last_error


def test_redelivering(world):
    actor = Actor.for_user(world.operator)
    webhook, _ = create(actor)
    finish(make_job(world.operator, world.upload))
    delivery = WebhookDelivery.objects.get()
    WebhookDelivery.objects.update(status="failed", attempts=7, next_attempt_at=None)
    queued = webhooks.redeliver(actor, webhook.pk, delivery.pk)
    assert (queued.status, queued.attempts) == ("pending", 0)
    assert webhooks.deliver_due(client=FakeClient())["delivered"] == 1
    assert AuditLog.objects.get(action="webhook.redeliver").details == {
        "delivery_id": str(delivery.id)
    }
    with pytest.raises(NotFound, match="has no delivery with id"):
        webhooks.redeliver(actor, webhook.pk, world.version.pk)
    test = webhooks.send_test(actor, webhook.pk)
    with pytest.raises(InvalidRequest, match="Test deliveries are not sent again"):
        webhooks.redeliver(actor, webhook.pk, test.pk)
    webhooks.update_webhook(actor, webhook.pk, active=False)
    with pytest.raises(Conflict, match="is disabled; enable it before redelivering"):
        webhooks.redeliver(actor, webhook.pk, delivery.pk)


def test_listing_deliveries(world):
    actor = Actor.for_user(world.operator)
    webhook, _ = create(actor)
    finish(make_job(world.operator, world.upload))
    webhooks.send_test(actor, webhook.pk)
    assert webhooks.deliver_due(client=FakeClient(failing()))["failed"] == 1  # the test
    assert [d.event for d in webhooks.list_deliveries(actor, webhook.pk)] == [
        "webhook.test",
        "job.succeeded",
    ]
    assert [d.event for d in webhooks.list_deliveries(actor, webhook.pk, status="pending")] == [
        "job.succeeded"
    ]


def test_admins_see_everyones_webhooks_and_disable_them(world, admin_actor):
    viewers = make_webhook(world.viewer, name="viewer's")
    make_webhook(world.operator, name="operator's", active=False)
    assert set(webhooks.list_all_webhooks(admin_actor)) == set(Webhook.objects.all())
    assert list(webhooks.list_all_webhooks(admin_actor, active=True)) == [viewers]
    assert list(webhooks.list_all_webhooks(admin_actor, owner_id=world.viewer.pk)) == [viewers]
    disabled = webhooks.disable_webhook(admin_actor, viewers.pk)
    assert (disabled.active, disabled.disabled_reason) == (False, "admin")
    assert AuditLog.objects.get(action="admin.webhook.disable").details == {
        "owner_id": world.viewer.pk
    }
    with pytest.raises(Conflict, match="is already disabled"):
        webhooks.disable_webhook(admin_actor, viewers.pk)
    with pytest.raises(NotFound):
        webhooks.disable_webhook(admin_actor, world.version.pk)
    for user in (world.viewer, world.author):
        with pytest.raises(PermissionDenied):
            webhooks.list_all_webhooks(Actor.for_user(user))
        with pytest.raises(PermissionDenied):
            webhooks.disable_webhook(Actor.for_user(user), viewers.pk)


# --------------------------------------------------------------------------- retention, commands


def test_retention_deletes_deliveries_with_the_job_records(world, admin_actor):
    RetentionPolicy.objects.filter(scope=RetentionScope.INSTALLATION).update(
        days={"job_records": 30}
    )
    RetentionPolicy.objects.create(
        scope=RetentionScope.DATASET, dataset=world.dataset, days={"job_records": None}
    )
    webhook = make_webhook(world.operator)
    expired = make_job(world.operator, world.upload)
    kept = make_job(world.operator, world.upload, dataset=world.dataset)  # its dataset keeps them
    due = make_job(world.operator, world.upload)
    for job in (expired, kept, due):
        finish(job)
    webhooks.send_test(Actor.for_user(world.operator), webhook.pk)
    WebhookDelivery.objects.update(created_at=timezone.now() - timedelta(days=31))
    WebhookDelivery.objects.exclude(job=due).update(status=DeliveryStatus.DELIVERED)
    recent = make_job(world.operator, world.upload)
    finish(recent)
    report = retention.sweep(admin_actor, dry_run=True)
    assert report.deliveries == 2 and WebhookDelivery.objects.count() == 5
    assert retention.sweep(admin_actor).deliveries == 2  # the expired job's and the test's
    assert set(WebhookDelivery.objects.values_list("job", "status")) == {
        (kept.pk, "delivered"),
        (due.pk, "pending"),  # still to be sent: kept, whatever its age
        (recent.pk, "pending"),
    }
    entry = AuditLog.objects.get(action="retention.purge", object_type="")
    assert entry.details == {"kind": "webhook deliveries", "count": 2}
    assert retention.sweep(admin_actor).deliveries == 0
    assert AuditLog.objects.filter(action="retention.purge", object_type="").count() == 1


def test_rotate_secrets_re_encrypts_webhook_secrets(world, settings):
    from cryptography.fernet import Fernet

    webhook = make_webhook(world.viewer)
    new_key = Fernet.generate_key().decode()
    settings.FORKLIFT_SECRETS_KEYS = [new_key, *settings.FORKLIFT_SECRETS_KEYS]
    secret_backend.backend.cache_clear()
    try:
        out = StringIO()
        call_command("rotate_secrets", stdout=out)
        assert "and 1 webhooks." in out.getvalue()
        rotated = Webhook.objects.get(pk=webhook.pk).secret_ciphertext
        assert secret_backend.EnvSecretBackend([new_key]).decrypt(rotated) == {"secret": SECRET}
        Webhook.objects.filter(pk=webhook.pk).update(secret_ciphertext="damaged")
        with pytest.raises(CommandError, match=f"Webhook 'hook' \\({webhook.pk}\\)"):
            call_command("rotate_secrets", stdout=StringIO())
    finally:
        secret_backend.backend.cache_clear()


def test_the_dispatcher_sends_due_deliveries(world, monkeypatch):
    make_webhook(world.operator)
    finish(make_job(world.operator, world.upload))
    client = FakeClient()
    monkeypatch.setattr(webhook_client, "Client", lambda: client)
    out, err = StringIO(), StringIO()
    call_command("dispatch", stdout=out, stderr=err)
    assert "Webhooks: delivered 1, retrying 0, failed 0, skipped 0." in out.getvalue()
    assert err.getvalue() == "" and len(client.requests) == 1


def test_a_failing_delivery_step_does_not_stop_the_dispatcher(world, monkeypatch):
    def broken(**kwargs):
        raise RuntimeError("the database went away")

    monkeypatch.setattr(webhooks, "deliver_due", broken)
    out, err = StringIO(), StringIO()
    with pytest.raises(CommandError, match="1 dispatch steps failed"):
        call_command("dispatch", stdout=out, stderr=err)
    assert "Schedules:" in out.getvalue()
    assert "The webhooks step failed; it runs again on the next pass." in err.getvalue()
