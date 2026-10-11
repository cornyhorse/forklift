"""Webhooks: signed HTTPS notifications of job outcomes (design section 5.4).

A webhook belongs to one user and hears about the jobs of one dataset, the jobs its owner
requested (in the UI or through their API tokens) or, for admins, every job. When a job reaches
a terminal state (succeeded, failed, cancelled), :func:`job_finished` queues one delivery per
webhook that asked for that event, in the transaction that finishes the job (an outbox): a
delivery is queued only when the webhook's owner can see the job, and that is checked again
before it is sent. The dispatcher (``forklift-web dispatch``) sends due deliveries with
:func:`deliver_due` through :mod:`forklift_web.webhook_client`, which talks only to public
addresses; the gateway never sends anything while it answers a request (test events are queued
for the dispatcher too). A failed attempt holds back every delivery of its webhook until the
retry (a circuit breaker), one pass takes the webhooks in turns, and a webhook whose attempts
keep failing is disabled.

Every delivery is a POST of the JSON body stored with it, signed with the webhook's secret::

    Forklift-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>

plus ``Forklift-Event``, ``Forklift-Delivery`` (the delivery's id, the same on every retry) and
``User-Agent: forklift-webhooks/<version>``. Payloads describe the job (ids, kind, status,
times, counts, the error code and message) and link to the API; they never contain rows, file
contents, presigned URLs or secrets. docs/platform/webhooks.md shows how a receiver checks them.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import time
from datetime import timedelta
from datetime import timezone as dt_timezone
from typing import Optional
from urllib.parse import urlsplit

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from forklift_web import __version__, secret_backend, webhook_client
from forklift_web.core.choices import (
    EVENT_OF_STATUS,
    JOB_EVENTS,
    DeliveryStatus,
    JobKind,
    WebhookDisabledReason,
    WebhookEvent,
    WebhookScope,
)
from forklift_web.core.models import Job, Webhook, WebhookDelivery
from forklift_web.errors import Conflict, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Action, Actor, allowed, check
from forklift_web.services import audit, datasets, installation

logger = logging.getLogger(__name__)

SECRET_PREFIX = "fkwh_"
PREFIX_LENGTH = 12
USER_AGENT = f"forklift-webhooks/{__version__}"
# Waits after the 1st, 2nd, ... failed attempt (of a delivery, or of its webhook in a row,
# whichever is more); a delivery whose attempts are all used up has failed.
BACKOFF_SECONDS = (60, 5 * 60, 30 * 60, 2 * 3600, 6 * 3600, 12 * 3600)
# How long a dispatcher holds a delivery it is sending (longer than any one attempt).
CLAIM_SECONDS = 120
MAX_MESSAGE = 2000
EDITABLE = ("name", "url", "events", "kinds", "scope", "dataset_id", "active")


# --------------------------------------------------------------------------- signing


def signature(secret: str, timestamp: int, body: bytes) -> str:
    """The ``Forklift-Signature`` header of ``body`` sent at ``timestamp``."""
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"t={timestamp},v1={digest.hexdigest()}"


def _headers(delivery: WebhookDelivery, secret: str, timestamp: int) -> dict:
    return {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "Forklift-Event": delivery.event,
        "Forklift-Delivery": str(delivery.id),
        "Forklift-Signature": signature(secret, timestamp, delivery.payload.encode()),
        "Connection": "close",
    }


def _new_secret() -> tuple:
    """A new secret, its prefix and its ciphertext."""
    raw = SECRET_PREFIX + secrets.token_urlsafe(32)
    return raw, raw[:PREFIX_LENGTH], secret_backend.backend().encrypt({"secret": raw})


def _secret(webhook: Webhook) -> str:
    return secret_backend.backend().decrypt(webhook.secret_ciphertext)["secret"]


# --------------------------------------------------------------------------- payloads


def _iso(value) -> Optional[str]:
    return None if value is None else value.astimezone(dt_timezone.utc).isoformat()[:-6] + "Z"


def _api_url(path: str) -> str:
    return f"{settings.FORKLIFT_PUBLIC_URL}/api/v1{path}"


def job_summary(job: Job) -> dict:
    """What a payload says about ``job``: no rows, file contents, URLs to data or secrets."""
    result = job.result if isinstance(job.result, dict) else {}
    dataset = job.dataset
    return {
        "id": str(job.id),
        "kind": job.kind,
        "status": job.status,
        "dataset": None if dataset is None else {"id": str(dataset.id), "name": dataset.name},
        "schedule_id": None if job.schedule_id is None else str(job.schedule_id),
        "classification": job.classification,
        "attempt": job.attempt,
        "created_at": _iso(job.created_at),
        "started_at": _iso(job.started_at),
        "finished_at": _iso(job.finished_at),
        "counts": result.get("counts") or {},
        "error": (
            {"code": job.error_code, "message": job.error_message[:MAX_MESSAGE]}
            if job.error_code
            else None
        ),
        "url": _api_url(f"/jobs/{job.id}"),
        "artifacts_url": _api_url(f"/jobs/{job.id}/artifacts"),
    }


def _delivery(webhook: Webhook, event: str, job: Optional[Job], summary: Optional[dict], now):
    delivery = WebhookDelivery(webhook=webhook, job=job, event=event, created_at=now)
    body = {
        "id": str(delivery.id),
        "event": event,
        "created_at": _iso(now),
        "webhook": {"id": str(webhook.id), "name": webhook.name},
        "job": summary,
    }
    delivery.payload = json.dumps(body, separators=(",", ":"))
    return delivery


# --------------------------------------------------------------------------- the outbox


def _skip_reason(webhook: Webhook, job: Optional[Job]) -> str:
    """Why ``webhook`` may not hear about ``job`` (None: a test event) now ('' when it may)."""
    owner = webhook.owner
    if not owner.is_active:
        return "The webhook's owner is deactivated."
    if job is None:
        return ""  # its owner may try a disabled webhook too
    if not webhook.active:
        return "The webhook is disabled."
    actor = Actor.for_user(owner)
    if not allowed(actor, Action.JOB_VIEW, job) or (
        webhook.scope == WebhookScope.DATASET
        and not allowed(actor, Action.DATASET_VIEW, job.dataset)
    ):
        return "The webhook's owner may no longer see this job."
    if webhook.scope == WebhookScope.ALL_JOBS and not allowed(actor, Action.WEBHOOK_ALL_JOBS):
        return "The webhook's owner is no longer an admin, which a webhook for every job needs."
    return ""


def job_finished(job: Job) -> int:
    """Queue the outcome of ``job``, which has just finished, for each webhook that asked for it
    and whose owner can see the job. Call it in the transaction that finishes the job; returns
    how many deliveries were queued."""
    event = EVENT_OF_STATUS[job.status]
    candidates = (
        Webhook.objects.select_related("owner")
        .filter(active=True, events__contains=[event], kinds__contains=[job.kind])
        .filter(
            Q(scope=WebhookScope.ALL_JOBS)
            | Q(scope=WebhookScope.DATASET, dataset_id=job.dataset_id)
            | Q(scope=WebhookScope.OWN_JOBS, owner_id=job.requested_by_id)
        )
    )
    now = timezone.now()
    summary = None
    deliveries = []
    for webhook in candidates:
        if _skip_reason(webhook, job):
            continue
        summary = summary or job_summary(job)
        delivery = _delivery(webhook, event, job, summary, now)
        delivery.next_attempt_at = now
        deliveries.append(delivery)
    WebhookDelivery.objects.bulk_create(deliveries)
    return len(deliveries)


# --------------------------------------------------------------------------- sending


def _claim(now, served: set) -> Optional[WebhookDelivery]:
    """The next due delivery of a webhook not in ``served`` and not backing off (test events do
    not wait for that), held for CLAIM_SECONDS so that no other dispatcher sends it too."""
    with transaction.atomic():
        delivery = (
            WebhookDelivery.objects.select_for_update(skip_locked=True, of=("self",))
            .select_related("webhook__owner", "job__dataset")
            .filter(status=DeliveryStatus.PENDING, next_attempt_at__lte=now)
            .filter(
                Q(job__isnull=True)
                | Q(webhook__backoff_until__isnull=True)
                | Q(webhook__backoff_until__lte=now)
            )
            .exclude(webhook_id__in=served)
            .order_by("next_attempt_at", "created_at", "id")
            .first()
        )
        if delivery is not None:
            delivery.next_attempt_at = now + timedelta(seconds=CLAIM_SECONDS)
            delivery.save(update_fields=["next_attempt_at"])
    return delivery


def _fail(webhook: Webhook, attempts: int, now):
    """An attempt to deliver to ``webhook`` failed (the delivery's ``attempts``-th): hold back
    all its deliveries for the next backoff step, and disable it once too many attempts in a row
    failed. Returns when the next attempt may be made."""
    webhook.consecutive_failures += 1
    step = min(max(attempts, webhook.consecutive_failures), len(BACKOFF_SECONDS))
    webhook.backoff_until = now + timedelta(seconds=BACKOFF_SECONDS[step - 1])
    fields = ["consecutive_failures", "backoff_until"]
    limit = installation.get("webhook_disable_after_failures")
    if webhook.active and webhook.consecutive_failures >= limit:
        webhook.active = False
        webhook.disabled_reason = WebhookDisabledReason.FAILURES
        fields += ["active", "disabled_reason"]
        audit.record(
            Actor.for_system("dispatch"),
            "webhook.disable",
            webhook,
            {"reason": "failures", "consecutive_failures": webhook.consecutive_failures},
        )
        logger.warning(
            "Webhook disabled after failed deliveries",
            extra={"webhook_id": str(webhook.id), "failures": webhook.consecutive_failures},
        )
    webhook.save(update_fields=fields)
    return webhook.backoff_until


def _record(delivery: WebhookDelivery, outcome, now, *, retry: bool) -> str:
    """Store how an attempt went; returns delivered, retrying, failed or skipped."""
    with transaction.atomic():
        webhook = Webhook.objects.select_for_update().filter(pk=delivery.webhook_id).first()
        if webhook is None:
            return "skipped"  # deleted, with its deliveries, while the request was out
        attempts = delivery.attempts + 1
        fields = {
            "attempts": attempts,
            "last_attempt_at": now,
            "last_status_code": outcome.status_code,
            "last_error": outcome.error[:300],
        }
        if outcome.delivered:
            result = "delivered"
            fields.update(status=DeliveryStatus.DELIVERED, delivered_at=now, next_attempt_at=None)
            # The receiver answers again (a test that got through says so too): stop backing off
            Webhook.objects.filter(pk=webhook.pk).filter(
                Q(consecutive_failures__gt=0) | Q(backoff_until__isnull=False)
            ).update(consecutive_failures=0, backoff_until=None)
        elif not retry:
            result = "failed"  # a test event: tried once, counted for nothing
            fields.update(status=DeliveryStatus.FAILED, next_attempt_at=None)
        else:
            next_attempt = _fail(webhook, attempts, now)
            if attempts <= len(BACKOFF_SECONDS):
                result = "retrying"
                fields["next_attempt_at"] = next_attempt
            else:
                result = "failed"
                fields.update(status=DeliveryStatus.FAILED, next_attempt_at=None)
        WebhookDelivery.objects.filter(pk=delivery.pk).update(**fields)
    return result


def _send(delivery: WebhookDelivery, client, clock, *, retry: bool) -> str:
    """POST ``delivery`` once, signed now, and record the outcome. Test events (``retry=False``)
    are tried once and do not count towards backing off or disabling the webhook."""
    try:
        secret = _secret(delivery.webhook)
    except secret_backend.SecretError:
        outcome = webhook_client.Outcome(
            False,
            error="The webhook's secret could not be decrypted with FORKLIFT_SECRETS_KEYS; an "
            "admin needs to check the deployment's keys.",
        )
    else:
        headers = _headers(delivery, secret, int(clock().timestamp()))
        outcome = client.post(delivery.webhook.url, delivery.payload.encode(), headers)
    return _record(delivery, outcome, clock(), retry=retry)


def deliver_due(*, now=None, limit: int = 50, budget_seconds: float = 30.0, client=None) -> dict:
    """One pass of the dispatcher: send due deliveries until ``limit`` were sent or
    ``budget_seconds`` passed, in rounds that take one delivery (its oldest due) of each webhook,
    so that one webhook's backlog or slow receiver cannot keep the others waiting. Returns how
    many were delivered, will be retried, failed for good and were skipped (the webhook was
    disabled or its owner may no longer see the job)."""
    clock = timezone.now if now is None else (lambda: now)
    counts = {"delivered": 0, "retrying": 0, "failed": 0, "skipped": 0}
    stop = time.monotonic() + budget_seconds
    served = set()
    for _ in range(limit):
        if time.monotonic() >= stop:
            break
        delivery = _claim(clock(), served)
        if delivery is None and served:
            served.clear()  # each webhook with something due had its turn: the next round
            delivery = _claim(clock(), served)
        if delivery is None:
            break
        served.add(delivery.webhook_id)
        reason = _skip_reason(delivery.webhook, delivery.job)
        if reason:
            WebhookDelivery.objects.filter(pk=delivery.pk).update(
                status=DeliveryStatus.SKIPPED, last_error=reason, next_attempt_at=None
            )
            counts["skipped"] += 1
            continue
        client = client or webhook_client.Client()
        counts[_send(delivery, client, clock, retry=delivery.job_id is not None)] += 1
    if any(counts.values()):
        logger.info("Webhook deliveries sent", extra=counts)
    return counts


# --------------------------------------------------------------------------- managing webhooks


def _find(queryset, webhook_id) -> Webhook:
    webhook = queryset.filter(pk=webhook_id).first()
    if webhook is None:
        raise NotFound(f"There is no webhook with id {webhook_id}.")
    return webhook


def _fresh(webhook_id) -> Webhook:
    """The webhook as it is now (after a change the caller was allowed to make)."""
    return _find(Webhook.objects.select_related("owner", "dataset"), webhook_id)


def list_webhooks(actor: Actor):
    """The caller's own webhooks."""
    check(actor, Action.WEBHOOK_VIEW)
    return Webhook.objects.filter(owner=actor.user).select_related("owner", "dataset")


def get_webhook(actor: Actor, webhook_id, action: Action = Action.WEBHOOK_VIEW) -> Webhook:
    """One of the caller's own webhooks (``action``: what the caller is about to do with it)."""
    check(actor, action)
    webhook = _find(Webhook.objects.select_related("owner", "dataset"), webhook_id)
    check(actor, action, webhook)
    return webhook


def _locked(actor: Actor, webhook_id) -> Webhook:
    check(actor, Action.WEBHOOK_MANAGE)
    webhook = _find(Webhook.objects.select_for_update(), webhook_id)
    check(actor, Action.WEBHOOK_MANAGE, webhook)
    return webhook


def _subset(value, choices, what: str) -> list:
    if not isinstance(value, list) or not value or any(item not in choices for item in value):
        raise InvalidRequest(f"{what} must list one or more of: {', '.join(choices)}.")
    return sorted(set(value))


def _check_name(name) -> str:
    if not isinstance(name, str) or not name.strip() or len(name) > 100:
        raise InvalidRequest("A webhook needs a name of 1 to 100 characters.")
    return name.strip()


def _check_url(url) -> str:
    try:
        webhook_client.check_url(url)
    except webhook_client.UrlRefused as refused:
        raise InvalidRequest(str(refused), code="url_refused") from None
    return url


def _check_scope(actor: Actor, scope, dataset_id) -> None:
    if scope not in WebhookScope.values:
        raise InvalidRequest(
            f"Unknown webhook scope {scope!r}; scopes: {', '.join(WebhookScope.values)}."
        )
    if scope != WebhookScope.DATASET:
        if dataset_id is not None:
            raise InvalidRequest("dataset_id is only for webhooks with scope 'dataset'.")
        if scope == WebhookScope.ALL_JOBS and not allowed(actor, Action.WEBHOOK_ALL_JOBS):
            raise PermissionDenied(
                "Only admins may have a webhook for every job; choose scope 'own_jobs' or "
                "'dataset'.",
                code="role_insufficient",
            )
        return
    if dataset_id is None:
        raise InvalidRequest("A webhook with scope 'dataset' needs dataset_id.")
    datasets.get_dataset(actor, dataset_id)


def _host(url: str) -> str:
    """The host of ``url``, for the audit log (a query string may hold the receiver's key)."""
    return urlsplit(url).hostname or ""


def create_webhook(
    actor: Actor,
    *,
    name: str,
    url: str,
    events: list,
    scope: str = WebhookScope.OWN_JOBS,
    dataset_id=None,
    kinds: Optional[list] = None,
) -> tuple:
    """A new webhook of the caller; returns (webhook, secret). The secret is shown only now."""
    check(actor, Action.WEBHOOK_MANAGE)
    name = _check_name(name)
    url = _check_url(url)
    events = _subset(events, [event.value for event in JOB_EVENTS], "events")
    kinds = _subset([JobKind.RUN.value] if kinds is None else kinds, JobKind.values, "kinds")
    _check_scope(actor, scope, dataset_id)
    most = installation.get("webhook_max_per_owner")
    if Webhook.objects.filter(owner=actor.user).count() >= most:
        raise Conflict(
            f"You have {most} webhooks, the most one user may have (installation setting "
            "webhook_max_per_owner); delete one first."
        )
    raw, prefix, ciphertext = _new_secret()
    with transaction.atomic():
        webhook = Webhook.objects.create(
            owner=actor.user,
            name=name,
            url=url,
            events=events,
            kinds=kinds,
            scope=scope,
            dataset_id=dataset_id,
            secret_ciphertext=ciphertext,
            secret_prefix=prefix,
        )
        audit.record(
            actor,
            "webhook.create",
            webhook,
            {
                "host": _host(url),
                "events": events,
                "kinds": kinds,
                "scope": scope,
                "dataset_id": dataset_id,
            },
        )
    return _fresh(webhook.pk), raw


def _validated_changes(actor: Actor, webhook: Webhook, changes: dict) -> dict:
    unknown = sorted(set(changes) - set(EDITABLE))
    if unknown:
        raise InvalidRequest(
            f"These webhook fields cannot be changed: {', '.join(unknown)} (changeable: "
            f"{', '.join(EDITABLE)})."
        )
    if "scope" in changes and changes["scope"] != WebhookScope.DATASET:
        changes.setdefault("dataset_id", None)
    if "name" in changes:
        changes["name"] = _check_name(changes["name"])
    if "url" in changes:
        _check_url(changes["url"])
    if "events" in changes:
        changes["events"] = _subset(
            changes["events"], [event.value for event in JOB_EVENTS], "events"
        )
    if "kinds" in changes:
        changes["kinds"] = _subset(changes["kinds"], JobKind.values, "kinds")
    if "scope" in changes or "dataset_id" in changes:
        _check_scope(
            actor,
            changes.get("scope", webhook.scope),
            changes.get("dataset_id", webhook.dataset_id),
        )
    if "active" in changes and not isinstance(changes["active"], bool):
        raise InvalidRequest("active must be true or false.")
    return {field: value for field, value in changes.items() if getattr(webhook, field) != value}


def update_webhook(actor: Actor, webhook_id, **changes) -> Webhook:
    """Change any of ``EDITABLE``. Enabling or disabling a webhook starts its failure count and
    backoff anew."""
    with transaction.atomic():
        webhook = _locked(actor, webhook_id)
        changed = _validated_changes(actor, webhook, changes)
        if changed:
            details = {
                field: {"from": getattr(webhook, field), "to": value}
                for field, value in changed.items()
                if field != "url"
            }
            if "url" in changed:
                details["url"] = {"from": _host(webhook.url), "to": _host(changed["url"])}
            for field, value in changed.items():
                setattr(webhook, field, value)
            if "active" in changed:
                webhook.consecutive_failures = 0
                webhook.backoff_until = None
                webhook.disabled_reason = "" if webhook.active else WebhookDisabledReason.OWNER
            webhook.updated_at = timezone.now()
            webhook.save()
            audit.record(actor, "webhook.update", webhook, {"changed": details})
    return _fresh(webhook_id)


def delete_webhook(actor: Actor, webhook_id) -> None:
    with transaction.atomic():
        webhook = _locked(actor, webhook_id)
        audit.record(actor, "webhook.delete", webhook, {"host": _host(webhook.url)})
        webhook.delete()


def rotate_secret(actor: Actor, webhook_id) -> tuple:
    """A new secret for the webhook; returns (webhook, secret). Every delivery sent from now on,
    retries included, is signed with it; the old one stops working at once."""
    raw, prefix, ciphertext = _new_secret()
    with transaction.atomic():
        webhook = _locked(actor, webhook_id)
        webhook.secret_ciphertext = ciphertext
        webhook.secret_prefix = prefix
        webhook.updated_at = timezone.now()
        webhook.save(update_fields=["secret_ciphertext", "secret_prefix", "updated_at"])
        audit.record(actor, "webhook.rotate_secret", webhook, {"prefix": prefix})
    return _fresh(webhook_id), raw


def list_deliveries(actor: Actor, webhook_id, *, status: Optional[str] = None):
    """The webhook's deliveries, newest first."""
    webhook = get_webhook(actor, webhook_id)
    found = webhook.deliveries.all()
    return found.filter(status=status) if status else found


def send_test(actor: Actor, webhook_id) -> WebhookDelivery:
    """Queue a ``webhook.test`` event, due now: the dispatcher sends it on its next pass, once
    (it is never retried), through the same guard as every delivery. A webhook has at most one
    test event waiting; returns the pending delivery."""
    with transaction.atomic():
        webhook = _locked(actor, webhook_id)
        waiting = webhook.deliveries.filter(job__isnull=True, status=DeliveryStatus.PENDING)
        if waiting.exists():
            raise Conflict(
                f"A test event of webhook {webhook.name!r} is still waiting for the dispatcher; "
                "its delivery log shows when it was sent.",
                code="test_pending",
            )
        now = timezone.now()
        delivery = _delivery(webhook, WebhookEvent.TEST, None, None, now)
        delivery.next_attempt_at = now
        delivery.save()
        audit.record(actor, "webhook.test", webhook, {"delivery_id": str(delivery.id)})
    return delivery


def redeliver(actor: Actor, webhook_id, delivery_id) -> WebhookDelivery:
    """Queue a delivery again, due now and with all its retries; the dispatcher sends it on its
    next pass (or once the webhook's backoff ends) with the same id, so receivers that saw it
    before can recognise it."""
    with transaction.atomic():
        webhook = _locked(actor, webhook_id)
        if not webhook.active:
            raise Conflict(f"Webhook {webhook.name!r} is disabled; enable it before redelivering.")
        delivery = webhook.deliveries.select_for_update().filter(pk=delivery_id).first()
        if delivery is None:
            raise NotFound(f"Webhook {webhook.name!r} has no delivery with id {delivery_id}.")
        if delivery.job_id is None:
            raise InvalidRequest("Test deliveries are not sent again; send a new test instead.")
        delivery.status = DeliveryStatus.PENDING
        delivery.attempts = 0
        delivery.next_attempt_at = timezone.now()
        delivery.save(update_fields=["status", "attempts", "next_attempt_at"])
        audit.record(actor, "webhook.redeliver", webhook, {"delivery_id": str(delivery.id)})
    return delivery


# --------------------------------------------------------------------------- administration


def list_all_webhooks(actor: Actor, *, active: Optional[bool] = None, owner_id=None):
    """Every user's webhooks (admins)."""
    check(actor, Action.ANY_WEBHOOK_VIEW)
    found = Webhook.objects.select_related("owner", "dataset")
    if active is not None:
        found = found.filter(active=active)
    if owner_id is not None:
        found = found.filter(owner_id=owner_id)
    return found


def disable_webhook(actor: Actor, webhook_id) -> Webhook:
    """An admin disables any user's webhook (its owner can enable it again)."""
    check(actor, Action.ANY_WEBHOOK_MANAGE)
    with transaction.atomic():
        webhook = _find(Webhook.objects.select_for_update(), webhook_id)
        if not webhook.active:
            raise Conflict(f"Webhook {webhook.name!r} is already disabled.")
        webhook.active = False
        webhook.disabled_reason = WebhookDisabledReason.ADMIN
        webhook.updated_at = timezone.now()
        webhook.save(update_fields=["active", "disabled_reason", "updated_at"])
        audit.record(actor, "admin.webhook.disable", webhook, {"owner_id": webhook.owner_id})
    return _fresh(webhook_id)
