"""Webhooks and their deliveries (the outbox of job outcomes)."""

from __future__ import annotations

import uuid

from django.db import models
from django.db.models import Q
from django.utils import timezone

from forklift_web.core.choices import (
    DeliveryStatus,
    JobKind,
    WebhookDisabledReason,
    WebhookScope,
)
from forklift_web.core.models.accounts import User
from forklift_web.core.models.catalog import Dataset
from forklift_web.core.models.jobs import Job


def _default_kinds() -> list:
    return [JobKind.RUN.value]


class Webhook(models.Model):
    """An HTTPS endpoint that hears about the outcomes of jobs its owner can see.

    ``scope`` picks the jobs: every job of ``dataset``, the owner's own jobs (requested in the
    UI or through their API tokens), or, for admins, every job; ``events`` and ``kinds`` narrow
    them further. Deliveries are signed with ``secret_ciphertext``, the secret encrypted by the
    secret backend; ``secret_prefix`` (its first characters) helps recognise it.

    After a failed attempt, nothing more is sent to the webhook before ``backoff_until`` (its
    circuit breaker): not the retry, not the deliveries that came due meanwhile, not new ones. A
    receiver that is down therefore costs one attempt per backoff step, however many deliveries
    wait for it, and the wait grows with ``consecutive_failures`` (failed attempts in a row; an
    attempt that gets through clears both). A webhook is disabled by its owner, by an admin, or
    once ``webhook_disable_after_failures`` attempts in a row failed.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name="webhooks")
    name = models.CharField(max_length=100)
    url = models.CharField(max_length=2048)
    secret_ciphertext = models.TextField()
    secret_prefix = models.CharField(max_length=16)
    events = models.JSONField(default=list)
    kinds = models.JSONField(default=_default_kinds)
    scope = models.CharField(max_length=16, choices=WebhookScope.choices)
    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE, null=True, blank=True, related_name="webhooks"
    )
    active = models.BooleanField(default=True)
    disabled_reason = models.CharField(
        max_length=16, choices=WebhookDisabledReason.choices, blank=True, default=""
    )
    consecutive_failures = models.PositiveIntegerField(default=0)
    backoff_until = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["name", "id"]
        constraints = [
            models.CheckConstraint(
                condition=(Q(scope=WebhookScope.DATASET) & Q(dataset__isnull=False))
                | (~Q(scope=WebhookScope.DATASET) & Q(dataset__isnull=True)),
                name="webhook_dataset_scope",
            )
        ]
        indexes = [models.Index(fields=["scope", "active"], name="webhook_scope_active")]

    @property
    def endpoint(self) -> str:
        """The URL without its query string, which may hold the receiver's credentials."""
        return self.url.split("?", 1)[0]

    def __str__(self) -> str:
        return self.name


class WebhookDelivery(models.Model):
    """One event for one webhook: the exact JSON body, and how sending it went.

    ``payload`` is stored as the text that is sent, so that a retry sends (and signs) the same
    bytes; the delivery's id is the ``Forklift-Delivery`` header and stays the same across
    retries. ``last_error`` is the gateway's own short description of the last failure, never
    the receiver's answer. Test deliveries have no job: the dispatcher sends them once, without
    waiting for the webhook's backoff, and never retries them.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    webhook = models.ForeignKey(Webhook, on_delete=models.CASCADE, related_name="deliveries")
    job = models.ForeignKey(
        Job, on_delete=models.CASCADE, null=True, blank=True, related_name="webhook_deliveries"
    )
    event = models.CharField(max_length=32)
    payload = models.TextField()
    status = models.CharField(
        max_length=16, choices=DeliveryStatus.choices, default=DeliveryStatus.PENDING
    )
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(null=True, blank=True)
    last_attempt_at = models.DateTimeField(null=True, blank=True)
    last_status_code = models.PositiveSmallIntegerField(null=True, blank=True)
    last_error = models.CharField(max_length=300, blank=True, default="")
    delivered_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-created_at", "id"]
        indexes = [
            models.Index(
                fields=["next_attempt_at"],
                condition=Q(status=DeliveryStatus.PENDING),
                name="webhook_delivery_due",
            ),
            models.Index(fields=["webhook", "-created_at"], name="webhook_delivery_log"),
            models.Index(fields=["status", "created_at"], name="webhook_delivery_age"),
        ]

    def __str__(self) -> str:
        return f"{self.event} delivery {self.id}"
