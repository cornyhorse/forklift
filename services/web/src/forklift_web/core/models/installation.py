"""Installation settings, retention policies and the audit log."""

from __future__ import annotations

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.db.models import Q
from django.utils import timezone

from forklift_web.core.choices import Classification, RetentionScope
from forklift_web.core.models.accounts import User
from forklift_web.core.models.catalog import Dataset, ImmutableError


class InstallationSetting(models.Model):
    """An admin-set value; keys and defaults are defined in services.installation."""

    key = models.CharField(max_length=64, primary_key=True)
    value = models.JSONField()
    updated_at = models.DateTimeField(default=timezone.now)
    updated_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )

    class Meta:
        ordering = ["key"]


class RetentionPolicy(models.Model):
    """How long to keep each kind of object, at one level (design section 5.7).

    ``days`` maps a retention kind (``uploads``, ``data``, ``bad_rows``, ``previews``,
    ``metadata``, ``job_records``) to a number of days, or to null for "keep until deleted". A
    kind that is missing inherits from the next, less specific level (dataset, then
    classification, then installation); with no policy at any level, objects are kept.
    """

    scope = models.CharField(max_length=16, choices=RetentionScope.choices)
    classification = models.CharField(
        max_length=16, choices=Classification.choices, blank=True, default=""
    )
    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE, null=True, blank=True, related_name="retention"
    )
    days = models.JSONField(default=dict)
    updated_at = models.DateTimeField(default=timezone.now)
    updated_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )

    class Meta:
        ordering = ["scope", "classification"]
        constraints = [
            models.UniqueConstraint(
                fields=["scope"],
                condition=Q(scope="installation"),
                name="retention_one_installation_policy",
            ),
            models.UniqueConstraint(
                fields=["classification"],
                condition=Q(scope="classification"),
                name="retention_one_policy_per_classification",
            ),
            models.UniqueConstraint(
                fields=["dataset"],
                condition=Q(scope="dataset"),
                name="retention_one_policy_per_dataset",
            ),
        ]


class AuditLog(models.Model):
    """Who did what to which object, when and from where. Entries are never changed.

    ``details`` holds what changed (field names, roles, counts), never passwords, tokens,
    connection secrets, presigned URLs or cell values.
    """

    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    actor = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    actor_label = models.CharField(max_length=200)
    token_prefix = models.CharField(max_length=16, blank=True, default="")
    action = models.CharField(max_length=64, db_index=True)
    object_type = models.CharField(max_length=64, blank=True, default="")
    object_id = models.CharField(max_length=64, blank=True, default="")
    object_repr = models.CharField(max_length=300, blank=True, default="")
    details = models.JSONField(default=dict, encoder=DjangoJSONEncoder)
    request_id = models.CharField(max_length=64, blank=True, default="")
    ip = models.GenericIPAddressField(null=True, blank=True)

    class Meta:
        ordering = ["-id"]
        indexes = [models.Index(fields=["object_type", "object_id"])]

    def save(self, *args, **kwargs):
        if not self._state.adding:
            raise ImmutableError("Audit log entries cannot be changed.")
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ImmutableError("Audit log entries cannot be deleted.")
