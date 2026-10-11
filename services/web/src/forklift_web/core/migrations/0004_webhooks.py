"""Webhooks and their deliveries (forklift_web.services.webhooks)."""

import uuid

import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models

import forklift_web.core.models.webhooks


class Migration(migrations.Migration):
    dependencies = [("core", "0003_schedules")]

    operations = [
        migrations.CreateModel(
            name="Webhook",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("name", models.CharField(max_length=100)),
                ("url", models.CharField(max_length=2048)),
                ("secret_ciphertext", models.TextField()),
                ("secret_prefix", models.CharField(max_length=16)),
                ("events", models.JSONField(default=list)),
                (
                    "kinds",
                    models.JSONField(default=forklift_web.core.models.webhooks._default_kinds),
                ),
                (
                    "scope",
                    models.CharField(
                        choices=[
                            ("dataset", "Every job of one dataset"),
                            ("own_jobs", "Jobs I requested (also through my API tokens)"),
                            ("all_jobs", "Every job (admins)"),
                        ],
                        max_length=16,
                    ),
                ),
                ("active", models.BooleanField(default=True)),
                (
                    "disabled_reason",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("owner", "Disabled by its owner"),
                            ("admin", "Disabled by an admin"),
                            ("failures", "Disabled after repeated failed deliveries"),
                        ],
                        default="",
                        max_length=16,
                    ),
                ),
                ("consecutive_failures", models.PositiveIntegerField(default=0)),
                ("backoff_until", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "dataset",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="webhooks",
                        to="core.dataset",
                    ),
                ),
                (
                    "owner",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="webhooks",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"ordering": ["name", "id"]},
        ),
        migrations.CreateModel(
            name="WebhookDelivery",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("event", models.CharField(max_length=32)),
                ("payload", models.TextField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("delivered", "Delivered"),
                            ("failed", "Failed"),
                            ("skipped", "Skipped"),
                        ],
                        default="pending",
                        max_length=16,
                    ),
                ),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("next_attempt_at", models.DateTimeField(blank=True, null=True)),
                ("last_attempt_at", models.DateTimeField(blank=True, null=True)),
                ("last_status_code", models.PositiveSmallIntegerField(blank=True, null=True)),
                ("last_error", models.CharField(blank=True, default="", max_length=300)),
                ("delivered_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="webhook_deliveries",
                        to="core.job",
                    ),
                ),
                (
                    "webhook",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="deliveries",
                        to="core.webhook",
                    ),
                ),
            ],
            options={"ordering": ["-created_at", "id"]},
        ),
        migrations.AddIndex(
            model_name="webhook",
            index=models.Index(fields=["scope", "active"], name="webhook_scope_active"),
        ),
        migrations.AddConstraint(
            model_name="webhook",
            constraint=models.CheckConstraint(
                condition=models.Q(
                    models.Q(("scope", "dataset"), ("dataset__isnull", False)),
                    models.Q(
                        models.Q(("scope", "dataset"), _negated=True), ("dataset__isnull", True)
                    ),
                    _connector="OR",
                ),
                name="webhook_dataset_scope",
            ),
        ),
        migrations.AddIndex(
            model_name="webhookdelivery",
            index=models.Index(
                condition=models.Q(("status", "pending")),
                fields=["next_attempt_at"],
                name="webhook_delivery_due",
            ),
        ),
        migrations.AddIndex(
            model_name="webhookdelivery",
            index=models.Index(fields=["webhook", "-created_at"], name="webhook_delivery_log"),
        ),
        migrations.AddIndex(
            model_name="webhookdelivery",
            index=models.Index(fields=["status", "created_at"], name="webhook_delivery_age"),
        ),
    ]
