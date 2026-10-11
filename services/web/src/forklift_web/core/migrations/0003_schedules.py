"""Schedules (forklift_web.services.schedules)."""

import uuid

import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("core", "0002_ordered_json_documents")]

    operations = [
        migrations.CreateModel(
            name="Schedule",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("cron", models.CharField(max_length=200)),
                ("timezone", models.CharField(default="UTC", max_length=64)),
                ("enabled", models.BooleanField(default=True)),
                ("next_run_at", models.DateTimeField(blank=True, null=True)),
                ("last_run_at", models.DateTimeField(blank=True, null=True)),
                (
                    "last_outcome",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("queued", "Queued a run"),
                            ("skipped_overlap", "Skipped: the previous run had not finished"),
                            ("missed", "Missed: too late to catch up"),
                            ("failed_to_enqueue", "Could not queue a run"),
                        ],
                        default="",
                        max_length=32,
                    ),
                ),
                ("last_message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "dataset",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="schedules",
                        to="core.dataset",
                    ),
                ),
                (
                    "last_job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="core.job",
                    ),
                ),
            ],
            options={"ordering": ["created_at"]},
        ),
        migrations.AddField(
            model_name="job",
            name="schedule",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="jobs",
                to="core.schedule",
            ),
        ),
        migrations.AddField(
            model_name="job",
            name="scheduled_for",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddConstraint(
            model_name="job",
            constraint=models.UniqueConstraint(
                condition=models.Q(("schedule__isnull", False)),
                fields=("idempotency_key",),
                name="job_schedule_slot",
            ),
        ),
        migrations.AddIndex(
            model_name="schedule",
            index=models.Index(
                condition=models.Q(("enabled", True)), fields=["next_run_at"], name="schedule_due"
            ),
        ),
    ]
