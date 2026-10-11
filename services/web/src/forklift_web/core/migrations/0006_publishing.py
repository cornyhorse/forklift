"""Resumable publishing (forklift_web.services.publishing): the publishing status and the
publisher's lease on a job."""

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("core", "0005_sign_in_throttle")]

    operations = [
        migrations.AddField(
            model_name="job",
            name="publish_attempts",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="job",
            name="publish_error",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="job",
            name="publish_lease_expires_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="job",
            name="publish_lease_token",
            field=models.UUIDField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="job",
            name="status",
            field=models.CharField(
                choices=[
                    ("queued", "Queued"),
                    ("running", "Running"),
                    ("publishing", "Publishing"),
                    ("succeeded", "Succeeded"),
                    ("failed", "Failed"),
                    ("cancelled", "Cancelled"),
                ],
                default="queued",
                max_length=16,
            ),
        ),
        migrations.AddIndex(
            model_name="job",
            index=models.Index(
                condition=models.Q(("status", "publishing")),
                fields=["publish_lease_expires_at"],
                name="job_publishing_leases",
            ),
        ),
    ]
