"""Sign-in throttling (forklift_web.services.sign_in)."""

import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("core", "0004_webhooks")]

    operations = [
        migrations.CreateModel(
            name="SignInThrottle",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                (
                    "kind",
                    models.CharField(
                        choices=[("user", "Username"), ("ip", "Client address")], max_length=8
                    ),
                ),
                ("key", models.CharField(max_length=150)),
                ("failures", models.PositiveIntegerField(default=0)),
                (
                    "window_started_at",
                    models.DateTimeField(default=django.utils.timezone.now),
                ),
                (
                    "locked_until",
                    models.DateTimeField(blank=True, db_index=True, null=True),
                ),
                ("updated_at", models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                "ordering": ["kind", "key"],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("kind", "key"), name="sign_in_throttle_one_per_key"
                    )
                ],
            },
        ),
    ]
