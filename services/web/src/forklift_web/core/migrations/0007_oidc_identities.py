"""Sign-ins through the OpenID Provider, linked to accounts (forklift_web.services.oidc)."""

import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("core", "0006_publishing")]

    operations = [
        migrations.CreateModel(
            name="OidcIdentity",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("issuer", models.CharField(max_length=255)),
                ("subject", models.CharField(max_length=255)),
                (
                    "linked_by",
                    models.CharField(
                        choices=[
                            ("created", "Account created at the first sign-in"),
                            ("email", "Linked by a verified email address"),
                        ],
                        max_length=16,
                    ),
                ),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("last_used_at", models.DateTimeField(blank=True, null=True)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="oidc_identities",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["issuer", "subject"],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("issuer", "subject"), name="oidc_identity_one_per_sub"
                    )
                ],
            },
        ),
    ]
