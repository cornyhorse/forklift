"""People, service accounts and the tokens they use."""

from __future__ import annotations

import uuid

from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone

from forklift_web.core.choices import Role


class User(AbstractUser):
    """A local account (OIDC later, through the same model).

    ``role`` is one of the four roles of design section 5.2. ``can_view_raw_rows`` is the extra
    permission that ``sensitive`` data needs to be previewed or to have its outputs and bad rows
    downloaded; it applies to every role, admins included (an admin can grant it, audited).
    A service account has no password and signs in only with API tokens.
    """

    role = models.CharField(max_length=16, choices=Role.choices, default=Role.VIEWER)
    can_view_raw_rows = models.BooleanField(default=False)
    is_service_account = models.BooleanField(default=False)

    class Meta:
        ordering = ["username"]

    def __str__(self) -> str:
        return self.username


class TokenBase(models.Model):
    """A bearer token stored as a SHA-256 hash; ``prefix`` (its first characters) finds it.

    The token itself is shown once, when it is created. Tokens are random 256-bit strings, so a
    plain SHA-256 is enough (no salt or slow hash is needed for values nobody chose).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100)
    prefix = models.CharField(max_length=16, unique=True)
    token_hash = models.CharField(max_length=64)
    created_at = models.DateTimeField(default=timezone.now)
    expires_at = models.DateTimeField(null=True, blank=True)
    last_used_at = models.DateTimeField(null=True, blank=True)
    revoked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True
        ordering = ["-created_at"]

    def is_usable(self, now=None) -> bool:
        now = now or timezone.now()
        return self.revoked_at is None and (self.expires_at is None or self.expires_at > now)

    def __str__(self) -> str:
        return f"{self.name} ({self.prefix}...)"


class ApiToken(TokenBase):
    """A token for /api/v1. Its scopes can only narrow what its owner's role allows."""

    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name="api_tokens")
    scopes = models.JSONField(default=list)
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    revoked_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )

    class Meta(TokenBase.Meta):
        pass


class WorkerToken(TokenBase):
    """A worker's bootstrap token: it can only call /internal/v1 (lease, heartbeat, presign,
    complete), never the public API."""

    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    revoked_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )

    class Meta(TokenBase.Meta):
        pass
