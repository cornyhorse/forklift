"""Failed sign-ins, counted per username and per client address (services.sign_in)."""

from __future__ import annotations

from django.db import models
from django.utils import timezone

from forklift_web.core.choices import SignInThrottleKind


class SignInThrottle(models.Model):
    """Failed password checks of one normalised username (``user``) or one client address
    (``ip``) since ``window_started_at``; ``locked_until`` is set when a limit is reached.

    A username is counted the same way whether or not an account has it, so the rows do not
    tell which usernames exist. They hold no passwords.
    """

    kind = models.CharField(max_length=8, choices=SignInThrottleKind.choices)
    key = models.CharField(max_length=150)
    failures = models.PositiveIntegerField(default=0)
    window_started_at = models.DateTimeField(default=timezone.now)
    locked_until = models.DateTimeField(null=True, blank=True, db_index=True)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["kind", "key"]
        constraints = [
            models.UniqueConstraint(fields=["kind", "key"], name="sign_in_throttle_one_per_key")
        ]

    def __str__(self) -> str:
        return f"{self.kind} {self.key}"
