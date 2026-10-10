"""Create a worker token and print it (once) on standard output."""

from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from forklift_web.policy import Actor
from forklift_web.services import workers


class Command(BaseCommand):
    help = (
        "Create a worker token for /internal/v1 and print it on standard output (the only time "
        "it is shown); give it to the workers as FORKLIFT_WORKER_TOKEN."
    )

    def add_arguments(self, parser):
        parser.add_argument("--name", required=True, help="Names the token, e.g. the worker pool")
        parser.add_argument(
            "--expires-days", type=int, default=None, help="Days until it expires (default: never)"
        )

    def handle(self, *args, name, expires_days, **options):
        expires_at = None
        if expires_days is not None:
            expires_at = timezone.now() + timedelta(days=expires_days)
        token, raw = workers.create_worker_token(
            Actor.for_system("create_worker_token"), name=name, expires_at=expires_at
        )
        self.stderr.write(f"Created worker token {token.prefix}... ({name}); it is shown once:")
        self.stdout.write(raw)
