"""Return jobs whose lease expired to the queue (every lease call does this too)."""

from __future__ import annotations

from django.core.management.base import BaseCommand

from forklift_web.services import queue


class Command(BaseCommand):
    help = (
        "Requeue running jobs whose lease expired, fail those without attempts left and "
        "cancel those whose cancellation was requested."
    )

    def handle(self, *args, **options):
        counts = queue.requeue_expired_leases()
        self.stdout.write(
            f"Requeued {counts['requeued']}, failed {counts['failed']} and cancelled "
            f"{counts['cancelled']} jobs with expired leases."
        )
