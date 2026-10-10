"""Delete what retention policies say has expired (the retention sweeper)."""

from __future__ import annotations

import time
from pathlib import Path

from django.core.management.base import BaseCommand

from forklift_web.policy import Actor
from forklift_web.services import retention


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


class Command(BaseCommand):
    help = (
        "Delete expired uploads, artifacts and job records from the store and the database, "
        "recording each deletion in the audit log. With --every, keep sweeping."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Only count what would go")
        parser.add_argument(
            "--every", type=float, default=None, help="Sweep again every SECONDS (a loop)"
        )
        parser.add_argument(
            "--iterations", type=int, default=None, help="With --every: stop after N sweeps"
        )
        parser.add_argument(
            "--heartbeat",
            default=None,
            help="Touch this file after each sweep (a health check can watch how old it is)",
        )

    def handle(self, *args, dry_run, every, iterations, heartbeat, **options):
        actor = Actor.for_system("sweep_retention")
        done = 0
        while True:
            report = retention.sweep(actor, dry_run=dry_run)
            verb = "Would delete" if dry_run else "Deleted"
            self.stdout.write(
                f"{verb} {report.expired_uploads} expired pending uploads, {report.uploads} "
                f"uploads, {report.artifacts} artifacts and {report.jobs} job records."
            )
            for error in report.errors:
                self.stderr.write(error)
            if heartbeat:
                Path(heartbeat).touch()
            done += 1
            if every is None or (iterations is not None and done >= iterations):
                return
            _sleep(every)
