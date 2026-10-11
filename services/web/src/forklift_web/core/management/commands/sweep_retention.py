"""Delete what retention policies say has expired (the retention sweeper)."""

from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand

from forklift_web.core.management.loop import Stop, stopped_by_sigterm
from forklift_web.policy import Actor
from forklift_web.services import retention


def _sleep(stop: Stop, seconds: float) -> None:
    stop.wait(seconds)


class Command(BaseCommand):
    help = (
        "Delete expired uploads, artifacts and job records from the store and the database, and "
        "abort stale multipart output uploads, recording each deletion in the audit log. With "
        "--every, keep sweeping; SIGTERM stops the loop after the sweep in progress."
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
        with stopped_by_sigterm() as stop:
            while True:
                report = retention.sweep(actor, dry_run=dry_run)
                verb = "Would delete" if dry_run else "Deleted"
                abort = "would abort" if dry_run else "aborted"
                self.stdout.write(
                    f"{verb} {report.expired_uploads} expired pending uploads, "
                    f"{report.uploads} uploads, {report.artifacts} artifacts and {report.jobs} "
                    f"job records; {abort} {report.output_uploads} stale multipart output "
                    f"uploads. {verb} {report.deliveries} webhook deliveries."
                )
                for error in report.errors:
                    self.stderr.write(error)
                if heartbeat:
                    Path(heartbeat).touch()
                done += 1
                if every is None or (iterations is not None and done >= iterations):
                    return
                _sleep(stop, every)  # returns at once once SIGTERM has arrived
                if stop.requested:
                    self.stderr.write("Stopping: SIGTERM received.")
                    return
