"""The publisher: copy successful runs to their datasets' s3 destinations (services.publishing).

A service of its own rather than a dispatch step: one copy can take an hour, and schedules and
webhook deliveries must not wait for it.
"""

from __future__ import annotations

import logging
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from forklift_web.core.management.loop import Stop, stopped_by_sigterm
from forklift_web.services import publishing

logger = logging.getLogger(__name__)


def _sleep(stop: Stop, seconds: float) -> None:
    stop.wait(seconds)


def _touch(path: Path) -> None:
    try:
        path.touch()
    except OSError:  # the health check notices; publishing must not fail for it
        logger.warning("The heartbeat file cannot be touched", extra={"path": str(path)})


class Command(BaseCommand):
    help = (
        "Publish the successful runs that wait for their s3 destination: copy their outputs "
        "(data first, manifest.json last), then mark them succeeded. With --every, keep "
        "publishing; a pass that fails is logged and the next one tries again. SIGTERM stops "
        "the loop once the run being copied is done (no further run is claimed)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--every", type=float, default=None, help="Publish again every SECONDS (a loop)"
        )
        parser.add_argument(
            "--iterations", type=int, default=None, help="With --every: stop after N passes"
        )
        parser.add_argument(
            "--heartbeat",
            default=None,
            help="Touch this file after each pass that did not fail, and every few seconds "
            "while a long copy runs (a health check can watch how old it is)",
        )

    def handle(self, *args, every, iterations, heartbeat, **options):
        touch = (lambda: _touch(Path(heartbeat))) if heartbeat else (lambda: None)
        done = failures = 0
        with stopped_by_sigterm() as stop:
            while True:
                try:
                    counts = publishing.publish_due(heartbeat=touch, stop=lambda: stop.requested)
                except Exception:
                    failures += 1
                    logger.exception("A publishing pass failed")
                    self.stderr.write("The publishing pass failed; the next one tries again.")
                else:
                    touch()
                    self.stdout.write(
                        f"Published {counts['published']}, retrying {counts['retrying']}, failed "
                        f"{counts['failed']}, lost to another publisher {counts['lost']}."
                    )
                done += 1
                if every is None or (iterations is not None and done >= iterations):
                    if failures:
                        raise CommandError(
                            f"{failures} publishing passes failed; the log has why."
                        )
                    return
                _sleep(stop, every)  # returns at once once SIGTERM has arrived
                if stop.requested:
                    self.stderr.write("Stopping: SIGTERM received.")
                    return
