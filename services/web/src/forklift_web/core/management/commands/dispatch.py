"""The dispatcher: enqueue the runs of due schedules (and whatever else a pass has to do)."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from forklift_web.services import schedules, webhooks

logger = logging.getLogger(__name__)


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _schedules() -> str:
    counts = schedules.fire_due()
    return (
        f"Schedules: queued {counts['queued']}, skipped {counts['skipped_overlap']} (still "
        f"running), missed {counts['missed']}, failed to queue {counts['failed_to_enqueue']}, "
        f"errors {counts['errors']}."
    )


def _webhooks() -> str:
    counts = webhooks.deliver_due()
    return (
        f"Webhooks: delivered {counts['delivered']}, retrying {counts['retrying']}, failed "
        f"{counts['failed']}, skipped {counts['skipped']}."
    )


# A pass runs each step in turn; a step returns a line for the output. A step that fails is
# logged and counted, and stops neither the other steps nor the loop.
STEPS = [("schedules", _schedules), ("webhooks", _webhooks)]


class Command(BaseCommand):
    help = (
        "Enqueue the runs of schedules that are due and send the webhook deliveries that are "
        "due. With --every, keep dispatching; a step that fails is logged and runs again on "
        "the next pass."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--every", type=float, default=None, help="Dispatch again every SECONDS (a loop)"
        )
        parser.add_argument(
            "--iterations", type=int, default=None, help="With --every: stop after N passes"
        )
        parser.add_argument(
            "--heartbeat",
            default=None,
            help="Touch this file after each pass in which every step succeeded (a health "
            "check can watch how old it is)",
        )

    def handle(self, *args, every, iterations, heartbeat, **options):
        done = failures = 0
        while True:
            failed = self._pass()
            failures += failed
            if heartbeat and not failed:
                Path(heartbeat).touch()
            done += 1
            if every is None or (iterations is not None and done >= iterations):
                if failures:
                    raise CommandError(f"{failures} dispatch steps failed; the log has why.")
                return
            _sleep(every)

    def _pass(self) -> int:
        """Run every step; returns how many failed."""
        failed = 0
        for name, step in STEPS:
            try:
                self.stdout.write(step())
            except Exception:
                failed += 1
                logger.exception("Dispatch step failed", extra={"step": name})
                self.stderr.write(f"The {name} step failed; it runs again on the next pass.")
        return failed
