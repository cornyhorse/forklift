"""Stopping the commands that keep running (``dispatch``, ``publish``, ``sweep_retention``).

In a container such a command is often process 1, and Linux delivers no signal to process 1 that
it has no handler for: without one, ``docker compose stop`` and a Kubernetes pod deletion wait out
their grace period and then kill it. While a loop runs, SIGTERM asks it to stop: the pass in
progress finishes (the publisher claims no further run), the wait before the next pass ends at
once, and the command returns.
"""

from __future__ import annotations

import signal
import threading
from contextlib import contextmanager
from typing import Iterator


class Stop:
    """Set once SIGTERM has arrived."""

    def __init__(self) -> None:
        self._event = threading.Event()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def request(self) -> None:
        self._event.set()

    def wait(self, seconds: float) -> None:
        """Wait ``seconds`` before the next pass, or less if a stop is requested meanwhile."""
        self._event.wait(seconds)


@contextmanager
def stopped_by_sigterm() -> Iterator[Stop]:
    """A :class:`Stop` that SIGTERM sets while the block runs; the previous handler is put back
    afterwards. Call it from the main thread (where Django runs commands)."""
    stop = Stop()
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: stop.request())
    try:
        yield stop
    finally:
        signal.signal(signal.SIGTERM, previous)
