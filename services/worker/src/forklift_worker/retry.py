"""Exponential backoff with jitter, and a retry loop whose waits a shutdown can interrupt."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, TypeVar

T = TypeVar("T")

# ``wait(seconds)`` returns True when it was interrupted (threading.Event.wait has this shape).
Wait = Callable[[float], bool]


class Interrupted(Exception):
    """A wait between attempts was interrupted (the worker is stopping or the job is over)."""


@dataclass
class Backoff:
    """Delays that start at ``initial`` and grow by ``factor`` up to ``maximum``.

    Each delay is drawn between half and all of the current step ("equal jitter"), so workers
    that failed together do not retry together.
    """

    initial: float
    maximum: float
    factor: float = 2.0
    rng: random.Random = field(default_factory=random.Random)
    _step: float = field(init=False, default=0.0)

    def next(self) -> float:
        self._step = (
            self.initial if self._step == 0 else min(self._step * self.factor, self.maximum)
        )
        return self._step / 2 + self.rng.uniform(0, self._step / 2)

    def reset(self) -> None:
        self._step = 0.0


def retry(
    call: Callable[[], T],
    *,
    attempts: int,
    retryable: Callable[[Exception], bool],
    backoff: Backoff,
    wait: Wait,
    on_retry: Callable[[Exception, int, float], None] = lambda error, attempt, delay: None,
) -> T:
    """``call()`` up to ``attempts`` times while it raises a ``retryable`` error.

    The last error is raised when the attempts run out; a non-retryable one at once. Raises
    Interrupted when ``wait`` is interrupted between attempts.
    """
    attempt = 1
    while True:
        try:
            return call()
        except Exception as error:
            if attempt >= attempts or not retryable(error):
                raise
            delay = backoff.next()
            on_retry(error, attempt, delay)
            if wait(delay):
                raise Interrupted() from error
            attempt += 1
