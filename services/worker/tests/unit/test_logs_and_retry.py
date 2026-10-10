"""Structured logs, and backoff with retries."""

from __future__ import annotations

import io
import json
import logging
import random

import pytest

from forklift_worker import logs
from forklift_worker.retry import Backoff, Interrupted, retry


@pytest.fixture
def restore_logging():
    root = logging.getLogger(logs.LOGGER_NAME)
    saved = (list(root.handlers), root.level, root.propagate)
    yield
    root.handlers[:], root.level, root.propagate = saved[0], saved[1], saved[2]


def test_json_logs_carry_context_as_keys(restore_logging):
    stream = io.StringIO()
    logs.configure("info", "json", stream)
    logs.logger("job").info("job leased %s", "now", extra={"job_id": "j1", "attempt": 2})
    logs.logger("job").debug("hidden")
    try:
        raise ValueError("broken")
    except ValueError:
        logs.logger().exception("failed")
    first, second = [
        json.loads(line) for line in stream.getvalue().splitlines() if line.startswith("{")
    ]
    assert first["message"] == "job leased now"
    assert first["level"] == "info" and first["logger"] == "forklift_worker.job"
    assert first["job_id"] == "j1" and first["attempt"] == 2
    assert first["time"].endswith("Z")
    assert "ValueError: broken" in second["exception"]


def test_text_logs(restore_logging):
    stream = io.StringIO()
    logs.configure("debug", "text", stream)
    logs.logger().info("started", extra={"lanes": ["batch"]})
    logs.logger().warning("plain")
    try:
        raise KeyError("k")
    except KeyError:
        logs.logger().error("oops", exc_info=True)
    lines = stream.getvalue().splitlines()
    assert lines[0] == "info    started [lanes=['batch']]"
    assert lines[1] == "warning plain"
    assert lines[2] == "error   oops" and "KeyError" in stream.getvalue()


def test_configure_replaces_its_handler(restore_logging):
    logs.configure("info", "json", io.StringIO())
    logs.configure("info", "json", io.StringIO())
    assert len(logging.getLogger(logs.LOGGER_NAME).handlers) == 1


def test_backoff_grows_to_its_maximum_with_jitter():
    backoff = Backoff(1.0, 4.0, rng=random.Random(1))
    delays = [backoff.next() for _ in range(5)]
    steps = [1, 2, 4, 4, 4]
    for delay, step in zip(delays, steps):
        assert step / 2 <= delay <= step
    backoff.reset()
    assert backoff.next() <= 1.0


def test_retry_until_success_or_a_permanent_error():
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("down")
        return "ok"

    retried = []
    result = retry(
        flaky,
        attempts=5,
        retryable=lambda error: isinstance(error, ConnectionError),
        backoff=Backoff(0.001, 0.001),
        wait=lambda seconds: False,
        on_retry=lambda error, attempt, delay: retried.append(attempt),
    )
    assert result == "ok" and retried == [1, 2]

    def permanent():
        raise ValueError("bad")

    with pytest.raises(ValueError):
        retry(permanent, attempts=5, retryable=lambda e: False, backoff=Backoff(1, 1), wait=None)


def test_retry_gives_up_or_is_interrupted():
    def down():
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(
            down,
            attempts=2,
            retryable=lambda e: True,
            backoff=Backoff(0.001, 0.001),
            wait=lambda s: False,
        )
    with pytest.raises(Interrupted):
        retry(
            down, attempts=5, retryable=lambda e: True, backoff=Backoff(1, 1), wait=lambda s: True
        )
