"""Fixtures for forklift-client's tests: a fake gateway on a real local HTTP server, and a
client of it with quick retries."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))

from fake_api import TOKEN, FakeForklift  # noqa: E402

from forklift_client import ForkliftClient, RetryPolicy  # noqa: E402

if importlib.util.find_spec("airflow") is None:
    collect_ignore = ["with_airflow"]  # the Airflow tests need apache-airflow (extra "airflow")

QUICK = RetryPolicy(attempts=3, initial_delay=0.001, max_delay=0.004, max_retry_after=5)


@pytest.fixture
def fake():
    with FakeForklift() as server:
        yield server
    assert server.violations == [], "requests or answers that contracts/openapi.json refuses"


@pytest.fixture
def client(fake) -> ForkliftClient:
    return ForkliftClient(fake.url, TOKEN, timeout=10, retry=QUICK)


@pytest.fixture
def waits(client) -> list:
    """The waits between retries the client chose (it does not actually wait)."""
    chosen: list = []
    client._sleep = chosen.append
    return chosen
