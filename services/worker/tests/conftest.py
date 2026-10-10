"""Fixtures for the worker's tests: a fake gateway, settings pointing at it, a worker run."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable

import pytest

TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))

from fake_gateway import FakeGateway  # noqa: E402

from forklift_worker.settings import Settings  # noqa: E402
from forklift_worker.supervisor import Supervisor  # noqa: E402

FAKE_ENGINE = TESTS / "fake_engine.py"


@pytest.fixture
def gateway():
    with FakeGateway() as fake:
        yield fake
    assert fake.contract_errors == [], "the worker reported a JobResult the contract refuses"


@pytest.fixture
def make_settings(tmp_path: Path, gateway: FakeGateway) -> Callable[..., Settings]:
    """Settings for a worker that talks to ``gateway`` and runs the fake engine."""

    def build(**overrides: Any) -> Settings:
        token_file = tmp_path / "secrets" / "worker-token"
        token_file.parent.mkdir(exist_ok=True)
        token_file.write_text(gateway.token + "\n")
        values: dict[str, Any] = {
            "gateway": gateway.url + "/internal/v1",
            "token_file": token_file,
            "scratch": tmp_path / "scratch",
            "worker_id": "test-worker",
            "engine_command": [sys.executable, str(FAKE_ENGINE)],
            "engine_read_path": [TESTS],
            "allow_root": True,
            "idle_min_seconds": 0.01,
            "idle_max_seconds": 0.02,
            "heartbeat_seconds": 0.05,
            "kill_grace_seconds": 5,
            "http_timeout": 10,
            "max_jobs": 1,
        }
        values.update(overrides)
        return Settings(**values)

    return build


@pytest.fixture
def run_worker(make_settings) -> Callable[..., Supervisor]:
    """Run a supervisor until it has done ``max_jobs`` (default 1) jobs; returns it."""

    def run(**overrides: Any) -> Supervisor:
        supervisor = Supervisor(make_settings(**overrides))
        supervisor.exit_code = supervisor.run()
        return supervisor

    return run
