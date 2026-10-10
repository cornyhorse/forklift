"""The forklift-worker command: settings errors, a clean run, SIGTERM, and python -m."""

from __future__ import annotations

import importlib
import logging
import os
import runpy
import signal
import sys
import threading

import pytest

from forklift_worker import cli, logs
from forklift_worker.supervisor import EXIT_CONFIG, EXIT_OK, Supervisor


@pytest.fixture(autouse=True)
def restore_logging():
    root = logging.getLogger(logs.LOGGER_NAME)
    saved = (list(root.handlers), root.level, root.propagate)
    yield
    root.handlers[:], root.level, root.propagate = saved


@pytest.fixture
def environment(monkeypatch, tmp_path, gateway, make_settings):
    """FORKLIFT_WORKER_* variables for a worker that runs the fake engine against ``gateway``."""
    settings = make_settings()
    variables = {
        "FORKLIFT_WORKER_GATEWAY": gateway.url,
        "FORKLIFT_WORKER_TOKEN_FILE": str(settings.token_file),
        "FORKLIFT_WORKER_SCRATCH": str(settings.scratch),
        "FORKLIFT_WORKER_ENGINE_COMMAND": " ".join(settings.engine_command),
        "FORKLIFT_WORKER_ENGINE_READ_PATHS": str(settings.engine_read_path[0]),
        "FORKLIFT_WORKER_ALLOW_ROOT": "1",
        "FORKLIFT_WORKER_IDLE_MIN_SECONDS": "0.01",
        "FORKLIFT_WORKER_IDLE_MAX_SECONDS": "0.02",
        "FORKLIFT_WORKER_LOG_FORMAT": "text",
    }
    for name, value in variables.items():
        monkeypatch.setenv(name, value)
    return settings


def exit_code(argv) -> int:
    with pytest.raises(SystemExit) as caught:
        cli.main(argv)
    return caught.value.code


def test_invalid_settings_exit_with_2(capsys, monkeypatch):
    monkeypatch.delenv("FORKLIFT_WORKER_GATEWAY", raising=False)
    assert exit_code(["--scratch", "/s", "--token-file", "/t"]) == EXIT_CONFIG
    assert (
        "forklift-worker: --gateway / FORKLIFT_WORKER_GATEWAY is required"
        in capsys.readouterr().err
    )


def test_a_run_from_the_environment(environment, gateway, capsys):
    previous = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    job = gateway.enqueue()
    assert exit_code(["--max-jobs", "1"]) == EXIT_OK
    assert job.completed["result"]["status"] == "succeeded"
    assert "forklift-worker started" in capsys.readouterr().err
    assert (signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)) == previous


def test_sigterm_stops_the_worker(environment, gateway):
    previous = signal.getsignal(signal.SIGTERM)

    def send_sigterm() -> None:
        gateway.wait_for(lambda: len(gateway.calls("leases")) >= 2)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=send_sigterm, daemon=True).start()
    assert exit_code([]) == EXIT_OK
    assert signal.getsignal(signal.SIGTERM) is previous


def test_a_worker_that_cannot_start_exits_with_2(environment, capsys):
    holder = Supervisor(environment)
    holder.prepare()
    try:
        assert exit_code([]) == EXIT_CONFIG
    finally:
        holder.release()
    assert "Another forklift-worker is using the scratch directory" in capsys.readouterr().err


def test_python_dash_m(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["forklift-worker", "--version"])
    with pytest.raises(SystemExit) as caught:
        runpy.run_module("forklift_worker", run_name="__main__")
    assert caught.value.code == 0
    assert "forklift-worker 0.1.0" in capsys.readouterr().out


def test_importing_the_main_module_runs_nothing(monkeypatch):
    monkeypatch.delitem(sys.modules, "forklift_worker.__main__", raising=False)
    module = importlib.import_module("forklift_worker.__main__")
    assert module.main is cli.main


def test_the_supervisor_imports_neither_the_engine_nor_pyarrow():
    import subprocess

    modules = [f"forklift_worker.{name}" for name in ("cli", "supervisor", "job", "sandbox")]
    heavy = ("forklift", "pyarrow", "boto3")
    code = (
        f"import sys; import {', '.join(modules)}; "
        f"print(sorted(m for m in sys.modules if m.split('.')[0] in {heavy!r}))"
    )
    done = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "[]"
