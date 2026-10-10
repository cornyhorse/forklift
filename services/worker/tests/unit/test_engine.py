"""Reading the engine's progress and stderr, and the command line it gets."""

from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import sys

import pytest

from forklift_worker import engine
from forklift_worker.engine import StderrTail, bounded_lines, progress_event
from forklift_worker.redact import Redactor


def test_long_lines_are_skipped_whole():
    stream = io.BytesIO(b"short\n" + b"x" * 50 + b"\nnext\nend")
    assert list(bounded_lines(stream, limit=10)) == [b"short\n", b"next\n", b"end"]
    assert list(bounded_lines(io.BytesIO(b"y" * 30), limit=10)) == []


def test_progress_keeps_small_whole_numbers_under_simple_names():
    line = json.dumps(
        {
            "rows_read": 10,
            "rows_written": 4,
            "flag": True,
            "Bad Key": 1,
            "negative": -1,
            "huge": 2**63,
            "ratio": 0.5,
            "phase": "reading",
        }
    ).encode()
    assert progress_event(line) == {"rows_read": 10, "rows_written": 4}
    assert progress_event(b"not json") is None
    assert progress_event(b"[1]") is None
    assert progress_event(b'{"text": "x"}') is None
    many = json.dumps({f"k{n}": n for n in range(50)}).encode()
    assert len(progress_event(many)) == engine.MAX_PROGRESS_KEYS


def test_the_stderr_tail_is_bounded_and_redacted():
    tail = StderrTail(Redactor(["topsecret"]))
    for number in range(30):
        tail.add(f"line {number} topsecret\n".encode())
    text = tail.text()
    assert text.splitlines()[0] == "line 10 [redacted]"
    assert len(text.splitlines()) == engine.STDERR_TAIL_LINES
    tail.add(b"z" * 10000)
    assert len(tail.text()) == engine.STDERR_TAIL_CHARS


class Broken(io.BytesIO):
    def readline(self, *args):
        raise ValueError("I/O operation on closed file")


def test_readers_stop_quietly_when_their_pipe_is_closed():
    engine._read_progress(Broken(), lambda event: None)
    engine._read_stderr(Broken(), StderrTail(Redactor()), {})
    seen = []
    engine._read_progress(io.BytesIO(b'{"rows_read": 1}\nnoise\n'), seen.append)
    assert seen == [{"rows_read": 1}]


def test_signalling_a_process_group_that_is_gone():
    process = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    process.wait()
    engine._signal_group(process, 15)  # no error


def test_processes_an_engine_leaves_behind_are_killed(gateway, run_worker, caplog):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "escape"}}
    caplog.set_level(logging.WARNING, logger="forklift_worker")
    run_worker()

    report = json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])
    with pytest.raises(ProcessLookupError):
        os.kill(report["escaped"], 0)
    assert "killed processes the engine left running" in caplog.text


def test_kill_orphans_spares_engines_and_its_own_session(monkeypatch):
    orphan = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    sibling = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    running = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    engine._engines.add(running.pid)
    try:
        assert orphan.pid in engine.kill_orphans()
        assert sibling.poll() is None and running.poll() is None
    finally:
        engine._engines.discard(running.pid)
        for process in (sibling, running):
            process.kill()
            process.wait()
    orphan.wait()


def test_an_orphan_reaped_by_someone_else(monkeypatch):
    orphan = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )

    def already_reaped(pid, options):
        raise ChildProcessError(10, "No child processes")

    monkeypatch.setattr(engine.os, "waitpid", already_reaped)
    assert orphan.pid in engine.kill_orphans()
    orphan.wait()


def test_processes_that_vanish_while_children_are_listed(monkeypatch):
    def vanished(path, *args, **kwargs):
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(engine, "open", vanished, raising=False)
    assert engine._children() == []
