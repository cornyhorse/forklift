"""The dispatcher command: one pass, the loop, the heartbeat and steps that fail."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import DatabaseError
from django.utils import timezone
from world import World, make_schedule

from forklift_web.core.management.commands import dispatch
from forklift_web.core.models import Job
from forklift_web.services import schedules

pytestmark = pytest.mark.django_db


def run(*args) -> tuple:
    out, err = StringIO(), StringIO()
    call_command("dispatch", *args, stdout=out, stderr=err)
    return out.getvalue(), err.getvalue()


def test_one_pass_enqueues_due_schedules():
    world = World.build()
    schedule = make_schedule(
        world.author,
        world.version,
        world.connection,
        cron="* * * * *",
        next_run_at=timezone.now().replace(second=0, microsecond=0),
    )
    out, err = run()
    assert (
        "Schedules: queued 1, skipped 0 (still running), missed 0, failed to queue 0, errors 0."
        in out
    )
    assert err == ""
    assert Job.objects.get(schedule=schedule).requested_by is None


def test_a_loop_and_its_heartbeat(monkeypatch, tmp_path):
    heartbeat = tmp_path / "dispatcher.alive"
    sleeps, touched = [], []

    def sleep(seconds):
        sleeps.append(seconds)
        touched.append(heartbeat.exists())

    monkeypatch.setattr(dispatch, "_sleep", sleep)
    out, _ = run("--every", "10", "--iterations", "3", "--heartbeat", str(heartbeat))
    assert out.count("Schedules: queued 0") == 3
    assert sleeps == [10.0, 10.0] and touched == [True, True] and heartbeat.exists()


def test_the_loop_waits_between_passes():
    out, _ = run("--every", "0.001", "--iterations", "2")
    assert out.count("Schedules: queued 0") == 2


def test_a_failing_step_stops_neither_the_other_steps_nor_the_loop(monkeypatch, tmp_path, caplog):
    calls = []

    def flaky():
        calls.append("flaky")
        if len(calls) == 1:
            raise DatabaseError("the database went away")
        return "Flaky: fine."

    monkeypatch.setattr(dispatch, "STEPS", [("flaky", flaky), ("other", lambda: "Other: fine.")])
    heartbeat = tmp_path / "dispatcher.alive"
    ages = []
    monkeypatch.setattr(dispatch, "_sleep", lambda seconds: ages.append(heartbeat.exists()))
    with caplog.at_level(logging.ERROR, logger="forklift_web.core.management.commands.dispatch"):
        with pytest.raises(CommandError, match="1 dispatch steps failed; the log has why."):
            run("--every", "5", "--iterations", "3", "--heartbeat", str(heartbeat))
    assert calls == ["flaky"] * 3
    record = next(r for r in caplog.records if r.message == "Dispatch step failed")
    assert record.step == "flaky" and record.exc_info
    # No heartbeat after the pass that failed; one after each pass that did not
    assert ages == [False, True] and heartbeat.exists()


def test_one_failing_pass_fails_the_command(monkeypatch):
    def broken():
        raise RuntimeError("a bug")

    monkeypatch.setattr(dispatch, "STEPS", [("broken", broken)])
    out, err = StringIO(), StringIO()
    with pytest.raises(CommandError, match="1 dispatch steps failed"):
        call_command("dispatch", stdout=out, stderr=err)
    assert err.getvalue() == "The broken step failed; it runs again on the next pass.\n"
    assert out.getvalue() == ""


def test_the_schedule_step_counts_runs_it_missed(monkeypatch):
    world = World.build()
    now = datetime(2026, 10, 10, 5, 0, tzinfo=dt_timezone.utc)
    monkeypatch.setattr(schedules, "utc_now", lambda: now)
    make_schedule(
        world.author, world.version, world.connection, next_run_at=now - timedelta(hours=3)
    )
    out, _ = run()
    assert "Schedules: queued 0, skipped 0 (still running), missed 1, failed to queue 0" in out
