"""The looping commands stop on SIGTERM (as process 1 in a container they get no default one)."""

from __future__ import annotations

import os
import signal
import threading
from io import StringIO

import pytest
from django.core.management import call_command
from world import World

from forklift_web.core.management import loop
from forklift_web.core.management.commands import dispatch, publish, sweep_retention
from forklift_web.services import publishing, retention

pytestmark = pytest.mark.django_db


def terminate() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


def run(name: str, *args) -> tuple:
    out, err = StringIO(), StringIO()
    call_command(name, *args, stdout=out, stderr=err)
    return out.getvalue(), err.getvalue()


def sigterm_during_the_pass(monkeypatch, name: str) -> None:
    """Make the command's pass send SIGTERM to this process while it works."""
    if name == "dispatch":
        monkeypatch.setattr(dispatch, "STEPS", [("step", lambda: terminate() or "Step: done.")])
    elif name == "publish":

        def publish_due(**kwargs):
            terminate()
            assert kwargs["stop"]() is True  # the publisher claims no further run
            return {"published": 0, "retrying": 0, "failed": 0, "lost": 0}

        monkeypatch.setattr(publishing, "publish_due", publish_due)
    else:
        World.build()
        sweep = retention.sweep
        monkeypatch.setattr(retention, "sweep", lambda *a, **k: terminate() or sweep(*a, **k))


COMMANDS = {"dispatch": dispatch, "publish": publish, "sweep_retention": sweep_retention}


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_sigterm_during_a_pass_stops_the_loop_after_that_pass(monkeypatch, name):
    sigterm_during_the_pass(monkeypatch, name)
    waits = []
    monkeypatch.setattr(COMMANDS[name], "_sleep", lambda stop, s: waits.append(stop.requested))
    before = signal.getsignal(signal.SIGTERM)

    out, err = run(name, "--every", "3600", "--iterations", "5")

    assert len(out.splitlines()) == 1  # one pass, its one line
    assert waits == [True]  # the wait after the pass ends at once, and the loop with it
    assert err.endswith("Stopping: SIGTERM received.\n")
    assert signal.getsignal(signal.SIGTERM) is before  # the previous handler is put back


def test_sigterm_ends_the_wait_between_passes(monkeypatch):
    # The pass arranges a SIGTERM shortly after it ends, while the command waits for an hour;
    # an alarm fails the test instead of hanging it if the wait does not end
    monkeypatch.setattr(dispatch, "STEPS", [("step", lambda: arm() or "Step: done.")])

    def arm():
        threading.Timer(0.2, terminate).start()

    def too_long(signum, frame):
        raise AssertionError("SIGTERM did not end the wait between passes")

    previous = signal.signal(signal.SIGALRM, too_long)
    signal.alarm(30)
    try:
        out, err = run("dispatch", "--every", "3600")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    assert out == "Step: done.\n" and err == "Stopping: SIGTERM received.\n"


def test_a_single_pass_finishes_its_work_on_sigterm(monkeypatch):
    sigterm_during_the_pass(monkeypatch, "dispatch")
    out, err = run("dispatch")
    assert out == "Step: done.\n" and err == ""


def test_stop_wait_returns_once_requested():
    stop = loop.Stop()
    assert stop.requested is False
    stop.request()
    stop.wait(3600)  # returns at once
    assert stop.requested is True
