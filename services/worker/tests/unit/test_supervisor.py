"""The supervisor loop: starting, idling, stopping, and refusing to start."""

from __future__ import annotations

import logging
import threading

import pytest

from forklift_worker import supervisor as supervisor_module
from forklift_worker.isolation import Isolation, Platform
from forklift_worker.supervisor import EXIT_GATEWAY, EXIT_OK, StartupError, Supervisor


def stop_when(gateway, supervisor, predicate, *, twice: bool = False) -> threading.Thread:
    """Request a stop (as SIGTERM does) once ``predicate()`` holds."""

    def watch() -> None:
        gateway.wait_for(predicate)
        supervisor.request_stop()
        if twice:
            supervisor.request_stop()

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


def test_an_idle_worker_backs_off_and_stops_on_request(gateway, make_settings):
    supervisor = Supervisor(make_settings(max_jobs=0))
    stop_when(gateway, supervisor, lambda: len(gateway.calls("leases")) >= 3)
    assert supervisor.run() == EXIT_OK
    assert supervisor.outcomes == []


def test_a_draining_worker_hands_back_a_job_that_runs_too_long(gateway, make_settings, caplog):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    supervisor = Supervisor(make_settings(max_jobs=0, drain_seconds=0.2))
    stop_when(gateway, supervisor, lambda: any(b["rows_read"] == 5 for b in job.heartbeats))
    caplog.set_level(logging.INFO, logger="forklift_worker")
    assert supervisor.run() == EXIT_OK
    assert supervisor.outcomes == ["abandoned"]
    assert job.completed is None
    assert "--drain-seconds (0.2) is over" in caplog.text


def test_a_second_stop_request_stops_running_jobs_at_once(gateway, make_settings, caplog):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    supervisor = Supervisor(make_settings(max_jobs=0, drain_seconds=3600))
    stop_when(
        gateway, supervisor, lambda: any(b["rows_read"] == 5 for b in job.heartbeats), twice=True
    )
    caplog.set_level(logging.INFO, logger="forklift_worker")
    assert supervisor.run() == EXIT_OK
    assert supervisor.outcomes == ["abandoned"]
    assert "a second stop signal" in caplog.text


def test_a_job_that_finishes_while_draining_is_reported(gateway, make_settings):
    job = gateway.enqueue()
    supervisor = Supervisor(make_settings(max_jobs=0, drain_seconds=60))
    stop_when(gateway, supervisor, lambda: len(gateway.calls("leases")) >= 1)
    assert supervisor.run() == EXIT_OK
    assert job.completed["result"]["status"] == "succeeded"


def test_the_gateway_down_and_back(gateway, make_settings, caplog):
    gateway.fail("leases", 503, times=3)
    job = gateway.enqueue()
    caplog.set_level(logging.INFO, logger="forklift_worker")
    supervisor = Supervisor(make_settings())
    assert supervisor.run() == EXIT_OK
    assert job.completed is not None
    assert caplog.text.count("lease request failed") == 1, "an outage is logged once"
    assert "the gateway is reachable again" in caplog.text


@pytest.mark.parametrize("fault", [401, 404, 422])
def test_a_gateway_that_refuses_the_worker_stops_it(gateway, make_settings, fault):
    gateway.fail("leases", fault)
    supervisor = Supervisor(make_settings())
    assert supervisor.run() == EXIT_GATEWAY
    assert supervisor.error is not None


def test_a_token_revoked_during_a_job_stops_the_worker(gateway, make_settings):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    gateway.fail("heartbeat", 401)
    supervisor = Supervisor(make_settings())
    assert supervisor.run() == EXIT_GATEWAY
    assert supervisor.outcomes == ["abandoned"]


def test_max_jobs_with_several_slots_leases_no_more_than_that(gateway, make_settings):
    jobs = [gateway.enqueue() for _ in range(3)]
    supervisor = Supervisor(make_settings(concurrency=2, max_jobs=2))
    assert supervisor.run() == EXIT_OK
    assert len(supervisor.outcomes) == 2
    assert sum(job.completed is not None for job in jobs) == 2
    assert len(gateway.queue) == 1


def test_a_job_leased_after_a_halt_is_handed_straight_back(gateway, make_settings):
    supervisor = Supervisor(make_settings())
    supervisor.halt("testing")

    class Runner:
        def run(self, lease, control):
            return "abandoned" if control.abandon.is_set() else "ran"

    supervisor._run_job(Runner(), lease=None)
    assert supervisor.outcomes == ["abandoned"]


def test_leftover_scratch_directories_are_removed(make_settings, caplog):
    settings = make_settings()
    leftover = settings.scratch / "job-old-a1-1234"
    (leftover / "out").mkdir(parents=True)
    (settings.scratch / "keep-me").mkdir()
    caplog.set_level(logging.INFO, logger="forklift_worker")
    supervisor = Supervisor(settings)
    supervisor.prepare()
    supervisor.release()
    supervisor.release()
    assert not leftover.exists() and (settings.scratch / "keep-me").exists()
    assert "removed scratch directories" in caplog.text


def test_two_workers_cannot_share_a_scratch_directory(make_settings):
    first = Supervisor(make_settings())
    first.prepare()
    with pytest.raises(StartupError, match="Another forklift-worker is using"):
        Supervisor(make_settings()).prepare()
    first.release()


def test_an_unusable_scratch_directory(make_settings, tmp_path):
    (tmp_path / "file").write_text("x")
    with pytest.raises(StartupError, match="cannot be used"):
        Supervisor(make_settings(scratch=tmp_path / "file" / "scratch")).prepare()


def test_isolation_problems_stop_the_start(make_settings):
    settings = make_settings(allow_root=False)
    platform = Platform(
        landlock_abi=0, network_namespace=False, network_namespace_problem="", euid=0
    )
    supervisor = Supervisor(settings, isolation=Isolation(settings, platform))
    with pytest.raises(StartupError, match="running as root"):
        supervisor.run()


def test_startup_warnings_are_logged(make_settings, gateway, caplog):
    settings = make_settings(landlock="auto")
    platform = Platform(
        landlock_abi=0, network_namespace=False, network_namespace_problem="", euid=0
    )
    gateway.enqueue()
    caplog.set_level(logging.INFO, logger="forklift_worker")
    supervisor = Supervisor(
        settings, isolation=Isolation(settings, platform), engine_version="9.9"
    )
    assert supervisor.run() == EXIT_OK
    assert "no Landlock support" in caplog.text
    assert gateway.calls("leases")[0]["body"]["engine_version"] == "9.9"


def test_the_engine_version(monkeypatch):
    assert supervisor_module.installed_engine_version() != "unknown"

    def missing(name):
        raise supervisor_module.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(supervisor_module.metadata, "version", missing)
    assert supervisor_module.installed_engine_version() == "unknown"


def test_halt_and_fatal_happen_once(make_settings, caplog):
    supervisor = Supervisor(make_settings())
    caplog.set_level(logging.INFO, logger="forklift_worker")
    supervisor.fatal(RuntimeError("first"))
    supervisor.fatal(RuntimeError("second"))
    supervisor.halt("again")
    assert str(supervisor.error) == "first"
    assert caplog.text.count("stopping running jobs") == 1
    assert "second" not in caplog.text


def test_an_internal_error_stops_the_worker(gateway, make_settings, caplog):
    gateway.enqueue()

    class Broken:
        def run(self, lease, control):
            raise RuntimeError("a bug")

    supervisor = Supervisor(make_settings(max_jobs=0), runner_factory=lambda s, g: Broken())
    assert supervisor.run() == supervisor_module.EXIT_INTERNAL
    assert supervisor.outcomes == ["crashed"]
    assert "internal error while running a job" in caplog.text and "a bug" in caplog.text


def test_a_supervisor_that_cannot_be_a_subreaper_says_so(make_settings, monkeypatch):
    def refuse():
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(supervisor_module.linux, "set_child_subreaper", refuse)
    supervisor = Supervisor(make_settings())
    warnings = supervisor.prepare()
    supervisor.release()
    assert any("cannot become its engines' subreaper" in warning for warning in warnings)
