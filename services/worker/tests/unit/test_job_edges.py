"""A job's rarer paths: interruptions in every phase, gateway refusals, scratch clean-up."""

from __future__ import annotations

import logging
import os
import stat

import pytest

from forklift_worker import job as job_module
from forklift_worker.retry import Interrupted
from forklift_worker.supervisor import EXIT_GATEWAY


def waiting_stage_input(http, item, workdir, *, stopped, wait, on_progress, **kwargs):
    """Staging that takes until the job is stopped, then reports the interruption."""
    on_progress(1)
    wait(30)
    raise Interrupted()


def finishing_stage_input(http, item, workdir, *, stopped, wait, on_progress, **kwargs):
    """Staging that finishes just as the job is stopped."""
    wait(30)


def test_cancel_while_staging(gateway, run_worker, monkeypatch):
    monkeypatch.setattr(job_module, "stage_input", waiting_stage_input)
    job = gateway.enqueue()
    gateway.cancel_when = lambda job, progress: "bytes_staged" in progress
    run_worker()

    result = job.completed["result"]
    assert result["status"] == "cancelled"
    assert result["error"]["message"] == "The job was cancelled while its input was being staged."


def test_cancel_right_after_staging(gateway, run_worker, monkeypatch):
    monkeypatch.setattr(job_module, "stage_input", finishing_stage_input)
    job = gateway.enqueue()
    gateway.cancel_when = lambda job, progress: True  # the first heartbeat comes while staging
    run_worker()

    assert job.completed["result"]["status"] == "cancelled"


def test_a_lease_lost_while_staging(gateway, run_worker, monkeypatch):
    monkeypatch.setattr(job_module, "stage_input", waiting_stage_input)
    job = gateway.enqueue()
    gateway.lost_when = lambda job, progress: "bytes_staged" in progress
    supervisor = run_worker()

    assert job.completed is None and supervisor.outcomes == ["abandoned"]


def test_cancel_while_uploading(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("put", 503, times=10)
    gateway.cancel_when = lambda job, progress: bool(gateway.calls("put"))
    run_worker()

    result = job.completed["result"]
    assert result["status"] == "cancelled"
    assert "while its artifacts were being uploaded" in result["error"]["message"]


def test_the_workers_staging_limit_can_be_lower_than_the_gateways(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    run_worker(stage_max_bytes=4)

    assert job.completed["result"]["status"] == "succeeded"
    assert len(gateway.calls("get")) == 1, "streamed: only the engine read the input"


def test_an_engine_command_that_does_not_exist(gateway, run_worker):
    job = gateway.enqueue()
    run_worker(engine_command=["/no/such/forklift"])

    error = job.completed["result"]["error"]
    assert error["code"] == "INTERNAL" and error["retryable"] is True
    assert (
        "sandbox could not be set up on this worker: cannot start the engine command"
        in error["message"]
    )


def test_an_engine_that_exits_125_itself(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "exit", "code": 125, "stderr": ["odd"]}}
    run_worker()

    assert "exited with code 125" in job.completed["result"]["error"]["message"]


def test_a_job_without_artifacts_is_completed_without_presigning(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["kind"] = "validate_schema"
    job.spec["options"] = {
        "fake_engine": {
            "mode": "result",
            "result": {
                "job_id": job.job_id,
                "status": "succeeded",
                "artifacts": [],
                "error": None,
            },
        }
    }
    run_worker()

    assert job.completed["artifacts"] == []
    assert gateway.calls("presign") == []


def test_a_token_refused_at_presign(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("presign", 401)
    supervisor = run_worker()

    assert supervisor.exit_code == EXIT_GATEWAY
    assert job.completed is None


def test_presign_while_the_gateway_is_down(gateway, run_worker, monkeypatch):
    monkeypatch.setattr(job_module, "PRESIGN_ATTEMPTS", 1)
    job = gateway.enqueue()
    gateway.fail("presign", 503)
    supervisor = run_worker()

    assert job.completed is None and supervisor.outcomes == ["abandoned"]


@pytest.mark.parametrize(
    "fault, outcome, exit_code",
    [(409, "abandoned", 0), (400, "unreported", 0), (401, "unreported", EXIT_GATEWAY)],
)
def test_complete_refused(gateway, run_worker, fault, outcome, exit_code):
    gateway.enqueue()
    gateway.fail("complete", fault)
    supervisor = run_worker()

    assert supervisor.outcomes == [outcome]
    assert supervisor.exit_code == exit_code


def test_a_lease_lost_while_completing(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("complete", 503, times=100)
    gateway.lost_when = lambda job, progress: bool(gateway.calls("complete"))
    supervisor = run_worker()

    assert job.completed is None and supervisor.outcomes == ["abandoned"]


def test_a_scratch_directory_that_cannot_be_removed_is_logged(
    gateway, run_worker, monkeypatch, caplog
):
    def refuse(path):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(job_module, "remove_tree", refuse)
    job = gateway.enqueue()
    caplog.set_level(logging.ERROR, logger="forklift_worker")
    supervisor = run_worker()

    assert job.completed["result"]["status"] == "succeeded"
    assert supervisor.outcomes == ["succeeded"]
    assert "scratch directory could not be removed" in caplog.text


def test_removal_goes_through_directories_the_engine_locked(tmp_path):
    locked = tmp_path / "locked"
    (locked / "inner").mkdir(parents=True)
    (locked / "file").write_text("x")
    os.chmod(locked / "inner", 0)
    os.chmod(locked, stat.S_IRUSR | stat.S_IXUSR)
    job_module._make_writable_and_retry(os.unlink, str(locked / "file"), None)
    job_module._make_writable_and_retry(os.rmdir, str(locked / "inner"), None)
    assert os.listdir(locked) == []
    job_module.remove_tree(tmp_path / "locked")
    assert not locked.exists()


def test_a_scratch_directory_that_cannot_be_created_fails_the_job(
    gateway, run_worker, monkeypatch
):
    def refuse(self, lease):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(job_module.JobRunner, "_make_workdir", refuse)
    job = gateway.enqueue()
    run_worker()

    error = job.completed["result"]["error"]
    assert error["code"] == "INTERNAL" and error["retryable"] is True
    assert "could not create the job's scratch directory" in error["message"]
    assert "No space left on device" in error["message"]
