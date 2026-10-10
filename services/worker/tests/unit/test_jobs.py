"""A worker running jobs end to end against the fake gateway, with the fake engine."""

from __future__ import annotations

import hashlib
import json
import logging
import signal

import pytest
from fake_gateway import make_spec

from forklift_worker.supervisor import EXIT_GATEWAY, EXIT_OK, Supervisor


def scratch_entries(supervisor) -> list[str]:
    return sorted(path.name for path in supervisor.settings.scratch.iterdir())


def completed(job) -> dict:
    assert job.completed is not None, "the job was not completed"
    return job.completed


# --------------------------------------------------------------------------- success


def test_a_staged_job_uploads_its_artifacts_and_completes(gateway, run_worker):
    job = gateway.enqueue()
    supervisor = run_worker()

    assert supervisor.exit_code == EXIT_OK
    report = completed(job)
    assert report["attempt"] == 1
    assert report["result"]["status"] == "succeeded"
    assert report["result"]["error"] is None
    names = [artifact["name"] for artifact in report["artifacts"]]
    assert names[-1] == "manifest.json", "the manifest is uploaded last"
    assert sorted(names) == ["bad_rows.parquet", "data.parquet", "manifest.json"]
    for artifact in report["artifacts"]:
        stored = gateway.objects[artifact["key"]]
        assert artifact["key"] == f"jobs/{job.job_id}/attempt-1/{artifact['name']}"
        assert artifact["bytes"] == len(stored)
        assert artifact["sha256"] == hashlib.sha256(stored).hexdigest()
    by_name = {artifact["name"]: artifact for artifact in report["artifacts"]}
    assert by_name["data.parquet"]["kind"] == "data"
    assert by_name["data.parquet"]["rows"] == 2
    # The engine's own (wrong) sizes and hashes are replaced by the supervisor's.
    data_entry = report["result"]["artifacts"][0]
    assert data_entry["sha256"] == by_name["data.parquet"]["sha256"]
    assert data_entry["bytes"] == by_name["data.parquet"]["bytes"]
    # The engine read the staged copy: its output is the input upper-cased.
    assert gateway.objects[by_name["data.parquet"]["key"]] == b"ID,NAME\n1,A\n"
    # Uploads were PUT with Content-MD5 and the headers the gateway asked for.
    for put in gateway.calls("put"):
        assert put["headers"]["content-type"] == "application/octet-stream"
        assert put["headers"]["content-md5"]
    assert scratch_entries(supervisor) == [".forklift-worker.lock"]


def test_the_lease_request_describes_the_worker(gateway, run_worker):
    gateway.enqueue()
    run_worker(lanes=["batch", "interactive"])

    lease = gateway.calls("leases")[0]
    assert lease["headers"]["authorization"] == f"Bearer {gateway.token}"
    assert lease["body"]["worker_id"] == "test-worker"
    assert lease["body"]["lanes"] == ["batch", "interactive"]
    assert lease["body"]["spec_versions"] == [1]
    assert lease["body"]["worker_version"] == "0.1.0"
    assert lease["body"]["engine_version"]


def test_heartbeats_carry_the_engines_progress(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    gateway.cancel_when = lambda job, progress: progress["rows_read"] == 5
    run_worker(heartbeat_seconds=0.02)

    running = [beat for beat in job.heartbeats if beat["rows_read"] == 5]
    assert running[0]["bytes_read"] == 50
    assert all(
        set(beat) >= {"rows_read", "rows_rejected", "bytes_read"} for beat in job.heartbeats
    )


def test_a_failed_job_keeps_its_bad_rows(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "fail"}}
    run_worker()

    report = completed(job)
    assert report["result"]["status"] == "failed"
    assert report["result"]["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"
    assert [artifact["name"] for artifact in report["artifacts"]] == ["bad_rows.parquet"]


def test_a_large_input_is_streamed_with_its_host_as_the_only_allowed_one(gateway, run_worker):
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    run_worker()

    report = completed(job)
    assert report["result"]["status"] == "succeeded"
    probe = json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])
    argv = probe["argv"]
    assert argv.count("--allow-url-host") == 1
    assert argv[argv.index("--allow-url-host") + 1] == gateway.host
    assert "--progress-jsonl" in argv
    # Only the engine fetched the input: the supervisor staged nothing.
    assert len(gateway.calls("get")) == 1
    assert gateway.objects[f"jobs/{job.job_id}/attempt-1/data.parquet"] == b"ID,NAME\n1,A\n"


# --------------------------------------------------------------------------- the engine's process


def test_the_engine_gets_a_scrubbed_environment_and_limits(gateway, run_worker, monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-value")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-key-id")
    monkeypatch.setenv("FORKLIFT_WORKER_GATEWAY", "http://gateway.internal:8081")
    monkeypatch.setenv("SOME_API_TOKEN", "token-value")
    monkeypatch.setenv("TZ", "Europe/Berlin")
    monkeypatch.setenv("ODBCSYSINI", "/etc/odbc")
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    supervisor = run_worker(engine_env=["ODBCSYSINI"], limit_open_files=256)

    probe = json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])
    env = probe["env"]
    assert not [name for name in env if name.startswith(("AWS_", "FORKLIFT_"))]
    assert "SOME_API_TOKEN" not in env
    values = " ".join(env.values())
    assert gateway.token not in values and gateway.url not in values
    assert env["TZ"] == "Europe/Berlin"
    assert env["ODBCSYSINI"] == "/etc/odbc"
    workdir = probe["cwd"]
    assert workdir.startswith(str(supervisor.settings.scratch / f"job-{job.job_id}-a1-"))
    assert env["HOME"] == workdir and env["TMPDIR"] == workdir + "/tmp"
    limits = probe["rlimits"]
    assert limits["core"] == [0, 0]
    assert limits["nofile"] == [256, 256]
    assert limits["as"][0] > 0 and limits["fsize"][0] > 0 and limits["cpu"][0] > 0


def test_cancel_stops_the_engine_and_completes_as_cancelled(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    gateway.cancel_when = lambda job, progress: progress["rows_read"] == 5
    run_worker()

    report = completed(job)
    assert report["result"]["status"] == "cancelled"
    assert report["result"]["error"]["code"] == "CANCELLED"
    assert report["artifacts"] == []
    assert gateway.calls("put") == []


def test_an_engine_that_ignores_sigterm_is_killed(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "stubborn"}}
    gateway.cancel_when = lambda job, progress: progress["rows_read"] == 5
    run_worker(kill_grace_seconds=0.2)

    result = completed(job)["result"]
    assert result["status"] == "cancelled"
    assert "ignored SIGTERM and was killed" in result["error"]["message"]


def test_an_engine_over_its_wall_clock_limit_fails_the_job(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    job.spec["limits"]["max_seconds"] = 0.3
    run_worker()

    error = completed(job)["result"]["error"]
    assert error["code"] == "LIMIT_EXCEEDED"
    assert "0.3 seconds" in error["message"]


def test_a_lost_lease_stops_the_engine_and_reports_nothing(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    gateway.fail("heartbeat", 409)
    supervisor = run_worker()

    assert job.completed is None
    assert gateway.calls("presign") == [] and gateway.calls("complete") == []
    assert supervisor.outcomes == ["abandoned"]
    assert scratch_entries(supervisor) == [".forklift-worker.lock"]


def test_a_lease_without_accepted_heartbeats_is_given_up(gateway, run_worker):
    gateway.lease_seconds = 0.3
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    gateway.fail("heartbeat", 503, times=1000)
    supervisor = run_worker()

    assert job.completed is None
    assert supervisor.outcomes == ["abandoned"]


# --------------------------------------------------------------------------- crashes


@pytest.mark.parametrize(
    "behaviour, code, phrase",
    [
        ({"mode": "exit", "code": 3, "stderr": ["Traceback", "boom"]}, "INTERNAL", "boom"),
        ({"mode": "exit", "code": 2, "stderr": ["bad spec"]}, "SPEC_INVALID", "exit code 2"),
        ({"mode": "signal", "signal": signal.SIGSEGV}, "INTERNAL", "RLIMIT_AS"),
        ({"mode": "signal", "signal": signal.SIGKILL}, "INTERNAL", "out-of-memory"),
        ({"mode": "signal", "signal": signal.SIGXCPU}, "LIMIT_EXCEEDED", "RLIMIT_CPU"),
        ({"mode": "result", "text": "{not json"}, "INTERNAL", "not JSON"),
        (
            {"mode": "result", "result": {"job_id": "someone-else", "status": "succeeded"}},
            "INTERNAL",
            "another job",
        ),
    ],
)
def test_an_engine_crash_becomes_a_failed_result(gateway, run_worker, behaviour, code, phrase):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": behaviour}
    run_worker()

    result = completed(job)["result"]
    assert result["status"] == "failed"
    assert result["error"]["code"] == code
    assert phrase in result["error"]["message"]
    assert completed(job)["artifacts"] == []


def _result_listing(job_id: str, *artifacts) -> dict:
    return {
        "spec_version": 1,
        "job_id": job_id,
        "status": "succeeded",
        "counts": {"total_rows": 1},
        "artifacts": list(artifacts),
        "error": None,
    }


@pytest.mark.parametrize(
    "files, symlinks, artifact, phrase",
    [
        ({}, {"out/link.parquet": "/etc/passwd"}, "out/link.parquet", "leaves the scratch"),
        ({}, {"out/inner": "/etc"}, "out/inner/passwd", "leaves the scratch"),
        ({}, {}, "../outside.parquet", "leaves the scratch"),
        ({}, {}, "out/missing.parquet", "did not write it"),
        ({"out/bad name.parquet": "x"}, {}, "out/bad name.parquet", "upload name"),
    ],
)
def test_artifacts_outside_scratch_or_missing_are_refused(
    gateway, run_worker, files, symlinks, artifact, phrase
):
    job = gateway.enqueue()
    job.spec["options"] = {
        "fake_engine": {
            "mode": "result",
            "files": files,
            "symlinks": symlinks,
            "result": _result_listing(job.job_id, {"kind": "data", "path": artifact}),
        }
    }
    run_worker()

    result = completed(job)["result"]
    assert result["error"]["code"] == "INTERNAL"
    assert phrase in result["error"]["message"]
    assert result["counts"] == {"total_rows": 1}, "the engine's counts are kept"
    assert gateway.calls("put") == []


# --------------------------------------------------------------------------- inputs


def test_a_missing_input_fails_as_unreadable(gateway, run_worker):
    location = gateway.put_object("uploads/x/people.csv", b"id\n1\n")
    del gateway.objects["uploads/x/people.csv"]
    job = gateway.enqueue(make_spec("x", location))
    run_worker()

    error = completed(job)["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert "no longer exists" in error["message"]


@pytest.mark.parametrize("fault", [503, "drop"])
def test_a_transient_download_failure_is_retried(gateway, run_worker, fault):
    job = gateway.enqueue()
    gateway.fail("get", fault)
    run_worker()

    assert completed(job)["result"]["status"] == "succeeded"
    assert len(gateway.calls("get")) == 2


def test_an_input_that_changed_since_it_was_queued_is_refused(gateway, run_worker):
    location = gateway.put_object("uploads/y/people.csv", b"id\n1\n")
    gateway.objects["uploads/y/people.csv"] = b"id\n1\n2\n"
    job = gateway.enqueue(make_spec("y", location))
    run_worker()

    error = completed(job)["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert "changed after the job was queued" in error["message"]


def test_a_spec_with_an_s3_location_is_refused(gateway, run_worker):
    job = gateway.enqueue(make_spec("z", {"type": "s3", "uri": "s3://bucket/key.csv"}))
    run_worker()

    error = completed(job)["result"]["error"]
    assert error["code"] == "SPEC_INVALID"
    assert "presigned_url" in error["message"]
    assert gateway.calls("get") == []


# --------------------------------------------------------------------------- uploads


def test_a_transient_upload_failure_is_retried(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("put", 503)
    run_worker()

    report = completed(job)
    assert report["result"]["status"] == "succeeded"
    assert len(report["artifacts"]) == 3


def test_a_refused_upload_fails_the_job(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("put", 403)
    run_worker()

    result = completed(job)["result"]
    assert result["status"] == "failed"
    assert "refused the upload of the artifact" in result["error"]["message"]
    assert completed(job)["artifacts"] == []


def test_a_presign_the_gateway_refuses_fails_the_job(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("presign", 400)
    run_worker()

    error = completed(job)["result"]["error"]
    assert "would not presign" in error["message"]


def test_a_lease_lost_at_presign_reports_nothing(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("presign", 409)
    supervisor = run_worker()

    assert job.completed is None
    assert supervisor.outcomes == ["abandoned"]


def test_complete_is_retried_while_the_gateway_is_unavailable(gateway, run_worker):
    job = gateway.enqueue()
    gateway.fail("complete", 503, times=2)
    supervisor = run_worker()

    assert completed(job)["result"]["status"] == "succeeded"
    assert len(gateway.calls("complete")) == 3
    assert supervisor.outcomes == ["succeeded"]


# --------------------------------------------------------------------------- secrets


def test_connection_strings_never_reach_results_or_logs(gateway, run_worker, caplog):
    connection = "Driver={PostgreSQL Unicode};Server=db;Uid=loader;Pwd=hunter2-very-secret"
    spec = make_spec("sql-1", {"type": "sql", "connection_string": connection})
    spec["input"]["format"] = "sql"
    spec["options"] = {
        "fake_engine": {
            "mode": "exit",
            "code": 1,
            "stderr": [f"cannot connect with {connection}", "password hunter2-very-secret"],
        }
    }
    job = gateway.enqueue(spec, job_id="sql-1")
    caplog.set_level(logging.DEBUG, logger="forklift_worker")
    supervisor = run_worker()

    report = json.dumps(completed(job))
    assert "hunter2-very-secret" not in report
    assert "[redacted]" in report
    assert "hunter2-very-secret" not in caplog.text
    assert scratch_entries(supervisor) == [".forklift-worker.lock"]


# --------------------------------------------------------------------------- the supervisor


def test_a_refused_worker_token_stops_the_worker(gateway, make_settings, caplog):
    supervisor = Supervisor(make_settings())
    gateway.token = "the-current-token"
    supervisor.exit_code = supervisor.run()

    assert supervisor.exit_code == EXIT_GATEWAY
    assert "refused the worker token" in caplog.text
    assert "the-current-token" not in caplog.text
    assert "worker-token-0123456789" not in caplog.text


def test_jobs_run_concurrently_in_their_own_slots(gateway, run_worker):
    first, second = gateway.enqueue(), gateway.enqueue()
    for job in (first, second):
        job.spec["options"] = {"fake_engine": {"mode": "hang"}}
    # Each job is cancelled only once both run at the same time.
    gateway.cancel_when = lambda job, progress: all(
        any(beat["rows_read"] == 5 for beat in other.heartbeats) for other in (first, second)
    )
    supervisor = run_worker(concurrency=2, max_jobs=2)

    assert sorted(supervisor.outcomes) == ["cancelled", "cancelled"]
    workers = {call["body"]["worker_id"] for call in gateway.calls("leases")}
    assert workers <= {"test-worker-1", "test-worker-2"}


def test_a_result_too_large_to_report_is_shortened(gateway, run_worker, caplog):
    job = gateway.enqueue()
    result = _result_listing(job.job_id)
    result["warnings"] = [f"warning {n}" for n in range(150)]
    job.spec["options"] = {"fake_engine": {"mode": "result", "result": result}}
    caplog.set_level(logging.WARNING, logger="forklift_worker")
    run_worker()

    warnings = completed(job)["result"]["warnings"]
    assert len(warnings) == 101 and warnings[-1] == "... and 50 more warnings"
    assert "too large to report" in caplog.text
