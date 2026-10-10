"""``forklift run-job``: exit codes, the result file, progress lines on stdout, logs on stderr,
and SIGTERM cancelling the job."""

from __future__ import annotations

import json
import os
import signal

import jsonschema
import pytest

import forklift.jobs
from forklift.cli import main
from forklift.jobs import contract
from forklift.jobs.runner import run_job as real_run_job

RESULT_VALIDATOR = jsonschema.Draft202012Validator(contract.jobresult_schema())
ROWS = "".join(f"{i},n{i}\n" for i in range(30))


@pytest.fixture
def job(tmp_path):
    base = tmp_path / "scratch"
    (base / "in").mkdir(parents=True)
    (base / "in" / "people.csv").write_text("id,name\n" + ROWS)
    spec = {
        "spec_version": 1,
        "job_id": "cli-1",
        "kind": "run",
        "input": {"format": "csv", "location": {"type": "file", "path": "in/people.csv"}},
        "output": {"location": {"type": "file", "path": "out/"}},
        "options": {"batch_size": 10},
    }
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec))
    return {"base": base, "spec": path, "result": tmp_path / "results" / "result.json"}


def _main(job, *extra, spec=None):
    argv = [
        "run-job",
        str(spec or job["spec"]),
        "--base-dir",
        str(job["base"]),
        "--result",
        str(job["result"]),
        *extra,
    ]
    try:
        main(argv)
    except SystemExit as stop:
        return stop.code
    return 0


def _result(job):
    document = json.loads(job["result"].read_text())
    assert RESULT_VALIDATOR.is_valid(document)
    return document


class TestRunJob:
    def test_success_writes_the_result_and_logs_to_stderr(self, job, capsys):
        assert _main(job) == 0

        result = _result(job)
        assert result["status"] == "succeeded" and result["counts"]["total_rows"] == 30
        out, err = capsys.readouterr()
        assert out == ""
        assert "Job cli-1: run of a csv input (file)" in err
        assert f"Job cli-1: succeeded; result written to {job['result']}" in err
        assert not [p for p in job["result"].parent.iterdir() if p.name.startswith(".")]

    def test_progress_lines_on_stdout(self, job, capsys):
        assert _main(job, "--progress-jsonl") == 0
        lines = capsys.readouterr().out.splitlines()
        assert [json.loads(line)["rows_read"] for line in lines] == [10, 20, 30]

    def test_failed_job_exits_with_1(self, job, capsys):
        spec = json.loads(job["spec"].read_text())
        spec["input"]["location"]["path"] = "in/missing.csv"
        job["spec"].write_text(json.dumps(spec))

        assert _main(job) == 1
        assert _result(job)["error"]["code"] == "INPUT_UNREADABLE"
        assert "Error (INPUT_UNREADABLE): FileNotFoundError: Input file not found" in (
            capsys.readouterr().err
        )

    def test_invalid_spec_exits_with_2(self, job):
        spec = json.loads(job["spec"].read_text())
        spec["kind"] = "nope"
        job["spec"].write_text(json.dumps(spec))

        assert _main(job) == 2
        result = _result(job)
        assert result["job_id"] == "cli-1" and result["error"]["code"] == "SPEC_INVALID"

    @pytest.mark.parametrize("content", ["{not json", None])
    def test_unreadable_spec_exits_with_2(self, job, content, capsys):
        spec = job["spec"]
        if content is None:
            spec = spec.with_name("missing.json")
        else:
            spec.write_text(content)

        assert _main(job, spec=spec) == 2
        result = _result(job)
        assert result["job_id"] is None and result["error"]["code"] == "SPEC_INVALID"
        assert f"Cannot read the job spec {spec}" in capsys.readouterr().err

    def test_base_dir_must_exist(self, job):
        job["base"] = job["base"] / "missing"
        assert _main(job) == 2
        result = _result(job)
        assert result["job_id"] == "cli-1"
        assert "is not a directory" in result["error"]["message"]

    def test_allowed_hosts_reach_run_job(self, job, monkeypatch):
        seen = {}

        def fake(document, **kwargs):
            seen.update(kwargs)
            return real_run_job(document, **kwargs)

        monkeypatch.setattr(forklift.jobs, "run_job", fake)
        assert _main(job, "--allow-url-host", "store:9000", "--allow-url-host", "s2") == 0
        assert seen["allowed_url_hosts"] == ["store:9000", "s2"]
        assert seen["progress"] is None and seen["cancel"]() is False

    def test_sigterm_cancels_the_job(self, job, monkeypatch, capsys):
        before = signal.getsignal(signal.SIGTERM)

        def terminated_after_the_first_batch(document, **kwargs):
            report = kwargs["progress"]

            def progress(event):
                os.kill(os.getpid(), signal.SIGTERM)
                report(event)

            return real_run_job(document, **dict(kwargs, progress=progress))

        monkeypatch.setattr(forklift.jobs, "run_job", terminated_after_the_first_batch)
        assert _main(job, "--progress-jsonl") == 1

        result = _result(job)
        assert result["status"] == "cancelled" and result["error"]["code"] == "CANCELLED"
        out, err = capsys.readouterr()
        assert len(out.splitlines()) == 1
        assert "SIGTERM received: cancelling the job" in err
        assert signal.getsignal(signal.SIGTERM) == before

    def test_result_is_never_left_half_written(self, job, monkeypatch):
        def broken_dump(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(json, "dump", broken_dump)
        with pytest.raises(OSError, match="disk full"):
            _main(job)
        assert list(job["result"].parent.iterdir()) == []
