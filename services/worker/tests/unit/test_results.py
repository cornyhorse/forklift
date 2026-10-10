"""The engine's result as untrusted input, artifacts in scratch, and the supervisor's results."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import signal

import pytest

from forklift_worker import results
from forklift_worker.results import (
    ResultInvalid,
    artifact_name,
    bounded,
    collect_artifacts,
    crash_message,
    normalised,
    read_result,
    synthesized,
)


def good(**changes) -> dict:
    result = {
        "spec_version": 1,
        "job_id": "j",
        "status": "succeeded",
        "artifacts": [{"kind": "data", "path": "out/data.parquet", "rows": 3}],
        "error": None,
    }
    result.update(changes)
    return result


def test_a_result_file_is_read_and_checked(tmp_path):
    path = tmp_path / "result.json"
    assert read_result(path, "j") is None
    path.write_text(json.dumps(good()))
    assert read_result(path, "j")["status"] == "succeeded"
    path.unlink()
    os.symlink("/etc/passwd", path)
    with pytest.raises(ResultInvalid, match="it is a symbolic link"):
        read_result(path, "j")
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ResultInvalid, match="not a regular file"):
        read_result(path, "j")


def test_an_oversized_result_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(results, "RESULT_MAX_BYTES", 10)
    path = tmp_path / "result.json"
    path.write_text(json.dumps(good()))
    with pytest.raises(ResultInvalid, match="larger than 10 bytes"):
        read_result(path, "j")


@pytest.mark.parametrize(
    "result, phrase",
    [
        ([], "not a JSON object"),
        (good(job_id="other"), "for another job"),
        (good(status="done"), "status 'done'"),
        (good(error={"code": "INTERNAL", "message": "m"}), "succeeded but gives an error"),
        (good(status="failed"), "failed but gives no error"),
        (good(status="failed", error={"code": "OOPS", "message": "x"}), "known code"),
        (good(status="failed", error={"code": "INTERNAL"}), "without a message"),
        (good(status="cancelled", error={"code": "INTERNAL", "message": "m"}), "another code"),
        (good(artifacts={}), "not a list"),
        (good(artifacts=[{"kind": "data"}] * 101), "at most 100"),
        (good(artifacts=["x"]), "without a known kind"),
        (good(artifacts=[{"kind": "data", "path": 3}]), "without a path"),
        (good(artifacts=[{"kind": "data", "path": ""}]), "without a path"),
        (good(artifacts=[{"kind": "data", "path": "p", "rows": "3"}]), "not a count"),
        (good(artifacts=[{"kind": "data", "path": "p", "rows": False}]), "not a count"),
        (good(artifacts=[{"kind": "data", "path": "p", "rows": -1}]), "not a count"),
    ],
)
def test_malformed_results(result, phrase):
    with pytest.raises(ResultInvalid, match=phrase):
        normalised(result, "j")


def test_results_are_normalised_to_the_contract():
    result = good(
        job_id=None,
        counts={"total_rows": 3, "bad": -1, "flag": True, 4: 1},
        validation_summary=[],
        warnings=["w", 5],
        schema_extensions="x",
        artifacts=[
            {"kind": "data", "path": "out/d", "rows": 3, "bytes": 10, "sha256": "a" * 64},
            {"kind": "manifest", "path": "out/m", "bytes": -1, "sha256": "short", "extra": 1},
        ],
        surprise="dropped",
    )
    assert normalised(result, "j") == {
        "spec_version": 1,
        "job_id": "j",
        "status": "succeeded",
        "counts": {"total_rows": 3},
        "schema_extensions": [],
        "validation_summary": {},
        "warnings": ["w"],
        "artifacts": [
            {"kind": "data", "path": "out/d", "rows": 3, "bytes": 10, "sha256": "a" * 64},
            {"kind": "manifest", "path": "out/m", "rows": None, "bytes": None, "sha256": None},
        ],
        "error": None,
    }
    failed = normalised(good(status="failed", error={"code": "INTERNAL", "message": "m"}), "j")
    assert failed["error"] == {"code": "INTERNAL", "message": "m", "retryable": False}
    cancelled = good(
        status="cancelled", error={"code": "CANCELLED", "message": "m", "retryable": True}
    )
    assert normalised(cancelled, "j")["error"]["retryable"] is True
    del cancelled["artifacts"]
    assert normalised(cancelled, "j")["artifacts"] == []


def test_synthesized_results_keep_the_engines_findings_but_not_its_artifacts():
    base = good(counts={"total_rows": 4}, warnings=["w"], validation_summary={"X": 1})
    result = synthesized("j", "failed", "INTERNAL", "m", retryable=True, base=base)
    assert result["counts"] == {"total_rows": 4} and result["warnings"] == ["w"]
    assert result["validation_summary"] == {"X": 1}
    assert result["artifacts"] == []
    assert result["error"] == {"code": "INTERNAL", "message": "m", "retryable": True}
    assert synthesized("j", "succeeded", None)["error"] is None


@pytest.mark.parametrize(
    "returncode, code, phrase",
    [
        (2, "SPEC_INVALID", "refused the job spec"),
        (7, "INTERNAL", "exited with code 7"),
        (-signal.SIGXCPU, "LIMIT_EXCEEDED", "limit of 30 seconds"),
        (-signal.SIGKILL, "INTERNAL", "out-of-memory killer"),
        (-signal.SIGSEGV, "INTERNAL", "RLIMIT_AS is 4096 bytes"),
        (-signal.SIGTERM, "INTERNAL", "killed by SIGTERM without"),
        (-200, "INTERNAL", "killed by signal 200"),
    ],
)
def test_crash_messages(returncode, code, phrase):
    got_code, message = crash_message(
        returncode, "last line", {"cpu": [30, 40], "as": [4096, 4096]}
    )
    assert got_code == code
    assert phrase in message


def test_crash_messages_without_limits_or_stderr():
    _, message = crash_message(-signal.SIGSEGV, "", {})
    assert message == "The engine was killed by SIGSEGV without writing a result."


def test_artifact_names():
    assert artifact_name("out/data.parquet", ["out"]) == "data.parquet"
    assert artifact_name("out/parts/0.parquet", ["out"]) == "parts/0.parquet"
    assert artifact_name("report.json", ["out"]) == "report.json"
    assert artifact_name("out", ["out"]) == "out"
    assert artifact_name("x/y", ["."]) == "x/y"


def test_artifacts_are_hashed_and_the_manifest_comes_last(tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "manifest.json").write_text("{}")
    (tmp_path / "out" / "data.parquet").write_bytes(b"PAR1")
    result = good(
        artifacts=[
            {"kind": "manifest", "path": "out/manifest.json"},
            {"kind": "data", "path": "out/data.parquet", "rows": 1},
        ]
    )
    files = collect_artifacts(result, tmp_path, ["out"])
    assert [item.name for item in files] == ["data.parquet", "manifest.json"]
    data = files[0]
    assert data.bytes == 4 and data.index == 1 and data.rows == 1
    assert data.sha256 == hashlib.sha256(b"PAR1").hexdigest()
    assert data.md5_base64 == base64.b64encode(hashlib.md5(b"PAR1").digest()).decode()
    assert data.entry("k") == {
        "kind": "data",
        "name": "data.parquet",
        "key": "k",
        "bytes": 4,
        "sha256": data.sha256,
        "rows": 1,
    }


def test_duplicate_and_odd_artifacts_are_refused(tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "a").write_text("x")
    twice = good(artifacts=[{"kind": "data", "path": "out/a"}, {"kind": "data", "path": "out/a"}])
    with pytest.raises(ResultInvalid, match="listed twice"):
        collect_artifacts(twice, tmp_path, ["out"])
    for path in ("", "out\\a", "/etc/passwd"):
        with pytest.raises(ResultInvalid, match="leaves the scratch"):
            collect_artifacts(good(artifacts=[{"kind": "data", "path": path}]), tmp_path, ["out"])
    os.mkfifo(tmp_path / "out" / "fifo")
    with pytest.raises(ResultInvalid, match="not a regular file"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/fifo"}]), tmp_path, [])


def test_a_hard_linked_or_unopenable_artifact_is_refused(tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "elsewhere").write_text("secret")
    os.link(tmp_path / "elsewhere", tmp_path / "out" / "linked")
    with pytest.raises(ResultInvalid, match="has other names"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/linked"}]), tmp_path, [])
    (tmp_path / "out" / "file").write_text("x")
    with pytest.raises(ResultInvalid, match="leaves the scratch directory \\(through"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/file/x"}]), tmp_path, [])
    with pytest.raises(ResultInvalid, match="leaves the scratch"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/../x"}]), tmp_path, [])


def test_an_unreadable_artifact(tmp_path, monkeypatch):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "a").write_text("x")
    real_open = os.open

    def refuse(path, flags, *args, **kwargs):
        if path == "a":
            raise PermissionError(13, "Permission denied")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(results.os, "open", refuse)
    with pytest.raises(ResultInvalid, match="cannot be opened \\(Permission denied\\)"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/a"}]), tmp_path, [])


def test_an_artifact_in_a_missing_directory(tmp_path):
    with pytest.raises(ResultInvalid, match="did not write it"):
        collect_artifacts(good(artifacts=[{"kind": "data", "path": "out/none/x"}]), tmp_path, [])


def test_a_result_is_bounded_to_what_the_gateway_takes():
    small = synthesized("j", "failed", "INTERNAL", "m")
    assert bounded(small) is small
    noisy = synthesized("j", "failed", "INTERNAL", "e" * 9000)
    noisy["warnings"] = ["w" * 3000] * 150
    trimmed = bounded(noisy)
    assert (
        len(trimmed["warnings"]) == 101 and trimmed["warnings"][-1] == "... and 50 more warnings"
    )
    assert len(trimmed["warnings"][0]) == 2000 and len(trimmed["error"]["message"]) == 8000
    assert trimmed["counts"] == noisy["counts"]
    huge = synthesized("j", "succeeded", None)
    huge["validation_summary"] = {f"NOT_NULL:column_{n}": 1 for n in range(60000)}
    replaced = bounded(huge)
    assert replaced["status"] == "failed" and replaced["validation_summary"] == {}
    assert "larger than the 1048576 bytes the gateway accepts" in replaced["error"]["message"]
