"""forklift-mcp: what ``run_job overwrite=true`` may replace (``forklift.mcp.previous``).

Review findings 2 and 3: only files an earlier job file lists as its artifacts, that are still
those files (kind, place, regular file, size and SHA-256), never the input; nothing is deleted
before the new job was accepted.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os

import pytest

from forklift.jobs import JobResult
from forklift.mcp import previous
from forklift.mcp.previous import Holding, plan
from forklift.mcp.workspace import ToolInputError

JOB = "forklift-job.json"


def _record(path, kind="data", relative=None, **changes):
    data = path.read_bytes()
    record = {
        "kind": kind,
        "path": relative or path.name,
        "rows": None,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    record.update(changes)
    return record


def _job(directory, artifacts):
    result = JobResult(job_id="earlier", status="succeeded").to_dict()
    result["artifacts"] = artifacts
    (directory / JOB).write_text(json.dumps({"format": "forklift-mcp-job/1", "result": result}))


def _read(path):
    return json.loads(path.read_text())


@pytest.fixture
def out(tmp_path):
    directory = tmp_path / "out"
    directory.mkdir()
    (directory / "data.parquet").write_bytes(b"old data")
    return directory


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text("id\n1\n")
    return path


class TestPlan:
    def test_no_earlier_job_file(self, out, source):
        earlier = plan(out, JOB, source, _read)
        assert earlier.job_file is None and earlier.replace == [] and earlier.left == []
        assert earlier.view(str, 50) == {
            "job_file": None,
            "replaced": [],
            "replaced_total": 0,
            "left_alone": [],
            "left_alone_total": 0,
        }
        assert plan(out / "missing", JOB, source, _read).job_file is None

    def test_files_that_are_still_the_earlier_runs(self, out, source):
        (out / "manifest.json").write_text("{}")
        _job(out, [_record(out / "data.parquet"), _record(out / "manifest.json", "manifest")])
        earlier = plan(out, JOB, source, _read)
        assert earlier.replace == [out / "data.parquet", out / "manifest.json"]
        assert earlier.left == [] and earlier.job_file == out / JOB

    def test_what_is_left_alone_and_why(self, out, source, tmp_path, monkeypatch):
        # A kind the contract may add later is not replaced until this server knows it
        monkeypatch.setattr(previous, "REPLACEABLE_KINDS", ("data", "manifest"))
        (tmp_path / "elsewhere.txt").write_text("x")
        (out / "sub").mkdir()
        (out / "sub" / "keep.xlsx").write_bytes(b"k")
        (out / "notes.txt").write_text("mine")
        (out / "link.parquet").symlink_to(out / "data.parquet")
        os.mkfifo(out / "pipe.parquet")
        (out / "changed.parquet").write_bytes(b"new content")
        (out / "edited.parquet").write_bytes(b"12345678")
        input_here = out / "in.csv"
        input_here.write_text("id\n1\n")
        _job(
            out,
            [
                _record(out / "notes.txt", "report"),
                _record(tmp_path / "elsewhere.txt", relative="../elsewhere.txt"),
                _record(out / "sub" / "keep.xlsx", relative="sub/keep.xlsx"),
                {
                    "kind": "data",
                    "path": "gone.parquet",
                    "rows": None,
                    "bytes": 1,
                    "sha256": "0" * 64,
                },
                {"kind": "data", "path": "x" * 300, "rows": None, "bytes": 1, "sha256": "0" * 64},
                _record(out / "data.parquet", relative="link.parquet"),
                {
                    "kind": "data",
                    "path": "pipe.parquet",
                    "rows": None,
                    "bytes": 0,
                    "sha256": "0" * 64,
                },
                _record(input_here, relative="in.csv"),
                _record(out / "changed.parquet", bytes=3),
                _record(out / "edited.parquet", sha256="f" * 64),
                _record(out / "data.parquet", sha256=None),
            ],
        )
        earlier = plan(out, JOB, input_here, _read)
        reasons = {path.name: reason for path, reason in earlier.left}
        assert earlier.replace == []
        assert reasons == {
            "notes.txt": "kind 'report' is not a file a job writes",
            "elsewhere.txt": "it is not a file directly in output_dir (a job writes only there)",
            "keep.xlsx": "it is not a file directly in output_dir (a job writes only there)",
            "gone.parquet": "it no longer exists",
            "x" * 300: "it cannot be examined (File name too long)",
            "link.parquet": "it is a symbolic link",
            "pipe.parquet": "it is not a regular file",
            "in.csv": "it is the input of this run",
            "changed.parquet": "it changed since that run (its size or SHA-256 differ from "
            "the record)",
            "edited.parquet": "it changed since that run (its size or SHA-256 differ from "
            "the record)",
            "data.parquet": "it changed since that run (its size or SHA-256 differ from "
            "the record)",
        }
        assert sorted(os.listdir(out)) == sorted(
            [
                JOB,
                "changed.parquet",
                "data.parquet",
                "edited.parquet",
                "in.csv",
                "link.parquet",
                "notes.txt",
                "pipe.parquet",
                "sub",
            ]
        )
        view = earlier.view(lambda path: path.name, 2)
        assert len(view["left_alone"]) == 2 and view["left_alone_total"] == 11

    def test_a_file_that_cannot_be_read_is_left_alone(self, out, source, monkeypatch):
        _job(out, [_record(out / "data.parquet")])

        def unreadable(path):
            raise PermissionError(errno.EACCES, "Permission denied")

        monkeypatch.setattr(previous, "_sha256", unreadable)
        earlier = plan(out, JOB, source, _read)
        assert earlier.left == [(out / "data.parquet", "it cannot be read (Permission denied)")]

    def test_an_input_that_vanished_is_not_the_file(self, out, tmp_path):
        _job(out, [_record(out / "data.parquet")])
        assert plan(out, JOB, tmp_path / "gone.csv", _read).replace == [out / "data.parquet"]

    @pytest.mark.parametrize(
        "content, reason",
        [
            ("[1]", "not read as an earlier run's result: list indices must be integers"),
            ("{}", "not read as an earlier run's result: 'result'"),
            ('{"result": {}}', "not read as an earlier run's result: Invalid job result"),
        ],
    )
    def test_job_files_that_are_not_results(self, out, source, content, reason):
        (out / JOB).write_text(content)
        earlier = plan(out, JOB, source, _read)
        assert earlier.replace == [] and earlier.left[0][1].startswith(reason)
        assert earlier.job_file == out / JOB  # it is replaced by the new run's

    def test_a_job_file_that_cannot_be_read(self, out, source):
        (out / JOB).write_text("{}")

        def refuse(path):
            raise ToolInputError("the earlier job file is 99 bytes, more than the 10 read")

        earlier = plan(out, JOB, source, refuse)
        assert earlier.left == [
            (
                out / JOB,
                "not read as an earlier run's result: the earlier job file is 99 "
                "bytes, more than the 10 read",
            )
        ]

    def test_a_job_file_that_is_a_link_is_not_followed(self, out, source, tmp_path):
        target = tmp_path / "elsewhere.json"
        _job(tmp_path, [_record(out / "data.parquet")])
        (tmp_path / JOB).rename(target)
        (out / JOB).symlink_to(target)
        earlier = plan(out, JOB, source, _read)
        assert earlier.replace == []
        assert earlier.left == [
            (out / JOB, "not read as an earlier run's result: it is a " "symbolic link")
        ]

    def test_a_job_file_name_taken_by_a_directory(self, out, source):
        (out / JOB).mkdir()
        with pytest.raises(ToolInputError, match="has a forklift-job.json that is not a file"):
            plan(out, JOB, source, _read)


class TestHolding:
    def test_moved_aside_then_put_back(self, out, source):
        _job(out, [_record(out / "data.parquet")])
        held = Holding(plan(out, JOB, source, _read))
        assert sorted(os.listdir(out)) == [held.directory.name]
        assert held.directory.name.startswith(".forklift-mcp-previous-")
        held.restore()
        assert sorted(os.listdir(out)) == ["data.parquet", JOB]
        assert (out / "data.parquet").read_bytes() == b"old data"
        held.restore()  # twice is harmless

    def test_moved_aside_then_discarded(self, out, source):
        _job(out, [_record(out / "data.parquet")])
        held = Holding(plan(out, JOB, source, _read))
        held.discard()
        assert os.listdir(out) == []
        held.discard()

    def test_nothing_to_move(self, out, source):
        held = Holding(plan(out, JOB, source, _read))
        assert held.directory is None
        held.restore()
        held.discard()
        assert os.listdir(out) == ["data.parquet"]

    def test_a_move_that_fails_changes_nothing(self, out, source, monkeypatch):
        _job(out, [_record(out / "data.parquet")])
        earlier = plan(out, JOB, source, _read)
        real_rename = os.rename
        calls = []

        def rename(path, target):
            calls.append(path)
            if len(calls) == 2:
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            real_rename(path, target)

        monkeypatch.setattr(previous.os, "rename", rename)
        with pytest.raises(ToolInputError, match=r"cannot be moved aside \(Invalid cross-dev"):
            Holding(earlier)
        assert sorted(os.listdir(out)) == ["data.parquet", JOB]
