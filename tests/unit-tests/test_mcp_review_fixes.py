"""forklift-mcp: the findings of the independent review (REVIEW-MCPL), one test class each.

Every test here failed before its fix. Numbers are the review's: 1-14 should-fix, N1-N10 nits.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.jobs import JobError, JobResult
from forklift.mcp import explain, staging, tools
from forklift.mcp.tools import JOB_FILE, Toolbox, summarize_schema
from forklift.mcp.values import compact_size
from forklift.mcp.workspace import ToolInputError, Workspace

SRC = Path(__file__).resolve().parents[2] / "src"
PEOPLE = (
    "id,name,age,email\n"
    "1,Ann,34,ann@example.org\n"
    "2,Bob,abc,bob@example.org\n"
    "3,Cy,200,cy@example.org\n"
    "4,Di,41,di@example.org\n"
)
COLUMNS = {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "email": {"type": "string"},
}
#: x-validation without badRowsHandling: the engine's default threshold (10%) applies
FIELD_RULES_ONLY = {
    "properties": COLUMNS,
    "x-validation": {"fieldValidations": {"age": {"range": {"min": 0, "max": 120}}}},
}


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "root"
    (root / "in").mkdir(parents=True)
    (root / "in" / "people.csv").write_text(PEOPLE)
    return root


@pytest.fixture
def toolbox(root):
    return Toolbox(Workspace([root]))


def _record(path, kind="data"):
    data = path.read_bytes()
    return {
        "kind": kind,
        "path": path.name,
        "rows": None,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _crafted_job(directory, artifacts=(), **fields):
    result = dict(JobResult(job_id="crafted", status="succeeded").to_dict(), **fields)
    result["artifacts"] = list(artifacts)
    (directory / JOB_FILE).write_text(
        json.dumps({"format": tools.JOB_FILE_FORMAT, "result": result})
    )


def _size(result):
    return compact_size(result)


class TestFinding1ThresholdOffInTypedPreview:
    def test_the_engines_default_threshold_is_switched_off_too(self, toolbox):
        result = toolbox.preview("in/people.csv", schema=FIELD_RULES_ONLY)
        assert result["status"] == "succeeded" and result["row_count"] == 2
        assert result["sample"]["rejected_rows"] == 2
        assert result["notes"] == [tools._THRESHOLD_NOTE]

    def test_a_bad_rows_handling_that_is_not_an_object_is_left_to_the_engine(self, toolbox):
        schema = dict(FIELD_RULES_ONLY)
        schema["x-validation"] = dict(FIELD_RULES_ONLY["x-validation"], badRowsHandling="x")
        result = toolbox.preview("in/people.csv", schema=schema)
        assert result["status"] == "failed" and result["error"]["code"] == "SCHEMA_INVALID"
        assert result["notes"] == []
        assert tools._without_threshold({"x-validation": 1}) == ({"x-validation": 1}, False)


class TestFinding2NothingDeletedUntilTheEngineAccepted:
    @pytest.fixture
    def earlier(self, toolbox, root):
        assert toolbox.run_job("in/people.csv", "out")["status"] == "succeeded"
        return sorted(os.listdir(root / "out"))

    def test_an_unknown_encoding_is_refused_before_anything_changes(
        self, toolbox, root, earlier
    ):
        with pytest.raises(ToolInputError, match="unknown encoding 'no-such-encoding'"):
            toolbox.run_job(
                "in/people.csv",
                "out",
                overwrite=True,
                input_options={"encoding": "no-such-encoding"},
            )
        assert sorted(os.listdir(root / "out")) == earlier
        assert toolbox.get_job("out")["status"] == "succeeded"

    def test_a_job_the_engine_refuses_puts_the_earlier_files_back(
        self, toolbox, root, earlier, monkeypatch
    ):
        refused = JobResult(
            job_id="x",
            status="failed",
            error=JobError(code="SPEC_INVALID", message="input: refused", retryable=False),
        )
        monkeypatch.setattr(tools, "run_job", lambda spec, base_dir: refused)
        with pytest.raises(ToolInputError, match="The job was refused: input: refused"):
            toolbox.run_job("in/people.csv", "out", overwrite=True)
        assert sorted(os.listdir(root / "out")) == earlier
        assert toolbox.get_job("out")["status"] == "succeeded"

    def test_a_crash_puts_the_earlier_files_back(self, toolbox, root, earlier, monkeypatch):
        def crash(spec, base_dir):
            raise RuntimeError("boom")

        monkeypatch.setattr(tools, "run_job", crash)
        with pytest.raises(RuntimeError):
            toolbox.run_job("in/people.csv", "out", overwrite=True)
        assert sorted(os.listdir(root / "out")) == earlier

    def test_the_new_run_replaces_them(self, toolbox, root, earlier):
        result = toolbox.run_job("in/people.csv", "out", overwrite=True, schema=FIELD_RULES_ONLY)
        assert result["status"] == "failed"
        assert sorted(os.listdir(root / "out")) == ["bad_rows.parquet", JOB_FILE]
        replaced = result["previous_run"]["replaced"]
        assert "out/data.parquet" in replaced and result["previous_run"]["left_alone"] == []

    def test_a_dry_run_says_what_would_be_replaced(self, toolbox, root, earlier):
        result = toolbox.run_job("in/people.csv", "out", overwrite=True, dry_run=True)
        assert "out/data.parquet" in result["would_run"]["previous_run"]["replaced"]
        assert sorted(os.listdir(root / "out")) == earlier


class TestFinding3OverwriteReplacesOnlyTheEarlierRunsOwnFiles:
    def test_the_input_and_other_files_a_job_file_lists_stay(self, root):
        out = root / "data"
        (out / "sub").mkdir(parents=True)
        (out / "orders.csv").write_text(PEOPLE)
        (out / "notes.txt").write_text("mine")
        (out / "sub" / "keep.xlsx").write_bytes(b"k")
        _crafted_job(
            out,
            [
                _record(out / "orders.csv"),
                dict(_record(out / "notes.txt"), sha256="0" * 64),
                dict(_record(out / "sub" / "keep.xlsx"), path="sub/keep.xlsx"),
            ],
        )
        result = Toolbox(Workspace([root])).run_job("data/orders.csv", "data", overwrite=True)
        assert result["status"] == "succeeded"
        assert (out / "orders.csv").read_text() == PEOPLE
        assert (out / "notes.txt").read_text() == "mine"
        assert (out / "sub" / "keep.xlsx").read_bytes() == b"k"
        reasons = {i["path"]: i["reason"] for i in result["previous_run"]["left_alone"]}
        assert reasons["data/orders.csv"] == "it is the input of this run"
        assert reasons["data/notes.txt"].startswith("it changed since that run")
        assert reasons["data/sub/keep.xlsx"].startswith("it is not a file directly in")


class TestFinding4ResultsAreBounded:
    @pytest.fixture
    def wide(self, root):
        names = [f"c{i:04d}_" + "n" * 1000 for i in range(300)]
        (root / "wide.csv").write_text(",".join(names) + "\n" + ",".join(["1"] * 300) + "\n")
        return names

    def test_raw_preview_columns(self, toolbox, wide):
        result = toolbox.preview("wide.csv", max_bytes=1024)
        assert len(result["columns"]) == 100 and result["columns_total"] == 300
        assert result["columns_truncated"] is True and result["truncated_names"] == 100
        assert all(len(column) == 256 for column in result["columns"])
        assert result["row_count"] == 1 and result["rows"][0] == ["1"] * 100
        # max_bytes bounds the rows; the names are bounded by their own caps
        assert _size(result) < 1024 + 100 * (256 + 3) + 2048

    def test_a_wide_but_ordinary_file_shows_rows(self, toolbox, root):
        names = [f"col_{i:05d}" for i in range(10_000)]
        (root / "w.csv").write_text(",".join(names) + "\n" + ",".join(["v"] * 10_000) + "\n")
        result = toolbox.preview("w.csv")
        assert result["row_count"] == 1 and result["columns_total"] == 10_000
        assert _size(result) < 65536 + 4096

    def test_generate_schema_summary(self, toolbox, wide):
        result = toolbox.generate_schema("wide.csv", max_bytes=1024)
        assert _size(result["summary"]) <= 1024 + 512
        assert result["summary"]["column_count"] == 300
        assert result["summary"]["columns_truncated"] is True

    def test_summary_lists_share_the_byte_limit(self):
        schema = {
            "title": "t" * 1000,
            "properties": {f"c{i}" * 100: {"type": "string"} for i in range(50)},
            "required": [f"c{i}" * 100 for i in range(50)],
            **{f"x-{i}" * 100: {} for i in range(60)},
        }
        summary = summarize_schema(schema, 4096)
        assert len(summary["title"]) == 256 and summary["truncated_names"] > 0
        assert summary["extensions_total"] == 60 and len(summary["extensions"]) < 60
        assert summary["required_total"] == 50 and summary["columns"] == []
        assert _size(summary) < 4096 + 1024

    def test_validate_schema_lists(self, toolbox, wide):
        schema = {"properties": {name: {"type": "integer"} for name in wide}, "required": wide}
        result = toolbox.validate_schema("wide.csv", schema=schema)
        assert result["valid"] is True
        assert result["columns_total"] == 300 and len(result["columns"]) < 300
        assert result["schema_columns_total"] == 300
        assert result["schema_columns_not_in_input"] == []
        assert result["schema_columns_not_in_input_total"] == 0
        assert _size(result) < tools.LISTS_MAX_BYTES + 8192

    def test_long_warnings_and_finding_keys_are_cut(self):
        warnings = tools._bounded_warnings(["w" * 5000] * 60)
        assert len(warnings["warnings"]) == 50 and warnings["warnings_total"] == 60
        assert all(len(warning) == 2000 for warning in warnings["warnings"])
        findings = tools._bounded_findings({"RANGE_VIOLATION:" + "c" * 900: 2, "x" * 900: 1})
        assert set(findings["validation_summary"]) == {
            "RANGE_VIOLATION:" + "c" * 256,
            "x" * 256,
        }


class TestFinding5BadRowsAreReadInBoundedMemory:
    def test_a_hostile_bad_rows_file(self, tmp_path):
        # 1,100 rows of 200 KB cells, compressed to a few hundred bytes: reading them whole
        # (the earlier batches of 1,024 rows) raised peak memory by about 600 MiB
        script = textwrap.dedent(
            f"""
            import json, resource, sys
            sys.path.insert(0, {str(SRC)!r})
            from pathlib import Path
            import pyarrow as pa, pyarrow.parquet as pq
            from forklift.jobs import JobResult
            from forklift.mcp.tools import Toolbox, JOB_FILE_FORMAT
            from forklift.mcp.workspace import Workspace
            root = Path(sys.argv[1]); out = root / "out"; out.mkdir(parents=True)
            cell = "a" * 200_000
            table = pa.table({{"id": ["x"] * 1100, "blob": [cell] * 1100,
                               "_rejection_reason": ["type_conversion_failed"] * 1100}})
            pq.write_table(table, str(out / "bad_rows.parquet"), compression="zstd")
            del table, cell
            result = JobResult(job_id="j", status="succeeded").to_dict()
            result["artifacts"] = [{{"kind": "bad_rows", "path": "bad_rows.parquet",
                                     "rows": None, "bytes": None, "sha256": None}}]
            (out / "forklift-job.json").write_text(json.dumps(
                {{"format": JOB_FILE_FORMAT, "result": result}}))
            box = Toolbox(Workspace([root]))
            before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            page = box.get_bad_rows("out", limit=1, max_bytes=1024)
            explained = box.explain_failure("out", rows=1, max_bytes=1024)
            after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            print(json.dumps({{"grew_kib": after - before, "rows": page["row_count"],
                               "first": explained["rejected"]["first_rows"]["row_count"],
                               "cut": page["truncated_cells"]}}))
            """
        )
        done = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert done.returncode == 0, done.stderr
        measured = json.loads(done.stdout)
        assert measured["rows"] == 1 and measured["first"] == 1 and measured["cut"] >= 1
        assert measured["grew_kib"] < 100 * 1024


class TestFinding6PagingAlwaysMovesOn:
    def test_a_row_larger_than_max_bytes_is_shown_cut(self, toolbox, root):
        columns = ["id"] + [f"note{i}" for i in range(40)]
        lines = [",".join(columns)] + [
            ",".join([f"x{r}"] + ["n" * 2500] * 40) for r in range(3)
        ]
        (root / "in" / "wide.csv").write_text("\n".join(lines) + "\n")
        schema = {"properties": {"id": {"type": "integer"}}}
        toolbox.run_job("in/wide.csv", "out", schema=schema)
        offset, pages = 0, []
        while offset is not None:
            page = toolbox.get_bad_rows("out", offset=offset, max_bytes=1024)
            assert page["row_count"] == 1 and page["limit"] == "bytes"
            assert page["truncated_cells"] == 40 and _size(page["rows"]) <= 1024
            pages.append(page["rows"][0][0])
            offset = page["next_offset"]
        assert pages == ["x0", "x1", "x2"]
        first = toolbox.explain_failure("out", max_bytes=1024)["rejected"]["first_rows"]
        assert first["row_count"] == 1 and first["limit"] == "bytes"


class TestFinding7ValidateDoesNotPassAnEmptyCopy:
    def test_an_empty_file(self, toolbox, root):
        (root / "in" / "empty.csv").write_text("\n\n")
        with pytest.raises(ToolInputError, match="'in/empty.csv' has no records"):
            toolbox.validate_schema("in/empty.csv", schema={"required": ["a"]})

    def test_a_first_record_longer_than_the_copy(self, toolbox, root, monkeypatch):
        monkeypatch.setattr(staging, "MAX_SAMPLE_COPY_CHARS", 1000)
        (root / "in" / "line.csv").write_text("a" * 5000 + "\n1\n")
        for call in (toolbox.validate_schema, toolbox.preview):
            with pytest.raises(ToolInputError, match="first record of input_path .* longer"):
                call("in/line.csv", schema={"required": ["a"]})


class TestFinding8SpecificMessages:
    @pytest.mark.parametrize("options", [{"delimiter": '"'}, {"quote_char": ","}])
    def test_a_dialect_the_csv_module_refuses(self, toolbox, options):
        with pytest.raises(ToolInputError, match="do not make a CSV dialect"):
            toolbox.preview("in/people.csv", input_options=options)

    def test_a_schema_nested_too_deeply(self, toolbox, root):
        (root / "deep.json").write_text("[" * 100_000)
        with pytest.raises(ToolInputError, match="'deep.json' is nested too deeply"):
            toolbox.get_schema("deep.json")
        with pytest.raises(ToolInputError, match="nested too deeply"):
            toolbox.preview("in/people.csv", schema_path="deep.json")

    def test_a_file_that_cannot_be_opened(self, toolbox, root, monkeypatch):
        (root / "s.json").write_text("{}")
        real_open = open

        def refuse(path, *args, **kwargs):
            if str(path).endswith("s.json"):
                raise PermissionError(13, "Permission denied")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(tools, "open", refuse, raising=False)
        with pytest.raises(ToolInputError, match="'s.json' cannot be read: Permission denied"):
            toolbox.get_schema("s.json")

    def test_an_output_path_that_cannot_be_written(self, toolbox, monkeypatch):
        def full(target, text):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(tools, "_write_text_atomically", full)
        with pytest.raises(ToolInputError, match="'s.json' cannot be written: No space left"):
            toolbox.generate_schema("in/people.csv", output_path="s.json")

    def test_an_input_that_vanished_before_the_run(self, toolbox, root, monkeypatch):
        real_plan = tools.previous.plan

        def vanish(*args):
            (root / "in" / "people.csv").unlink()
            return real_plan(*args)

        monkeypatch.setattr(tools.previous, "plan", vanish)
        with pytest.raises(ToolInputError, match="input_path 'in/people.csv' does not exist"):
            toolbox.run_job("in/people.csv", "out", overwrite=True)


class TestFinding10TypedPreviewMessages:
    def test_messages_do_not_name_files_of_the_deleted_copy(self, toolbox, monkeypatch):
        def failed(spec, base_dir):
            (base_dir / "out").mkdir()
            pq.write_table(pa.table({"_rejection_reason": ["X"]}), str(base_dir / "out" / "b.p"))
            return JobResult.from_dict(
                {
                    "spec_version": 1,
                    "job_id": "j",
                    "status": "failed",
                    "counts": {"total_rows": 1, "invalid_rows": 1},
                    "schema_extensions": [],
                    "validation_summary": {},
                    "warnings": ["see out/data.parquet"],
                    "artifacts": [
                        {"kind": "bad_rows", "path": "out/b.p", "rows": 1, "bytes": 1,
                         "sha256": None}
                    ],
                    "error": {"code": "CONSTRAINT_VIOLATION", "retryable": False,
                              "message": "All rows are in out/b.p (the reason says why)."},
                }
            )  # fmt: skip

        monkeypatch.setattr(tools, "run_job", failed)
        result = toolbox.preview("in/people.csv", schema={"properties": COLUMNS})
        assert result["error"]["message"] == (
            "All rows are in sample.rejection_reasons of this result (the rows a preview "
            "rejects are not kept) (the reason says why)."
        )
        assert result["sample"]["rejection_reasons"][0]["reason"] == "X"


class TestFinding11TypedPreviewReadsOnlyTheRowsItShows:
    def test_a_workbook(self, toolbox, root):
        openpyxl = pytest.importorskip("openpyxl")
        book = openpyxl.Workbook()
        sheet = book.active
        sheet.append(["id", "name"])
        for number in range(500):
            sheet.append([number, f"n{number}"])
        book.save(root / "in" / "big.xlsx")
        schema = toolbox.generate_schema("in/big.xlsx")["document"]
        result = toolbox.preview("in/big.xlsx", schema=schema, rows=5)
        assert result["row_count"] == 5 and result["sample"]["rows_read"] == 6
        assert result["notes"] == []

    @pytest.mark.parametrize(
        "sheet, end",
        [
            ({}, 12),
            ({"header": 2}, 14),
            ({"header": {"mode": "absent"}}, 11),
            ({"header": {"mode": "absent"}, "dataStartRow": 5}, 15),
            ({"dataStartRow": 7}, 17),
            ({"dataEndRow": 4}, 4),
            ({"header": {"row": None}}, 12),
        ],
    )
    def test_the_last_row_read(self, sheet, end):
        assert tools._first_rows(sheet, 10)["dataEndRow"] == end

    @pytest.mark.parametrize(
        "sheet",
        ["x", {"header": "x"}, {"header": {"row": -1}}, {"dataStartRow": "2"}, {"dataEndRow": 1.5}],
    )
    def test_settings_the_engine_will_refuse_stay(self, sheet):
        assert tools._first_rows(sheet, 10) == sheet

    def test_both_forms_of_x_excel(self):
        document = {"x-excel": {"sheet": "Data", "header": {"row": 1}, "dateSystem": "1900"}}
        limited, bounded = tools._first_rows_of_sheets(document, 3)
        assert bounded and limited["x-excel"] == {
            "header": {"row": 1},
            "dateSystem": "1900",
            "sheets": [{"select": {"name": "Data"}, "header": {"row": 1}, "dataEndRow": 6}],
        }
        for value, select in ((2, {"index": 2}), ({"regex": "^D"}, {"regex": "^D"})):
            sheets = tools._first_rows_of_sheets({"x-excel": {"sheet": value}}, 3)[0]
            assert sheets["x-excel"]["sheets"][0]["select"] == select
        listed = tools._first_rows_of_sheets({"x-excel": {"sheets": [{}, 5]}}, 3)[0]
        assert listed["x-excel"]["sheets"] == [{"dataEndRow": 5}, 5]
        for excel in ({"sheet": True}, {}, "x"):
            assert tools._first_rows_of_sheets({"x-excel": excel}, 3)[1] is False

    def test_a_workbook_schema_without_sheet_settings_says_so(self, toolbox, root):
        openpyxl = pytest.importorskip("openpyxl")
        book = openpyxl.Workbook()
        book.active.append(["id"])
        book.save(root / "in" / "b.xlsx")
        result = toolbox.preview("in/b.xlsx", schema={"properties": {"id": {}}, "x-excel": {}})
        assert result["notes"] == [tools._WORKBOOK_NOTE]


class TestFinding13EngineScratchNextToTheOutput:
    def test_the_base_is_the_deepest_directory_holding_both(self, root, monkeypatch):
        (root / "project" / "in").mkdir(parents=True)
        (root / "project" / "in" / "p.csv").write_text(PEOPLE)
        seen = []
        real_run_job = tools.run_job

        def spy(spec, base_dir):
            seen.append(base_dir)
            return real_run_job(spec, base_dir=base_dir)

        monkeypatch.setattr(tools, "run_job", spy)
        toolbox = Toolbox(Workspace([root]))
        result = toolbox.run_job(
            "project/in/p.csv",
            "project/out",
            schema={**FIELD_RULES_ONLY, "x-validation": dict(FIELD_RULES_ONLY["x-validation"])},
        )
        assert seen == [root / "project"]
        # Messages name files as results do: relative to the first root
        assert "project/out/bad_rows.parquet" in result["error"]["message"]
        assert result["artifacts"][0]["path"] == "project/out/bad_rows.parquet"
        assert toolbox.get_job("project/out")["error"] == result["error"]


class TestFinding14Wording:
    def test_get_bad_rows_without_a_file(self, toolbox):
        toolbox.run_job("in/people.csv", "out")
        result = toolbox.get_bad_rows("out", offset=3)
        assert result["message"] == (
            "This run kept no bad_rows.parquet: it rejected no rows, or it stopped before "
            "keeping them (see counts and error in get_job)"
        )
        # Review nit N4: the same keys as a page of rows
        assert {"offset", "columns", "truncated", "limit", "next_offset"} <= set(result)
        assert result["offset"] == 3

    def test_the_threshold_error_says_the_rows_may_not_be_kept(self):
        meaning = explain.ERROR_HELP["BAD_ROWS_THRESHOLD_EXCEEDED"][0]
        assert "thresholdMode early" in meaning and "may not be kept" in meaning

    def test_a_cancelled_run_and_next_steps(self, root, toolbox):
        (root / "out").mkdir()
        error = {"code": "CANCELLED", "message": "cancelled", "retryable": False}
        _crafted_job(root / "out", status="cancelled", error=error)
        assert toolbox.explain_failure("out")["summary"] == "The run was cancelled."
        clean = toolbox.run_job("in/people.csv", "clean")
        assert clean["next"].startswith("get_job shows this result again")
        rejected = toolbox.run_job("in/people.csv", "lenient", schema={"properties": COLUMNS})
        assert rejected["next"] == "get_bad_rows shows the rejected rows"


class TestNitN1LinkedJobFile:
    def test_a_job_file_that_links_out_of_the_roots_is_refused(self, tmp_path, root):
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.csv").write_text("id\n1\n")
        Toolbox(Workspace([outside])).run_job("secret.csv", "run")
        (root / "run").mkdir()
        (root / "run" / JOB_FILE).symlink_to(outside / "run" / JOB_FILE)
        with pytest.raises(ToolInputError, match="outside the directories"):
            Toolbox(Workspace([root])).get_job("run")


class TestNitN2FileModes:
    def test_new_files_follow_the_umask_and_replaced_ones_keep_their_mode(self, toolbox, root):
        previous = os.umask(0o022)
        try:
            (root / "kept.json").write_text("{}")
            os.chmod(root / "kept.json", 0o640)
            toolbox.generate_schema("in/people.csv", output_path="kept.json", overwrite=True)
            toolbox.generate_schema("in/people.csv", output_path="new.json")
            toolbox.run_job("in/people.csv", "out")
        finally:
            os.umask(previous)
        assert stat.S_IMODE((root / "kept.json").stat().st_mode) == 0o640
        assert stat.S_IMODE((root / "new.json").stat().st_mode) == 0o644
        assert stat.S_IMODE((root / "out" / JOB_FILE).stat().st_mode) == 0o644
