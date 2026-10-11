"""forklift-mcp tools (``forklift.mcp.tools.Toolbox``) against real files and the real engine.

The tools are plain methods; ``test_mcp_server.py`` calls them through the MCP protocol and
``test_mcp_stdio.py`` through a real ``forklift-mcp`` process. These tests need no MCP SDK.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import jsonschema
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.jobs import JobResult, contract
from forklift.mcp import tools
from forklift.mcp.tools import JOB_FILE, Toolbox, bounded_document, summarize_schema
from forklift.mcp.workspace import ToolInputError, Workspace

RESULT_VALIDATOR = jsonschema.Draft202012Validator(contract.jobresult_schema())

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
#: Rejects age 200 (x-validation) and 'abc' (type conversion): 2 of 4 rows, over the 10% limit
STRICT = {
    "type": "object",
    "properties": COLUMNS,
    "required": ["id"],
    "x-validation": {
        "badRowsHandling": {"maxBadRowsPercent": 10},
        "fieldValidations": {"age": {"range": {"min": 0, "max": 120}}},
    },
}
#: The same rules with a threshold the file passes
LENIENT = dict(
    STRICT,
    **{
        "x-validation": {
            "badRowsHandling": {"maxBadRowsPercent": 60},
            "fieldValidations": {"age": {"range": {"min": 0, "max": 120}}},
        }
    },
)


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "root"
    (root / "in").mkdir(parents=True)
    (root / "in" / "people.csv").write_text(PEOPLE)
    return root


@pytest.fixture
def toolbox(root):
    return Toolbox(Workspace([root]))


@pytest.fixture
def read_only(root):
    return Toolbox(Workspace([root], read_only=True))


def _write_json(path, document):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document))
    return path


def _crafted_job(directory, artifacts=(), **fields):
    """A job file as run_job writes it, with a result made up for the test."""
    result = dict(JobResult(job_id="crafted", status="succeeded").to_dict(), **fields)
    result["artifacts"] = [
        dict({"rows": None, "bytes": None, "sha256": None}, **artifact) for artifact in artifacts
    ]
    return _write_json(directory / JOB_FILE, {"format": tools.JOB_FILE_FORMAT, "result": result})


def _many_rows(path, count):
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "name"])
        for number in range(count):
            writer.writerow([number, f"n{number}"])


def _workbook(path, rows):
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.title = "People"
    for row in rows:
        sheet.append(row)
    book.save(path)


# ====================================================================== get_schema


class TestGetSchema:
    def test_summary_and_document(self, toolbox, root):
        schema = {
            "title": "People",
            "properties": {
                "id": {"type": "integer", "minimum": 1, "x-unique": True},
                "email": {"type": "string", "format": "email", "x-special-type": "email"},
                "notes": "not an object",
            },
            "required": ["id", 7],
            "x-validation": {},
            "x-primaryKey": {"columns": ["id"]},
        }
        _write_json(root / "schemas" / "people.json", schema)
        result = toolbox.get_schema("schemas/people.json")
        assert result["path"] == "schemas/people.json"
        assert result["summary"] == {
            "title": "People",
            "column_count": 3,
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "required": True,
                    "extensions": ["x-unique"],
                    "constraints": ["minimum", "x-unique"],
                },
                {
                    "name": "email",
                    "type": "string",
                    "required": False,
                    "format": "email",
                    "extensions": ["x-special-type"],
                },
                {"name": "notes", "type": None, "required": False},
            ],
            "columns_truncated": False,
            "required": ["id"],
            "extensions": ["x-primaryKey", "x-validation"],
        }
        assert result["document"] == schema and result["truncated"] is False
        assert result["document_bytes"] == len(json.dumps(schema, separators=(",", ":")))
        assert result["bytes"] == (root / "schemas" / "people.json").stat().st_size
        assert result["caps"] == {"max_bytes": 65536, "listed_columns": 500}

    def test_a_large_document_is_cut_at_max_bytes(self, toolbox, root):
        schema = {"properties": {f"col_é{i}": {"type": "string"} for i in range(200)}}
        _write_json(root / "big.json", schema)
        result = toolbox.get_schema("big.json", max_bytes=1024)
        assert result["document"] is None and result["truncated"] is True
        assert len(result["document_text"].encode("utf-8")) <= 1024
        assert result["document_text"].startswith('{"properties":{"col_é0"')
        assert result["summary"]["column_count"] == 200

    def test_summary_bounds_and_odd_shapes(self, monkeypatch):
        monkeypatch.setattr(tools, "MAX_LISTED_COLUMNS", 2)
        summary = summarize_schema(
            {"title": 5, "properties": {"a": {}, "b": {}, "c": {}}, "required": ["a", "b", "c"]}
        )
        assert summary["title"] is None and summary["columns_truncated"] is True
        assert [c["name"] for c in summary["columns"]] == ["a", "b"]
        assert summary["required"] == ["a", "b"]
        assert summarize_schema({"properties": [], "required": None})["column_count"] == 0

    def test_bounded_document_fits_exactly(self):
        document = {"a": "b"}
        assert bounded_document(document, len('{"a":"b"}'))["truncated"] is False
        assert bounded_document(document, len('{"a":"b"}') - 1)["truncated"] is True

    @pytest.mark.parametrize(
        "content, message",
        [
            (b"{not json", "is not valid JSON: Expecting property name"),
            (b"\xff\xfe\x00", "is not UTF-8 text"),
            (b"[1, 2]", "is not a schema: its JSON is not an object"),
        ],
    )
    def test_unusable_files(self, toolbox, root, content, message):
        (root / "s.json").write_bytes(content)
        with pytest.raises(ToolInputError, match=message):
            toolbox.get_schema("s.json")

    def test_a_file_too_large_to_read(self, toolbox, root, monkeypatch):
        monkeypatch.setattr(tools, "MAX_SCHEMA_FILE_BYTES", 10)
        _write_json(root / "s.json", {"properties": {"a": {"type": "string"}}})
        with pytest.raises(ToolInputError, match=r"is 41 bytes, more than the 10 read"):
            toolbox.get_schema("s.json")

    @pytest.mark.parametrize("value", [1023, 1048577, True, "2048"])
    def test_max_bytes_is_checked(self, toolbox, value):
        with pytest.raises(ToolInputError, match="max_bytes must be"):
            toolbox.get_schema("in/people.csv", max_bytes=value)

    def test_outside_the_root(self, toolbox):
        with pytest.raises(ToolInputError, match="outside the directories"):
            toolbox.get_schema("/etc/hostname")


# ====================================================================== generate_schema


class TestGenerateSchema:
    def test_generates_without_writing(self, toolbox, root):
        result = toolbox.generate_schema("in/people.csv")
        assert result["status"] == "succeeded" and result["error"] is None
        assert result["input"] == {
            "path": "in/people.csv",
            "format": "csv",
            "bytes": len(PEOPLE),
            "sampled": False,
        }
        assert result["rows_analyzed"] == 4
        assert [c["name"] for c in result["summary"]["columns"]] == ["id", "name", "age", "email"]
        assert result["document"]["properties"]["id"]["type"] == "integer"
        assert result["output_path"] is None and result["written"] is False
        assert result["caps"]["sample_rows"] == 1000
        assert sorted(os.listdir(root)) == ["in"]

    def test_writes_output_path(self, toolbox, root):
        result = toolbox.generate_schema("in/people.csv", output_path="schemas/people.json")
        assert result["written"] is True and result["output_path"] == "schemas/people.json"
        written = json.loads((root / "schemas" / "people.json").read_text())
        assert written == result["document"]
        assert not [p for p in (root / "schemas").iterdir() if p.name.startswith(".")]

    def test_an_existing_output_path_needs_overwrite(self, toolbox, root):
        (root / "s.json").write_text("{}")
        with pytest.raises(ToolInputError, match="already exists"):
            toolbox.generate_schema("in/people.csv", output_path="s.json")
        assert (root / "s.json").read_text() == "{}"
        toolbox.generate_schema("in/people.csv", output_path="s.json", overwrite=True)
        assert "properties" in json.loads((root / "s.json").read_text())

    def test_dry_run_writes_nothing(self, toolbox, root):
        result = toolbox.generate_schema("in/people.csv", output_path="s.json", dry_run=True)
        assert result["dry_run"] is True and result["written"] is False
        assert result["output_path"] == "s.json" and result["summary"]["column_count"] == 4
        assert not (root / "s.json").exists()

    def test_read_only_refuses_output_path_but_not_a_dry_run(self, read_only, root):
        with pytest.raises(ToolInputError, match="--read-only"):
            read_only.generate_schema("in/people.csv", output_path="s.json")
        assert read_only.generate_schema("in/people.csv", output_path="s.json", dry_run=True)
        assert read_only.generate_schema("in/people.csv")["status"] == "succeeded"
        assert not (root / "s.json").exists()

    def test_only_the_first_rows_are_copied_and_read(self, toolbox, root):
        _many_rows(root / "in" / "many.csv", 500)
        result = toolbox.generate_schema("in/many.csv", sample_rows=50, infer_primary_key=True)
        assert result["input"]["sampled"] is True and result["rows_analyzed"] == 50
        assert result["document"]["x-primaryKey"]["columns"] == ["id"]

    def test_tab_separated_files(self, toolbox, root):
        (root / "in" / "people.tsv").write_text("id\tname\n1\tAnn\n")
        result = toolbox.generate_schema("in/people.tsv")
        assert [c["name"] for c in result["summary"]["columns"]] == ["id", "name"]
        (root / "in" / "semi.tsv").write_text("id;name\n1;Ann\n")
        result = toolbox.generate_schema("in/semi.tsv", input_options={"delimiter": ";"})
        assert result["summary"]["column_count"] == 2

    def test_excel(self, toolbox, root):
        _workbook(root / "in" / "people.xlsx", [["id", "name"], [1, "Ann"], [2, "Bob"]])
        result = toolbox.generate_schema("in/people.xlsx")
        assert result["input"]["format"] == "excel" and result["input"]["sampled"] is False
        assert [c["name"] for c in result["summary"]["columns"]] == ["id", "name"]

    def test_a_failed_generation_is_a_result(self, toolbox, root):
        (root / "in" / "latin.csv").write_bytes(b"id,name\n1,Ren\xe9\n")
        result = toolbox.generate_schema("in/latin.csv", output_path="s.json")
        assert result["status"] == "failed" and result["error"]["code"] == "ENCODING_ERROR"
        assert "summary" not in result and result["written"] is False
        assert not (root / "s.json").exists()

    def test_arguments_are_checked(self, toolbox, root):
        with pytest.raises(ToolInputError, match="sample_rows must be from 1 to 100000"):
            toolbox.generate_schema("in/people.csv", sample_rows=0)
        (root / "in" / "data.bin").write_text("x")
        with pytest.raises(ToolInputError, match="cannot be told from its extension"):
            toolbox.generate_schema("in/data.bin")
        assert toolbox.generate_schema("in/data.bin", format="csv")["status"] == "succeeded"
        with pytest.raises(ToolInputError, match="format 'sql' is not read by the local server"):
            toolbox.generate_schema("in/people.csv", format="sql")
        with pytest.raises(ToolInputError, match=r"input_options is invalid:\n- input_options"):
            toolbox.generate_schema("in/people.csv", input_options={"delimiter": ";;"})
        with pytest.raises(ToolInputError, match=r"- input_options.nope: unknown field"):
            toolbox.generate_schema("in/people.csv", input_options={"nope": 1})


# ====================================================================== validate_schema


class TestValidateSchema:
    def test_a_schema_that_fits(self, toolbox, root):
        _write_json(root / "lenient.json", LENIENT)
        result = toolbox.validate_schema("in/people.csv", schema_path="lenient.json")
        assert result["valid"] is True and result["status"] == "succeeded"
        assert result["schema"] == "lenient.json" and result["error"] is None
        assert result["columns"] == ["id", "name", "age", "email"]
        assert result["schema_columns_not_in_input"] == []
        assert result["counts"]["invalid_rows"] == 2
        assert result["validation_summary"] == {"VALIDATION_ERROR:age": 1}
        assert result["schema_extensions"] == ["x-validation"] and result["notes"] == []
        assert result["rejected_rows"] is None

    def test_a_schema_that_rejects_too_many_rows(self, toolbox):
        result = toolbox.validate_schema("in/people.csv", schema=STRICT)
        assert result["valid"] is False and result["status"] == "failed"
        assert result["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"
        # The sample's rejected rows are in the result: the message points there, not at a file
        message = result["error"]["message"]
        assert ".forklift-job-" not in message and "out/bad_rows.parquet" not in message
        assert (
            "All rows rejected by the checks are in rejected_rows of this result (the first of "
            "them; run_job keeps the file)"
        ) in message
        assert result["rejected_rows"] == {
            "total_rows": 2,
            "offset": 0,
            "columns": ["id", "name", "age", "email", "_rejection_reason"],
            "columns_total": 5,
            "columns_truncated": False,
            "rows": [
                ["2", "Bob", "abc", "bob@example.org", "type_conversion_failed"],
                ["3", "Cy", "200", "cy@example.org", "VALIDATION_ERROR:age"],
            ],
            "row_count": 2,
            "truncated": False,
            "limit": None,
            "truncated_cells": 0,
            "reasons": [
                {
                    "rule": "VALIDATION_ERROR",
                    "column": "age",
                    "reason": "VALIDATION_ERROR:age",
                    "rows": 1,
                },
                {
                    "rule": "type_conversion_failed",
                    "column": None,
                    "reason": "type_conversion_failed",
                    "rows": 1,
                },
            ],
        }
        assert result["caps"]["rejected_rows"] == 10 and result["caps"]["max_bytes"] == 65536
        assert result["schema"] == "inline" and "rejected_rows" in result["notes"][0]

    def test_rejected_rows_of_the_sample_are_bounded(self, toolbox, root, monkeypatch):
        monkeypatch.setattr(tools, "VALIDATE_REJECTED_ROWS", 1)
        result = toolbox.validate_schema("in/people.csv", schema=STRICT)
        rejected = result["rejected_rows"]
        assert rejected["total_rows"] == 2 and rejected["row_count"] == 1
        assert rejected["truncated"] is True and rejected["limit"] == "rows"
        assert not [p for p in root.iterdir() if p.name != "in"]

    def test_a_missing_column(self, toolbox):
        schema = {"properties": dict(COLUMNS, phone={"type": "string"}), "required": ["phone"]}
        result = toolbox.validate_schema("in/people.csv", schema=schema)
        assert result["valid"] is False and result["error"]["code"] == "COLUMN_MISSING"
        assert result["schema_columns_not_in_input"] == ["phone"]

    def test_extensions_can_be_left_out(self, toolbox):
        result = toolbox.validate_schema(
            "in/people.csv", schema=STRICT, apply_schema_extensions=False
        )
        assert result["valid"] is True and result["schema_extensions"] == []

    def test_the_schema_is_required_once(self, toolbox, root):
        with pytest.raises(ToolInputError, match="a schema is needed"):
            toolbox.validate_schema("in/people.csv")
        _write_json(root / "s.json", STRICT)
        with pytest.raises(ToolInputError, match="not both"):
            toolbox.validate_schema("in/people.csv", schema=STRICT, schema_path="s.json")
        _write_json(root / "list.json", [STRICT])
        with pytest.raises(ToolInputError, match="schema_path 'list.json' is not a schema"):
            toolbox.validate_schema("in/people.csv", schema_path="list.json")

    def test_excel_inputs_are_refused(self, toolbox, root):
        _workbook(root / "in" / "people.xlsx", [["id"], [1]])
        with pytest.raises(ToolInputError, match="checks a schema against a CSV input"):
            toolbox.validate_schema("in/people.xlsx", schema=STRICT)

    def test_an_unknown_encoding_is_refused(self, toolbox):
        with pytest.raises(
            ToolInputError, match=r"(?s)The job was refused: .*unknown encoding 'nope'"
        ):
            toolbox.validate_schema(
                "in/people.csv", schema=STRICT, input_options={"encoding": "nope"}
            )

    def test_sample_rows_is_checked(self, toolbox):
        with pytest.raises(ToolInputError, match="sample_rows"):
            toolbox.validate_schema("in/people.csv", schema=STRICT, sample_rows=100_001)


# ====================================================================== preview


class TestPreview:
    def test_raw_rows(self, toolbox):
        result = toolbox.preview("in/people.csv", rows=2)
        assert result["mode"] == "raw" and result["status"] == "succeeded"
        assert result["columns"] == ["id", "name", "age", "email"]
        assert result["rows"] == [
            ["1", "Ann", "34", "ann@example.org"],
            ["2", "Bob", "abc", "bob@example.org"],
        ]
        assert result["truncated"] is True and result["limit"] == "rows"
        assert result["caps"] == {
            "rows": 2,
            "max_rows": 500,
            "max_bytes": 65536,
            "max_cell_chars": 2000,
            "warnings": 50,
        }
        assert "sheet" not in result

    def test_all_rows_fit(self, toolbox):
        result = toolbox.preview("in/people.csv")
        assert result["row_count"] == 4 and result["truncated"] is False

    def test_blank_lines_are_not_counted_as_rows(self, toolbox, root):
        (root / "in" / "gaps.csv").write_text("id,name\n\n1,Ann\n\n\n2,Bob\n3,Cy\n")
        result = toolbox.preview("in/gaps.csv", rows=2)
        assert result["rows"] == [["1", "Ann"], ["2", "Bob"]] and result["limit"] == "rows"

    def test_the_byte_limit(self, toolbox, root):
        (root / "in" / "wide.csv").write_text("a\n" + ("x" * 600 + "\n") * 5)
        result = toolbox.preview("in/wide.csv", max_bytes=1024)
        assert result["limit"] == "bytes" and result["row_count"] < 5

    def test_excel_rows_and_sheet(self, toolbox, root):
        _workbook(root / "in" / "people.xlsx", [["id", "name"], [1, "Ann"], [2, "Bob"]])
        result = toolbox.preview("in/people.xlsx")
        assert result["sheet"] == "People" and result["rows"] == [["1", "Ann"], ["2", "Bob"]]

    def test_a_workbook_too_large_to_copy(self, toolbox, root, monkeypatch):
        _workbook(root / "in" / "people.xlsx", [["id"], [1]])
        monkeypatch.setattr(tools, "MAX_EXCEL_COPY_BYTES", 100)
        with pytest.raises(ToolInputError, match="workbooks up to 100 bytes"):
            toolbox.preview("in/people.xlsx")

    def test_typed_rows_and_what_the_schema_rejects(self, toolbox, root):
        _write_json(root / "strict.json", STRICT)
        result = toolbox.preview("in/people.csv", schema_path="strict.json")
        assert result["mode"] == "typed" and result["status"] == "succeeded"
        assert result["columns"] == [
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int64"},
            {"name": "email", "type": "string"},
        ]
        assert result["rows"] == [
            [1, "Ann", 34, "ann@example.org"],
            [4, "Di", 41, "di@example.org"],
        ]
        assert result["sample"] == {
            "rows_read": 4,
            "rejected_rows": 2,
            "rejection_reasons": [
                {
                    "rule": "VALIDATION_ERROR",
                    "column": "age",
                    "reason": "VALIDATION_ERROR:age",
                    "rows": 1,
                },
                {
                    "rule": "type_conversion_failed",
                    "column": None,
                    "reason": "type_conversion_failed",
                    "rows": 1,
                },
            ],
        }
        assert "threshold is not applied to a preview" in result["notes"][0]
        assert result["schema"] == "strict.json"
        assert json.loads((root / "strict.json").read_text()) == STRICT  # not changed

    def test_typed_rows_without_rejections(self, toolbox):
        schema = {"properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}
        result = toolbox.preview("in/people.csv", schema=schema, rows=1)
        # Columns the schema does not name are kept as text
        assert result["rows"] == [[1, "Ann", "34", "ann@example.org"]]
        assert result["limit"] == "rows"
        assert result["sample"]["rejection_reasons"] == [] and result["notes"] == []

    def test_typed_preview_of_a_failing_schema(self, toolbox):
        schema = {"properties": COLUMNS, "x-primaryKey": {"columns": ["phone"]}}
        result = toolbox.preview("in/people.csv", schema=schema)
        assert result["status"] == "failed" and result["error"]["code"] == "COLUMN_MISSING"
        assert result["rows"] == [] and result["columns"] == []

    def test_typed_preview_shows_at_most_max_columns(self, toolbox, root, monkeypatch):
        monkeypatch.setattr(tools, "MAX_COLUMNS", 2)
        result = toolbox.preview("in/people.csv", schema={"properties": COLUMNS}, rows=1)
        assert [c["name"] for c in result["columns"]] == ["id", "name"]
        assert result["columns_total"] == 4 and result["rows"] == [[1, "Ann"]]

    def test_an_undecodable_file_is_copied_as_bytes(self, toolbox, root):
        (root / "in" / "latin.csv").write_bytes(b"id,name\n1,Ren\xe9\n")
        result = toolbox.preview("in/latin.csv")
        assert result["status"] == "failed" and result["error"]["code"] == "ENCODING_ERROR"
        result = toolbox.preview("in/latin.csv", input_options={"encoding": "latin-1"})
        assert result["rows"] == [["1", "René"]]

    def test_rows_the_csv_module_refuses_are_copied_as_bytes(self, toolbox, monkeypatch):
        class Refusing:
            Error = csv.Error

            @staticmethod
            def reader(*args, **kwargs):
                raise csv.Error("field larger than field limit (131072)")

        monkeypatch.setattr(tools, "csv", Refusing)  # only the copy step, not the engine
        result = toolbox.preview("in/people.csv")
        assert result["row_count"] == 4 and result["input"]["sampled"] is False

    def test_the_copy_stops_at_its_size_limit(self, toolbox, root, monkeypatch):
        monkeypatch.setattr(tools, "MAX_SAMPLE_COPY_CHARS", 60)
        result = toolbox.preview("in/people.csv")
        assert result["input"]["sampled"] is True and 1 <= result["row_count"] < 4
        (root / "in" / "latin.csv").write_bytes(b"id,name\n" + b"1,Ren\xe9\n" * 20)
        result = toolbox.preview("in/latin.csv", input_options={"encoding": "ascii"})
        assert result["input"]["sampled"] is True
        (root / "in" / "short.csv").write_bytes(b"id\n\xff\n")
        result = toolbox.preview("in/short.csv", input_options={"encoding": "ascii"})
        assert result["input"]["sampled"] is False

    def test_a_file_name_the_contract_cannot_hold(self, toolbox, root):
        (root / "in" / "a\\b.csv").write_text(PEOPLE)
        result = toolbox.preview("in/a\\b.csv", rows=1)
        assert result["input"]["path"] == "in/a\\b.csv" and result["row_count"] == 1

    def test_a_file_that_is_not_a_workbook(self, toolbox, root):
        (root / "in" / "bad.xlsx").write_text("id,name\n")
        result = toolbox.preview("in/bad.xlsx")
        assert result["status"] == "failed" and result["error"]["code"] == "INPUT_UNREADABLE"
        assert "'bad.xlsx' is not a valid .xlsx (zip) file" in result["error"]["message"]

    def test_messages_name_the_input_not_the_copy(self):
        staged = tools._Staged("in/people.csv", "csv", {"path": "data/people.csv"})
        assert staged.clean("in/people.csv in .forklift-job-ab_1/validate-out/x") == (
            "data/people.csv in <temporary directory>/validate-out/x"
        )
        assert staged.clean_list(["in/people.csv"]) == ["data/people.csv"]

    def test_rows_and_bytes_are_checked(self, toolbox):
        with pytest.raises(ToolInputError, match="rows must be from 1 to 500"):
            toolbox.preview("in/people.csv", rows=501)
        with pytest.raises(ToolInputError, match="max_bytes must be from 1024 to 1048576"):
            toolbox.preview("in/people.csv", max_bytes=10)
        with pytest.raises(ToolInputError, match="rows must be an integer"):
            toolbox.preview("in/people.csv", rows=2.5)


# ====================================================================== run_job and after


def _job_file(directory):
    return json.loads((directory / JOB_FILE).read_text())


class TestRunJob:
    def test_a_successful_run(self, toolbox, root):
        result = toolbox.run_job(
            "in/people.csv",
            "out/run1",
            schema=LENIENT,
            job_id="people-1",
            options={"batch_size": 2},
        )
        assert result["job_id"] == "people-1" and result["status"] == "succeeded"
        assert result["error"] is None and result["counts"]["valid_rows"] == 2
        assert result["output_dir"] == "out/run1"
        assert result["result_file"] == "out/run1/forklift-job.json"
        kinds = [artifact["kind"] for artifact in result["artifacts"]]
        assert kinds[:2] == ["data", "bad_rows"] and "manifest" in kinds
        assert all(artifact["exists"] for artifact in result["artifacts"])
        assert result["artifacts"][0]["path"] == "out/run1/data.parquet"
        assert result["next"] == "get_bad_rows shows the rejected rows"
        job = _job_file(root / "out" / "run1")
        assert job["format"] == "forklift-mcp-job/1" and job["schema"] == "inline"
        assert job["input"] == {
            "path": str(root / "in" / "people.csv"),
            "format": "csv",
            "bytes": len(PEOPLE),
        }
        assert RESULT_VALIDATOR.is_valid(job["result"])
        assert job["result"]["artifacts"][0]["path"] == "data.parquet"
        assert not [p for p in root.iterdir() if p.name.startswith(".")]
        assert toolbox.get_job("out/run1") == {k: v for k, v in result.items() if k != "next"}

    def test_a_failed_run_keeps_its_result(self, toolbox, root):
        _write_json(root / "strict.json", STRICT)
        result = toolbox.run_job("in/people.csv", "out", schema_path="strict.json")
        assert result["status"] == "failed"
        assert result["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"
        assert "out/bad_rows.parquet" in result["error"]["message"]
        assert [a["kind"] for a in result["artifacts"]] == ["bad_rows"]
        assert result["schema"] == "strict.json"
        assert result["next"].startswith("explain_failure")
        assert _job_file(root / "out")["result"]["status"] == "failed"

    def test_dry_run(self, toolbox, root):
        result = toolbox.run_job("in/people.csv", "out", schema=STRICT, dry_run=True)
        assert result["dry_run"] is True and result["writable"] is True
        assert result["would_run"]["output_dir"] == "out"
        assert result["would_run"]["result_file"] == "out/forklift-job.json"
        assert result["spec"]["kind"] == "run" and "schema" not in result["spec"]
        assert result["spec"]["output"]["location"] == {"type": "file", "path": "out/"}
        assert not (root / "out").exists()

    def test_read_only(self, read_only, root):
        assert read_only.run_job("in/people.csv", "out", dry_run=True)["writable"] is False
        with pytest.raises(ToolInputError, match="run_job would write files.*--read-only"):
            read_only.run_job("in/people.csv", "out")
        assert not (root / "out").exists()

    def test_a_non_empty_output_dir_is_refused(self, toolbox, root):
        with pytest.raises(ToolInputError, match="'in' is not empty"):
            toolbox.run_job("in/people.csv", "in")
        with pytest.raises(ToolInputError, match="'in' is not empty"):
            toolbox.run_job("in/people.csv", "in", dry_run=True)

    def test_overwrite_replaces_the_files_of_the_earlier_run(self, toolbox, root):
        assert toolbox.run_job("in/people.csv", "out", schema=STRICT)["status"] == "failed"
        out = root / "out"
        (out / "notes.txt").write_text("mine")
        schema = {"properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}
        result = toolbox.run_job("in/people.csv", "out", schema=schema, overwrite=True)
        assert result["status"] == "succeeded"
        assert not (out / "bad_rows.parquet").exists()
        assert (out / "notes.txt").read_text() == "mine"
        assert (out / "data.parquet").exists()

    def test_overwrite_with_an_unreadable_earlier_job_file(self, toolbox, root):
        out = root / "out"
        out.mkdir()
        (out / JOB_FILE).write_text("{broken")
        (out / "data.parquet").write_text("old")
        result = toolbox.run_job("in/people.csv", "out", overwrite=True)
        assert result["status"] == "succeeded" and _job_file(out)["format"]

    def test_overwrite_never_deletes_outside_the_directory(self, toolbox, root):
        out = root / "out"
        out.mkdir()
        (root / "keep.txt").write_text("keep")
        (out / "old.parquet").write_text("old")
        _crafted_job(
            out,
            [
                {"kind": "data", "path": "../keep.txt"},
                {"kind": "data", "path": "old.parquet"},
                {"kind": "metadata", "path": "gone.json"},
            ],
        )
        toolbox.run_job("in/people.csv", "out", overwrite=True)
        assert (root / "keep.txt").read_text() == "keep"
        assert not (out / "old.parquet").exists()

    def test_overwrite_into_the_root_itself(self, toolbox, root):
        result = toolbox.run_job("in/people.csv", ".", overwrite=True, dry_run=True)
        assert result["spec"]["output"]["location"]["path"] == "./"
        assert toolbox.run_job("in/people.csv", ".", overwrite=True)["status"] == "succeeded"
        assert (root / "data.parquet").exists() and (root / JOB_FILE).exists()

    def test_input_and_output_in_different_roots(self, tmp_path, root):
        other = tmp_path / "other"
        other.mkdir()
        toolbox = Toolbox(Workspace([root, other]))
        with pytest.raises(ToolInputError, match="must be inside the same --root"):
            toolbox.run_job("in/people.csv", str(other / "out"))

    def test_nested_roots_use_one_that_holds_both(self, root):
        toolbox = Toolbox(Workspace([root / "in", root]))
        result = toolbox.run_job(str(root / "in" / "people.csv"), str(root / "out"))
        assert result["status"] == "succeeded"
        assert result["output_dir"] == str(root / "out")
        assert result["input"]["path"] == "people.csv"

    def test_invalid_options(self, toolbox):
        with pytest.raises(
            ToolInputError, match=r"(?s)valid job.*- options.batch_size: must be at least 1"
        ):
            toolbox.run_job("in/people.csv", "out", options={"batch_size": 0})
        with pytest.raises(ToolInputError, match=r"job_id"):
            toolbox.run_job("in/people.csv", "out", job_id="no spaces")

    def test_a_job_the_engine_refuses_writes_no_result(self, toolbox, root):
        with pytest.raises(ToolInputError, match=r"(?s)The job was refused: .*unknown encoding"):
            toolbox.run_job("in/people.csv", "out", input_options={"encoding": "nope"})
        assert not (root / "out").exists()

    def test_a_directory_being_written_is_refused(self, toolbox, root):
        with toolbox.workspace.claim(root / "out", "output_dir"):
            with pytest.raises(ToolInputError, match="is being written by another call"):
                toolbox.run_job("in/people.csv", "out")

    def test_a_result_that_cannot_be_written(self, toolbox, root, monkeypatch):
        def refuse(target, text):
            raise PermissionError(13, "Permission denied")

        monkeypatch.setattr(tools, "_write_text_atomically", refuse)
        result = toolbox.run_job("in/people.csv", "out")
        assert result["status"] == "succeeded" and result["result_file"] is None
        assert result["result_file_error"] == (
            "The result could not be written to out/forklift-job.json (PermissionError: "
            "Permission denied); get_job and explain_failure cannot read this run"
        )
        assert result["artifacts"][0]["exists"] is True

    def test_excel(self, toolbox, root):
        _workbook(root / "in" / "people.xlsx", [["id", "name"], [1, "Ann"], [2, "Bob"]])
        result = toolbox.run_job("in/people.xlsx", "out")
        assert result["status"] == "succeeded" and result["input"]["format"] == "excel"
        assert result["artifacts"][0]["kind"] == "data" and result["artifacts"][0]["rows"] == 2

    def test_an_input_that_vanished_is_a_failed_run(self, toolbox, root, monkeypatch):
        real_run_job = tools.run_job

        def vanish(spec, base_dir):
            (root / "in" / "people.csv").unlink()
            return real_run_job(spec, base_dir=base_dir)

        monkeypatch.setattr(tools, "run_job", vanish)
        result = toolbox.run_job("in/people.csv", "out")
        assert result["status"] == "failed" and result["error"]["code"] == "INPUT_UNREADABLE"
        assert result["input"]["bytes"] == len(PEOPLE)
        assert toolbox.explain_failure("out")["error"]["code"] == "INPUT_UNREADABLE"


class TestGetJob:
    def test_the_job_file_itself(self, toolbox):
        toolbox.run_job("in/people.csv", "out")
        assert toolbox.get_job("out/forklift-job.json")["status"] == "succeeded"

    def test_a_directory_without_a_job_file(self, toolbox):
        with pytest.raises(ToolInputError, match="'in' has no forklift-job.json: run_job writes"):
            toolbox.get_job("in")
        with pytest.raises(ToolInputError, match="has no forklift-job.json"):
            toolbox.get_job("in/forklift-job.json")
        with pytest.raises(ToolInputError, match="'in/other.json' does not exist"):
            toolbox.get_job("in/other.json")

    @pytest.mark.parametrize(
        "document, message",
        [
            ({"format": "something-else"}, "is not a job file written by forklift-mcp"),
            ([1], "is not a job file written by forklift-mcp"),
            ({"format": "forklift-mcp-job/1", "result": {}}, "does not hold a valid job result"),
        ],
    )
    def test_files_that_are_not_job_files(self, toolbox, root, document, message):
        _write_json(root / "out" / JOB_FILE, document)
        with pytest.raises(ToolInputError, match=message):
            toolbox.get_job("out")

    def test_a_job_file_that_names_files_elsewhere(self, toolbox, root):
        (root / "out").mkdir()
        _crafted_job(root / "out", [{"kind": "data", "path": "../in/people.csv"}])
        with pytest.raises(ToolInputError, match="names a file outside its directory"):
            toolbox.get_job("out")

    def test_a_job_file_too_large(self, toolbox, root, monkeypatch):
        toolbox.run_job("in/people.csv", "out")
        monkeypatch.setattr(tools, "MAX_JOB_FILE_BYTES", 100)
        with pytest.raises(ToolInputError, match="more than the 100 read"):
            toolbox.get_job("out")

    def test_files_that_were_removed_and_bounded_lists(self, toolbox, root, monkeypatch):
        toolbox.run_job("in/people.csv", "out", schema=LENIENT)
        (root / "out" / "data.parquet").unlink()
        monkeypatch.setattr(tools, "MAX_LIST_ITEMS", 1)
        result = toolbox.get_job("out")
        assert result["artifacts"] == [
            {
                "kind": "data",
                "path": "out/data.parquet",
                "rows": 2,
                "bytes": result["artifacts"][0]["bytes"],
                "exists": False,
            }
        ]
        assert result["artifacts_total"] > 1

    def test_a_minimal_job_file(self, toolbox, root):
        (root / "out").mkdir()
        _crafted_job(root / "out")
        view = toolbox.get_job("out")
        assert view["input"] == {"path": None, "format": None, "bytes": None}
        assert view["schema"] is None and view["artifacts"] == []


class TestExplainFailure:
    def test_a_threshold_failure(self, toolbox, root):
        toolbox.run_job("in/people.csv", "out", schema=STRICT)
        result = toolbox.explain_failure("out", rows=1)
        assert result["status"] == "failed"
        assert result["summary"] == (
            "The run failed with BAD_ROWS_THRESHOLD_EXCEEDED after reading 4 rows. 2 rejected "
            "rows are kept; most common reasons: VALIDATION_ERROR:age (1), "
            "type_conversion_failed (1)."
        )
        error = result["error"]
        assert error["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED" and error["retryable"] is False
        assert "maxBadRowsPercent" in error["meaning"] and error["next_steps"]
        rejected = result["rejected"]
        assert rejected["path"] == "out/bad_rows.parquet" and rejected["total_rows"] == 2
        assert rejected["reasons"][1]["meaning"] == (
            "a value could not be converted to its column's type"
        )
        assert "meaning" not in rejected["reasons"][0]
        first = rejected["first_rows"]
        assert first["columns"] == ["id", "name", "age", "email", "_rejection_reason"]
        assert first["rows"] == [["2", "Bob", "abc", "bob@example.org", "type_conversion_failed"]]
        assert first["truncated"] is True and first["limit"] == "rows"
        assert result["docs"] == [
            "forklift://docs/schemas/X_VALIDATION_DOCUMENTATION.md",
            "forklift://docs/schemas/README.md",
            "forklift://docs/schemas/X_CSV_DOCUMENTATION.md",
        ]
        assert result["caps"]["rows"] == 1 and result["caps"]["max_rows"] == 50

    def test_a_successful_run_with_findings(self, toolbox, root):
        schema = dict(LENIENT, properties=dict(COLUMNS, name={"type": "string", "maxLength": 2}))
        toolbox.run_job("in/people.csv", "out", schema=schema)
        result = toolbox.explain_failure("out")
        assert result["error"] is None and result["status"] == "succeeded"
        assert result["summary"] == (
            "The run succeeded: 1 of 4 rows were written and 3 rejected. 3 rejected rows are "
            "kept; most common reasons: LENGTH_VIOLATION:name (1), VALIDATION_ERROR:age (1), "
            "type_conversion_failed (1)."
        )
        assert {
            "rule": "VALIDATION_ERROR",
            "column": "age",
            "key": "VALIDATION_ERROR:age",
            "count": 1,
        } in result["findings"]
        assert "forklift://docs/schemas/X_CONSTRAINT_HANDLING_DOCUMENTATION.md" in result["docs"]

    def test_a_run_without_rejected_rows(self, toolbox):
        toolbox.run_job("in/people.csv", "out")
        result = toolbox.explain_failure("out")
        assert result["rejected"] is None and result["findings"] == []
        assert result["summary"] == "The run succeeded: 4 of 4 rows were written and 0 rejected."

    def test_a_result_without_counts_and_no_pages(self, toolbox, root, monkeypatch):
        error = {"code": "INTERNAL", "message": "RuntimeError: boom", "retryable": False}
        (root / "out").mkdir()
        _crafted_job(root / "out", status="failed", error=error)
        monkeypatch.setattr(tools, "schema_pages", lambda: [])
        explained = toolbox.explain_failure("out")
        assert explained["summary"] == "The run failed with INTERNAL."
        assert explained["docs"] == [] and explained["error"]["next_steps"]

    def test_arguments_are_checked(self, toolbox):
        with pytest.raises(ToolInputError, match="rows must be from 1 to 50"):
            toolbox.explain_failure("out", rows=51)
        with pytest.raises(ToolInputError, match="max_bytes"):
            toolbox.explain_failure("out", max_bytes=0)


class TestGetBadRows:
    @pytest.fixture
    def failed(self, toolbox):
        toolbox.run_job("in/people.csv", "out", schema=STRICT)
        return toolbox

    def test_pages_of_rows(self, failed):
        first = failed.get_bad_rows("out", limit=1)
        assert first["total_rows"] == 2 and first["row_count"] == 1
        assert first["next_offset"] == 1 and first["truncated"] is True
        assert first["path"] == "out/bad_rows.parquet" and first["status"] == "failed"
        second = failed.get_bad_rows("out", offset=1)
        assert second["rows"] == [["3", "Cy", "200", "cy@example.org", "VALIDATION_ERROR:age"]]
        assert second["next_offset"] is None and second["truncated"] is False
        assert failed.get_bad_rows("out", offset=5)["rows"] == []

    def test_selected_columns(self, failed):
        result = failed.get_bad_rows("out", columns=["age", "_rejection_reason", "age"])
        assert result["columns"] == ["age", "_rejection_reason"]
        assert result["rows"][0] == ["abc", "type_conversion_failed"]
        assert result["columns_truncated"] is True and result["columns_total"] == 5
        with pytest.raises(ToolInputError, match="columns 'phone' are not in the bad rows"):
            failed.get_bad_rows("out", columns=["phone"])

    def test_column_limits(self, failed, monkeypatch):
        monkeypatch.setattr(tools, "MAX_COLUMNS", 2)
        result = failed.get_bad_rows("out")
        assert result["columns"] == ["id", "_rejection_reason"]
        with pytest.raises(ToolInputError, match="at most 2 columns"):
            failed.get_bad_rows("out", columns=["id", "name", "age"])

    def test_default_columns_without_a_reason_column(self, toolbox, root, monkeypatch):
        out = root / "out"
        out.mkdir()
        pq.write_table(pa.table({"a": ["1"], "b": ["2"], "c": ["3"]}), str(out / "bad.parquet"))
        _crafted_job(out, [{"kind": "bad_rows", "path": "bad.parquet"}])
        monkeypatch.setattr(tools, "MAX_COLUMNS", 2)
        assert toolbox.get_bad_rows("out")["columns"] == ["a", "b"]

    def test_a_file_that_is_not_parquet(self, failed, root):
        (root / "out" / "bad_rows.parquet").write_text("not parquet")
        for call in (failed.get_bad_rows, failed.explain_failure):
            with pytest.raises(
                ToolInputError, match="out/bad_rows.parquet cannot be read as Parq"
            ):
                call("out")

    def test_the_byte_limit(self, failed):
        result = failed.get_bad_rows("out", max_bytes=1024, columns=["id"])
        assert result["limit"] is None and result["row_count"] == 2

    def test_a_run_without_bad_rows(self, toolbox):
        toolbox.run_job("in/people.csv", "out")
        result = toolbox.get_bad_rows("out")
        assert result["path"] is None and result["total_rows"] == 0 and result["rows"] == []
        assert "rejected no rows" in result["message"]

    @pytest.mark.parametrize(
        "arguments, message",
        [
            ({"offset": -1}, "offset must be at least 0"),
            ({"limit": 0}, "limit must be from 1 to 200"),
            ({"limit": 201}, "limit must be from 1 to 200"),
            ({"max_bytes": 2_000_000}, "max_bytes"),
        ],
    )
    def test_arguments_are_checked(self, toolbox, arguments, message):
        with pytest.raises(ToolInputError, match=message):
            toolbox.get_bad_rows("out", **arguments)


def test_every_error_code_is_explained():
    from forklift.jobs import ERROR_CODES
    from forklift.mcp import explain

    assert set(explain.ERROR_HELP) == set(ERROR_CODES)
    docs = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    pages = {path.name for path in docs.glob("*.md")}
    named = {page for _, _, listed in explain.ERROR_HELP.values() for page in listed}
    named |= {page for listed in explain.RULE_PAGES.values() for page in listed}
    assert named <= pages


def test_an_atomic_write_that_fails_leaves_nothing_behind(tmp_path):
    target = tmp_path / "taken"
    (target / "inside").mkdir(parents=True)
    with pytest.raises(OSError):
        tools._write_text_atomically(target, "{}")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["taken"]
