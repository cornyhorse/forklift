"""The job contract: JobSpec / JobResult and their published JSON Schemas (contracts/).

The schemas in ``contracts/`` are generated from the dataclasses; these tests fail when the
checked-in files are out of date, and check that the Python validation (``from_dict``) and the
JSON Schema accept and refuse the same documents, so that the gateway (which validates with the
schema) and the engine (which validates with ``from_dict``) never disagree.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import jsonschema
import pytest

from forklift.jobs import (
    ContractError,
    FileLocation,
    InputSpec,
    JobResult,
    JobSpec,
    OutputSpec,
    PresignedUrlLocation,
    SqlLocation,
    SqlTableLocation,
    contract,
)
from forklift.jobs._model import _describe
from forklift.jobs.result import Artifact, JobError

REPO = Path(__file__).resolve().parents[2]
CONTRACTS = REPO / "contracts"

SPEC_VALIDATOR = jsonschema.Draft202012Validator(contract.jobspec_schema())
RESULT_VALIDATOR = jsonschema.Draft202012Validator(contract.jobresult_schema())


def _spec(**changes):
    document = {
        "spec_version": 1,
        "job_id": "01JB2X",
        "kind": "run",
        "input": {"format": "csv", "location": {"type": "file", "path": "in/people.csv"}},
        "output": {"location": {"type": "file", "path": "out/"}},
    }
    document.update(changes)
    return document


def _with(document, path, value):
    """``document`` with the value at dotted ``path`` replaced (or removed for ...)."""
    document = copy.deepcopy(document)
    *parents, last = path.split(".")
    target = document
    for key in parents:
        target = target[key]
    if value is ...:
        del target[last]
    else:
        target[last] = value
    return document


def _accepted(document) -> bool:
    try:
        JobSpec.from_dict(document)
        return True
    except ContractError:
        return False


class TestPublishedSchemas:
    def test_checked_in_schemas_are_up_to_date(self):
        stale = contract.outdated_schemas(CONTRACTS)
        assert stale == {}, (
            f"contracts/ is out of date ({stale}); regenerate it with "
            "python -m forklift.jobs.contract"
        )

    @pytest.mark.parametrize("name", sorted(contract.SCHEMA_FILES))
    def test_schemas_are_valid_draft_2020_12(self, name):
        schema = json.loads((CONTRACTS / name).read_text())
        jsonschema.Draft202012Validator.check_schema(schema)
        assert schema["$id"].endswith(name)

    def test_main_writes_and_checks_the_files(self, tmp_path, capsys):
        target = tmp_path / "contracts"

        assert contract.main([str(target), "--check"]) == 1
        assert "missing" in capsys.readouterr().err
        assert contract.main([str(target)]) == 0
        assert "Wrote" in capsys.readouterr().out
        assert contract.main([str(target), "--check"]) == 0

        (target / "jobresult.schema.json").write_text("{}\n")
        assert contract.main([str(target), "--check"]) == 1
        err = capsys.readouterr().err
        assert "jobresult.schema.json: differs from the generated schema" in err
        assert "python -m forklift.jobs.contract" in err


VALID_SPECS = [
    _spec(),
    _spec(kind="preview", output=None),
    _spec(kind="generate_schema"),
    _spec(kind="validate_schema", schema={"properties": {}}),
    _spec(input={"format": "csv", "location": {"type": "s3", "uri": "s3://b/k.csv"}}),
    _spec(
        input={
            "format": "csv",
            "location": {"type": "presigned_url", "url": "https://h/o?X-Amz=1", "size": 5},
            "options": {"delimiter": ";", "footer_detection": {"stop_on_blank": True}},
        }
    ),
    _spec(
        input={"format": "sql", "location": {"type": "sql", "connection_string": "DSN=x"}},
        schema={"x-sql": {}},
        output={
            "location": {
                "type": "sql_table",
                "connection_string": "DSN=y",
                "table": "people",
                "mode": "upsert",
                "key_columns": ["id"],
            },
            "artifacts": {"type": "file", "path": "parquet/"},
        },
    ),
    _spec(
        input={
            "format": "excel",
            "location": {"type": "file", "path": "b.xlsx"},
            "options": {"sheet": 0},
        }
    ),
    _spec(
        input={
            "format": "excel",
            "location": {"type": "file", "path": "b.xlsx"},
            "options": {"sheet": "S"},
        }
    ),
    _spec(limits={"max_input_bytes": 1, "max_seconds": 0.5, "max_rows": 3}),
    _spec(options={"batch_size": 1, "preview_rows": 10_000, "sample_rows": 1}),
    _spec(output={"location": {"type": "s3", "uri": "s3://b/out/"}, "compression": "zstd"}),
    _spec(
        input={
            "format": "csv",
            "location": {"type": "file", "path": "a.csv"},
            "options": {"footer_detection": {"column_index": 0, "patterns": ["^TOTAL"]}},
        }
    ),
]

INVALID_SPECS = {
    "not an object": [],
    "wrong version": _spec(spec_version=2),
    "version missing": _with(_spec(), "spec_version", ...),
    "bad job id": _spec(job_id="has space"),
    "unknown kind": _spec(kind="runn"),
    "unknown field": _spec(extra=1),
    "run without output": _spec(output=None),
    "absolute path": _with(_spec(), "input.location.path", "/etc/passwd"),
    "parent path": _with(_spec(), "input.location.path", "in/../../x"),
    "backslash path": _with(_spec(), "input.location.path", "in\\x.csv"),
    "drive path": _with(_spec(), "input.location.path", "C:x.csv"),
    "unknown location": _with(_spec(), "input.location", {"type": "ftp", "path": "x"}),
    "location without type": _with(_spec(), "input.location", {"path": "x"}),
    "location type not a string": _with(_spec(), "input.location", {"type": 1}),
    "location not an object": _with(_spec(), "input.location", "in/x.csv"),
    "sql_table as input": _with(
        _spec(), "input.location", {"type": "sql_table", "connection_string": "x", "table": "t"}
    ),
    "presigned as output": _with(
        _spec(), "output.location", {"type": "presigned_url", "url": "https://h/"}
    ),
    "presigned for excel": _spec(
        input={"format": "excel", "location": {"type": "presigned_url", "url": "https://h/x"}}
    ),
    "presigned not http": _with(
        _spec(), "input.location", {"type": "presigned_url", "url": "ftp://h/x"}
    ),
    "negative size": _with(
        _spec(), "input.location", {"type": "presigned_url", "url": "https://h/x", "size": -1}
    ),
    "sql format with file": _spec(
        input={"format": "sql", "location": {"type": "file", "path": "x"}}, schema={}
    ),
    "sql location with csv": _spec(
        input={"format": "csv", "location": {"type": "sql", "connection_string": "x"}}
    ),
    "sql without schema": _spec(
        input={"format": "sql", "location": {"type": "sql", "connection_string": "x"}}
    ),
    "empty connection string": _spec(
        input={"format": "sql", "location": {"type": "sql", "connection_string": ""}}, schema={}
    ),
    "validate without schema": _spec(kind="validate_schema"),
    "upsert without keys": _with(
        _spec(),
        "output.location",
        {"type": "sql_table", "connection_string": "x", "table": "t", "mode": "upsert"},
    ),
    "empty key list": _with(
        _spec(),
        "output.location",
        {"type": "sql_table", "connection_string": "x", "table": "t", "key_columns": []},
    ),
    "unknown table mode": _with(
        _spec(),
        "output.location",
        {"type": "sql_table", "connection_string": "x", "table": "t", "mode": "merge"},
    ),
    "artifacts for a file output": _with(
        _spec(), "output.artifacts", {"type": "file", "path": "a/"}
    ),
    "preview to s3": _spec(
        kind="preview", output={"location": {"type": "s3", "uri": "s3://b/p/"}}
    ),
    "unknown compression": _with(_spec(), "output.compression", "lzma"),
    "two character delimiter": _with(_spec(), "input.options", {"delimiter": ";;"}),
    "unknown option": _with(_spec(), "input.options", {"delimeter": ";"}),
    "header mode": _with(_spec(), "input.options", {"header_mode": "maybe"}),
    "sheet as list": _with(_spec(), "input.options", {"sheet": ["a"]}),
    "patterns without column": _with(
        _spec(), "input.options", {"footer_detection": {"patterns": ["x"]}}
    ),
    "column without patterns": _with(
        _spec(), "input.options", {"footer_detection": {"column_index": 1}}
    ),
    "empty patterns": _with(
        _spec(), "input.options", {"footer_detection": {"column_index": 1, "patterns": []}}
    ),
    "comment rows not strings": _with(_spec(), "input.options", {"comment_rows": [1]}),
    "batch size zero": _spec(options={"batch_size": 0}),
    "batch size bool": _spec(options={"batch_size": True}),
    "batch size float": _spec(options={"batch_size": 1.5}),
    "too many preview rows": _spec(options={"preview_rows": 10_001}),
    "zero seconds": _spec(limits={"max_seconds": 0}),
    "seconds as text": _spec(limits={"max_seconds": "1"}),
    "schema not an object": _spec(schema=[]),
    "options not an object": _spec(options=1),
}


class TestValidationAgreesWithTheSchema:
    @pytest.mark.parametrize("document", VALID_SPECS)
    def test_valid_specs_are_accepted_by_both(self, document):
        assert SPEC_VALIDATOR.is_valid(document), list(SPEC_VALIDATOR.iter_errors(document))
        spec = JobSpec.from_dict(document)
        assert SPEC_VALIDATOR.is_valid(spec.to_dict())
        assert JobSpec.from_dict(spec.to_dict()) == spec

    @pytest.mark.parametrize("name", sorted(INVALID_SPECS))
    def test_invalid_specs_are_refused_by_both(self, name):
        document = INVALID_SPECS[name]
        assert not SPEC_VALIDATOR.is_valid(document)
        assert not _accepted(document)


class TestMessages:
    def _problems(self, document):
        with pytest.raises(ContractError) as caught:
            JobSpec.from_dict(document)
        return dict(caught.value.problems), str(caught.value)

    def test_every_problem_is_listed_with_its_field(self):
        document = _spec(kind="runn", limits={"max_rows": 0})
        document["input"]["options"] = {"delimeter": ";", "header_search_rows": "ten"}

        problems, text = self._problems(document)

        assert problems == {
            "kind": "'runn' is not one of 'run', 'preview', 'validate_schema', 'generate_schema'",
            "input.options.delimeter": problems["input.options.delimeter"],
            "input.options.header_search_rows": "must be an integer, got 'ten'",
            "limits.max_rows": "must be at least 1, got 0",
        }
        assert "did you mean 'delimiter'?" in problems["input.options.delimeter"]
        assert text.startswith("Invalid job spec (4 problems):\n- kind: ")

    def test_missing_field_says_what_it_is(self):
        problems, text = self._problems(_with(_spec(), "input.location.path", ...))
        assert problems["input.location.path"].startswith("required field is missing (Path")
        assert text.startswith("Invalid job spec (1 problem):")

    def test_unknown_field_without_a_close_match_lists_the_allowed_ones(self):
        problems, _ = self._problems(_with(_spec(), "output.location.zzz", 1))
        assert problems["output.location.zzz"] == "unknown field; allowed: path, type"

    def test_location_problems(self):
        problems, _ = self._problems(_with(_spec(), "input.location", {"type": "ftp"}))
        assert problems["input.location.type"] == (
            "'ftp' is not allowed here; expected one of 'file', 's3', 'presigned_url', 'sql'"
        )
        problems, _ = self._problems(_with(_spec(), "input.location", {"path": "x"}))
        assert problems["input.location.type"].startswith("required field is missing (one of")
        problems, _ = self._problems(_with(_spec(), "input.location", "x.csv"))
        assert problems["input.location"].startswith("must be an object with a 'type'")
        problems, _ = self._problems(_with(_spec(), "input.location.path", "../x"))
        assert "relative to the base directory" in problems["input.location.path"]

    def test_cross_field_problems(self):
        problems, _ = self._problems(_spec(output=None))
        assert problems == {"output": "is required for kind 'run'"}
        problems, _ = self._problems(_spec(kind="validate_schema"))
        assert problems == {"schema": "is required for kind 'validate_schema'"}
        sql = {"format": "sql", "location": {"type": "sql", "connection_string": "x"}}
        problems, _ = self._problems(_spec(input=sql))
        assert problems == {"schema": "is required for sql inputs"}
        problems, _ = self._problems(_spec(input=dict(sql, format="csv")))
        assert "format 'sql' reads from a 'sql' location" in problems["input.location.type"]
        presigned = {
            "format": "excel",
            "location": {"type": "presigned_url", "url": "https://h/x"},
        }
        problems, _ = self._problems(_spec(input=presigned))
        assert "stage the excel file" in problems["input.location.type"]
        s3_out = {"location": {"type": "s3", "uri": "s3://b/p/"}}
        problems, _ = self._problems(_spec(kind="preview", output=s3_out))
        assert problems == {
            "output.location.type": "kind 'preview' writes its artifact to a 'file' directory, "
            "not to 's3'"
        }
        upsert = {"type": "sql_table", "connection_string": "x", "table": "t", "mode": "upsert"}
        problems, _ = self._problems(_with(_spec(), "output.location", upsert))
        assert problems == {"output.location.key_columns": "is required for mode 'upsert'"}
        problems, _ = self._problems(
            _with(_spec(), "output.artifacts", {"type": "file", "path": "a"})
        )
        assert problems["output.artifacts"].startswith("only applies to sql_table outputs")

    def test_constraint_messages(self):
        options = {"delimiter": "", "escape_char": "ab", "sheet": 1.5}
        problems, _ = self._problems(_with(_spec(), "input.options", options))
        assert problems == {
            "input.options.delimiter": "must have at least 1 character(s)",
            "input.options.escape_char": "must have at most 1 character(s)",
            "input.options.sheet": "must be a string or an integer, got 1.5",
        }
        problems, _ = self._problems(
            _spec(options={"preview_rows": 20_000}, limits={"max_seconds": -1})
        )
        assert problems == {
            "options.preview_rows": "must be at most 10000, got 20000",
            "limits.max_seconds": "must be greater than 0, got -1",
        }
        problems, _ = self._problems(_spec(spec_version=3, schema=[1], options=[]))
        assert problems == {
            "spec_version": "must be 1, got 3",
            "schema": "must be an object, got a list",
            "options": "must be an object, got a list",
        }
        problems, _ = self._problems(_with(_spec(), "input.options", {"comment_rows": "#"}))
        assert problems == {"input.options.comment_rows": "must be a list, got '#'"}
        problems, _ = self._problems(
            _with(_spec(), "input.options", {"footer_detection": {"patterns": []}})
        )
        assert problems == {
            "input.options.footer_detection.patterns": "must have at least 1 item(s)"
        }

    def test_describe_keeps_values_short(self):
        assert _describe("x" * 100) == repr("x" * 57 + "...")
        assert _describe({"a": 1}) == "an object"
        assert _describe(None) == "null" and _describe(False) == "false"
        assert _describe(object()) == "object"


class TestModels:
    def test_secrets_are_hidden_from_repr(self):
        spec = JobSpec(
            job_id="j",
            kind="run",
            input=InputSpec(format="sql", location=SqlLocation(connection_string="Pwd=hunter2")),
            schema={},
            output=OutputSpec(
                location=SqlTableLocation(connection_string="Pwd=hunter3", table="t")
            ),
        )
        presigned = PresignedUrlLocation(url="https://h/o?X-Amz-Signature=abc")
        assert "hunter" not in repr(spec)
        assert "Signature" not in repr(presigned)

    def test_to_dict_puts_the_version_first_and_leaves_out_unset_options(self):
        data = JobSpec.from_dict(_spec()).to_dict()
        assert list(data)[:2] == ["spec_version", "job_id"]
        assert data["input"] == {
            "format": "csv",
            "location": {"type": "file", "path": "in/people.csv"},
            "options": {},
        }

    def test_python_construction_has_defaults(self):
        spec = JobSpec(
            job_id="j",
            kind="preview",
            input=InputSpec(format="csv", location=FileLocation(path="a.csv")),
        )
        assert spec.spec_version == 1 and spec.options.preview_rows == 100
        assert SPEC_VALIDATOR.is_valid(spec.to_dict())


VALID_RESULTS = [
    {
        "spec_version": 1,
        "job_id": "j",
        "status": "succeeded",
        "counts": {"total_rows": 3},
        "schema_extensions": [],
        "validation_summary": {"UNIQUE_VIOLATION:id": 1},
        "warnings": [],
        "artifacts": [
            {
                "kind": "data",
                "path": "out/data.parquet",
                "rows": 3,
                "bytes": 10,
                "sha256": "a" * 64,
            },
            {"kind": "data", "path": "s3://b/k", "rows": None, "bytes": None, "sha256": None},
        ],
        "error": None,
    },
    {
        "spec_version": 1,
        "job_id": None,
        "status": "failed",
        "counts": {},
        "schema_extensions": [],
        "validation_summary": {},
        "warnings": [],
        "artifacts": [],
        "error": {"code": "SPEC_INVALID", "message": "bad", "retryable": False},
    },
    {
        "spec_version": 1,
        "job_id": "j",
        "status": "cancelled",
        "counts": {},
        "schema_extensions": [],
        "validation_summary": {},
        "warnings": [],
        "artifacts": [],
        "error": {"code": "CANCELLED", "message": "stopped", "retryable": False},
    },
]


def _result(**changes):
    document = copy.deepcopy(VALID_RESULTS[0])
    document.update(changes)
    return document


INVALID_RESULTS = {
    "succeeded with an error": _result(
        error={"code": "INTERNAL", "message": "x", "retryable": False}
    ),
    "failed without an error": _result(status="failed"),
    "cancelled with another code": _result(
        status="cancelled", error={"code": "INTERNAL", "message": "x", "retryable": False}
    ),
    "unknown status": _result(status="done"),
    "unknown code": _result(
        status="failed", error={"code": "OOPS", "message": "x", "retryable": True}
    ),
    "negative count": _result(counts={"total_rows": -1}),
    "count as text": _result(counts={"total_rows": "3"}),
    "unknown artifact kind": _result(
        artifacts=[{"kind": "log", "path": "x", "rows": None, "bytes": None, "sha256": None}]
    ),
    "short sha": _result(
        artifacts=[{"kind": "data", "path": "x", "rows": None, "bytes": None, "sha256": "ab"}]
    ),
    "artifact without bytes": _result(
        artifacts=[{"kind": "data", "path": "x", "rows": None, "sha256": None}]
    ),
    "missing counts": {k: v for k, v in _result().items() if k != "counts"},
}


class TestResults:
    @pytest.mark.parametrize("document", VALID_RESULTS)
    def test_valid_results_are_accepted_by_both(self, document):
        assert RESULT_VALIDATOR.is_valid(document)
        result = JobResult.from_dict(document)
        assert result.to_dict() == document

    @pytest.mark.parametrize("name", sorted(INVALID_RESULTS))
    def test_invalid_results_are_refused_by_both(self, name):
        document = INVALID_RESULTS[name]
        assert not RESULT_VALIDATOR.is_valid(document)
        with pytest.raises(ContractError):
            JobResult.from_dict(document)

    def test_rule_messages(self):
        with pytest.raises(ContractError) as caught:
            JobResult.from_dict(INVALID_RESULTS["cancelled with another code"])
        assert dict(caught.value.problems) == {
            "error.code": "must be 'CANCELLED' when status is 'cancelled'"
        }
        with pytest.raises(ContractError, match="must be null when status is 'succeeded'"):
            JobResult.from_dict(INVALID_RESULTS["failed without an error"])

    def test_result_built_in_python(self):
        result = JobResult(
            job_id="j",
            status="failed",
            artifacts=[Artifact(kind="bad_rows", path="out/bad_rows.parquet", rows=2)],
            error=JobError(code="BAD_ROWS_THRESHOLD_EXCEEDED", message="too many"),
        )
        assert not result.succeeded
        assert RESULT_VALIDATOR.is_valid(result.to_dict())
        assert JobResult(job_id="j", status="succeeded").succeeded
