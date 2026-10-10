"""Tests for ExtensionPipeline stages and build_extension_pipeline's input checks."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.forklift_core import import_csv
from forklift.engine.processors.extensions import (
    ExtensionPipeline,
    build_extension_pipeline,
)
from forklift.processors.base import ValidationResult


def _import(tmp_path, csv_text, schema):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema))
    csv_path = tmp_path / "in.csv"
    csv_path.write_text(csv_text)
    results = import_csv(csv_path, tmp_path / "out", schema_file=schema_file)
    return results, pq.read_table(tmp_path / "out" / "data.parquet")


class TestRecord:
    def test_only_failed_results_are_counted(self):
        pipeline = ExtensionPipeline()

        pipeline.record(
            [
                ValidationResult(is_valid=True, error_code="RANGE", column_name="x"),
                ValidationResult(is_valid=False, error_code="RANGE", column_name="x"),
                ValidationResult(is_valid=False),
            ]
        )

        assert pipeline.summary == {"RANGE:x": 1, "VALIDATION_ERROR": 1}


class _DropRows:
    """A validation stage that drops rows 1 and 2 and explains only row 1."""

    def process_batch(self, batch):
        keep = pa.array([i not in (1, 2) for i in range(batch.num_rows)])
        results = [
            ValidationResult(is_valid=True, row_index=0),
            ValidationResult(is_valid=False, error_code="RANGE", column_name="x", row_index=1),
            ValidationResult(is_valid=False, error_code="LOST", row_index=None),
            ValidationResult(is_valid=False, error_code="FAR", row_index=99),
        ]
        return batch.filter(keep), results


class TestRejectionReasons:
    def test_rows_without_a_matching_result_are_blamed_on_the_stage(self):
        pipeline = ExtensionPipeline(validator=_DropRows())
        batch = pa.RecordBatch.from_pydict({"x": [10, -1, -2, 30]})

        stage = pipeline.post_convert(batch)

        assert stage.kept.to_pydict() == {"x": [10, 30]}
        assert stage.rejected.to_pydict() == {"x": [-1, -2]}
        assert stage.reasons == ["RANGE:x", "X-VALIDATION"]
        assert pipeline.summary == {"RANGE:x": 1, "LOST": 1, "FAR": 1}


class TestRowHashColumns:
    def test_row_hash_without_input_hash_or_row_numbers(self, tmp_path):
        results, table = _import(
            tmp_path,
            "a,b\n1,2\n3,4\n",
            {"properties": {"a": {}, "b": {}}, "x-rowHash": {"enabled": True}},
        )

        assert results.schema_extensions == ["x-rowHash"]
        assert table.column_names == ["a", "b", "row_hash"]
        hashes = table.column("row_hash").to_pylist()
        assert len(set(hashes)) == 2 and all(len(h) == 64 for h in hashes)

    def test_header_only_input_gets_every_configured_row_hash_column(self, tmp_path):
        row_hash = {"enabled": True, "inputHashEnabled": True, "rowNumberEnabled": True}
        results, table = _import(
            tmp_path, "a,b\n", {"properties": {"a": {}, "b": {}}, "x-rowHash": row_hash}
        )

        assert results.total_rows == 0
        assert table.num_rows == 0
        assert table.column_names == [
            "a",
            "b",
            "row_hash",
            "_input_hash",
            "_rownum_in_source_file",
            "_rownum",
        ]
        assert table.schema.field("_rownum").type == pa.int64()


class TestCalculatedColumnsConfig:
    @pytest.mark.parametrize("config, kind", [(["total"], "list"), ("a + b", "str")])
    def test_value_that_is_not_an_object_is_a_configuration_error(self, config, kind):
        schema = {"properties": {"a": {}}, "x-calculatedColumns": config}

        with pytest.raises(
            ValueError, match=f"x-calculatedColumns: must be an object, got {kind}"
        ):
            build_extension_pipeline(schema, ["a"], log=lambda message: None)


class TestRulesForAbsentColumns:
    def test_quality_rules_for_declared_columns_the_input_lacks_are_left_out(self):
        logged = []
        schema = {
            "properties": {"a": {}, "gone": {"type": "integer"}},
            "x-dataQuality": {"fieldSpecificRules": {"gone": {"min": 1}, "a": {"min": 0}}},
        }

        pipeline = build_extension_pipeline(schema, ["a"], log=logged.append)

        assert pipeline.applied == ["x-dataQuality"]
        assert logged == [
            "x-dataQuality rules for column(s) 'gone' are not checked: "
            "the columns are not in the input"
        ]
        assert schema["x-dataQuality"]["fieldSpecificRules"] == {
            "gone": {"min": 1},
            "a": {"min": 0},
        }  # the caller's schema is not modified
        _, results = pipeline.quality.process_batch(pa.RecordBatch.from_pydict({"a": [-1, 2]}))
        assert {r.column_name for r in results if not r.is_valid} == {"a"}

    def test_property_constraints_for_columns_the_input_lacks_are_left_out(self, tmp_path):
        results, table = _import(
            tmp_path,
            "a\n5\n50\n",
            {
                "properties": {
                    "a": {"type": "integer", "maximum": 10},
                    "gone": {"type": "integer", "minimum": 0},
                }
            },
        )

        assert results.warnings == [
            "properties rules for column(s) 'gone' are not checked: "
            "the columns are not in the input"
        ]
        assert table.column("a").to_pylist() == [5]
        assert results.invalid_rows == 1
