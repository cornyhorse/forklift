"""SchemaValidator: coercion outcomes, batch structure checks and schema definition parsing."""

from __future__ import annotations

import re
from datetime import datetime

import pyarrow as pa
import pytest

from forklift.processors.schema_validator import (
    SchemaValidationMode,
    SchemaValidator,
    SchemaValidatorConfig,
)
from forklift.processors.schema_validator.constraints import ConstraintValidator
from forklift.processors.schema_validator.type_converter import TypeConverter, parse_arrow_type


def codes(results):
    return [r.error_code for r in results if not r.is_valid]


def coercing(columns, **config):
    return SchemaValidator(
        {"columns": columns},
        config=SchemaValidatorConfig(
            allow_type_coercion=True, validation_mode=SchemaValidationMode.PERMISSIVE, **config
        ),
    )


class TestCoercion:
    def test_only_mismatching_schema_columns_are_cast(self):
        validator = coercing(
            [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}],
            extra_columns_allowed=True,
        )
        batch = pa.RecordBatch.from_pydict({"a": [1], "b": [2], "extra": [3]})

        out, results = validator.process_batch(batch)

        assert out.schema.types == [pa.int64(), pa.string(), pa.int64()]
        assert out.column("b").to_pylist() == ["2"] and codes(results) == []

    def test_a_cast_arrow_does_not_implement_is_a_type_mismatch(self):
        validator = coercing([{"name": "t", "type": "time32[s]"}])

        out, results = validator.process_batch(pa.RecordBatch.from_pydict({"t": ["10:00:00"]}))

        assert out.schema.field("t").type == pa.string()
        [result] = [r for r in results if not r.is_valid]
        assert result.error_code == "TYPE_MISMATCH_NO_COERCION" and result.column_name == "t"
        assert result.error_message == (
            "Column 't' type mismatch: expected time32[s], got string, coercion not possible"
        )

    def test_values_that_fail_are_nulled_and_nulls_stay_null(self):
        validator = coercing([{"name": "n", "type": "int64"}])

        out, results = validator.process_batch(pa.RecordBatch.from_pydict({"n": ["1", None, "x"]}))

        assert out.column("n").to_pylist() == [1, None, None]
        assert [(r.error_code, r.row_index) for r in results if not r.is_valid] == [
            ("COERCION_FAILED", 2)
        ]


class TestBatchStructure:
    def test_a_missing_batch_is_reported_not_crashing(self):
        validator = SchemaValidator({"columns": [{"name": "a", "type": "int64"}]})

        out, results = validator.process_batch(None)

        assert out is None and codes(results) == ["NULL_BATCH"]

    def test_an_empty_batch_with_a_minimum_row_count(self):
        validator = SchemaValidator(
            {"columns": [{"name": "a", "type": "int64"}]},
            config=SchemaValidatorConfig(min_row_count=1),
        )
        empty = pa.RecordBatch.from_pydict({"a": pa.array([], pa.int64())})

        _, results = validator.process_batch(empty)

        assert codes(results) == ["EMPTY_BATCH", "MIN_ROW_COUNT_VIOLATION"]

    def test_columns_in_the_expected_order_pass_the_order_check(self):
        validator = SchemaValidator(
            {"columns": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}]},
            config=SchemaValidatorConfig(check_column_order=True),
        )

        _, results = validator.process_batch(pa.RecordBatch.from_pydict({"a": [1], "b": [2]}))

        assert codes(results) == []

    def test_a_null_share_below_the_threshold_passes(self):
        validator = SchemaValidator(
            {"columns": [{"name": "a", "type": "int64"}]},
            config=SchemaValidatorConfig(max_null_percentage=50.0),
        )

        _, results = validator.process_batch(pa.RecordBatch.from_pydict({"a": [1, None, 3, 4]}))

        assert codes(results) == []


class TestSchemaDefinition:
    def test_entries_that_are_not_objects_are_skipped(self):
        validator = SchemaValidator({"columns": ["junk", {"name": "a", "type": "int64"}]})

        assert list(validator.expected_columns) == ["a"]
        assert validator.schema == pa.schema([pa.field("a", pa.int64())])

    def test_null_constraints_become_an_empty_dict(self):
        validator = SchemaValidator(
            {"columns": [{"name": "a", "type": "int64", "constraints": None}]}
        )

        assert validator.expected_columns["a"].constraints == {}


class TestTypeConverter:
    def test_a_non_string_type_is_rejected(self):
        with pytest.raises(ValueError, match="^Data type must be a string, got int$"):
            parse_arrow_type(5)

    @pytest.mark.parametrize(
        "text, expected",
        [("time32[s]", pa.time32("s")), ("TIME64[ns]", pa.time64("ns"))],
    )
    def test_time_types(self, text, expected):
        assert parse_arrow_type(text) == expected

    @pytest.mark.parametrize("text", ["time32[us]", "time64[s]"])
    def test_a_time_unit_the_width_does_not_support(self, text):
        with pytest.raises(
            ValueError, match=re.escape(f"Invalid time unit in data type '{text}'")
        ):
            parse_arrow_type(text)

    def test_a_definition_without_columns_has_no_schema(self):
        assert TypeConverter.convert_dict_to_arrow_schema({}) is None
        assert TypeConverter.convert_dict_to_arrow_schema({"columns": ["junk"]}) is None

    def test_an_unknown_target_type_cannot_be_coerced_to(self):
        assert TypeConverter.can_coerce_type(pa.string(), "no_such_type") is False


class TestConstraintHelpers:
    def test_a_range_without_bounds_checks_nothing(self):
        column = pa.array([1, 2])

        assert ConstraintValidator.validate_range_constraints(column, "a", {"min": None}) == []

    def test_an_aware_bound_against_naive_timestamps_is_a_configuration_error(self):
        column = pa.array([datetime(2024, 1, 1)], pa.timestamp("us"))

        with pytest.raises(ValueError, match="cannot be compared with its values"):
            ConstraintValidator.validate_range_constraints(
                column, "ts", {"min": "2024-01-01T00:00:00+00:00"}
            )

    def test_unhashable_allowed_values_are_matched_by_equality(self):
        column = pa.array([[1, 2], [3]])

        results = ConstraintValidator.validate_enum_constraints(column, "pair", [[1, 2], "x"])

        assert [(r.error_code, r.row_index) for r in results] == [("ENUM_VIOLATION", 1)]
