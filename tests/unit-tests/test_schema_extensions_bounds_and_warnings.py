"""schema_extensions: range bounds, referenced columns and warnings for inert content."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from forklift.processors.schema_extensions import (
    build_data_validator,
    referenced_columns,
    unsupported_extension_keys,
)


def range_schema(**bounds):
    return {"x-validation": {"fieldValidations": {"a": {"range": bounds}}}}


class TestRangeBounds:
    def test_a_date_object_bound_checks_dates(self):
        validator = build_data_validator(range_schema(min=date(2024, 1, 1)))
        batch = pa.RecordBatch.from_pydict({"a": [date(2023, 12, 31), date(2024, 1, 2)]})

        clean, results = validator.process_batch(batch)

        assert clean.column("a").to_pylist() == [date(2024, 1, 2)]
        assert [r.row_index for r in results] == [0]

    def test_a_numeric_string_bound_checks_numbers(self):
        validator = build_data_validator(range_schema(max="10.5"))

        clean, _ = validator.process_batch(pa.RecordBatch.from_pydict({"a": [10.5, 11.0]}))

        assert clean.column("a").to_pylist() == [10.5]

    @pytest.mark.parametrize("text", ["NaN", "Infinity", "-inf"])
    def test_a_non_finite_numeric_string_is_rejected(self, text):
        with pytest.raises(ValueError) as error:
            build_data_validator(range_schema(min=text))

        assert str(error.value) == (
            "x-validation.fieldValidations.a.range.min: must be a finite number"
        )

    def test_a_decimal_nan_bound_is_rejected(self):
        with pytest.raises(ValueError) as error:
            build_data_validator(range_schema(min=Decimal("NaN")))

        assert str(error.value) == (
            "x-validation.fieldValidations.a.range: Range bounds cannot be NaN"
        )

    def test_an_aware_and_a_naive_date_time_cannot_be_ordered(self):
        with pytest.raises(ValueError) as error:
            build_data_validator(
                range_schema(min="2024-01-01T00:00:00+00:00", max="2024-12-31T00:00:00")
            )

        assert str(error.value) == (
            "x-validation.fieldValidations.a.range: min and max cannot be compared"
        )


class TestReferencedColumns:
    def test_rules_that_check_nothing_refer_to_no_column(self):
        schema = {
            "x-validation": {"fieldValidations": {"a": {}, "b": {"required": False}}},
            "x-dataQuality": {"fieldSpecificRules": {"c": {}}},
            "properties": {"d": "string", "e": {"type": "string"}},
        }

        assert referenced_columns(schema) == {}

    def test_rules_that_check_something_are_listed(self):
        schema = {
            "x-validation": {"fieldValidations": {"a": {"required": True}}},
            "x-dataQuality": {"fieldSpecificRules": {"c": {"min": 1}}},
            "properties": {"d": "string", "e": {"maximum": 3}},
        }

        assert referenced_columns(schema) == {
            "x-validation": ["a"],
            "x-dataQuality": ["c"],
            "properties": ["e"],
        }


class TestUnsupportedKeyWarnings:
    def test_items_that_are_not_objects_are_skipped(self):
        schema = {
            "x-uniqueConstraints": ["id", {"columns": ["id"], "condition": "x > 1"}],
            "x-dataQuality": {"fieldSpecificRules": {"a": "text", "b": {"color": "red"}}},
        }

        assert unsupported_extension_keys(schema) == [
            "x-uniqueConstraints[1].condition is not supported and is ignored "
            "(the constraint applies to every row)",
            "x-dataQuality.fieldSpecificRules.b.color is not supported and is ignored",
        ]

    def test_descriptions_and_empty_values_of_a_field_rule_are_not_reported(self):
        schema = {
            "x-validation": {
                "fieldValidations": {
                    "a": {"description": "the id", "range": None, "notes": [], "colour": "red"}
                }
            }
        }

        assert unsupported_extension_keys(schema) == [
            "x-validation.fieldValidations.a.colour is not supported and is ignored"
        ]
