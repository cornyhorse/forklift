"""data_validation: uniqueness strategies, unhashable keys, range bounds and the bad rows store."""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pytest

from forklift.processors.data_validation import (
    BadRowsConfig,
    BadRowsHandler,
    DataValidationProcessor,
    FieldValidationRule,
    RangeValidation,
    ValidationConfig,
    ValidationRules,
)


def processor(*rules, strategy="first_wins"):
    return DataValidationProcessor(
        ValidationConfig(
            field_validations=list(rules),
            bad_rows_config=BadRowsConfig(fail_on_exceed_threshold=False),
            uniqueness_strategy=strategy,
        )
    )


class TestUniqueness:
    def test_unhashable_values_are_compared_by_their_representation(self):
        validator = processor(FieldValidationRule(field_name="tags", unique=True))
        batch = pa.RecordBatch.from_pydict({"tags": [["a", "b"], ["c"], ["a", "b"]]})

        clean, results = validator.process_batch(batch)

        assert clean.column("tags").to_pylist() == [["a", "b"], ["c"]]
        assert [(r.row_index, r.column_name) for r in results] == [(2, "tags")]

    def test_validate_row_leaves_batch_level_strategies_to_process_batch(self):
        validator = processor(
            FieldValidationRule(field_name="id", unique=True), strategy="last_wins"
        )
        batch = pa.RecordBatch.from_pydict({"id": [1, 1]})

        assert validator._validate_row(batch, 0) == (True, [])
        assert validator._validate_row(batch, 1) == (True, [])
        assert validator.unique_value_tracker == {"id": set()}

    def test_a_unique_field_missing_from_the_batch_checks_nothing(self):
        validator = processor(
            FieldValidationRule(field_name="id", unique=True), strategy="last_wins"
        )
        batch = pa.RecordBatch.from_pydict({"name": ["a", "a"]})

        clean, results = validator.process_batch(batch)

        assert clean.num_rows == 2 and results == []


class TestRangeBounds:
    def test_a_bound_that_is_not_a_number_is_a_configuration_error(self):
        with pytest.raises(ValueError, match="Range bounds must be numbers or ISO dates"):
            ValidationRules.validate_range("age", 5, RangeValidation(min_value="abc"))

    def test_a_number_bound_for_a_date_value_is_a_configuration_error(self):
        with pytest.raises(ValueError, match="Range bound '5' is not a date"):
            ValidationRules.validate_range("day", date(2024, 1, 1), RangeValidation(min_value=5))

    @pytest.mark.parametrize("bound", [float("nan"), "NaN"])
    def test_a_nan_bound_is_rejected(self, bound):
        with pytest.raises(ValueError, match="Range bounds cannot be NaN"):
            ValidationRules.check_range_config(RangeValidation(max_value=bound))


class TestBadRowsStore:
    def test_a_column_that_is_null_in_every_bad_row_becomes_a_string_column(self):
        handler = BadRowsHandler(BadRowsConfig(include_validation_errors=False))
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2], "note": pa.array([None, None], pa.int64())}
        )

        handler.add_bad_row(batch, 0, ["bad"])
        handler.add_bad_row(batch, 1, ["bad"])
        rows = handler.get_bad_rows_batch()

        assert rows.schema.field("note").type == pa.string()
        assert rows.column("note").to_pylist() == [None, None]
        assert rows.column("id").to_pylist() == [1, 2]

    def test_clear_bad_rows_forgets_rows_and_count(self):
        handler = BadRowsHandler(BadRowsConfig())
        handler.add_bad_row(pa.RecordBatch.from_pydict({"id": [1]}), 0, ["bad"])

        handler.clear_bad_rows()

        assert handler.bad_rows == [] and handler.get_bad_rows_count() == 0
        assert handler.get_bad_rows_batch() is None
