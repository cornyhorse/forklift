"""Calculated columns: NULL arguments, duplicated input columns, fail_on_error=False."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.processors.calculated_columns import (
    CalculatedColumn,
    CalculatedColumnsConfig,
    CalculatedColumnsProcessor,
    ExpressionEvaluator,
    get_available_functions,
)


def duplicated_batch():
    """A batch with two columns called ``a``."""
    return pa.RecordBatch.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"])


class TestFunctionsWithNullOrEdgeArguments:
    def setup_method(self):
        self.functions = get_available_functions()

    def test_substring_with_a_negative_length_is_empty(self):
        assert self.functions["substring"]("hello", 1, -2) == ""

    def test_round_of_null_is_null(self):
        assert self.functions["round"](None) is None

    def test_replace_in_null_is_null(self):
        assert self.functions["replace"](None, "a", "b") is None


class TestDuplicatedInputColumns:
    def test_evaluate_expression_rejects_an_ambiguous_column(self):
        evaluator = ExpressionEvaluator()

        with pytest.raises(ValueError, match="column name 'a' is ambiguous \\(duplicated\\)"):
            evaluator.evaluate_expression(duplicated_batch(), 0, "a + 1")

    def test_calculate_column_values_rejects_an_ambiguous_column(self):
        evaluator = ExpressionEvaluator(fail_on_error=False)
        column = CalculatedColumn(name="b", expression="a + 1", data_type=pa.int64())

        with pytest.raises(ValueError, match="column name 'a' is ambiguous \\(duplicated\\)"):
            evaluator.calculate_column_values(duplicated_batch(), column)


class TestProcessorWithoutFailOnError:
    def test_a_failing_column_is_reported_and_filled_with_nulls(self):
        config = CalculatedColumnsConfig(
            columns=[CalculatedColumn(name="b", expression="a + 1", data_type=pa.int64())],
            fail_on_error=False,
        )
        processor = CalculatedColumnsProcessor(config)

        batch, results = processor.process_batch(duplicated_batch())

        assert batch.schema.names == ["a", "a", "b"]
        assert batch.column(2).to_pylist() == [None]
        assert batch.schema.field("b").type == pa.int64()
        [result] = results
        assert not result.is_valid and result.error_code == "CALCULATION_ERROR"
        assert result.column_name == "b"
        assert result.error_message.startswith("Failed to calculate column 'b': ")
        assert "ambiguous" in result.error_message

    def test_a_result_of_the_wrong_type_is_reported_without_failing(self):
        config = CalculatedColumnsConfig(
            columns=[CalculatedColumn(name="b", expression="'text'", data_type=pa.int64())],
            fail_on_error=False,
        )
        processor = CalculatedColumnsProcessor(config)

        batch, results = processor.process_batch(pa.RecordBatch.from_pydict({"a": [1, 2]}))

        assert batch.column(1).to_pylist() == [None, None]
        assert [r.error_code for r in results] == ["CALCULATION_ERROR"]
        assert "cannot be converted to int64" in results[0].error_message
