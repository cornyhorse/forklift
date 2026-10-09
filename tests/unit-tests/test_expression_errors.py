"""Calculated-column expressions: errors say what to write instead."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.processors.calculated_columns.limits import ExpressionError
from forklift.processors.calculated_columns.safe_eval import (
    compile_expression,
    syntax_hints,
    unknown_function_message,
    unknown_name_message,
)
from forklift.processors.calculated_columns_factory import (
    create_calculated_columns_processor_from_schema,
)


def message(expression):
    with pytest.raises(ExpressionError) as error:
        compile_expression(expression)
    return str(error.value)


class TestCaseWhen:
    def test_the_old_standards_expression_gets_the_equivalent_conditional(self):
        text = message(
            "CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END"
        )

        assert "'CASE WHEN" in text and "is not supported" in text
        assert "'minor' if age < 18 else ('adult' if age < 65 else 'senior')" in text
        assert "if_then_else" in text and "X_CALCULATED_COLUMNS_DOCUMENTATION" in text

    def test_a_single_branch(self):
        text = message("case when salary > 10 then 'high' else 'low' end")

        assert "'high' if salary > 10 else 'low'" in text

    def test_missing_else_gives_none(self):
        assert "'x' if a > 1 else None" in message("CASE WHEN a > 1 THEN 'x' END")

    def test_sql_operators_inside_the_rewrite_are_converted_outside_strings_only(self):
        text = message("CASE WHEN a = 1 AND b IS NOT NULL THEN 'a = b AND c' ELSE 'z' END")

        assert "'a = b AND c' if a == 1 and b is not None else 'z'" in text

    def test_the_value_form_is_reported_without_a_rewrite(self):
        text = message("CASE status WHEN 'A' THEN 1 ELSE 0 END")

        assert "is not supported" in text and "For this expression" not in text


class TestOtherSqlHabits:
    @pytest.mark.parametrize(
        "expression, fragment",
        [
            ("age = 18", "write '==' to compare"),
            ("age IS NULL", "isnull(x)"),
            ("age IS NOT NULL", "x is not None"),
            ("a > 1 AND b < 2", "lowercase"),
            ("a OR b", "lowercase"),
            ("name || '!'", "concat(a, b, ...)"),
        ],
    )
    def test_hint(self, expression, fragment):
        assert fragment in message(expression)

    def test_python_already_explains_not_equal(self):
        text = message("age <> 18")

        assert "!=" in text and text.count("!=") == 1

    def test_an_unrecognised_mistake_gets_the_syntax_summary(self):
        text = message("a b")

        assert "Python-like syntax" in text and "coalesce(a, b)" in text

    def test_strings_are_not_mistaken_for_sql(self):
        assert syntax_hints("'CASE WHEN a = b AND c' + x") == []

    def test_valid_expressions_still_compile(self):
        compile_expression("'minor' if age < 18 else ('adult' if age < 65 else 'senior')")
        compile_expression("a == 1 and b is not None")


class TestNamesAndFunctions:
    FUNCTIONS = ["upper", "lower", "coalesce", "if_then_else", "length"]

    def test_unknown_function_with_a_close_match(self):
        text = unknown_function_message("coalesc", self.FUNCTIONS)

        assert "Unknown function 'coalesc'" in text and "Did you mean coalesce(...)?" in text
        assert "Available functions: coalesce, if_then_else, length, lower, upper" in text

    def test_unknown_function_that_differs_only_in_case(self):
        assert "Did you mean upper(...)?" in unknown_function_message("UPPER", self.FUNCTIONS)

    def test_unknown_function_without_a_match_still_lists_the_functions(self):
        text = unknown_function_message("years_from_timestamp", self.FUNCTIONS)

        assert "Did you mean" not in text and "Available functions" in text

    def test_unknown_name_with_columns(self):
        text = unknown_name_message("agee", ["age", "name", "salary"])

        assert "Did you mean 'age'?" in text
        assert "Columns available here: 'age', 'name', 'salary'" in text
        assert "x-columnMapping" in text

    def test_unknown_name_without_a_column_list(self):
        text = unknown_name_message("agee")

        assert "neither a column of the data nor a constant" in text
        assert "Columns available" not in text

    def test_the_processor_reports_the_unknown_function_with_help(self):
        processor = create_calculated_columns_processor_from_schema(
            {"expressions": [{"name": "c", "expression": "UPPER(name)", "dataType": "string"}]}
        )

        with pytest.raises(ValueError, match="Did you mean upper\\(\\.\\.\\.\\)"):
            processor.process_batch(pa.RecordBatch.from_pydict({"name": ["a"]}))


class TestResultTypeMismatch:
    def test_the_message_names_the_types_produced_never_the_values(self):
        processor = create_calculated_columns_processor_from_schema(
            {"expressions": [{"name": "c", "expression": "isnull(name)", "dataType": "string"}]}
        )

        with pytest.raises(ValueError) as error:
            processor.process_batch(pa.RecordBatch.from_pydict({"name": ["SECRET"]}))

        text = str(error.value)
        assert "produced bool values" in text and "dataType" in text and "SECRET" not in text
