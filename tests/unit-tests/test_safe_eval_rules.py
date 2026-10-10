"""safe_eval: what the expression compiler rejects and how the interpreter evaluates."""

from __future__ import annotations

import ast

import pytest

from forklift.processors.calculated_columns.limits import MAX_CALL_ARGUMENTS, ExpressionError
from forklift.processors.calculated_columns.safe_eval import (
    CompiledExpression,
    compile_expression,
    syntax_hints,
    unknown_name_message,
)


def compile_error(expression, **limits):
    with pytest.raises(ExpressionError) as error:
        compile_expression(expression, **limits)
    return str(error.value)


def evaluate(expression, variables=None, functions=None, constants=None):
    return compile_expression(expression).evaluate(
        variables or {}, functions or {}, constants or {}
    )


class TestCaseWhenWithoutARewrite:
    """A CASE expression that cannot be rewritten still gets the general advice."""

    @pytest.mark.parametrize(
        "expression",
        [
            "x + CASE WHEN a > 1 THEN 1 END",  # not a whole-expression CASE ... END
            "CASE WHEN a > 1 END",  # a branch without THEN
            "CASE WHEN a > 1 THEN 1 ELSE 2 WHEN b THEN 3 END",  # ELSE before the last WHEN
        ],
    )
    def test_hint_without_an_equivalent_conditional(self, expression):
        hints = syntax_hints(expression)

        assert "is not supported" in hints[0] and "if_then_else" in hints[0]
        assert "For this expression" not in hints[0]


class TestSyntaxHints:
    def test_not_equal_hint_when_python_reports_another_problem(self):
        text = compile_error("a <> b)")

        assert "unmatched ')'" in text and "write '!=' instead of '<>'" in text


class TestUnknownNameMessage:
    def test_columns_without_a_close_match_are_listed(self):
        text = unknown_name_message("total", ["zzz_other"])

        assert "Did you mean" not in text
        assert "Columns available here: 'zzz_other'." in text

    def test_an_empty_column_list_lists_nothing(self):
        text = unknown_name_message("total", [])

        assert text.startswith("Unknown name 'total': it is neither a column")
        assert "Did you mean" not in text and "Columns available" not in text
        assert text.endswith("are called by their new name in expressions.")


class TestCompilerRejects:
    def test_a_non_string_expression(self):
        assert compile_error(123) == "Expression must be a string"

    def test_an_expression_the_parser_cannot_hold(self):
        text = compile_error("-" * 20000 + "1", max_length=30000, max_nodes=50000)

        assert text == "Expression is not valid: MemoryError"

    def test_a_call_with_too_many_arguments(self):
        expression = "f(" + ", ".join(["1"] * (MAX_CALL_ARGUMENTS + 1)) + ")"

        assert compile_error(expression) == (
            f"A call may have at most {MAX_CALL_ARGUMENTS} arguments"
        )

    def test_keyword_argument_unpacking(self):
        assert "Keyword-argument unpacking ('**')" in compile_error("f(**options)")

    def test_a_double_underscore_function(self):
        assert compile_error("__import__('os')") == (
            "Names starting with double underscores are not allowed"
        )

    def test_a_double_underscore_keyword(self):
        assert "double underscores" in compile_error("f(__class__=1)")

    def test_plain_keyword_arguments_are_accepted(self):
        compiled = compile_expression("f(a, key=1, other=2)")

        assert compiled.function_names == ("f",) and "a" in compiled.names


class TestInterpreter:
    def test_a_list_display_on_the_right_of_in(self):
        assert evaluate("a in [1, 2]", {"a": 2}) is True
        assert evaluate("a not in [1, 2]", {"a": 2}) is False

    def test_in_null_is_null(self):
        assert evaluate("a in NULL", {"a": 1}, constants={"NULL": None}) is None

    def test_unary_minus_on_a_string_fails_cleanly(self):
        with pytest.raises(ExpressionError, match="Operator failed: TypeError"):
            evaluate("-a", {"a": "text"})

    def test_a_function_raising_an_arithmetic_error(self):
        with pytest.raises(ExpressionError) as error:
            evaluate("f()", functions={"f": lambda: 1 / 0})

        assert str(error.value) == "Function 'f' failed: ZeroDivisionError: division by zero"

    def test_a_function_called_with_the_wrong_arguments(self):
        with pytest.raises(ExpressionError, match="Function 'f' failed: TypeError"):
            evaluate("f(1, 2)", functions={"f": lambda x: x})

    def test_deep_nesting_is_reported_not_crashing(self):
        compiled = compile_expression("-" * 2000 + "1", max_nodes=5000)

        with pytest.raises(ExpressionError, match="Expression is nested too deeply"):
            compiled.evaluate({}, {}, {})

    def test_a_tree_that_was_not_compiled_cannot_use_other_syntax(self):
        tree = ast.parse("a.real", mode="eval")
        compiled = CompiledExpression("a.real", tree, ("a",), ())

        with pytest.raises(ExpressionError) as error:
            compiled.evaluate({"a": 1}, {}, {})

        assert str(error.value) == "Expression uses unsupported syntax: attribute access ('.')"
