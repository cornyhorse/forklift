"""calculated_columns.limits: results and operations that would grow without bound."""

from __future__ import annotations

import pytest

from forklift.processors.calculated_columns.limits import (
    MAX_INT_BITS,
    ExpressionError,
    check_result,
    checked_mul,
    checked_pow,
)
from forklift.processors.calculated_columns.safe_eval import compile_expression

HUGE = 1 << (MAX_INT_BITS + 10)


class TestCheckResultIntegers:
    def test_an_integer_far_larger_than_its_inputs_is_rejected(self):
        with pytest.raises(ExpressionError, match=f"exceeds the limit of {MAX_INT_BITS} bits"):
            check_result(HUGE, (2, 3))

    def test_an_integer_as_large_as_an_input_is_accepted(self):
        assert check_result(HUGE + 1, (HUGE,)) == HUGE + 1


class TestCheckedMul:
    def test_number_times_sequence_repeats_it(self):
        assert checked_mul(3, "ab") == "ababab"

    def test_number_times_a_huge_repetition_is_rejected(self):
        with pytest.raises(ExpressionError, match="Sequence repetition would exceed"):
            checked_mul(10**7, "ab")

    def test_two_huge_integers_are_not_multiplied(self):
        with pytest.raises(ExpressionError, match=f"exceeds the limit of {MAX_INT_BITS} bits"):
            checked_mul(HUGE, HUGE)


class TestCheckedPow:
    def test_a_non_numeric_exponent_gives_the_operators_type_error(self):
        with pytest.raises(TypeError):
            checked_pow(2, "x")

    def test_the_interpreter_reports_the_type_error(self):
        with pytest.raises(ExpressionError, match="Operator failed: TypeError"):
            compile_expression("2 ** e").evaluate({"e": "x"}, {}, {})

    def test_a_large_base_with_an_allowed_exponent_is_rejected_before_computing(self):
        with pytest.raises(ExpressionError, match=f"exceeds the limit of {MAX_INT_BITS} bits"):
            checked_pow(1 << 100, 1000)

    def test_a_small_power_is_computed(self):
        assert checked_pow(2, 10) == 1024
