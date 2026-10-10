"""_regex: invalid patterns and the nested-quantifier check inside assertions/conditionals."""

from __future__ import annotations

import pytest

from forklift.processors._regex import UnsafeRegexError, compile_pattern, pattern_matches


class TestInvalidPatterns:
    def test_a_repeat_count_too_large_names_the_problem(self):
        with pytest.raises(ValueError) as error:
            compile_pattern("a{99999999999}")

        assert str(error.value) == (
            "Invalid regular expression: the repetition number is too large"
        )
        assert not isinstance(error.value, UnsafeRegexError)

    def test_a_pattern_nested_too_deeply(self):
        with pytest.raises(ValueError, match="pattern is too deeply nested"):
            compile_pattern("(" * 990 + ")" * 990)

    def test_a_pattern_that_parses_but_does_not_compile(self):
        with pytest.raises(ValueError, match="look-behind requires fixed-width pattern"):
            compile_pattern("(?<=a+)b")


class TestNestedQuantifiersInAssertions:
    def test_inside_a_lookahead(self):
        with pytest.raises(UnsafeRegexError):
            compile_pattern("(?=(a+)+)x")

    def test_a_plain_lookahead_is_safe(self):
        compiled = compile_pattern("(?!x)y")

        assert pattern_matches(compiled, "zy") and not pattern_matches(compiled, "x")


class TestNestedQuantifiersInConditionals:
    def test_inside_the_yes_branch(self):
        with pytest.raises(UnsafeRegexError):
            compile_pattern("(a)?(?(1)(b+)+|c)")

    def test_a_conditional_without_a_no_branch_is_safe(self):
        compiled = compile_pattern("^(a)?(?(1)b)$")

        assert pattern_matches(compiled, "ab") and pattern_matches(compiled, "")
        assert not pattern_matches(compiled, "a")

    def test_a_conditional_with_both_branches_is_safe(self):
        compiled = compile_pattern("^(a)?(?(1)b|c)$")

        assert pattern_matches(compiled, "ab") and pattern_matches(compiled, "c")
        assert not pattern_matches(compiled, "b")

    def test_unsafe_patterns_are_accepted_on_request(self):
        compiled = compile_pattern("(?=(a+)+)a", allow_unsafe_regex=True)

        assert pattern_matches(compiled, "aaa")
