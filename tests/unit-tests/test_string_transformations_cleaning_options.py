"""Behaviour of StringTransformer options that the broader transformation suites leave out."""

import pyarrow as pa
import pytest

from forklift.utils.transformations.configs import RegexReplaceConfig, StringCleaningConfig
from forklift.utils.transformations.string_transformations import StringTransformer


def clean(values, **options):
    """Run string cleaning over ``values`` and return the Python list."""
    config = StringCleaningConfig(**options)
    return StringTransformer().apply_string_cleaning(pa.array(values), config).to_pylist()


class TestRegexReplaceErrors:
    def test_pattern_made_invalid_after_construction_raises_value_error(self):
        config = RegexReplaceConfig(pattern="a", replacement="b")
        config.pattern = "[unclosed"  # dataclasses are mutable; the apply step re-checks

        with pytest.raises(ValueError, match="Invalid regex_replace configuration"):
            StringTransformer().apply_regex_replace(pa.array(["abc"]), config)


class TestStringTrimming:
    def test_right_trim_with_characters_only_strips_the_right_side(self):
        result = StringTransformer().apply_string_trimming(
            pa.array(["xxhixx", None]), side="right", chars="x"
        )

        assert result.to_pylist() == ["xxhi", None]


class TestTabHandling:
    def test_tab_becomes_space_when_control_characters_are_kept(self):
        assert clean(["a\tb"], remove_control_chars=False, collapse_whitespace=False) == ["a b"]

    def test_tab_becomes_space_when_tabs_are_preserved(self):
        assert clean(["a\tb"], preserve_tabs=True, collapse_whitespace=False) == ["a b"]

    def test_tab_is_dropped_with_the_other_control_characters_by_default(self):
        assert clean(["a\tb"], collapse_whitespace=False) == ["ab"]


class TestFixCaseIssues:
    def test_acronym_keeps_its_trailing_punctuation(self):
        assert clean(["THE NASA, PROJECT"], fix_case_issues=True) == ["The NASA, Project"]

    def test_acronym_inside_a_hyphenated_first_word_stays_upper_case(self):
        assert clean(["USA-BASED COMPANY"], fix_case_issues=True) == ["USA-based Company"]

    def test_exception_word_inside_a_hyphenated_first_word_is_lower_case(self):
        assert clean(["SALT-AND-PEPPER SHAKER"], fix_case_issues=True) == [
            "Salt-and-pepper Shaker"
        ]

    def test_exception_word_keeps_its_trailing_punctuation(self):
        assert clean(["LORD OF, THE RINGS"], fix_case_issues=True) == ["Lord of, the Rings"]

    def test_hyphenated_later_word_title_cases_only_its_first_part(self):
        result = clean(["THE SALT-AND-PEPPER-USA SHAKER"], fix_case_issues=True)

        assert result == ["The Salt-and-pepper-USA Shaker"]


class TestAsciiOnly:
    def test_dropped_characters_leave_no_doubled_space(self):
        assert clean(["café 中文 test"], ascii_only=True) == ["cafe test"]

    def test_dropped_leading_characters_leave_no_leading_space(self):
        assert clean(["中文 test", "test 中文"], ascii_only=True) == ["test", "test"]

    def test_removed_standalone_accent_leaves_no_doubled_space(self):
        assert clean(["a ́ b"], remove_accents=True, unicode_normalize=None) == ["a b"]

    def test_without_whitespace_options_the_gaps_are_kept(self):
        result = clean(["中文 test"], ascii_only=True, strip_whitespace=False)

        assert result == [" test"]
