"""Construction-time validation of the transformation configuration dataclasses."""

import pytest

from forklift.utils.transformations.configs import (
    DateTimeTransformConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    StringPaddingConfig,
    StringReplaceConfig,
    resolve_separators,
)


class TestSeparatorTypes:
    @pytest.mark.parametrize(
        ("thousands", "decimal", "label"),
        [(5, None, "thousands_separator"), (None, 0.5, "decimal_separator")],
    )
    def test_non_string_separator_is_rejected(self, thousands, decimal, label):
        with pytest.raises(ValueError, match=f"{label} must be a string, got"):
            resolve_separators(thousands, decimal)

    def test_money_config_rejects_a_numeric_separator(self):
        with pytest.raises(ValueError, match="decimal_separator must be a string, got int"):
            MoneyTypeConfig(decimal_separator=1)

    def test_numeric_config_rejects_a_list_separator(self):
        with pytest.raises(ValueError, match="thousands_separator must be a string, got list"):
            NumericCleaningConfig(thousands_separator=[","])


class TestStringReplaceConfig:
    @pytest.mark.parametrize(("old", "new"), [(1, "x"), ("x", None)])
    def test_old_and_new_must_be_strings(self, old, new):
        with pytest.raises(ValueError, match="'old' and 'new' must be strings"):
            StringReplaceConfig(old=old, new=new)

    @pytest.mark.parametrize("count", [True, "2", 1.5])
    def test_count_must_be_an_integer(self, count):
        with pytest.raises(ValueError, match="'count' must be an integer"):
            StringReplaceConfig(old="a", new="b", count=count)


class TestStringPaddingConfig:
    @pytest.mark.parametrize("width", [False, "5", 2.0])
    def test_width_must_be_an_integer(self, width):
        with pytest.raises(ValueError, match="width must be an integer"):
            StringPaddingConfig(width=width)


class TestDateTimeTimezone:
    def test_blank_timezone_name_is_rejected(self):
        with pytest.raises(ValueError, match="timezone must be a non-empty IANA time zone name"):
            DateTimeTransformConfig(timezone="   ")
