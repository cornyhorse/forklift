"""Whitespace and separator handling of ``transformations.numeric_transformations``."""

import pyarrow as pa
import pytest

from forklift.utils.transformations.configs import MoneyTypeConfig, NumericCleaningConfig
from forklift.utils.transformations.numeric_transformations import NumericTransformer


class TestMoneyWhitespace:
    def test_padded_parenthesised_amount_is_negative_after_stripping(self):
        result = NumericTransformer().apply_money_conversion(
            pa.array([" ($5.00) ", " 7 "]), MoneyTypeConfig()
        )

        assert result.to_pylist() == [-5.0, 7.0]

    def test_padding_hides_the_parentheses_when_stripping_is_off(self):
        result = NumericTransformer().apply_money_conversion(
            pa.array([" ($5.00) ", " 7 "]), MoneyTypeConfig(strip_whitespace=False)
        )

        # "(5.00)" is not a number; the plain amount still parses
        assert result.to_pylist() == [None, 7.0]

    def test_amount_beyond_float_range_is_null(self):
        result = NumericTransformer().apply_money_conversion(
            pa.array(["$1e400", "$12.50"]), MoneyTypeConfig()
        )

        assert result.to_pylist() == [None, 12.5]


class TestNumericCleaning:
    def test_padded_number_parses_when_stripping_is_off(self):
        config = NumericCleaningConfig(strip_whitespace=False, allow_nan=False)

        result = NumericTransformer().apply_numeric_cleaning(pa.array([" 12 "]), config)

        assert result.to_pylist() == [12.0]

    def test_period_is_not_a_number_with_a_comma_decimal_separator(self):
        config = NumericCleaningConfig(decimal_separator=",", thousands_separator="")

        result = NumericTransformer().apply_numeric_cleaning(pa.array(["1.5", "2,5"]), config)

        assert result.to_pylist() == [None, 2.5]

    def test_malformed_number_names_the_row_when_nan_is_not_allowed(self):
        config = NumericCleaningConfig(
            decimal_separator=",", thousands_separator="", allow_nan=False
        )

        with pytest.raises(ValueError, match="^Cannot convert value at row 1 to double$"):
            NumericTransformer().apply_numeric_cleaning(pa.array(["2,5", "1.5"]), config)
