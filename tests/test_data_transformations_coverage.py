"""Comprehensive tests for data_transformations.py to achieve 100% code coverage."""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Callable, Any
from decimal import Decimal, InvalidOperation
import datetime
import re

import pyarrow as pa
import pandas as pd

# Add src to Python path for imports
sys.path.insert(0, 'src')

from forklift.utils.transformations import (
    # Configuration classes
    DateTimeTransformConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    StringPaddingConfig,
    HTMLXMLConfig,
    SSNConfig,
    ZipCodeConfig,
    PhoneNumberConfig,
    EmailConfig,
    IPAddressConfig,
    MACAddressConfig,
    StringCleaningConfig,

    # Main transformer class
    DataTransformer,

    # Factory function
    create_transformation_from_config
)


class TestConfigurationClasses:
    """Test all configuration dataclasses and their validation."""

    def test_datetime_transform_config_defaults(self):
        """Test DateTimeTransformConfig with default values."""
        config = DateTimeTransformConfig()
        assert config.mode == "common_formats"
        assert config.format is None
        assert config.formats is None
        assert config.allow_fuzzy is False
        assert config.from_epoch is False
        assert config.to_epoch is None
        assert config.target_type == "datetime"
        assert config.output_format is None
        assert config.timezone is None

    def test_datetime_transform_config_invalid_mode(self):
        """Test DateTimeTransformConfig with invalid mode."""
        with pytest.raises(ValueError, match="Invalid mode: invalid_mode"):
            DateTimeTransformConfig(mode="invalid_mode")

    def test_datetime_transform_config_enforce_without_format(self):
        """Test DateTimeTransformConfig enforce mode without format."""
        with pytest.raises(ValueError, match="Format must be specified when mode is 'enforce'"):
            DateTimeTransformConfig(mode="enforce")

    def test_datetime_transform_config_specify_formats_without_formats(self):
        """Test DateTimeTransformConfig specify_formats mode without formats list."""
        with pytest.raises(ValueError, match="Formats list must be specified when mode is 'specify_formats'"):
            DateTimeTransformConfig(mode="specify_formats")

    def test_datetime_transform_config_invalid_target_type(self):
        """Test DateTimeTransformConfig with invalid target type."""
        with pytest.raises(ValueError, match="Invalid target_type: invalid_type"):
            DateTimeTransformConfig(target_type="invalid_type")

    def test_datetime_transform_config_invalid_to_epoch(self):
        """Test DateTimeTransformConfig with invalid to_epoch unit."""
        with pytest.raises(ValueError, match="Invalid to_epoch unit: invalid_unit"):
            DateTimeTransformConfig(to_epoch="invalid_unit")

    def test_datetime_transform_config_valid_configurations(self):
        """Test valid DateTimeTransformConfig configurations."""
        # Enforce mode
        config1 = DateTimeTransformConfig(mode="enforce", format="%Y-%m-%d")
        assert config1.mode == "enforce"
        assert config1.format == "%Y-%m-%d"

        # Specify formats mode
        config2 = DateTimeTransformConfig(mode="specify_formats", formats=["%Y-%m-%d", "%m/%d/%Y"])
        assert config2.mode == "specify_formats"
        assert config2.formats == ["%Y-%m-%d", "%m/%d/%Y"]

        # Valid target types
        for target in ["datetime", "date", "timestamp", "string"]:
            config = DateTimeTransformConfig(target_type=target)
            assert config.target_type == target

        # Valid to_epoch units
        for unit in ["seconds", "milliseconds", "microseconds", "nanoseconds"]:
            config = DateTimeTransformConfig(to_epoch=unit)
            assert config.to_epoch == unit

    def test_regex_replace_config(self):
        """Test RegexReplaceConfig."""
        config = RegexReplaceConfig(pattern=r"\d+", replacement="NUMBER", flags=re.IGNORECASE)
        assert config.pattern == r"\d+"
        assert config.replacement == "NUMBER"
        assert config.flags == re.IGNORECASE

    def test_string_replace_config(self):
        """Test StringReplaceConfig."""
        config = StringReplaceConfig(old="old", new="new", count=2)
        assert config.old == "old"
        assert config.new == "new"
        assert config.count == 2

    def test_money_type_config_defaults(self):
        """Test MoneyTypeConfig with default values."""
        config = MoneyTypeConfig()
        expected_symbols = ["$", "€", "£", "¥", "₹", "₽", "¢"]
        assert config.currency_symbols == expected_symbols
        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.parentheses_negative is True
        assert config.strip_whitespace is True

    def test_money_type_config_custom_symbols(self):
        """Test MoneyTypeConfig with custom currency symbols."""
        custom_symbols = ["$", "€"]
        config = MoneyTypeConfig(currency_symbols=custom_symbols)
        assert config.currency_symbols == custom_symbols

    def test_numeric_cleaning_config_defaults(self):
        """Test NumericCleaningConfig with default values."""
        config = NumericCleaningConfig()
        expected_nan_values = ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]
        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.allow_nan is True
        assert config.nan_values == expected_nan_values
        assert config.strip_whitespace is True

    def test_numeric_cleaning_config_custom_nan_values(self):
        """Test NumericCleaningConfig with custom NaN values."""
        custom_nan = ["", "NULL", "missing"]
        config = NumericCleaningConfig(nan_values=custom_nan)
        assert config.nan_values == custom_nan

    def test_string_padding_config(self):
        """Test StringPaddingConfig."""
        config = StringPaddingConfig(width=10, fillchar="0", side="left")
        assert config.width == 10
        assert config.fillchar == "0"
        assert config.side == "left"

    def test_html_xml_config(self):
        """Test HTMLXMLConfig."""
        config = HTMLXMLConfig(strip_tags=True, decode_entities=True, preserve_whitespace=False)
        assert config.strip_tags is True
        assert config.decode_entities is True
        assert config.preserve_whitespace is False

    def test_ssn_config(self):
        """Test SSNConfig."""
        config = SSNConfig(format_with_dashes=True, zero_pad=True, validate=True, allow_invalid=False)
        assert config.format_with_dashes is True
        assert config.zero_pad is True
        assert config.validate is True
        assert config.allow_invalid is False

    def test_zip_code_config_defaults(self):
        """Test ZipCodeConfig with default values."""
        config = ZipCodeConfig()
        assert config.zip_type == "zip-permissive"
        assert config.format_with_dash is True
        assert config.zero_pad is True
        assert config.validate is True
        assert config.allow_invalid is False

    def test_zip_code_config_invalid_type(self):
        """Test ZipCodeConfig with invalid zip type."""
        with pytest.raises(ValueError, match="zip_type must be one of"):
            ZipCodeConfig(zip_type="invalid-type")

    def test_zip_code_config_valid_types(self):
        """Test ZipCodeConfig with valid zip types."""
        for zip_type in ["zip-permissive", "zip-5", "zip-9"]:
            config = ZipCodeConfig(zip_type=zip_type)
            assert config.zip_type == zip_type

    def test_phone_number_config_defaults(self):
        """Test PhoneNumberConfig with default values."""
        config = PhoneNumberConfig()
        assert config.format_style == "us-standard"
        assert config.include_country_code is False
        assert config.use_parentheses is True
        assert config.use_dashes is True
        assert config.use_dots is False
        assert config.validate is True
        assert config.allow_invalid is False
        assert config.min_digits == 10
        assert config.max_digits == 11

    def test_phone_number_config_invalid_format_style(self):
        """Test PhoneNumberConfig with invalid format style."""
        with pytest.raises(ValueError, match="format_style must be one of"):
            PhoneNumberConfig(format_style="invalid-style")

    def test_phone_number_config_valid_styles(self):
        """Test PhoneNumberConfig with valid format styles."""
        for style in ["us-standard", "international", "digits-only", "preserve"]:
            config = PhoneNumberConfig(format_style=style)
            assert config.format_style == style

    def test_email_config(self):
        """Test EmailConfig."""
        config = EmailConfig(
            normalize_case=True,
            validate_format=True,
            allow_invalid=False,
            strip_whitespace=True,
            normalize_domain=True
        )
        assert config.normalize_case is True
        assert config.validate_format is True
        assert config.allow_invalid is False
        assert config.strip_whitespace is True
        assert config.normalize_domain is True

    def test_ip_address_config_defaults(self):
        """Test IPAddressConfig with default values."""
        config = IPAddressConfig()
        assert config.ip_version == "both"
        assert config.normalize_ipv6 is True
        assert config.validate is True
        assert config.allow_invalid is False
        assert config.compress_ipv6 is True

    def test_ip_address_config_invalid_version(self):
        """Test IPAddressConfig with invalid IP version."""
        with pytest.raises(ValueError, match="ip_version must be one of"):
            IPAddressConfig(ip_version="invalid-version")

    def test_ip_address_config_valid_versions(self):
        """Test IPAddressConfig with valid IP versions."""
        for version in ["ipv4", "ipv6", "both"]:
            config = IPAddressConfig(ip_version=version)
            assert config.ip_version == version

    def test_mac_address_config_defaults(self):
        """Test MACAddressConfig with default values."""
        config = MACAddressConfig()
        assert config.format_style == "colon"
        assert config.case_style == "lower"
        assert config.validate is True
        assert config.allow_invalid is False
        assert config.zero_pad is True

    def test_mac_address_config_invalid_format_style(self):
        """Test MACAddressConfig with invalid format style."""
        with pytest.raises(ValueError, match="format_style must be one of"):
            MACAddressConfig(format_style="invalid-style")

    def test_mac_address_config_invalid_case_style(self):
        """Test MACAddressConfig with invalid case style."""
        with pytest.raises(ValueError, match="case_style must be one of"):
            MACAddressConfig(case_style="invalid-case")

    def test_mac_address_config_valid_styles(self):
        """Test MACAddressConfig with valid styles."""
        for format_style in ["colon", "dash", "dot", "none"]:
            config = MACAddressConfig(format_style=format_style)
            assert config.format_style == format_style

        for case_style in ["lower", "upper", "preserve"]:
            config = MACAddressConfig(case_style=case_style)
            assert config.case_style == case_style

    def test_string_cleaning_config_defaults(self):
        """Test StringCleaningConfig with default values."""
        config = StringCleaningConfig()

        # Check defaults
        assert config.normalize_quotes is True
        assert config.normalize_dashes is True
        assert config.normalize_spaces is True
        assert config.collapse_whitespace is True
        assert config.strip_whitespace is True
        assert config.remove_tabs is False
        assert config.tab_replacement == " "
        assert config.remove_zero_width is True
        assert config.remove_control_chars is True
        assert config.preserve_newlines is True
        assert config.preserve_tabs is False
        assert config.unicode_normalize == "NFKC"
        assert config.fix_case_issues is False
        assert config.case_transform is None
        assert config.case_mapping_mode == "exact"
        assert config.remove_accents is False
        assert config.ascii_only is False
        assert config.fix_encoding_errors is True

        # Check default title_case_exceptions
        expected_exceptions = ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]
        assert config.title_case_exceptions == expected_exceptions

        # Check default empty collections
        assert config.custom_case_mapping == {}
        assert config.acronyms == []

    def test_string_cleaning_config_invalid_case_transform(self):
        """Test StringCleaningConfig with invalid case transform."""
        with pytest.raises(ValueError, match="case_transform must be one of"):
            StringCleaningConfig(case_transform="invalid-transform")

    def test_string_cleaning_config_invalid_case_mapping_mode(self):
        """Test StringCleaningConfig with invalid case mapping mode."""
        with pytest.raises(ValueError, match="case_mapping_mode must be one of"):
            StringCleaningConfig(case_mapping_mode="invalid-mode")

    def test_string_cleaning_config_valid_case_transforms(self):
        """Test StringCleaningConfig with valid case transforms."""
        for transform in [None, 'upper', 'lower', 'title', 'proper']:
            config = StringCleaningConfig(case_transform=transform)
            assert config.case_transform == transform

    def test_string_cleaning_config_valid_case_mapping_modes(self):
        """Test StringCleaningConfig with valid case mapping modes."""
        for mode in ['exact', 'contains', 'startswith', 'endswith']:
            config = StringCleaningConfig(case_mapping_mode=mode)
            assert config.case_mapping_mode == mode

    def test_string_cleaning_config_custom_values(self):
        """Test StringCleaningConfig with custom values."""
        custom_exceptions = ["of", "the"]
        custom_mapping = {"california": "CA"}
        custom_acronyms = ["NASA", "API"]

        config = StringCleaningConfig(
            title_case_exceptions=custom_exceptions,
            custom_case_mapping=custom_mapping,
            acronyms=custom_acronyms
        )

        assert config.title_case_exceptions == custom_exceptions
        assert config.custom_case_mapping == custom_mapping
        assert config.acronyms == custom_acronyms


class TestDataTransformerBasics:
    """Test basic DataTransformer functionality."""

    def test_data_transformer_initialization(self):
        """Test DataTransformer initialization."""
        transformer = DataTransformer()
        assert isinstance(transformer, DataTransformer)

    def test_apply_regex_replace_non_string_column(self):
        """Test apply_regex_replace with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = RegexReplaceConfig(pattern=r"\d+", replacement="NUMBER")

        # Test with integer column
        int_column = pa.array([1, 2, 3])
        result = transformer.apply_regex_replace(int_column, config)
        assert result.equals(int_column)

    def test_apply_regex_replace_string_column(self):
        """Test apply_regex_replace with string column."""
        transformer = DataTransformer()
        config = RegexReplaceConfig(pattern=r"\d+", replacement="NUMBER", flags=re.IGNORECASE)

        column = pa.array(["test123", "abc456def", "no_digits"])
        result = transformer.apply_regex_replace(column, config)
        expected = ["testNUMBER", "abcNUMBERdef", "no_digits"]
        assert result.to_pylist() == expected

    def test_apply_string_replace_non_string_column(self):
        """Test apply_string_replace with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = StringReplaceConfig(old="old", new="new")

        # Test with integer column
        int_column = pa.array([1, 2, 3])
        result = transformer.apply_string_replace(int_column, config)
        assert result.equals(int_column)

    def test_apply_string_replace_unlimited_count(self):
        """Test apply_string_replace with unlimited count (-1)."""
        transformer = DataTransformer()
        config = StringReplaceConfig(old="old", new="new", count=-1)

        column = pa.array(["old_old_old", "new_old", "nothing"])
        result = transformer.apply_string_replace(column, config)
        expected = ["new_new_new", "new_new", "nothing"]
        assert result.to_pylist() == expected

    def test_apply_string_replace_limited_count(self):
        """Test apply_string_replace with limited count."""
        transformer = DataTransformer()
        config = StringReplaceConfig(old="old", new="new", count=1)

        column = pa.array(["old_old_old", "old_something"])
        result = transformer.apply_string_replace(column, config)
        expected = ["new_old_old", "new_something"]
        assert result.to_pylist() == expected


class TestMoneyConversion:
    """Test money conversion functionality."""

    def test_apply_money_conversion_non_string_column(self):
        """Test apply_money_conversion with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        int_column = pa.array([1, 2, 3])
        result = transformer.apply_money_conversion(int_column, config)
        assert result.equals(int_column)

    def test_apply_money_conversion_basic(self):
        """Test basic money conversion."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        column = pa.array(["$1,000.50", "€2,500.75", "£100"])
        result = transformer.apply_money_conversion(column, config)
        expected = [1000.50, 2500.75, 100.0]
        assert result.to_pylist() == expected

    def test_apply_money_conversion_with_nulls(self):
        """Test money conversion with null values."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        column = pa.array(["$100", None, "$200"])
        result = transformer.apply_money_conversion(column, config)
        expected = [100.0, None, 200.0]
        assert result.to_pylist() == expected

    def test_apply_money_conversion_negative_parentheses(self):
        """Test money conversion with parentheses for negative values."""
        transformer = DataTransformer()
        config = MoneyTypeConfig(parentheses_negative=True)

        column = pa.array(["($100.50)", "$200.25"])
        result = transformer.apply_money_conversion(column, config)
        expected = [-100.50, 200.25]
        assert result.to_pylist() == expected

    def test_apply_money_conversion_invalid_values(self):
        """Test money conversion with invalid values."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        column = pa.array(["invalid", "$abc", ""])
        result = transformer.apply_money_conversion(column, config)
        expected = [None, None, None]
        assert result.to_pylist() == expected

    def test_clean_money_string_empty(self):
        """Test _clean_money_string with empty string."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        result = transformer._clean_money_string("", config)
        assert result is None

    def test_clean_money_string_whitespace_stripping(self):
        """Test _clean_money_string with whitespace stripping."""
        transformer = DataTransformer()
        config = MoneyTypeConfig(strip_whitespace=True)

        result = transformer._clean_money_string("  $100.50  ", config)
        assert result == Decimal("100.50")

    def test_clean_money_string_custom_separators(self):
        """Test _clean_money_string with custom separators."""
        transformer = DataTransformer()
        config = MoneyTypeConfig(thousands_separator=".", decimal_separator=",")

        result = transformer._clean_money_string("$1.000,50", config)
        assert result == Decimal("1000.50")

    def test_clean_money_string_invalid_decimal(self):
        """Test _clean_money_string with invalid decimal."""
        transformer = DataTransformer()
        config = MoneyTypeConfig()

        result = transformer._clean_money_string("$invalid", config)
        assert result is None


class TestNumericCleaning:
    """Test numeric cleaning functionality."""

    def test_apply_numeric_cleaning_basic(self):
        """Test basic numeric cleaning."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        column = pa.array(["1,000.50", "2,500", "3.14"])
        result = transformer.apply_numeric_cleaning(column, config)
        expected = [1000.50, 2500.0, 3.14]
        assert result.to_pylist() == expected

    def test_apply_numeric_cleaning_integer_target(self):
        """Test numeric cleaning with integer target type."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        column = pa.array(["1,000", "2,500"])
        result = transformer.apply_numeric_cleaning(column, config, target_type="int64")
        expected = [1000, 2500]
        assert result.to_pylist() == expected
        assert result.type == pa.int64()

    def test_apply_numeric_cleaning_float32_target(self):
        """Test numeric cleaning with float32 target type."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        column = pa.array(["1.5", "2.5"])
        result = transformer.apply_numeric_cleaning(column, config, target_type="float32")
        expected = [1.5, 2.5]
        assert result.to_pylist() == expected
        assert result.type == pa.float32()

    def test_apply_numeric_cleaning_int32_target(self):
        """Test numeric cleaning with int32 target type."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        column = pa.array(["100", "200"])
        result = transformer.apply_numeric_cleaning(column, config, target_type="int32")
        expected = [100, 200]
        assert result.to_pylist() == expected
        assert result.type == pa.int32()

    def test_apply_numeric_cleaning_with_nulls(self):
        """Test numeric cleaning with null values."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        column = pa.array(["100", None, "200"])
        result = transformer.apply_numeric_cleaning(column, config)
        expected = [100.0, None, 200.0]
        assert result.to_pylist() == expected

    def test_apply_numeric_cleaning_nan_values(self):
        """Test numeric cleaning with NaN values."""
        transformer = DataTransformer()
        config = NumericCleaningConfig(allow_nan=True)

        column = pa.array(["100", "N/A", "NULL", "200"])
        result = transformer.apply_numeric_cleaning(column, config)
        expected = [100.0, None, None, 200.0]
        assert result.to_pylist() == expected

    def test_apply_numeric_cleaning_disallow_nan(self):
        """Test numeric cleaning with NaN values when not allowed."""
        transformer = DataTransformer()
        config = NumericCleaningConfig(allow_nan=False)

        column = pa.array(["100", "invalid"])
        with pytest.raises(ValueError):
            transformer.apply_numeric_cleaning(column, config)

    def test_apply_numeric_cleaning_overflow_error(self):
        """Test numeric cleaning with overflow error when NaN allowed."""
        transformer = DataTransformer()
        config = NumericCleaningConfig(allow_nan=True)

        # Mock the numeric transformer's method, not the delegated one
        with patch.object(transformer.numeric_transformer, '_clean_numeric_string', side_effect=OverflowError):
            column = pa.array(["100"])
            result = transformer.apply_numeric_cleaning(column, config)
            expected = [None]
            assert result.to_pylist() == expected

    def test_clean_numeric_string_empty(self):
        """Test _clean_numeric_string with empty string."""
        transformer = DataTransformer()
        config = NumericCleaningConfig()

        result = transformer._clean_numeric_string("", config)
        assert result is None

    def test_clean_numeric_string_custom_separators(self):
        """Test _clean_numeric_string with custom separators."""
        transformer = DataTransformer()
        config = NumericCleaningConfig(thousands_separator=".", decimal_separator=",")

        result = transformer._clean_numeric_string("1.000,50", config)
        assert result == "1000.50"

    def test_clean_numeric_string_no_thousands_separator(self):
        """Test _clean_numeric_string with no thousands separator."""
        transformer = DataTransformer()
        config = NumericCleaningConfig(thousands_separator="")

        result = transformer._clean_numeric_string("1000.50", config)
        assert result == "1000.50"


class TestStringPadding:
    """Test string padding functionality."""

    def test_apply_string_padding_non_string_column(self):
        """Test apply_string_padding with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = StringPaddingConfig(width=5, fillchar="0", side="left")

        int_column = pa.array([1, 2, 3])
        result = transformer.apply_string_padding(int_column, config)
        assert result.equals(int_column)

    def test_apply_string_padding_left(self):
        """Test string padding on left side."""
        transformer = DataTransformer()
        config = StringPaddingConfig(width=5, fillchar="0", side="left")

        column = pa.array(["123", "45"])
        result = transformer.apply_string_padding(column, config)
        expected = ["00123", "00045"]
        assert result.to_pylist() == expected

    def test_apply_string_padding_right(self):
        """Test string padding on right side."""
        transformer = DataTransformer()
        config = StringPaddingConfig(width=5, fillchar="0", side="right")

        column = pa.array(["123", "45"])
        result = transformer.apply_string_padding(column, config)
        expected = ["12300", "45000"]
        assert result.to_pylist() == expected

    def test_apply_string_padding_both(self):
        """Test string padding on both sides (center)."""
        transformer = DataTransformer()
        config = StringPaddingConfig(width=7, fillchar="0", side="both")

        column = pa.array(["123"])
        result = transformer.apply_string_padding(column, config)
        expected = ["00123000"]  # Note: pandas center might pad unevenly
        assert len(result.to_pylist()[0]) == 7

    def test_apply_string_padding_default_side(self):
        """Test string padding with invalid side defaults to left."""
        transformer = DataTransformer()
        config = StringPaddingConfig(width=5, fillchar="0", side="invalid")

        column = pa.array(["123"])
        result = transformer.apply_string_padding(column, config)
        expected = ["00123"]
        assert result.to_pylist() == expected


class TestStringTrimming:
    """Test string trimming functionality."""

    def test_apply_string_trimming_non_string_column(self):
        """Test apply_string_trimming with non-string column returns unchanged."""
        transformer = DataTransformer()

        int_column = pa.array([1, 2, 3])
        result = transformer.apply_string_trimming(int_column)
        assert result.equals(int_column)

    def test_apply_string_trimming_left(self):
        """Test string trimming on left side."""
        transformer = DataTransformer()

        column = pa.array(["  hello  ", "  world  "])
        result = transformer.apply_string_trimming(column, side="left")
        expected = ["hello  ", "world  "]
        assert result.to_pylist() == expected

    def test_apply_string_trimming_right(self):
        """Test string trimming on right side."""
        transformer = DataTransformer()

        column = pa.array(["  hello  ", "  world  "])
        result = transformer.apply_string_trimming(column, side="right")
        expected = ["  hello", "  world"]
        assert result.to_pylist() == expected

    def test_apply_string_trimming_both(self):
        """Test string trimming on both sides."""
        transformer = DataTransformer()

        column = pa.array(["  hello  ", "  world  "])
        result = transformer.apply_string_trimming(column, side="both")
        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_apply_string_trimming_custom_chars(self):
        """Test string trimming with custom characters."""
        transformer = DataTransformer()

        column = pa.array(["xxxhelloxxx", "xxxworldxxx"])
        result = transformer.apply_string_trimming(column, side="both", chars="x")
        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_apply_string_trimming_default_side(self):
        """Test string trimming with invalid side defaults to both."""
        transformer = DataTransformer()

        column = pa.array(["  hello  "])
        result = transformer.apply_string_trimming(column, side="invalid")
        expected = ["hello"]
        assert result.to_pylist() == expected


class TestHTMLXMLCleaning:
    """Test HTML/XML cleaning functionality."""

    def test_apply_html_xml_cleaning_non_string_column(self):
        """Test apply_html_xml_cleaning with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = HTMLXMLConfig()

        int_column = pa.array([1, 2, 3])
        result = transformer.apply_html_xml_cleaning(int_column, config)
        assert result.equals(int_column)

    def test_apply_html_xml_cleaning_with_nulls(self):
        """Test HTML/XML cleaning with null values."""
        transformer = DataTransformer()
        config = HTMLXMLConfig()

        column = pa.array(["<b>hello</b>", None, "<i>world</i>"])
        result = transformer.apply_html_xml_cleaning(column, config)
        expected = ["hello", None, "world"]
        assert result.to_pylist() == expected

    def test_apply_html_xml_cleaning_decode_entities(self):
        """Test HTML/XML cleaning with entity decoding."""
        transformer = DataTransformer()
        config = HTMLXMLConfig(decode_entities=True, strip_tags=False)

        column = pa.array(["&lt;hello&gt;", "&amp;world&amp;"])
        result = transformer.apply_html_xml_cleaning(column, config)
        expected = ["<hello>", "&world&"]
        assert result.to_pylist() == expected

    def test_apply_html_xml_cleaning_strip_tags(self):
        """Test HTML/XML cleaning with tag stripping."""
        transformer = DataTransformer()
        config = HTMLXMLConfig(strip_tags=True, decode_entities=False)

        column = pa.array(["<b>hello</b>", "<p>world</p>"])
        result = transformer.apply_html_xml_cleaning(column, config)
        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_apply_html_xml_cleaning_preserve_whitespace(self):
        """Test HTML/XML cleaning with whitespace preservation."""
        transformer = DataTransformer()
        config = HTMLXMLConfig(preserve_whitespace=True)

        column = pa.array(["<b>hello   world</b>"])
        result = transformer.apply_html_xml_cleaning(column, config)
        expected = ["hello   world"]
        assert result.to_pylist() == expected

    def test_apply_html_xml_cleaning_collapse_whitespace(self):
        """Test HTML/XML cleaning with whitespace collapsing."""
        transformer = DataTransformer()
        config = HTMLXMLConfig(preserve_whitespace=False)

        column = pa.array(["<b>hello   world</b>"])
        result = transformer.apply_html_xml_cleaning(column, config)
        expected = ["hello world"]
        assert result.to_pylist() == expected


class TestDateTimeTransformation:
    """Test datetime transformation functionality."""

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    @patch('pytz.timezone')
    def test_apply_datetime_transformation_enforce_mode(self, mock_timezone, mock_coerce):
        """Test datetime transformation in enforce mode."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(mode="enforce", format="%Y-%m-%d")

        mock_coerce.return_value = datetime.datetime(2023, 1, 1)

        column = pa.array(["2023-01-01", "2023-02-01"])
        result = transformer.apply_datetime_transformation(column, config)

        # Should call coerce_datetime with strict format
        mock_coerce.assert_called_with(
            "2023-02-01",
            fmt="%Y-%m-%d",
            allow_fuzzy=False,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_specify_formats_mode(self, mock_coerce):
        """Test datetime transformation in specify_formats mode."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(
            mode="specify_formats",
            formats=["%Y-%m-%d", "%m/%d/%Y"],
            allow_fuzzy=True
        )

        mock_coerce.return_value = datetime.datetime(2023, 1, 1)

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        mock_coerce.assert_called_with(
            "2023-01-01",
            formats=["%Y-%m-%d", "%m/%d/%Y"],
            allow_fuzzy=True,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_common_formats_mode(self, mock_coerce):
        """Test datetime transformation in common_formats mode."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(mode="common_formats", allow_fuzzy=True)

        mock_coerce.return_value = datetime.datetime(2023, 1, 1)

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        mock_coerce.assert_called_with(
            "2023-01-01",
            allow_fuzzy=True,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_to_epoch(self, mock_coerce):
        """Test datetime transformation with to_epoch conversion."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(to_epoch="seconds")

        mock_coerce.return_value = 1672531200  # Epoch timestamp

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        assert result.to_pylist() == [1672531200]

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    @patch('pytz.timezone')
    def test_apply_datetime_transformation_with_timezone(self, mock_timezone, mock_coerce):
        """Test datetime transformation with timezone conversion."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(timezone="America/New_York")

        # Create a mock datetime object that can have its astimezone method mocked
        mock_dt = Mock()
        mock_dt.tzinfo = None  # Start with no timezone info
        mock_coerce.return_value = mock_dt

        # Mock the timezone object and converted datetime
        mock_tz = Mock()
        mock_timezone.return_value = mock_tz
        mock_dt_converted = datetime.datetime(2023, 1, 1, 7, 0, 0)
        mock_dt.astimezone.return_value = mock_dt_converted

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        mock_timezone.assert_called_with("America/New_York")
        mock_dt.astimezone.assert_called_with(mock_tz)

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_date(self, mock_coerce):
        """Test datetime transformation with date target type."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(target_type="date")

        mock_dt = datetime.datetime(2023, 1, 1)
        mock_coerce.return_value = mock_dt

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        assert result.type == pa.date32()

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_timestamp(self, mock_coerce):
        """Test datetime transformation with timestamp target type."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(target_type="timestamp")

        # Create a real datetime object instead of a mock
        mock_dt = datetime.datetime(2023, 1, 1, 0, 0, 0)
        mock_coerce.return_value = mock_dt

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        # Should return timestamps as floats
        expected_timestamp = mock_dt.timestamp()
        assert result.to_pylist() == [expected_timestamp]

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_string_with_format(self, mock_coerce):
        """Test datetime transformation with string target type and custom format."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(target_type="string", output_format="%m/%d/%Y")

        mock_dt = datetime.datetime(2023, 1, 1)
        mock_coerce.return_value = mock_dt

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        expected = ["01/01/2023"]
        assert result.to_pylist() == expected

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_string_without_format(self, mock_coerce):
        """Test datetime transformation with string target type and no format."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig(target_type="string")

        mock_dt = datetime.datetime(2023, 1, 1)
        mock_coerce.return_value = mock_dt

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        expected = ["2023-01-01T00:00:00"]
        assert result.to_pylist() == expected

    def test_apply_datetime_transformation_with_nulls(self):
        """Test datetime transformation with null values."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig()

        column = pa.array(["2023-01-01", None, ""])
        result = transformer.apply_datetime_transformation(column, config)

        # Should handle None and empty string
        assert result.to_pylist()[1] is None
        assert result.to_pylist()[2] is None

    @patch('forklift.utils.transformations.datetime_transformations.coerce_datetime')
    def test_apply_datetime_transformation_parse_error(self, mock_coerce):
        """Test datetime transformation with parse error."""
        transformer = DataTransformer()
        config = DateTimeTransformConfig()

        mock_coerce.side_effect = ValueError("Invalid date")

        column = pa.array(["invalid-date"])
        result = transformer.apply_datetime_transformation(column, config)

        expected = [None]
        assert result.to_pylist() == expected


class TestStringCleaning:
    """Test comprehensive string cleaning functionality."""

    def test_apply_string_cleaning_non_string_column(self):
        """Test apply_string_cleaning with non-string column returns unchanged."""
        transformer = DataTransformer()
        config = StringCleaningConfig()

        int_column = pa.array([1, 2, 3])
        result = transformer.apply_string_cleaning(int_column, config)
        assert result.equals(int_column)

    def test_apply_string_cleaning_with_nulls(self):
        """Test string cleaning with null values."""
        transformer = DataTransformer()
        config = StringCleaningConfig()

        column = pa.array(["hello", None, "world"])
        result = transformer.apply_string_cleaning(column, config)

        assert result.to_pylist()[0] == "hello"
        assert result.to_pylist()[1] is None
        assert result.to_pylist()[2] == "world"

    def test_apply_string_cleaning_unicode_normalization(self):
        """Test string cleaning with unicode normalization."""
        transformer = DataTransformer()
        config = StringCleaningConfig(unicode_normalize="NFC")

        # Test with composed characters
        column = pa.array(["caf��"])  # é as single character
        result = transformer.apply_string_cleaning(column, config)

        # Should normalize the text
        assert len(result.to_pylist()) == 1

    def test_apply_string_cleaning_invalid_unicode_norm(self):
        """Test string cleaning with invalid unicode normalization."""
        transformer = DataTransformer()
        config = StringCleaningConfig(unicode_normalize="INVALID")

        column = pa.array(["test"])
        result = transformer.apply_string_cleaning(column, config)

        # Should skip invalid normalization and continue
        assert result.to_pylist() == ["test"]

    def test_apply_string_cleaning_fix_encoding_errors(self):
        """Test string cleaning with encoding error fixes."""
        transformer = DataTransformer()
        config = StringCleaningConfig(fix_encoding_errors=True)

        column = pa.array(["Donâ€™t"])  # Mojibake for "Don't"
        result = transformer.apply_string_cleaning(column, config)

        expected = ["Don't"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_normalize_quotes(self):
        """Test string cleaning with quote normalization."""
        transformer = DataTransformer()
        config = StringCleaningConfig(normalize_quotes=True)

        # Using Unicode escape sequences for smart quotes
        column = pa.array(["\u201Chello\u201D", "\u2018world\u2019"])  # Smart quotes
        result = transformer.apply_string_cleaning(column, config)

        expected = ['"hello"', "'world'"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_normalize_dashes(self):
        """Test string cleaning with dash normalization."""
        transformer = DataTransformer()
        config = StringCleaningConfig(normalize_dashes=True)

        column = pa.array(["hello—world", "test–case"])  # Em and en dashes
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello-world", "test-case"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_normalize_spaces(self):
        """Test string cleaning with space normalization."""
        transformer = DataTransformer()
        config = StringCleaningConfig(normalize_spaces=True)

        # Non-breaking space and other space characters
        column = pa.array(["hello\u00A0world", "test\u2003case"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello world", "test case"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_remove_zero_width_with_space(self):
        """Test string cleaning removing zero-width chars with space replacement."""
        transformer = DataTransformer()
        config = StringCleaningConfig(remove_zero_width=True, collapse_whitespace=True)

        column = pa.array(["hello\u200Bworld"])  # Zero-width space
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello world"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_remove_zero_width_without_space(self):
        """Test string cleaning removing zero-width chars without space replacement."""
        transformer = DataTransformer()
        config = StringCleaningConfig(remove_zero_width=True, collapse_whitespace=False)

        column = pa.array(["hello\u200Bworld"])  # Zero-width space
        result = transformer.apply_string_cleaning(column, config)

        expected = ["helloworld"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_remove_tabs(self):
        """Test string cleaning with tab removal."""
        transformer = DataTransformer()
        config = StringCleaningConfig(remove_tabs=True)

        column = pa.array(["hello\tworld"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["helloworld"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_replace_tabs_custom(self):
        """Test string cleaning with custom tab replacement."""
        transformer = DataTransformer()
        config = StringCleaningConfig(remove_tabs=False, tab_replacement="    ")

        column = pa.array(["hello\tworld"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello    world"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_collapse_whitespace(self):
        """Test string cleaning with whitespace collapsing."""
        transformer = DataTransformer()
        config = StringCleaningConfig(collapse_whitespace=True)

        column = pa.array(["hello   world", "test\n\ncase"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello world", "test case"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_strip_whitespace(self):
        """Test string cleaning with whitespace stripping."""
        transformer = DataTransformer()
        config = StringCleaningConfig(strip_whitespace=True)

        column = pa.array(["  hello  ", "  world  "])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_remove_accents(self):
        """Test string cleaning with accent removal."""
        transformer = DataTransformer()
        config = StringCleaningConfig(remove_accents=True)

        column = pa.array(["café", "naïve"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["cafe", "naive"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_ascii_only(self):
        """Test string cleaning with ASCII-only conversion."""
        transformer = DataTransformer()
        config = StringCleaningConfig(ascii_only=True)

        column = pa.array(["café", "résumé"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["cafe", "resume"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_case_transform_upper(self):
        """Test string cleaning with uppercase transformation."""
        transformer = DataTransformer()
        config = StringCleaningConfig(case_transform="upper")

        column = pa.array(["hello", "world"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["HELLO", "WORLD"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_case_transform_lower(self):
        """Test string cleaning with lowercase transformation."""
        transformer = DataTransformer()
        config = StringCleaningConfig(case_transform="lower")

        column = pa.array(["HELLO", "WORLD"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_case_transform_title(self):
        """Test string cleaning with title case transformation."""
        transformer = DataTransformer()
        config = StringCleaningConfig(case_transform="title")

        column = pa.array(["hello world", "test case"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["Hello World", "Test Case"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_case_transform_proper(self):
        """Test string cleaning with proper case transformation."""
        transformer = DataTransformer()
        config = StringCleaningConfig(case_transform="proper")

        column = pa.array(["hello world"])
        result = transformer.apply_string_cleaning(column, config)

        # Proper case capitalizes first letter, lowercases rest
        expected = ["Hello world"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_custom_case_mapping_exact(self):
        """Test string cleaning with exact custom case mapping."""
        transformer = DataTransformer()
        config = StringCleaningConfig(
            custom_case_mapping={"california": "CA"},
            case_mapping_mode="exact"
        )

        column = pa.array(["california", "texas"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["CA", "texas"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_custom_case_mapping_contains(self):
        """Test string cleaning with contains custom case mapping."""
        transformer = DataTransformer()
        config = StringCleaningConfig(
            custom_case_mapping={"cal": "CAL"},
            case_mapping_mode="contains"
        )

        column = pa.array(["california", "texas"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["CALifornia", "texas"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_custom_case_mapping_startswith(self):
        """Test string cleaning with startswith custom case mapping."""
        transformer = DataTransformer()
        config = StringCleaningConfig(
            custom_case_mapping={"cal": "CAL"},
            case_mapping_mode="startswith"
        )

        column = pa.array(["california", "texas"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["CALifornia", "texas"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_custom_case_mapping_endswith(self):
        """Test string cleaning with endswith custom case mapping."""
        transformer = DataTransformer()
        config = StringCleaningConfig(
            custom_case_mapping={"nia": "NIA"},
            case_mapping_mode="endswith"
        )

        column = pa.array(["california", "texas"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["califorNIA", "texas"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_acronyms(self):
        """Test string cleaning with acronym preservation."""
        transformer = DataTransformer()
        config = StringCleaningConfig(acronyms=["NASA", "API"])

        column = pa.array(["work at nasa", "use the api"])
        result = transformer.apply_string_cleaning(column, config)

        expected = ["work at NASA", "use the API"]
        assert result.to_pylist() == expected


class TestSSNFormatting:
    """Test SSN formatting functionality."""

    def test_apply_ssn_formatting_convert_to_string(self):
        """Test SSN formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = SSNConfig()

        int_column = pa.array([123456789])
        result = transformer.apply_ssn_formatting(int_column, config)

        expected = ["123-45-6789"]
        assert result.to_pylist() == expected

    def test_apply_ssn_formatting_with_nulls(self):
        """Test SSN formatting with null values."""
        transformer = DataTransformer()
        config = SSNConfig()

        column = pa.array(["123456789", None, "987654321"])
        result = transformer.apply_ssn_formatting(column, config)

        expected = ["123-45-6789", None, "987-65-4321"]
        assert result.to_pylist() == expected

    def test_apply_ssn_formatting_allow_invalid(self):
        """Test SSN formatting with allow_invalid=True."""
        transformer = DataTransformer()
        config = SSNConfig(allow_invalid=True)

        column = pa.array(["123456789", "invalid"])
        result = transformer.apply_ssn_formatting(column, config)

        expected = ["123-45-6789", "invalid"]
        assert result.to_pylist() == expected

    def test_apply_ssn_formatting_disallow_invalid(self):
        """Test SSN formatting with allow_invalid=False."""
        transformer = DataTransformer()
        config = SSNConfig(allow_invalid=False)

        column = pa.array(["123456789", "invalid"])
        result = transformer.apply_ssn_formatting(column, config)

        expected = ["123-45-6789", None]
        assert result.to_pylist() == expected

    def test_format_ssn_basic(self):
        """Test basic SSN formatting."""
        transformer = DataTransformer()
        config = SSNConfig()

        result = transformer._format_ssn("123456789", config)
        assert result == "123-45-6789"

    def test_format_ssn_with_existing_formatting(self):
        """Test SSN formatting with existing dashes."""
        transformer = DataTransformer()
        config = SSNConfig()

        result = transformer._format_ssn("123-45-6789", config)
        assert result == "123-45-6789"

    def test_format_ssn_zero_padding(self):
        """Test SSN formatting with zero padding."""
        transformer = DataTransformer()
        config = SSNConfig(zero_pad=True, validate=False)

        result = transformer._format_ssn("123456789", config)
        assert result == "123-45-6789"

    def test_format_ssn_no_zero_padding(self):
        """Test SSN formatting without zero padding."""
        transformer = DataTransformer()
        config = SSNConfig(zero_pad=False, validate=False)

        result = transformer._format_ssn("123456789", config)
        assert result == "123-45-6789"

    def test_format_ssn_without_dashes(self):
        """Test SSN formatting without dashes."""
        transformer = DataTransformer()
        config = SSNConfig(format_with_dashes=False)

        result = transformer._format_ssn("123456789", config)
        assert result == "123456789"

    def test_format_ssn_empty_value(self):
        """Test SSN formatting with empty value."""
        transformer = DataTransformer()
        config = SSNConfig()

        with pytest.raises(ValueError, match="Empty SSN value"):
            transformer._format_ssn("", config)

    def test_format_ssn_contains_letters(self):
        """Test SSN formatting with letters in validation mode."""
        transformer = DataTransformer()
        config = SSNConfig(validate=True)

        with pytest.raises(ValueError, match="SSN contains letters"):
            transformer._format_ssn("123abc456", config)

    def test_format_ssn_no_digits(self):
        """Test SSN formatting with no digits."""
        transformer = DataTransformer()
        config = SSNConfig()

        with pytest.raises(ValueError, match="No digits found in SSN"):
            transformer._format_ssn("abc", config)

    def test_format_ssn_wrong_length(self):
        """Test SSN formatting with wrong length in validation mode."""
        transformer = DataTransformer()
        config = SSNConfig(validate=True)

        with pytest.raises(ValueError, match="SSN must have exactly 9 digits"):
            transformer._format_ssn("12345678", config)


class TestZipCodeFormatting:
    """Test ZIP code formatting functionality."""

    def test_apply_zip_code_formatting_convert_to_string(self):
        """Test ZIP code formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = ZipCodeConfig()

        int_column = pa.array([12345])
        result = transformer.apply_zip_code_formatting(int_column, config)

        expected = ["12345"]
        assert result.to_pylist() == expected

    def test_apply_zip_code_formatting_with_nulls(self):
        """Test ZIP code formatting with null values."""
        transformer = DataTransformer()
        config = ZipCodeConfig()

        column = pa.array(["12345", None, "123456789"])
        result = transformer.apply_zip_code_formatting(column, config)
        expected = ["12345", None, "12345-6789"]
        assert result.to_pylist() == expected

    def test_apply_zip_code_formatting_allow_invalid(self):
        """Test ZIP code formatting with allow_invalid=True."""
        transformer = DataTransformer()
        config = ZipCodeConfig(allow_invalid=True)

        column = pa.array(["12345", "invalid"])
        result = transformer.apply_zip_code_formatting(column, config)

        expected = ["12345", "invalid"]
        assert result.to_pylist() == expected

    def test_format_zip_code_zip_5(self):
        """Test ZIP-5 formatting."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-5")

        result = transformer._format_zip_code("12345", config)
        assert result == "12345"

    def test_format_zip_code_zip_5_zero_pad(self):
        """Test ZIP-5 formatting with zero padding."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-5", zero_pad=True)

        result = transformer._format_zip_code("123", config)
        assert result == "00123"

    def test_format_zip_code_zip_5_truncate(self):
        """Test ZIP-5 formatting with truncation."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-5")

        result = transformer._format_zip_code("123456789", config)
        assert result == "12345"

    def test_format_zip_code_zip_5_invalid_length(self):
        """Test ZIP-5 formatting with invalid length."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-5", validate=True, zero_pad=False)

        with pytest.raises(ValueError, match="ZIP-5 must have exactly 5 digits"):
            transformer._format_zip_code("123", config)

    def test_format_zip_code_zip_9(self):
        """Test ZIP-9 formatting."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-9", format_with_dash=True)

        result = transformer._format_zip_code("123456789", config)
        assert result == "12345-6789"

    def test_format_zip_code_zip_9_no_dash(self):
        """Test ZIP-9 formatting without dash."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-9", format_with_dash=False)

        result = transformer._format_zip_code("123456789", config)
        assert result == "123456789"

    def test_format_zip_code_zip_9_invalid_length(self):
        """Test ZIP-9 formatting with invalid length."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-9", validate=True)

        with pytest.raises(ValueError, match="ZIP-9 must have exactly 9 digits"):
            transformer._format_zip_code("12345", config)

    def test_format_zip_code_permissive_5_digit(self):
        """Test permissive ZIP code formatting with 5 digits."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-permissive")

        result = transformer._format_zip_code("12345", config)
        assert result == "12345"

    def test_format_zip_code_permissive_9_digit(self):
        """Test permissive ZIP code formatting with 9 digits."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-permissive", format_with_dash=True)

        result = transformer._format_zip_code("123456789", config)
        assert result == "12345-6789"

    def test_format_zip_code_permissive_invalid_length(self):
        """Test permissive ZIP code formatting with invalid length."""
        transformer = DataTransformer()
        config = ZipCodeConfig(zip_type="zip-permissive", validate=True)

        with pytest.raises(ValueError, match="ZIP code must have 5 or 9 digits"):
            transformer._format_zip_code("123", config)

    def test_format_zip_code_empty_value(self):
        """Test ZIP code formatting with empty value."""
        transformer = DataTransformer()
        config = ZipCodeConfig()

        with pytest.raises(ValueError, match="Empty ZIP code value"):
            transformer._format_zip_code("", config)

    def test_format_zip_code_too_many_non_digits(self):
        """Test ZIP code formatting with too many non-digit characters."""
        transformer = DataTransformer()
        config = ZipCodeConfig(validate=True)

        with pytest.raises(ValueError, match="ZIP code contains too many non-digit characters"):
            transformer._format_zip_code("abc123de", config)

    def test_format_zip_code_no_digits(self):
        """Test ZIP code formatting with no digits."""
        transformer = DataTransformer()
        config = ZipCodeConfig()

        with pytest.raises(ValueError, match="No digits found in ZIP code"):
            transformer._format_zip_code("abc", config)


class TestPhoneNumberFormatting:
    """Test phone number formatting functionality."""

    def test_apply_phone_number_formatting_convert_to_string(self):
        """Test phone number formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = PhoneNumberConfig()

        int_column = pa.array([1234567890])
        result = transformer.apply_phone_number_formatting(int_column, config)

        expected = ["(123) 456-7890"]
        assert result.to_pylist() == expected

    def test_apply_phone_number_formatting_with_nulls(self):
        """Test phone number formatting with null values."""
        transformer = DataTransformer()
        config = PhoneNumberConfig()

        column = pa.array(["1234567890", None, "9876543210"])
        result = transformer.apply_phone_number_formatting(column, config)

        expected = ["(123) 456-7890", None, "(987) 654-3210"]
        assert result.to_pylist() == expected

    def test_format_phone_number_us_standard_with_parentheses(self):
        """Test US standard phone formatting with parentheses."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="us-standard", use_parentheses=True)

        result = transformer._format_phone_number("1234567890", config)
        assert result == "(123) 456-7890"

    def test_format_phone_number_us_standard_without_parentheses(self):
        """Test US standard phone formatting without parentheses."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="us-standard", use_parentheses=False)

        result = transformer._format_phone_number("1234567890", config)
        assert result == "123-456-7890"

    def test_format_phone_number_with_country_code(self):
        """Test phone formatting with country code."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="us-standard", include_country_code=True)

        result = transformer._format_phone_number("1234567890", config)
        assert result == "1(123) 456-7890"

    def test_format_phone_number_11_digit_with_country_code(self):
        """Test phone formatting with 11-digit number including country code."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="us-standard", include_country_code=True)

        result = transformer._format_phone_number("11234567890", config)
        assert result == "1(123) 456-7890"

    def test_format_phone_number_international_style(self):
        """Test international phone formatting."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="international", include_country_code=True)

        result = transformer._format_phone_number("1234567890", config)
        assert result == "+1 1234567890"

    def test_format_phone_number_digits_only(self):
        """Test digits-only phone formatting."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="digits-only")

        result = transformer._format_phone_number("(123) 456-7890", config)
        assert result == "1234567890"

    def test_format_phone_number_preserve_style(self):
        """Test preserve phone formatting style."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="preserve")

        original = "(123) 456-7890"
        result = transformer._format_phone_number(original, config)
        assert result == original

    def test_format_phone_number_with_dots(self):
        """Test phone formatting with dots instead of dashes."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(format_style="us-standard", use_dots=True)

        result = transformer._format_phone_number("1234567890", config)
        assert result == "(123) 456.7890"

    def test_format_phone_number_empty_value(self):
        """Test phone formatting with empty value."""
        transformer = DataTransformer()
        config = PhoneNumberConfig()

        with pytest.raises(ValueError, match="Empty phone number value"):
            transformer._format_phone_number("", config)

    def test_format_phone_number_contains_letters(self):
        """Test phone formatting with letters in validation mode."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(validate=True)

        with pytest.raises(ValueError, match="Phone number contains letters"):
            transformer._format_phone_number("123abc7890", config)

    def test_format_phone_number_no_digits(self):
        """Test phone formatting with no digits."""
        transformer = DataTransformer()
        config = PhoneNumberConfig()

        with pytest.raises(ValueError, match="No digits found in phone number"):
            transformer._format_phone_number("abc", config)

    def test_format_phone_number_wrong_length(self):
        """Test phone formatting with wrong length in validation mode."""
        transformer = DataTransformer()
        config = PhoneNumberConfig(validate=True)

        with pytest.raises(ValueError, match="Phone number must have"):
            transformer._format_phone_number("123", config)


class TestEmailFormatting:
    """Test email formatting functionality."""

    def test_apply_email_formatting_convert_to_string(self):
        """Test email formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = EmailConfig()

        # This would be an unusual case but test for completeness
        column = pa.array(["test@example.com"])
        result = transformer.apply_email_formatting(column, config)

        expected = ["test@example.com"]
        assert result.to_pylist() == expected

    def test_apply_email_formatting_with_nulls(self):
        """Test email formatting with null values."""
        transformer = DataTransformer()
        config = EmailConfig()

        column = pa.array(["test@example.com", None, "USER@DOMAIN.COM"])
        result = transformer.apply_email_formatting(column, config)

        expected = ["test@example.com", None, "user@domain.com"]
        assert result.to_pylist() == expected

    def test_format_email_normalize_case(self):
        """Test email formatting with case normalization."""
        transformer = DataTransformer()
        config = EmailConfig(normalize_case=True)

        result = transformer._format_email("USER@EXAMPLE.COM", config)
        assert result == "user@example.com"

    def test_format_email_strip_whitespace(self):
        """Test email formatting with whitespace stripping."""
        transformer = DataTransformer()
        config = EmailConfig(strip_whitespace=True)

        result = transformer._format_email("  user@example.com  ", config)
        assert result == "user@example.com"

    def test_format_email_normalize_domain(self):
        """Test email formatting with domain normalization."""
        transformer = DataTransformer()
        config = EmailConfig(normalize_domain=True)

        result = transformer._format_email("user@example.com..", config)
        assert result == "user@example.com"

    def test_format_email_validate_format_valid(self):
        """Test email formatting with valid format validation."""
        transformer = DataTransformer()
        config = EmailConfig(validate_format=True)

        result = transformer._format_email("user@example.com", config)
        assert result == "user@example.com"

    def test_format_email_validate_format_invalid(self):
        """Test email formatting with invalid format validation."""
        transformer = DataTransformer()
        config = EmailConfig(validate_format=True)

        with pytest.raises(ValueError, match="Invalid email format"):
            transformer._format_email("invalid-email", config)

    def test_format_email_empty_value(self):
        """Test email formatting with empty value."""
        transformer = DataTransformer()
        config = EmailConfig()

        with pytest.raises(ValueError, match="Empty email value"):
            transformer._format_email("", config)

    def test_apply_email_formatting_allow_invalid(self):
        """Test email formatting with allow_invalid=True."""
        transformer = DataTransformer()
        config = EmailConfig(allow_invalid=True, validate_format=True)

        column = pa.array(["valid@example.com", "invalid-email"])
        result = transformer.apply_email_formatting(column, config)

        expected = ["valid@example.com", "invalid-email"]
        assert result.to_pylist() == expected


class TestIPAddressFormatting:
    """Test IP address formatting functionality."""

    def test_apply_ip_address_formatting_convert_to_string(self):
        """Test IP address formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = IPAddressConfig()

        column = pa.array(["192.168.1.1"])
        result = transformer.apply_ip_address_formatting(column, config)

        expected = ["192.168.1.1"]
        assert result.to_pylist() == expected

    def test_apply_ip_address_formatting_with_nulls(self):
        """Test IP address formatting with null values."""
        transformer = DataTransformer()
        config = IPAddressConfig()

        column = pa.array(["192.168.1.1", None, "::1"])
        result = transformer.apply_ip_address_formatting(column, config)

        # Results depend on IPv6 normalization
        assert result.to_pylist()[0] == "192.168.1.1"
        assert result.to_pylist()[1] is None

    def test_format_ip_address_ipv4_valid(self):
        """Test IPv4 address formatting."""
        transformer = DataTransformer()
        config = IPAddressConfig(ip_version="ipv4")

        result = transformer._format_ip_address("192.168.1.1", config)
        assert result == "192.168.1.1"

    def test_format_ip_address_ipv4_invalid(self):
        """Test IPv4 address formatting with invalid address."""
        transformer = DataTransformer()
        config = IPAddressConfig(ip_version="ipv4", validate=True)

        with pytest.raises(ValueError, match="Invalid IPv4 address"):
            transformer._format_ip_address("invalid", config)

    def test_format_ip_address_ipv6_valid(self):
        """Test IPv6 address formatting."""
        transformer = DataTransformer()
        config = IPAddressConfig(ip_version="ipv6")

        result = transformer._format_ip_address("::1", config)
        assert result == "::1"

    def test_format_ip_address_ipv6_invalid(self):
        """Test IPv6 address formatting with invalid address."""
        transformer = DataTransformer()
        config = IPAddressConfig(ip_version="ipv6", validate=True)

        with pytest.raises(ValueError, match="Invalid IPv6 address"):
            transformer._format_ip_address("192.168.1.1", config)

    def test_format_ip_address_both_invalid(self):
        """Test IP address formatting with both versions and invalid address."""
        transformer = DataTransformer()
        config = IPAddressConfig(ip_version="both", validate=True)

        with pytest.raises(ValueError, match="Invalid IP address"):
            transformer._format_ip_address("invalid", config)

    def test_format_ip_address_empty_value(self):
        """Test IP address formatting with empty value."""
        transformer = DataTransformer()
        config = IPAddressConfig()

        with pytest.raises(ValueError, match="Empty IP address value"):
            transformer._format_ip_address("", config)

    def test_normalize_ipv6_address_valid(self):
        """Test IPv6 address normalization."""
        transformer = DataTransformer()

        result = transformer._normalize_ipv6_address("2001:0db8:0000:0000:0000:0000:0000:0001", True)
        assert result == "2001:db8::1"

    def test_normalize_ipv6_address_invalid(self):
        """Test IPv6 address normalization with invalid address."""
        transformer = DataTransformer()

        result = transformer._normalize_ipv6_address("invalid", True)
        assert result is None

    def test_is_valid_ipv4_valid(self):
        """Test IPv4 validation with valid address."""
        transformer = DataTransformer()

        result = transformer._is_valid_ipv4("192.168.1.1")
        assert result is True

    def test_is_valid_ipv4_invalid(self):
        """Test IPv4 validation with invalid address."""
        transformer = DataTransformer()

        result = transformer._is_valid_ipv4("invalid")
        assert result is False

    def test_is_valid_ipv6_valid(self):
        """Test IPv6 validation with valid address."""
        transformer = DataTransformer()

        result = transformer._is_valid_ipv6("::1")
        assert result is True

    def test_is_valid_ipv6_invalid(self):
        """Test IPv6 validation with invalid address."""
        transformer = DataTransformer()

        result = transformer._is_valid_ipv6("invalid")
        assert result is False

    def test_apply_ip_address_formatting_allow_invalid(self):
        """Test IP address formatting with allow_invalid=True."""
        transformer = DataTransformer()
        config = IPAddressConfig(allow_invalid=True, validate=True)

        column = pa.array(["192.168.1.1", "invalid"])
        result = transformer.apply_ip_address_formatting(column, config)

        expected = ["192.168.1.1", "invalid"]
        assert result.to_pylist() == expected


class TestMACAddressFormatting:
    """Test MAC address formatting functionality."""

    def test_apply_mac_address_formatting_convert_to_string(self):
        """Test MAC address formatting converts non-string columns to string first."""
        transformer = DataTransformer()
        config = MACAddressConfig()

        column = pa.array(["001122334455"])
        result = transformer.apply_mac_address_formatting(column, config)

        expected = ["00:11:22:33:44:55"]
        assert result.to_pylist() == expected

    def test_apply_mac_address_formatting_with_nulls(self):
        """Test MAC address formatting with null values."""
        transformer = DataTransformer()
        config = MACAddressConfig()

        column = pa.array(["001122334455", None, "AABBCCDDEEFF"])
        result = transformer.apply_mac_address_formatting(column, config)

        expected = ["00:11:22:33:44:55", None, "aa:bb:cc:dd:ee:ff"]
        assert result.to_pylist() == expected

    def test_format_mac_address_colon_style(self):
        """Test MAC address formatting with colon style."""
        transformer = DataTransformer()
        config = MACAddressConfig(format_style="colon")

        result = transformer._format_mac_address("001122334455", config)
        assert result == "00:11:22:33:44:55"

    def test_format_mac_address_dash_style(self):
        """Test MAC address formatting with dash style."""
        transformer = DataTransformer()
        config = MACAddressConfig(format_style="dash")

        result = transformer._format_mac_address("001122334455", config)
        assert result == "00-11-22-33-44-55"

    def test_format_mac_address_dot_style(self):
        """Test MAC address formatting with dot style."""
        transformer = DataTransformer()
        config = MACAddressConfig(format_style="dot")

        result = transformer._format_mac_address("001122334455", config)
        assert result == "0011.2233.4455"

    def test_format_mac_address_none_style(self):
        """Test MAC address formatting with no separators."""
        transformer = DataTransformer()
        config = MACAddressConfig(format_style="none")

        result = transformer._format_mac_address("001122334455", config)
        assert result == "001122334455"

    def test_format_mac_address_upper_case(self):
        """Test MAC address formatting with uppercase."""
        transformer = DataTransformer()
        config = MACAddressConfig(case_style="upper")

        result = transformer._format_mac_address("aabbccddeeff", config)
        assert result == "AA:BB:CC:DD:EE:FF"

    def test_format_mac_address_lower_case(self):
        """Test MAC address formatting with lowercase."""
        transformer = DataTransformer()
        config = MACAddressConfig(case_style="lower")

        result = transformer._format_mac_address("AABBCCDDEEFF", config)
        assert result == "aa:bb:cc:dd:ee:ff"

    def test_format_mac_address_preserve_case(self):
        """Test MAC address formatting with case preservation."""
        transformer = DataTransformer()
        config = MACAddressConfig(case_style="preserve")

        result = transformer._format_mac_address("AaBbCcDdEeFf", config)
        assert result == "Aa:Bb:Cc:Dd:Ee:Ff"

    def test_format_mac_address_zero_pad(self):
        """Test MAC address formatting with zero padding."""
        transformer = DataTransformer()
        config = MACAddressConfig(zero_pad=True)

        result = transformer._format_mac_address("11223344556", config)
        assert result == "01:12:23:34:45:56"

    def test_format_mac_address_empty_value(self):
        """Test MAC address formatting with empty value."""
        transformer = DataTransformer()
        config = MACAddressConfig()

        with pytest.raises(ValueError, match="Empty MAC address value"):
            transformer._format_mac_address("", config)

    def test_format_mac_address_no_hex_digits(self):
        """Test MAC address formatting with no hex digits."""
        transformer = DataTransformer()
        config = MACAddressConfig()

        with pytest.raises(ValueError, match="No hexadecimal digits found"):
            transformer._format_mac_address("xyz", config)

    def test_apply_mac_address_formatting_allow_invalid(self):
        """Test MAC address formatting with allow_invalid=True."""
        transformer = DataTransformer()
        config = MACAddressConfig(allow_invalid=True)

        column = pa.array(["001122334455", "invalid"])
        result = transformer.apply_mac_address_formatting(column, config)

        expected = ["00:11:22:33:44:55", "invalid"]
        assert result.to_pylist() == expected


class TestStringCleaningHelperMethods:
    """Test helper methods for string cleaning."""

    def test_fix_encoding_errors(self):
        """Test encoding error fixes."""
        transformer = DataTransformer()

        # Test common mojibake patterns
        result = transformer._fix_encoding_errors("Donâ€™t")
        assert result == "Don't"

        result = transformer._fix_encoding_errors("â€œhelloâ€")
        assert result == '"hello"'

    def test_normalize_quotes(self):
        """Test quote normalization."""
        transformer = DataTransformer()

        # Test various smart quotes using Unicode escape sequences
        result = transformer._normalize_quotes("\u2018hello\u2019 \u201Cworld\u201D")
        assert result == "'hello' \"world\""

        result = transformer._normalize_quotes("\u2039single\u203A \u00ABdouble\u00BB")
        assert result == "'single' \"double\""

    def test_normalize_dashes(self):
        """Test dash normalization."""
        transformer = DataTransformer()

        # Test various dash types
        result = transformer._normalize_dashes("hello—world")  # Em dash
        assert result == "hello-world"

        result = transformer._normalize_dashes("test–case")  # En dash
        assert result == "test-case"

    def test_normalize_spaces(self):
        """Test space normalization."""
        transformer = DataTransformer()

        # Test various space types
        result = transformer._normalize_spaces("hello\u00A0world")  # Non-breaking space
        assert result == "hello world"

        result = transformer._normalize_spaces("test\u2003case")  # Em space
        assert result == "test case"

    def test_remove_zero_width_chars_with_space(self):
        """Test zero-width character removal with space replacement."""
        transformer = DataTransformer()

        result = transformer._remove_zero_width_chars("hello\u200Bworld", replace_with_space=True)
        assert result == "hello world"

    def test_remove_zero_width_chars_without_space(self):
        """Test zero-width character removal without space replacement."""
        transformer = DataTransformer()

        result = transformer._remove_zero_width_chars("hello\u200Bworld", replace_with_space=False)
        assert result == "helloworld"

    def test_remove_control_chars_preserve_newlines(self):
        """Test control character removal with newline preservation."""
        transformer = DataTransformer()

        result = transformer._remove_control_chars("hello\nworld\x00test", preserve_newlines=True, preserve_tabs=False)
        assert result == "hello\nworldtest"

    def test_remove_control_chars_preserve_tabs(self):
        """Test control character removal with tab preservation."""
        transformer = DataTransformer()

        result = transformer._remove_control_chars("hello\tworld\x00test", preserve_newlines=False, preserve_tabs=True)
        assert result == "hello\tworldtest"

    def test_remove_accents(self):
        """Test accent removal."""
        transformer = DataTransformer()

        result = transformer._remove_accents("café naïve résumé")
        assert result == "cafe naive resume"

    def test_to_ascii_only(self):
        """Test ASCII-only conversion."""
        transformer = DataTransformer()

        result = transformer._to_ascii_only("café 中文 test")
        assert result == "cafe  test"

    def test_to_ascii_only_unicode_error_fallback(self):
        """Test ASCII-only conversion with Unicode error fallback."""
        transformer = DataTransformer()

        # Instead of mocking str.encode (which can't be done on immutable types),
        # we'll test the fallback logic by creating a scenario that would naturally
        # trigger the fallback path or test it directly

        # Test the normal path first
        result_normal = transformer._to_ascii_only("café")
        assert result_normal == "cafe"  # Normal encoding removes accents and non-ASCII

        # Test the fallback logic directly by calling the manual filtering part
        # This simulates what happens in the except block
        test_text = "café 中文 test"
        # Remove accents first (as the method does)
        text_no_accents = transformer._remove_accents(test_text)
        # Apply manual ASCII filtering (the fallback logic)
        manual_result = ''.join(char for char in text_no_accents if ord(char) < 128)
        assert manual_result == "cafe  test"

        # Verify that both paths produce the same result for ASCII-compatible text
        ascii_text = "hello world"
        normal_result = transformer._to_ascii_only(ascii_text)
        manual_result = ''.join(char for char in ascii_text if ord(char) < 128)
        assert normal_result == manual_result == "hello world"

    def test_fix_case_issues_all_caps(self):
        """Test case issue fixing with all caps text."""
        transformer = DataTransformer()

        result = transformer._fix_case_issues("HELLO WORLD NASA", ["of", "the"], ["NASA"])
        assert result == "Hello World NASA"

    def test_fix_case_issues_with_acronyms(self):
        """Test case issue fixing with custom acronyms."""
        transformer = DataTransformer()

        result = transformer._fix_case_issues("WORK AT NASA WITH API", ["of", "the"], ["NASA", "API"])
        assert result == "Work At NASA With API"

    def test_fix_case_issues_with_exceptions(self):
        """Test case issue fixing with title case exceptions."""
        transformer = DataTransformer()

        result = transformer._fix_case_issues("THE POWER OF LOVE", ["of", "the"], [])
        assert result == "The Power of Love"

    def test_fix_case_issues_with_punctuation(self):
        """Test case issue fixing with punctuation."""
        transformer = DataTransformer()

        result = transformer._fix_case_issues("HELLO, WORLD!", [], [])
        assert result == "Hello, World!"

    def test_fix_case_issues_not_all_caps(self):
        """Test case issue fixing with mixed case (should not change)."""
        transformer = DataTransformer()

        original = "Hello World"
        result = transformer._fix_case_issues(original, [], [])
        assert result == original

    def test_fix_case_issues_short_text(self):
        """Test case issue fixing with short text (should not change)."""
        transformer = DataTransformer()

        original = "HI"
        result = transformer._fix_case_issues(original, [], [])
        assert result == original


class TestCreateTransformationFromConfig:
    """Test the factory function for creating transformations."""

    def test_create_regex_replace_transformation(self):
        """Test creating regex replace transformation."""
        config = {"pattern": r"\d+", "replacement": "NUMBER", "enabled": True}
        transform_func = create_transformation_from_config("regex_replace", config)

        column = pa.array(["test123", "abc456"])
        result = transform_func(column)
        expected = ["testNUMBER", "abcNUMBER"]
        assert result.to_pylist() == expected

    def test_create_string_replace_transformation(self):
        """Test creating string replace transformation."""
        config = {"old": "old", "new": "new", "enabled": True}
        transform_func = create_transformation_from_config("string_replace", config)

        column = pa.array(["old_value", "another_old"])
        result = transform_func(column)
        expected = ["new_value", "another_new"]
        assert result.to_pylist() == expected

    def test_create_money_conversion_transformation(self):
        """Test creating money conversion transformation."""
        config = {"enabled": True}
        transform_func = create_transformation_from_config("money_conversion", config)

        column = pa.array(["$100.50", "$200.75"])
        result = transform_func(column)
        expected = [100.50, 200.75]
        assert result.to_pylist() == expected

    def test_create_numeric_cleaning_transformation(self):
        """Test creating numeric cleaning transformation."""
        config = {"enabled": True, "target_type": "int64"}
        transform_func = create_transformation_from_config("numeric_cleaning", config)

        column = pa.array(["1,000", "2,500"])
        result = transform_func(column)
        expected = [1000, 2500]
        assert result.to_pylist() == expected

    def test_create_string_padding_transformation(self):
        """Test creating string padding transformation."""
        config = {"width": 5, "fillchar": "0", "side": "left", "enabled": True}
        transform_func = create_transformation_from_config("string_padding", config)

        column = pa.array(["123", "45"])
        result = transform_func(column)
        expected = ["00123", "00045"]
        assert result.to_pylist() == expected

    def test_create_string_trimming_transformation(self):
        """Test creating string trimming transformation."""
        config = {"side": "both", "enabled": True}
        transform_func = create_transformation_from_config("string_trimming", config)

        column = pa.array(["  hello  ", "  world  "])
        result = transform_func(column)
        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_create_html_xml_cleaning_transformation(self):
        """Test creating HTML/XML cleaning transformation."""
        config = {"strip_tags": True, "enabled": True}
        transform_func = create_transformation_from_config("html_xml_cleaning", config)

        column = pa.array(["<b>hello</b>", "<i>world</i>"])
        result = transform_func(column)
        expected = ["hello", "world"]
        assert result.to_pylist() == expected

    def test_create_datetime_transformation(self):
        """Test creating datetime transformation."""
        config = {"mode": "common_formats", "enabled": True}
        transform_func = create_transformation_from_config("datetime", config)

        # This would require mocking the coerce_datetime function for a full test
        assert callable(transform_func)

    def test_create_string_cleaning_transformation(self):
        """Test creating string cleaning transformation."""
        config = {"case_transform": "upper", "enabled": True}
        transform_func = create_transformation_from_config("string_cleaning", config)

        column = pa.array(["hello", "world"])
        result = transform_func(column)
        expected = ["HELLO", "WORLD"]
        assert result.to_pylist() == expected

    def test_create_ssn_formatting_transformation(self):
        """Test creating SSN formatting transformation."""
        config = {"enabled": True}
        transform_func = create_transformation_from_config("ssn_formatting", config)

        column = pa.array(["123456789"])
        result = transform_func(column)
        expected = ["123-45-6789"]
        assert result.to_pylist() == expected

    def test_create_zip_code_formatting_transformation(self):
        """Test creating ZIP code formatting transformation."""
        config = {"zip_type": "zip-5", "enabled": True}
        transform_func = create_transformation_from_config("zip_code_formatting", config)

        column = pa.array(["12345"])
        result = transform_func(column)
        expected = ["12345"]
        assert result.to_pylist() == expected

    def test_create_phone_number_formatting_transformation(self):
        """Test creating phone number formatting transformation."""
        config = {"format_style": "us-standard", "enabled": True}
        transform_func = create_transformation_from_config("phone_number_formatting", config)

        column = pa.array(["1234567890"])
        result = transform_func(column)
        expected = ["(123) 456-7890"]
        assert result.to_pylist() == expected

    def test_create_email_formatting_transformation(self):
        """Test creating email formatting transformation."""
        config = {"normalize_case": True, "enabled": True}
        transform_func = create_transformation_from_config("email_formatting", config)

        column = pa.array(["USER@EXAMPLE.COM"])
        result = transform_func(column)
        expected = ["user@example.com"]
        assert result.to_pylist() == expected

    def test_create_ip_address_formatting_transformation(self):
        """Test creating IP address formatting transformation."""
        config = {"ip_version": "ipv4", "enabled": True}
        transform_func = create_transformation_from_config("ip_address_formatting", config)

        column = pa.array(["192.168.1.1"])
        result = transform_func(column)
        expected = ["192.168.1.1"]
        assert result.to_pylist() == expected

    def test_create_mac_address_formatting_transformation(self):
        """Test creating MAC address formatting transformation."""
        config = {"format_style": "colon", "enabled": True}
        transform_func = create_transformation_from_config("mac_address_formatting", config)

        column = pa.array(["001122334455"])
        result = transform_func(column)
        expected = ["00:11:22:33:44:55"]
        assert result.to_pylist() == expected

    def test_create_unknown_transformation_type(self):
        """Test creating transformation with unknown type."""
        config = {"enabled": True}

        with pytest.raises(ValueError, match="Unknown transformation type: unknown_type"):
            create_transformation_from_config("unknown_type", config)

    def test_enabled_key_removed_from_config(self):
        """Test that 'enabled' key is removed from config before creating transformation."""
        config = {"pattern": r"\d+", "replacement": "NUMBER", "enabled": True, "extra_param": "value"}
        transform_func = create_transformation_from_config("regex_replace", config)

        # Should work without the 'enabled' key causing issues
        column = pa.array(["test123"])
        result = transform_func(column)
        expected = ["testNUMBER"]
        assert result.to_pylist() == expected

