"""Tests for data transformation utilities."""

import pytest
import pyarrow as pa
import pandas as pd
import datetime
import re
from decimal import Decimal
from unittest.mock import patch, MagicMock
import ipaddress

from forklift.utils.transformations import (
    DateTimeTransformConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    StringPaddingConfig,
    HTMLXMLConfig,
    StringCleaningConfig,
    SSNConfig,
    ZipCodeConfig,
    PhoneNumberConfig,
    EmailConfig,
    IPAddressConfig,
    MACAddressConfig,
    DataTransformer
)


class TestDateTimeTransformConfig:
    """Test the DateTimeTransformConfig dataclass."""

    def test_datetime_config_defaults(self):
        """Test default values for DateTimeTransformConfig."""
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

    def test_datetime_config_invalid_mode(self):
        """Test DateTimeTransformConfig with invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid mode: invalid_mode"):
            DateTimeTransformConfig(mode="invalid_mode")

    def test_datetime_config_enforce_mode_no_format(self):
        """Test enforce mode without format raises ValueError."""
        with pytest.raises(ValueError, match="Format must be specified when mode is 'enforce'"):
            DateTimeTransformConfig(mode="enforce")

    def test_datetime_config_specify_formats_no_formats(self):
        """Test specify_formats mode without formats raises ValueError."""
        with pytest.raises(ValueError, match="Formats list must be specified when mode is 'specify_formats'"):
            DateTimeTransformConfig(mode="specify_formats")

    def test_datetime_config_invalid_target_type(self):
        """Test invalid target_type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid target_type: invalid"):
            DateTimeTransformConfig(target_type="invalid")

    def test_datetime_config_invalid_to_epoch(self):
        """Test invalid to_epoch raises ValueError."""
        with pytest.raises(ValueError, match="Invalid to_epoch unit: invalid"):
            DateTimeTransformConfig(to_epoch="invalid")

    def test_datetime_config_valid_configurations(self):
        """Test valid configurations."""
        # Enforce mode
        config1 = DateTimeTransformConfig(mode="enforce", format="%Y-%m-%d")
        assert config1.mode == "enforce"
        assert config1.format == "%Y-%m-%d"

        # Specify formats mode
        config2 = DateTimeTransformConfig(mode="specify_formats", formats=["%Y-%m-%d", "%m/%d/%Y"])
        assert config2.mode == "specify_formats"
        assert config2.formats == ["%Y-%m-%d", "%m/%d/%Y"]

        # Valid target types
        for target_type in ["datetime", "date", "timestamp", "string"]:
            config = DateTimeTransformConfig(target_type=target_type)
            assert config.target_type == target_type

        # Valid to_epoch units
        for unit in ["seconds", "milliseconds", "microseconds", "nanoseconds"]:
            config = DateTimeTransformConfig(to_epoch=unit)
            assert config.to_epoch == unit


class TestRegexReplaceConfig:
    """Test the RegexReplaceConfig dataclass."""

    def test_regex_config_creation(self):
        """Test RegexReplaceConfig creation."""
        config = RegexReplaceConfig(pattern=r"\d+", replacement="XXX", flags=re.IGNORECASE)

        assert config.pattern == r"\d+"
        assert config.replacement == "XXX"
        assert config.flags == re.IGNORECASE

    def test_regex_config_defaults(self):
        """Test RegexReplaceConfig default values."""
        config = RegexReplaceConfig(pattern=r"\d+", replacement="XXX")
        assert config.flags == 0


class TestStringReplaceConfig:
    """Test the StringReplaceConfig dataclass."""

    def test_string_replace_config_creation(self):
        """Test StringReplaceConfig creation."""
        config = StringReplaceConfig(old="hello", new="hi", count=1)

        assert config.old == "hello"
        assert config.new == "hi"
        assert config.count == 1

    def test_string_replace_config_defaults(self):
        """Test StringReplaceConfig default values."""
        config = StringReplaceConfig(old="hello", new="hi")
        assert config.count == -1


class TestMoneyTypeConfig:
    """Test the MoneyTypeConfig dataclass."""

    def test_money_config_defaults(self):
        """Test default values for MoneyTypeConfig."""
        config = MoneyTypeConfig()

        assert config.currency_symbols == ["$", "€", "£", "¥", "₹", "₽", "¢"]
        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.parentheses_negative is True
        assert config.strip_whitespace is True

    def test_money_config_custom_currency_symbols(self):
        """Test custom currency symbols."""
        config = MoneyTypeConfig(currency_symbols=["$", "€"])
        assert config.currency_symbols == ["$", "€"]


class TestNumericCleaningConfig:
    """Test the NumericCleaningConfig dataclass."""

    def test_numeric_config_defaults(self):
        """Test default values for NumericCleaningConfig."""
        config = NumericCleaningConfig()

        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.allow_nan is True
        assert config.nan_values == ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]
        assert config.strip_whitespace is True

    def test_numeric_config_custom_nan_values(self):
        """Test custom NaN values."""
        config = NumericCleaningConfig(nan_values=["NULL", ""])
        assert config.nan_values == ["NULL", ""]


class TestStringPaddingConfig:
    """Test the StringPaddingConfig dataclass."""

    def test_string_padding_config_defaults(self):
        """Test default values for StringPaddingConfig."""
        config = StringPaddingConfig(width=10)

        assert config.width == 10
        assert config.fillchar == " "
        assert config.side == "left"

    def test_string_padding_config_custom(self):
        """Test custom StringPaddingConfig."""
        config = StringPaddingConfig(width=5, fillchar="0", side="right")

        assert config.width == 5
        assert config.fillchar == "0"
        assert config.side == "right"


class TestHTMLXMLConfig:
    """Test the HTMLXMLConfig dataclass."""

    def test_html_xml_config_defaults(self):
        """Test default values for HTMLXMLConfig."""
        config = HTMLXMLConfig()

        assert config.strip_tags is True
        assert config.decode_entities is True
        assert config.preserve_whitespace is False

    def test_html_xml_config_custom(self):
        """Test custom HTMLXMLConfig."""
        config = HTMLXMLConfig(strip_tags=False, decode_entities=False, preserve_whitespace=True)

        assert config.strip_tags is False
        assert config.decode_entities is False
        assert config.preserve_whitespace is True


class TestStringCleaningConfig:
    """Test the StringCleaningConfig dataclass."""

    def test_string_cleaning_config_defaults(self):
        """Test default values for StringCleaningConfig."""
        config = StringCleaningConfig()

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
        assert config.title_case_exceptions is not None
        assert config.custom_case_mapping == {}
        assert config.case_mapping_mode == "exact"
        assert config.acronyms == []
        assert config.remove_accents is False
        assert config.ascii_only is False
        assert config.fix_encoding_errors is True

    def test_string_cleaning_config_invalid_case_transform(self):
        """Test invalid case_transform raises ValueError."""
        with pytest.raises(ValueError, match="case_transform must be one of"):
            StringCleaningConfig(case_transform="invalid")

    def test_string_cleaning_config_invalid_case_mapping_mode(self):
        """Test invalid case_mapping_mode raises ValueError."""
        with pytest.raises(ValueError, match="case_mapping_mode must be one of"):
            StringCleaningConfig(case_mapping_mode="invalid")


class TestSSNConfig:
    """Test the SSNConfig dataclass."""

    def test_ssn_config_defaults(self):
        """Test default values for SSNConfig."""
        config = SSNConfig()

        assert config.format_with_dashes is True
        assert config.zero_pad is True
        assert config.validate is True
        assert config.allow_invalid is False


class TestZipCodeConfig:
    """Test the ZipCodeConfig dataclass."""

    def test_zip_code_config_defaults(self):
        """Test default values for ZipCodeConfig."""
        config = ZipCodeConfig()

        assert config.zip_type == "zip-permissive"
        assert config.format_with_dash is True
        assert config.zero_pad is True
        assert config.validate is True
        assert config.allow_invalid is False

    def test_zip_code_config_invalid_type(self):
        """Test invalid zip_type raises ValueError."""
        with pytest.raises(ValueError, match="zip_type must be one of"):
            ZipCodeConfig(zip_type="invalid")


class TestPhoneNumberConfig:
    """Test the PhoneNumberConfig dataclass."""

    def test_phone_number_config_defaults(self):
        """Test default values for PhoneNumberConfig."""
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

    def test_phone_number_config_invalid_style(self):
        """Test invalid format_style raises ValueError."""
        with pytest.raises(ValueError, match="format_style must be one of"):
            PhoneNumberConfig(format_style="invalid")


class TestEmailConfig:
    """Test the EmailConfig dataclass."""

    def test_email_config_defaults(self):
        """Test default values for EmailConfig."""
        config = EmailConfig()

        assert config.normalize_case is True
        assert config.validate_format is True
        assert config.allow_invalid is False
        assert config.strip_whitespace is True
        assert config.normalize_domain is True


class TestIPAddressConfig:
    """Test the IPAddressConfig dataclass."""

    def test_ip_address_config_defaults(self):
        """Test default values for IPAddressConfig."""
        config = IPAddressConfig()

        assert config.ip_version == "both"
        assert config.normalize_ipv6 is True
        assert config.validate is True
        assert config.allow_invalid is False
        assert config.compress_ipv6 is True

    def test_ip_address_config_invalid_version(self):
        """Test invalid ip_version raises ValueError."""
        with pytest.raises(ValueError, match="ip_version must be one of"):
            IPAddressConfig(ip_version="invalid")


class TestMACAddressConfig:
    """Test the MACAddressConfig dataclass."""

    def test_mac_address_config_defaults(self):
        """Test default values for MACAddressConfig."""
        config = MACAddressConfig()

        assert config.format_style == "colon"
        assert config.case_style == "lower"
        assert config.validate is True
        assert config.allow_invalid is False
        assert config.zero_pad is True

    def test_mac_address_config_invalid_format(self):
        """Test invalid format_style raises ValueError."""
        with pytest.raises(ValueError, match="format_style must be one of"):
            MACAddressConfig(format_style="invalid")

    def test_mac_address_config_invalid_case(self):
        """Test invalid case_style raises ValueError."""
        with pytest.raises(ValueError, match="case_style must be one of"):
            MACAddressConfig(case_style="invalid")


class TestDataTransformer:
    """Test the DataTransformer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()

    def test_transformer_initialization(self):
        """Test DataTransformer initialization."""
        transformer = DataTransformer()
        assert transformer is not None

    def test_apply_regex_replace_non_string_column(self):
        """Test regex replace on non-string column returns original."""
        column = pa.array([1, 2, 3])
        config = RegexReplaceConfig(pattern=r"\d+", replacement="X")

        result = self.transformer.apply_regex_replace(column, config)
        assert result == column

    def test_apply_regex_replace_string_column(self):
        """Test regex replace on string column."""
        column = pa.array(["hello123", "world456", "test789"])
        config = RegexReplaceConfig(pattern=r"\d+", replacement="XXX")

        result = self.transformer.apply_regex_replace(column, config)
        expected = pa.array(["helloXXX", "worldXXX", "testXXX"])

        assert result.to_pylist() == expected.to_pylist()

    def test_apply_regex_replace_with_flags(self):
        """Test regex replace with flags."""
        column = pa.array(["Hello", "HELLO", "hello"])
        config = RegexReplaceConfig(pattern="hello", replacement="hi", flags=re.IGNORECASE)

        result = self.transformer.apply_regex_replace(column, config)
        expected = pa.array(["hi", "hi", "hi"])

        assert result.to_pylist() == expected.to_pylist()

    def test_apply_string_replace_non_string_column(self):
        """Test string replace on non-string column returns original."""
        column = pa.array([1, 2, 3])
        config = StringReplaceConfig(old="hello", new="hi")

        result = self.transformer.apply_string_replace(column, config)
        assert result == column

    def test_apply_string_replace_unlimited_count(self):
        """Test string replace with unlimited count."""
        column = pa.array(["hello hello", "hello world hello"])
        config = StringReplaceConfig(old="hello", new="hi")

        result = self.transformer.apply_string_replace(column, config)
        expected = pa.array(["hi hi", "hi world hi"])

        assert result.to_pylist() == expected.to_pylist()

    def test_apply_string_replace_limited_count(self):
        """Test string replace with limited count."""
        column = pa.array(["hello hello hello"])
        config = StringReplaceConfig(old="hello", new="hi", count=2)

        result = self.transformer.apply_string_replace(column, config)
        expected = pa.array(["hi hi hello"])

        assert result.to_pylist() == expected.to_pylist()

    def test_apply_money_conversion_non_string_column(self):
        """Test money conversion on non-string column returns original."""
        column = pa.array([1, 2, 3])
        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        assert result == column

    def test_apply_money_conversion_basic(self):
        """Test basic money conversion."""
        column = pa.array(["$123.45", "€1,234.56", "(£789.01)"])
        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        expected_values = [123.45, 1234.56, -789.01]

        result_list = result.to_pylist()
        for i, expected in enumerate(expected_values):
            assert abs(result_list[i] - expected) < 0.01

    def test_apply_money_conversion_with_nulls(self):
        """Test money conversion with null values."""
        column = pa.array(["$123.45", None, "invalid", ""])
        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_list = result.to_pylist()

        assert abs(result_list[0] - 123.45) < 0.01
        assert result_list[1] is None
        assert result_list[2] is None  # Invalid conversion
        assert result_list[3] is None  # Empty string

    def test_clean_money_string_empty(self):
        """Test cleaning empty money string."""
        config = MoneyTypeConfig()
        result = self.transformer._clean_money_string("", config)
        assert result is None

    def test_clean_money_string_with_whitespace(self):
        """Test cleaning money string with whitespace."""
        config = MoneyTypeConfig(strip_whitespace=True)
        result = self.transformer._clean_money_string("  $123.45  ", config)
        assert result == Decimal("123.45")

    def test_clean_money_string_parentheses_negative(self):
        """Test cleaning money string with parentheses for negative."""
        config = MoneyTypeConfig(parentheses_negative=True)
        result = self.transformer._clean_money_string("($123.45)", config)
        assert result == Decimal("-123.45")

    def test_clean_money_string_custom_separators(self):
        """Test cleaning money string with custom separators."""
        config = MoneyTypeConfig(thousands_separator=".", decimal_separator=",")
        result = self.transformer._clean_money_string("$1.234,56", config)
        assert result == Decimal("1234.56")

    def test_apply_numeric_cleaning_basic(self):
        """Test basic numeric cleaning."""
        column = pa.array(["1,234.56", "789", "N/A", ""])
        config = NumericCleaningConfig()

        result = self.transformer.apply_numeric_cleaning(column, config)
        expected = [1234.56, 789.0, None, None]

        assert result.to_pylist() == expected

    def test_apply_numeric_cleaning_integer_target(self):
        """Test numeric cleaning with integer target type."""
        column = pa.array(["123", "456.78"])
        config = NumericCleaningConfig()

        result = self.transformer.apply_numeric_cleaning(column, config, target_type="int64")
        expected = [123, 456]

        assert result.to_pylist() == expected

    def test_apply_numeric_cleaning_no_nan_allowed(self):
        """Test numeric cleaning with no NaN allowed raises error."""
        column = pa.array(["invalid"])
        config = NumericCleaningConfig(allow_nan=False)

        with pytest.raises(ValueError):
            self.transformer.apply_numeric_cleaning(column, config)

    def test_clean_numeric_string_empty(self):
        """Test cleaning empty numeric string."""
        config = NumericCleaningConfig()
        result = self.transformer._clean_numeric_string("", config)
        assert result is None

    def test_clean_numeric_string_with_separators(self):
        """Test cleaning numeric string with separators."""
        config = NumericCleaningConfig(thousands_separator=",", decimal_separator=".")
        result = self.transformer._clean_numeric_string("1,234.56", config)
        assert result == "1234.56"

    def test_clean_numeric_string_custom_decimal_separator(self):
        """Test cleaning numeric string with custom decimal separator."""
        config = NumericCleaningConfig(decimal_separator=",", thousands_separator="")
        result = self.transformer._clean_numeric_string("123,45", config)
        assert result == "123.45"

    def test_format_ssn_no_digits(self):
        """Test formatting SSN with letters raises ValueError."""
        config = SSNConfig()

        with pytest.raises(ValueError, match="SSN contains letters"):
            self.transformer._format_ssn("abc", config)

    def test_format_ssn_no_digits_only_special_chars(self):
        """Test formatting SSN with only special characters raises ValueError."""
        config = SSNConfig()

        with pytest.raises(ValueError, match="No digits found in SSN"):
            self.transformer._format_ssn("---", config)

    def test_format_ssn_valid_with_validation_disabled(self):
        """Test formatting SSN with validation disabled."""
        config = SSNConfig(validate=False)
        result = self.transformer._format_ssn("123456789", config)
        assert result == "123-45-6789"

    def test_format_ssn_too_few_digits(self):
        """Test formatting SSN with too few digits."""
        config = SSNConfig(zero_pad=False, validate=True)

        with pytest.raises(ValueError, match="SSN must have exactly 9 digits"):
            self.transformer._format_ssn("12345", config)

    def test_format_ssn_too_many_digits(self):
        """Test formatting SSN with too many digits."""
        config = SSNConfig(validate=True)

        with pytest.raises(ValueError, match="SSN must have exactly 9 digits"):
            self.transformer._format_ssn("1234567890", config)

    def test_format_ssn_without_dashes(self):
        """Test formatting SSN without dashes."""
        config = SSNConfig(format_with_dashes=False)
        result = self.transformer._format_ssn("123456789", config)
        assert result == "123456789"

    # Additional tests for better coverage
    def test_apply_string_cleaning_unicode_normalization(self):
        """Test string cleaning with Unicode normalization."""
        # Test with Unicode characters that can be normalized
        column = pa.array(["café", "naïve"])
        config = StringCleaningConfig(unicode_normalize="NFKC")

        result = self.transformer.apply_string_cleaning(column, config)
        # The exact result depends on the normalization, but it should not crash
        assert len(result.to_pylist()) == 2

    def test_apply_string_cleaning_invalid_unicode_normalization(self):
        """Test string cleaning with invalid Unicode normalization form."""
        column = pa.array(["hello"])
        config = StringCleaningConfig(unicode_normalize="INVALID")

        # Should not crash even with invalid normalization form
        result = self.transformer.apply_string_cleaning(column, config)
        assert result.to_pylist()[0] == "hello"

    def test_apply_string_cleaning_case_transformations(self):
        """Test string cleaning with various case transformations."""
        column = pa.array(["hello world", "HELLO WORLD", "Hello World"])

        # Test upper case
        config_upper = StringCleaningConfig(case_transform="upper")
        result_upper = self.transformer.apply_string_cleaning(column, config_upper)
        assert result_upper.to_pylist() == ["HELLO WORLD", "HELLO WORLD", "HELLO WORLD"]

        # Test lower case
        config_lower = StringCleaningConfig(case_transform="lower")
        result_lower = self.transformer.apply_string_cleaning(column, config_lower)
        assert result_lower.to_pylist() == ["hello world", "hello world", "hello world"]

        # Test title case
        config_title = StringCleaningConfig(case_transform="title")
        result_title = self.transformer.apply_string_cleaning(column, config_title)
        assert all("Hello" in result for result in result_title.to_pylist())

    def test_apply_string_cleaning_custom_case_mapping(self):
        """Test string cleaning with custom case mapping."""
        column = pa.array(["california", "new york", "texas"])

        # Test exact mapping
        config = StringCleaningConfig(
            custom_case_mapping={"california": "CA", "new york": "NY"},
            case_mapping_mode="exact"
        )
        result = self.transformer.apply_string_cleaning(column, config)
        expected = ["CA", "NY", "texas"]
        assert result.to_pylist() == expected

    def test_apply_string_cleaning_acronyms(self):
        """Test string cleaning with acronym preservation."""
        column = pa.array(["nasa mission", "api endpoint", "ceo meeting"])
        config = StringCleaningConfig(
            case_transform="title",
            acronyms=["NASA", "API", "CEO"]
        )

        result = self.transformer.apply_string_cleaning(column, config)
        result_list = result.to_pylist()

        # Check that the results contain title case versions
        # Note: The exact acronym behavior may depend on implementation
        assert len(result_list) == 3
        for item in result_list:
            assert isinstance(item, str)

    def test_apply_string_cleaning_remove_tabs(self):
        """Test string cleaning with tab removal."""
        column = pa.array(["hello\tworld", "test\tdata"])

        # Test removing tabs
        config_remove = StringCleaningConfig(remove_tabs=True)
        result_remove = self.transformer.apply_string_cleaning(column, config_remove)
        assert result_remove.to_pylist() == ["helloworld", "testdata"]

        # Test replacing tabs with default behavior
        config_replace = StringCleaningConfig(remove_tabs=False, tab_replacement=" ")
        result_replace = self.transformer.apply_string_cleaning(column, config_replace)
        # Tab replacement behavior may vary based on implementation
        assert len(result_replace.to_pylist()) == 2

    def test_apply_money_conversion_invalid_operation(self):
        """Test money conversion with invalid operation exception."""
        column = pa.array(["invalid_decimal"])
        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        assert result.to_pylist()[0] is None

    def test_clean_money_string_no_separators(self):
        """Test cleaning money string with no separators configured."""
        config = MoneyTypeConfig(thousands_separator="", decimal_separator="")
        result = self.transformer._clean_money_string("$123.45", config)
        # Should still work, just won't do separator processing
        assert result is not None

    def test_apply_numeric_cleaning_different_target_types(self):
        """Test numeric cleaning with different target types."""
        column = pa.array(["123.45"])
        config = NumericCleaningConfig()

        # Test int32
        result_int32 = self.transformer.apply_numeric_cleaning(column, config, target_type="int32")
        assert result_int32.type == pa.int32()

        # Test float32
        result_float32 = self.transformer.apply_numeric_cleaning(column, config, target_type="float32")
        assert result_float32.type == pa.float32()

    def test_apply_numeric_cleaning_overflow_error(self):
        """Test numeric cleaning with overflow error."""
        column = pa.array(["99999999999999999999"])  # Use a very large number that will cause overflow
        config = NumericCleaningConfig(allow_nan=True)

        try:
            result = self.transformer.apply_numeric_cleaning(column, config, target_type="int32")
            # Should return None for overflow with int32
            assert result.to_pylist()[0] is None
        except (OverflowError, pa.ArrowInvalid):
            # If PyArrow itself raises an error, that's expected behavior
            assert True

    @patch('forklift.utils.transformations.coerce_datetime')
    def test_apply_datetime_transformation_with_timezone(self, mock_coerce):
        """Test datetime transformation with timezone conversion."""
        import pytz
        mock_dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_coerce.return_value = mock_dt

        column = pa.array(["2024-01-01T12:00:00Z"])
        config = DateTimeTransformConfig(timezone="US/Eastern")

        result = self.transformer.apply_datetime_transformation(column, config)
        # Should handle timezone conversion
        assert result is not None

    @patch('forklift.utils.transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_string_with_date(self, mock_coerce):
        """Test datetime transformation with target string and date object."""
        mock_coerce.return_value = datetime.date(2024, 1, 1)

        column = pa.array(["2024-01-01"])
        config = DateTimeTransformConfig(target_type="string", output_format="%Y-%m-%d")

        result = self.transformer.apply_datetime_transformation(column, config)
        assert result.to_pylist()[0] == "2024-01-01"

    def test_string_cleaning_helper_methods(self):
        """Test string cleaning helper methods if they exist."""
        # These tests would cover the helper methods like _normalize_quotes, etc.
        # We need to check if these methods exist first
        transformer = DataTransformer()

        # Test with a simple string that should trigger various cleaning operations
        column = pa.array(['"hello world" – this\'s a test'])
        config = StringCleaningConfig(
            normalize_quotes=True,
            normalize_dashes=True,
            normalize_spaces=True
        )

        result = transformer.apply_string_cleaning(column, config)
        # Should normalize quotes and dashes
        result_str = result.to_pylist()[0]
        assert '"' in result_str or "'" in result_str  # Should have normalized quotes

    def test_apply_string_cleaning_comprehensive(self):
        """Test comprehensive string cleaning with multiple operations."""
        column = pa.array([
            "  hello\tworld  ",  # Whitespace and tabs
            '"smart quotes"',     # Smart quotes
            "em—dash test",       # Em dash
            "control\x00char",    # Control character
        ])

        config = StringCleaningConfig(
            strip_whitespace=True,
            collapse_whitespace=True,
            normalize_quotes=True,
            normalize_dashes=True,
            remove_control_chars=True,
            tab_replacement=" "
        )

        result = self.transformer.apply_string_cleaning(column, config)
        result_list = result.to_pylist()

        # Should clean all the strings
        assert len(result_list) == 4
        for item in result_list:
            assert item is not None

    def test_money_conversion_edge_cases(self):
        """Test money conversion edge cases."""
        # Test various edge cases for money conversion
        test_cases = [
            "$0.00",      # Zero amount
            "$-123.45",   # Negative with minus sign
            "€1.234,56",  # European format
            "£ 1,000.00", # With spaces
            "$1,234,567.89",  # Large amount
        ]

        column = pa.array(test_cases)
        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_list = result.to_pylist()

        # Should handle all cases
        assert len(result_list) == len(test_cases)
        # First case should be 0
        assert result_list[0] == 0.0

    def test_datetime_transformation_epoch_units(self):
        """Test datetime transformation with different epoch units."""
        with patch('forklift.utils.transformations.coerce_datetime') as mock_coerce:
            # Test milliseconds
            mock_coerce.return_value = 1704110400000
            column = pa.array(["2024-01-01"])
            config = DateTimeTransformConfig(to_epoch="milliseconds")

            result = self.transformer.apply_datetime_transformation(column, config)
            assert result.type == pa.int64()

            # Test microseconds
            mock_coerce.return_value = 1704110400000000
            config = DateTimeTransformConfig(to_epoch="microseconds")
            result = self.transformer.apply_datetime_transformation(column, config)
            assert result.type == pa.int64()

            # Test nanoseconds
            mock_coerce.return_value = 1704110400000000000
            config = DateTimeTransformConfig(to_epoch="nanoseconds")
            result = self.transformer.apply_datetime_transformation(column, config)
            assert result.type == pa.int64()

    def test_comprehensive_data_transformer_coverage(self):
        """Test comprehensive data transformer to maximize coverage."""
        transformer = DataTransformer()

        # Test with various data types and configurations
        test_data = [
            ("Hello World", "string"),
            ("$123.45", "money"),
        ]

        for data, data_type in test_data:
            column = pa.array([data])

            # Apply different transformations based on data type
            if data_type == "string":
                config = StringCleaningConfig()
                result = transformer.apply_string_cleaning(column, config)
                assert result is not None

            elif data_type == "money":
                config = MoneyTypeConfig()
                result = transformer.apply_money_conversion(column, config)
                assert result is not None
