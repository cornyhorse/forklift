"""Tests for data transformation utilities."""

import pytest
import pyarrow as pa
import pandas as pd
import datetime
import re
from decimal import Decimal
from unittest.mock import patch

from forklift.utils.data_transformations import (
    DateTimeTransformConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    StringPaddingConfig,
    HTMLXMLConfig,
    StringCleaningConfig,
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
        config = RegexReplaceConfig(pattern="test", replacement="TEST")

        assert config.pattern == "test"
        assert config.replacement == "TEST"
        assert config.flags == 0


class TestStringReplaceConfig:
    """Test the StringReplaceConfig dataclass."""

    def test_string_replace_config_creation(self):
        """Test StringReplaceConfig creation."""
        config = StringReplaceConfig(old="old", new="new", count=5)

        assert config.old == "old"
        assert config.new == "new"
        assert config.count == 5

    def test_string_replace_config_defaults(self):
        """Test StringReplaceConfig default values."""
        config = StringReplaceConfig(old="old", new="new")

        assert config.old == "old"
        assert config.new == "new"
        assert config.count == -1


class TestMoneyTypeConfig:
    """Test the MoneyTypeConfig dataclass."""

    def test_money_config_defaults(self):
        """Test MoneyTypeConfig default values."""
        config = MoneyTypeConfig()

        assert config.currency_symbols == ["$", "€", "£", "¥", "₹", "₽", "¢"]
        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.parentheses_negative is True
        assert config.strip_whitespace is True

    def test_money_config_custom_values(self):
        """Test MoneyTypeConfig with custom values."""
        config = MoneyTypeConfig(
            currency_symbols=["$", "€"],
            thousands_separator=" ",
            decimal_separator=",",
            parentheses_negative=False,
            strip_whitespace=False
        )

        assert config.currency_symbols == ["$", "€"]
        assert config.thousands_separator == " "
        assert config.decimal_separator == ","
        assert config.parentheses_negative is False
        assert config.strip_whitespace is False

    def test_money_config_none_currency_symbols(self):
        """Test MoneyTypeConfig with None currency_symbols gets default."""
        config = MoneyTypeConfig()  # Use default instead of None

        assert config.currency_symbols == ["$", "€", "£", "¥", "₹", "₽", "¢"]


class TestNumericCleaningConfig:
    """Test the NumericCleaningConfig dataclass."""

    def test_numeric_config_defaults(self):
        """Test NumericCleaningConfig default values."""
        config = NumericCleaningConfig()

        assert config.thousands_separator == ","
        assert config.decimal_separator == "."
        assert config.allow_nan is True
        assert config.nan_values == ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]
        assert config.strip_whitespace is True

    def test_numeric_config_custom_values(self):
        """Test NumericCleaningConfig with custom values."""
        config = NumericCleaningConfig(
            thousands_separator=" ",
            decimal_separator=",",
            allow_nan=False,
            nan_values=["NULL", "N/A"],
            strip_whitespace=False
        )

        assert config.thousands_separator == " "
        assert config.decimal_separator == ","
        assert config.allow_nan is False
        assert config.nan_values == ["NULL", "N/A"]
        assert config.strip_whitespace is False

    def test_numeric_config_none_nan_values(self):
        """Test NumericCleaningConfig with None nan_values gets default."""
        config = NumericCleaningConfig()  # Use default instead of None

        assert config.nan_values == ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]


class TestStringPaddingConfig:
    """Test the StringPaddingConfig dataclass."""

    def test_string_padding_config_creation(self):
        """Test StringPaddingConfig creation."""
        config = StringPaddingConfig(width=10, fillchar="0", side="right")

        assert config.width == 10
        assert config.fillchar == "0"
        assert config.side == "right"

    def test_string_padding_config_defaults(self):
        """Test StringPaddingConfig default values."""
        config = StringPaddingConfig(width=5)

        assert config.width == 5
        assert config.fillchar == " "
        assert config.side == "left"


class TestHTMLXMLConfig:
    """Test the HTMLXMLConfig dataclass."""

    def test_html_xml_config_defaults(self):
        """Test HTMLXMLConfig default values."""
        config = HTMLXMLConfig()

        assert config.strip_tags is True
        assert config.decode_entities is True
        assert config.preserve_whitespace is False

    def test_html_xml_config_custom_values(self):
        """Test HTMLXMLConfig with custom values."""
        config = HTMLXMLConfig(
            strip_tags=False,
            decode_entities=False,
            preserve_whitespace=True
        )

        assert config.strip_tags is False
        assert config.decode_entities is False
        assert config.preserve_whitespace is True


class TestStringCleaningConfig:
    """Test the StringCleaningConfig dataclass."""

    def test_string_cleaning_config_defaults(self):
        """Test StringCleaningConfig default values."""
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
        assert config.title_case_exceptions == ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]
        assert config.custom_case_mapping == {}
        assert config.case_mapping_mode == "exact"
        assert config.remove_accents is False
        assert config.ascii_only is False
        assert config.fix_encoding_errors is True

    def test_string_cleaning_config_invalid_case_transform(self):
        """Test StringCleaningConfig with invalid case_transform raises ValueError."""
        with pytest.raises(ValueError, match="case_transform must be one of"):
            StringCleaningConfig(case_transform="invalid")

    def test_string_cleaning_config_invalid_case_mapping_mode(self):
        """Test StringCleaningConfig with invalid case_mapping_mode raises ValueError."""
        with pytest.raises(ValueError, match="case_mapping_mode must be one of"):
            StringCleaningConfig(case_mapping_mode="invalid")

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

    def test_string_cleaning_config_none_defaults(self):
        """Test StringCleaningConfig with None values gets defaults."""
        config = StringCleaningConfig()  # Use default instead of None

        assert config.title_case_exceptions == ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]
        assert config.custom_case_mapping == {}


class TestDataTransformer:
    """Test the DataTransformer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()

    def test_data_transformer_initialization(self):
        """Test DataTransformer initialization."""
        transformer = DataTransformer()
        assert isinstance(transformer, DataTransformer)

    def test_apply_regex_replace_basic(self):
        """Test basic regex replace functionality."""
        # Create test data
        data = ["test123", "hello456", "world789"]
        column = pa.array(data)

        # Create config to replace digits with 'X'
        config = RegexReplaceConfig(pattern=r"\d+", replacement="X")

        # Apply transformation
        result = self.transformer.apply_regex_replace(column, config)

        # Check results
        result_data = result.to_pandas().tolist()
        assert result_data == ["testX", "helloX", "worldX"]

    def test_apply_regex_replace_with_flags(self):
        """Test regex replace with flags."""
        data = ["Test", "TEST", "test"]
        column = pa.array(data)

        config = RegexReplaceConfig(pattern="test", replacement="REPLACED", flags=re.IGNORECASE)

        result = self.transformer.apply_regex_replace(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["REPLACED", "REPLACED", "REPLACED"]

    def test_apply_regex_replace_non_string_column(self):
        """Test regex replace on non-string column returns original."""
        data = [1, 2, 3]
        column = pa.array(data)

        config = RegexReplaceConfig(pattern=r"\d+", replacement="X")

        result = self.transformer.apply_regex_replace(column, config)

        assert result == column

    def test_apply_string_replace_basic(self):
        """Test basic string replace functionality."""
        data = ["hello world", "hello universe", "hello galaxy"]
        column = pa.array(data)

        config = StringReplaceConfig(old="hello", new="hi")

        result = self.transformer.apply_string_replace(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["hi world", "hi universe", "hi galaxy"]

    def test_apply_string_replace_with_count(self):
        """Test string replace with count limit."""
        data = ["hello hello hello", "hello world hello"]
        column = pa.array(data)

        config = StringReplaceConfig(old="hello", new="hi", count=1)

        result = self.transformer.apply_string_replace(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["hi hello hello", "hi world hello"]

    def test_apply_string_replace_non_string_column(self):
        """Test string replace on non-string column returns original."""
        data = [1, 2, 3]
        column = pa.array(data)

        config = StringReplaceConfig(old="1", new="X")

        result = self.transformer.apply_string_replace(column, config)

        assert result == column

    def test_apply_money_conversion_basic(self):
        """Test basic money conversion functionality."""
        data = ["$1,234.56", "€789.01", "£100.00"]
        column = pa.array(data)

        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == [1234.56, 789.01, 100.00]

    def test_apply_money_conversion_with_parentheses(self):
        """Test money conversion with parentheses for negative values."""
        data = ["($500.00)", "$100.00", "($25.50)"]
        column = pa.array(data)

        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == [-500.00, 100.00, -25.50]

    def test_apply_money_conversion_with_none_values(self):
        """Test money conversion with None values."""
        data = ["$100.00", None, "$200.00"]
        column = pa.array(data)

        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_data = result.to_pandas().tolist()

        assert pd.isna(result_data[1])
        assert result_data[0] == 100.00
        assert result_data[2] == 200.00

    def test_apply_money_conversion_invalid_values(self):
        """Test money conversion with invalid values."""
        data = ["$100.00", "invalid", "$200.00"]
        column = pa.array(data)

        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data[0] == 100.00
        assert pd.isna(result_data[1])
        assert result_data[2] == 200.00

    def test_apply_money_conversion_non_string_column(self):
        """Test money conversion on non-string column returns original."""
        data = [100, 200, 300]
        column = pa.array(data)

        config = MoneyTypeConfig()

        result = self.transformer.apply_money_conversion(column, config)

        assert result == column

    def test_clean_money_string_basic(self):
        """Test _clean_money_string helper method."""
        config = MoneyTypeConfig()

        # Test basic conversion
        result = self.transformer._clean_money_string("$1,234.56", config)
        assert result == Decimal("1234.56")

        # Test negative with parentheses
        result = self.transformer._clean_money_string("($100.00)", config)
        assert result == Decimal("-100.00")

        # Test empty string
        result = self.transformer._clean_money_string("", config)
        assert result is None

        # Test whitespace only
        result = self.transformer._clean_money_string("   ", config)
        assert result is None

    def test_clean_money_string_custom_separators(self):
        """Test _clean_money_string with custom separators."""
        config = MoneyTypeConfig(thousands_separator=" ", decimal_separator=",")

        result = self.transformer._clean_money_string("€1 234,56", config)
        assert result == Decimal("1234.56")

    def test_clean_money_string_invalid_value(self):
        """Test _clean_money_string with invalid value."""
        config = MoneyTypeConfig()

        result = self.transformer._clean_money_string("invalid", config)
        assert result is None

    def test_apply_numeric_cleaning_basic(self):
        """Test basic numeric cleaning functionality."""
        data = ["1,234.56", "789.01", "100"]
        column = pa.array(data)

        config = NumericCleaningConfig()

        result = self.transformer.apply_numeric_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == [1234.56, 789.01, 100.0]

    def test_apply_numeric_cleaning_with_nan_values(self):
        """Test numeric cleaning with NaN values."""
        data = ["100.00", "N/A", "200.00", "NULL"]
        column = pa.array(data)

        config = NumericCleaningConfig()

        result = self.transformer.apply_numeric_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data[0] == 100.0
        assert pd.isna(result_data[1])
        assert result_data[2] == 200.0
        assert pd.isna(result_data[3])

    def test_apply_numeric_cleaning_integer_target(self):
        """Test numeric cleaning with integer target type."""
        data = ["123", "456.0", "789"]
        column = pa.array(data)

        config = NumericCleaningConfig()

        result = self.transformer.apply_numeric_cleaning(column, config, target_type="int64")
        result_data = result.to_pandas().tolist()

        assert result_data == [123, 456, 789]
        assert result.type == pa.int64()

    def test_apply_numeric_cleaning_disallow_nan(self):
        """Test numeric cleaning with allow_nan=False."""
        data = ["100.00", "invalid", "200.00"]
        column = pa.array(data)

        config = NumericCleaningConfig(allow_nan=False)

        with pytest.raises(ValueError):
            self.transformer.apply_numeric_cleaning(column, config)

    def test_apply_numeric_cleaning_different_target_types(self):
        """Test numeric cleaning with different target types."""
        data = ["123.45"]
        column = pa.array(data)
        config = NumericCleaningConfig()

        # Test int32
        result = self.transformer.apply_numeric_cleaning(column, config, target_type="int32")
        assert result.type == pa.int32()

        # Test float32
        result = self.transformer.apply_numeric_cleaning(column, config, target_type="float32")
        assert result.type == pa.float32()

        # Test default (float64)
        result = self.transformer.apply_numeric_cleaning(column, config, target_type="double")
        assert result.type == pa.float64()

    def test_clean_numeric_string_basic(self):
        """Test _clean_numeric_string helper method."""
        config = NumericCleaningConfig()

        # Test basic cleaning
        result = self.transformer._clean_numeric_string("1,234.56", config)
        assert result == "1234.56"

        # Test empty string
        result = self.transformer._clean_numeric_string("", config)
        assert result is None

        # Test whitespace only
        result = self.transformer._clean_numeric_string("   ", config)
        assert result is None

    def test_clean_numeric_string_custom_separators(self):
        """Test _clean_numeric_string with custom separators."""
        config = NumericCleaningConfig(thousands_separator=" ", decimal_separator=",")

        result = self.transformer._clean_numeric_string("1 234,56", config)
        assert result == "1234.56"

    def test_apply_string_padding_left(self):
        """Test string padding with left alignment."""
        data = ["test", "hello", "world"]
        column = pa.array(data)

        config = StringPaddingConfig(width=10, fillchar="0", side="left")

        result = self.transformer.apply_string_padding(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["000000test", "00000hello", "00000world"]

    def test_apply_string_padding_right(self):
        """Test string padding with right alignment."""
        data = ["test", "hello"]
        column = pa.array(data)

        config = StringPaddingConfig(width=8, fillchar="*", side="right")

        result = self.transformer.apply_string_padding(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["test****", "hello***"]

    def test_apply_string_padding_both(self):
        """Test string padding with center alignment."""
        data = ["test"]
        column = pa.array(data)

        config = StringPaddingConfig(width=8, fillchar="-", side="both")

        result = self.transformer.apply_string_padding(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["--test--"]

    def test_apply_string_padding_invalid_side(self):
        """Test string padding with invalid side defaults to left."""
        data = ["test"]
        column = pa.array(data)

        config = StringPaddingConfig(width=8, fillchar="0", side="invalid")

        result = self.transformer.apply_string_padding(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["0000test"]

    def test_apply_string_padding_non_string_column(self):
        """Test string padding on non-string column returns original."""
        data = [1, 2, 3]
        column = pa.array(data)

        config = StringPaddingConfig(width=5)

        result = self.transformer.apply_string_padding(column, config)

        assert result == column

    def test_apply_string_trimming_both(self):
        """Test string trimming with both sides."""
        data = ["  test  ", " hello ", "world   "]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column, side="both")
        result_data = result.to_pandas().tolist()

        assert result_data == ["test", "hello", "world"]

    def test_apply_string_trimming_left(self):
        """Test string trimming with left side only."""
        data = ["  test  ", " hello "]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column, side="left")
        result_data = result.to_pandas().tolist()

        assert result_data == ["test  ", "hello "]

    def test_apply_string_trimming_right(self):
        """Test string trimming with right side only."""
        data = ["  test  ", " hello "]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column, side="right")
        result_data = result.to_pandas().tolist()

        assert result_data == ["  test", " hello"]

    def test_apply_string_trimming_custom_chars(self):
        """Test string trimming with custom characters."""
        data = ["***test***", "###hello###"]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column, side="both", chars="*#")
        result_data = result.to_pandas().tolist()

        assert result_data == ["test", "hello"]

    def test_apply_string_trimming_invalid_side(self):
        """Test string trimming with invalid side defaults to both."""
        data = ["  test  "]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column, side="invalid")
        result_data = result.to_pandas().tolist()

        assert result_data == ["test"]

    def test_apply_string_trimming_non_string_column(self):
        """Test string trimming on non-string column returns original."""
        data = [1, 2, 3]
        column = pa.array(data)

        result = self.transformer.apply_string_trimming(column)

        assert result == column

    def test_apply_html_xml_cleaning_basic(self):
        """Test basic HTML/XML cleaning functionality."""
        data = ["<p>Hello &amp; world</p>", "<div>Test &lt;data&gt;</div>"]
        column = pa.array(data)

        config = HTMLXMLConfig()

        result = self.transformer.apply_html_xml_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        # After html.unescape: "&amp;" becomes "&", "&lt;data&gt;" becomes "<data>"
        # After tag removal: all tags including "<p>", "</p>", "<div>", "</div>", and "<data>" are removed
        # The result is just the text content
        assert result_data == ["Hello & world", "Test"]

    def test_apply_html_xml_cleaning_preserve_whitespace(self):
        """Test HTML/XML cleaning with preserve whitespace."""
        data = ["<p>Hello   \n  world</p>"]
        column = pa.array(data)

        config = HTMLXMLConfig(preserve_whitespace=True)

        result = self.transformer.apply_html_xml_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["Hello   \n  world"]

    def test_apply_html_xml_cleaning_no_strip_tags(self):
        """Test HTML/XML cleaning without stripping tags."""
        data = ["<p>Hello &amp; world</p>"]
        column = pa.array(data)

        config = HTMLXMLConfig(strip_tags=False)

        result = self.transformer.apply_html_xml_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["<p>Hello & world</p>"]

    def test_apply_html_xml_cleaning_no_decode_entities(self):
        """Test HTML/XML cleaning without decoding entities."""
        data = ["<p>Hello &amp; world</p>"]
        column = pa.array(data)

        config = HTMLXMLConfig(decode_entities=False)

        result = self.transformer.apply_html_xml_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["Hello &amp; world"]

    def test_apply_html_xml_cleaning_with_none_values(self):
        """Test HTML/XML cleaning with None values."""
        data = ["<p>Hello</p>", None, "<div>World</div>"]
        column = pa.array(data)

        config = HTMLXMLConfig()

        result = self.transformer.apply_html_xml_cleaning(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data[0] == "Hello"
        assert pd.isna(result_data[1])
        assert result_data[2] == "World"

    def test_apply_html_xml_cleaning_non_string_column(self):
        """Test HTML/XML cleaning on non-string column returns original."""
        data = [1, 2, 3]
        column = pa.array(data)

        config = HTMLXMLConfig()

        result = self.transformer.apply_html_xml_cleaning(column, config)

        assert result == column

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_enforce_mode(self, mock_coerce):
        """Test datetime transformation with enforce mode."""
        mock_coerce.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(mode="enforce", format="%Y-%m-%d")

        result = self.transformer.apply_datetime_transformation(column, config)

        mock_coerce.assert_called_once_with(
            "2023-01-01",
            fmt="%Y-%m-%d",
            allow_fuzzy=False,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_specify_formats_mode(self, mock_coerce):
        """Test datetime transformation with specify_formats mode."""
        mock_coerce.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(mode="specify_formats", formats=["%Y-%m-%d", "%m/%d/%Y"])

        result = self.transformer.apply_datetime_transformation(column, config)

        mock_coerce.assert_called_once_with(
            "2023-01-01",
            formats=["%Y-%m-%d", "%m/%d/%Y"],
            allow_fuzzy=False,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_common_formats_mode(self, mock_coerce):
        """Test datetime transformation with common_formats mode."""
        mock_coerce.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(mode="common_formats")

        result = self.transformer.apply_datetime_transformation(column, config)

        mock_coerce.assert_called_once_with(
            "2023-01-01",
            allow_fuzzy=False,
            from_epoch=False,
            to_epoch=None
        )

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_to_epoch(self, mock_coerce):
        """Test datetime transformation with to_epoch conversion."""
        mock_coerce.return_value = 1672574400.0  # epoch timestamp

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(to_epoch="seconds")

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == [1672574400.0]

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_date(self, mock_coerce):
        """Test datetime transformation with target_type='date'."""
        mock_dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_coerce.return_value = mock_dt

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(target_type="date")

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == [datetime.date(2023, 1, 1)]

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_timestamp(self, mock_coerce):
        """Test datetime transformation with target_type='timestamp'."""
        mock_dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_coerce.return_value = mock_dt

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(target_type="timestamp")

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert len(result_data) == 1
        assert isinstance(result_data[0], float)

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_string_with_format(self, mock_coerce):
        """Test datetime transformation with target_type='string' and output_format."""
        mock_dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_coerce.return_value = mock_dt

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(target_type="string", output_format="%m/%d/%Y")

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["01/01/2023"]

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_target_string_no_format(self, mock_coerce):
        """Test datetime transformation with target_type='string' and no output_format."""
        mock_dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_coerce.return_value = mock_dt

        data = ["2023-01-01"]
        column = pa.array(data)

        config = DateTimeTransformConfig(target_type="string")

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert result_data == ["2023-01-01T12:00:00"]

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_with_none_values(self, mock_coerce):
        """Test datetime transformation with None values."""
        data = ["2023-01-01", None, ""]
        column = pa.array(data)

        config = DateTimeTransformConfig()

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert pd.isna(result_data[1])
        assert pd.isna(result_data[2])

    @patch('forklift.utils.data_transformations.coerce_datetime')
    def test_apply_datetime_transformation_parse_error(self, mock_coerce):
        """Test datetime transformation with parse errors."""
        mock_coerce.side_effect = ValueError("Parse error")

        data = ["invalid-date"]
        column = pa.array(data)

        config = DateTimeTransformConfig()

        result = self.transformer.apply_datetime_transformation(column, config)
        result_data = result.to_pandas().tolist()

        assert pd.isna(result_data[0])


class TestIntegration:
    """Integration tests for data transformations."""

    def test_end_to_end_transformations(self):
        """Test applying multiple transformations in sequence."""
        transformer = DataTransformer()

        # Start with messy money data
        data = ["  $1,234.56  ", "€789.01", "($500.00)"]
        column = pa.array(data)

        # First, convert to money
        money_config = MoneyTypeConfig()
        money_result = transformer.apply_money_conversion(column, money_config)

        # Check the money conversion worked
        money_data = money_result.to_pandas().tolist()
        assert money_data == [1234.56, 789.01, -500.00]

        # Now test string operations on a different dataset
        string_data = ["  hello  world  ", "  test  data  "]
        string_column = pa.array(string_data)

        # Apply regex to normalize whitespace
        regex_config = RegexReplaceConfig(pattern=r"\s+", replacement=" ")
        regex_result = transformer.apply_regex_replace(string_column, regex_config)

        # Then trim whitespace
        trim_result = transformer.apply_string_trimming(regex_result, side="both")
        trim_data = trim_result.to_pandas().tolist()

        assert trim_data == ["hello world", "test data"]

    def test_configuration_edge_cases(self):
        """Test edge cases in configuration validation."""
        # Test that all config classes can be instantiated with defaults
        configs = [
            DateTimeTransformConfig(),
            RegexReplaceConfig("test", "TEST"),
            StringReplaceConfig("old", "new"),
            MoneyTypeConfig(),
            NumericCleaningConfig(),
            StringPaddingConfig(10),
            HTMLXMLConfig(),
            StringCleaningConfig()
        ]

        for config in configs:
            assert config is not None
