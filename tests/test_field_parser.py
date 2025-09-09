"""Tests for FWF field parsing utilities."""

from unittest.mock import patch

from forklift.schema.fwf.fields.parser import FieldParser


class TestFieldParser:
    """Test cases for FieldParser class."""

    def test_get_column_names_basic(self):
        """Test basic column name extraction without processing."""
        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"name": "field2", "start": 6, "length": 10},
            {"name": "field3", "start": 16, "length": 8}
        ]
        result = FieldParser.get_column_names(fields)
        assert result == ["field1", "field2", "field3"]

    def test_get_column_names_empty_list(self):
        """Test with empty fields list."""
        result = FieldParser.get_column_names([])
        assert result == []

    def test_get_column_names_missing_name(self):
        """Test with fields missing name attribute."""
        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"start": 6, "length": 10},  # No name
            {"name": "field3", "start": 16, "length": 8}
        ]
        result = FieldParser.get_column_names(fields)
        assert result == ["field1", "", "field3"]

    @patch('forklift.schema.fwf.fields.parser.ColumnNameProcessor.standardize_column_names')
    def test_get_column_names_with_standardization(self, mock_standardize):
        """Test column name extraction with standardization."""
        mock_standardize.return_value = ["std_field1", "std_field2"]

        fields = [
            {"name": "Field1", "start": 1, "length": 5},
            {"name": "Field2", "start": 6, "length": 10}
        ]

        result = FieldParser.get_column_names(fields, standardize_names="postgres")

        mock_standardize.assert_called_once_with(["Field1", "Field2"], "postgres", None)
        assert result == ["std_field1", "std_field2"]

    @patch('forklift.schema.fwf.fields.parser.ColumnNameProcessor.standardize_column_names')
    def test_get_column_names_with_deduplication(self, mock_standardize):
        """Test column name extraction with deduplication."""
        mock_standardize.return_value = ["field1", "field2_1"]

        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"name": "field1", "start": 6, "length": 10}
        ]

        result = FieldParser.get_column_names(fields, dedupe_names="suffix")

        mock_standardize.assert_called_once_with(["field1", "field1"], None, "suffix")
        assert result == ["field1", "field2_1"]

    @patch('forklift.schema.fwf.fields.parser.ColumnNameProcessor.standardize_column_names')
    def test_get_column_names_with_both_processing(self, mock_standardize):
        """Test column name extraction with both standardization and deduplication."""
        mock_standardize.return_value = ["std_field1", "std_field1_1"]

        fields = [
            {"name": "Field1", "start": 1, "length": 5},
            {"name": "Field1", "start": 6, "length": 10}
        ]

        result = FieldParser.get_column_names(fields, standardize_names="postgres", dedupe_names="suffix")

        mock_standardize.assert_called_once_with(["Field1", "Field1"], "postgres", "suffix")
        assert result == ["std_field1", "std_field1_1"]

    def test_get_column_names_for_flag_value_basic(self):
        """Test basic flag value column names without processing."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "field1", "start": 2, "length": 5},
            {"name": "field2", "start": 7, "length": 10}
        ]

        result = FieldParser.get_column_names_for_flag_value(flag_column, variant_fields)
        assert result == ["flag", "field1", "field2"]

    def test_get_column_names_for_flag_value_no_flag_column(self):
        """Test flag value column names with no flag column."""
        variant_fields = [
            {"name": "field1", "start": 2, "length": 5},
            {"name": "field2", "start": 7, "length": 10}
        ]

        result = FieldParser.get_column_names_for_flag_value(None, variant_fields)
        assert result == ["field1", "field2"]

    def test_get_column_names_for_flag_value_missing_flag_name(self):
        """Test flag value column names with flag column missing name."""
        flag_column = {"start": 1, "length": 1}  # No name
        variant_fields = [
            {"name": "field1", "start": 2, "length": 5}
        ]

        result = FieldParser.get_column_names_for_flag_value(flag_column, variant_fields)
        assert result == ["field1"]

    def test_get_column_names_for_flag_value_duplicate_names(self):
        """Test flag value column names with duplicate names."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "flag", "start": 2, "length": 5},  # Same as flag column
            {"name": "field2", "start": 7, "length": 10}
        ]

        result = FieldParser.get_column_names_for_flag_value(flag_column, variant_fields)
        assert result == ["flag", "field2"]

    def test_get_column_names_for_flag_value_empty_names(self):
        """Test flag value column names with empty field names."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "", "start": 2, "length": 5},  # Empty name
            {"start": 7, "length": 10}  # No name
        ]

        result = FieldParser.get_column_names_for_flag_value(flag_column, variant_fields)
        assert result == ["flag"]

    @patch('forklift.schema.fwf.fields.parser.ColumnNameProcessor.standardize_column_names')
    def test_get_column_names_for_flag_value_with_processing(self, mock_standardize):
        """Test flag value column names with standardization and deduplication."""
        mock_standardize.return_value = ["std_flag", "std_field1"]

        flag_column = {"name": "Flag", "start": 1, "length": 1}
        variant_fields = [{"name": "Field1", "start": 2, "length": 5}]

        result = FieldParser.get_column_names_for_flag_value(
            flag_column, variant_fields,
            standardize_names="postgres",
            dedupe_names="suffix"
        )

        mock_standardize.assert_called_once_with(["Flag", "Field1"], "postgres", "suffix")
        assert result == ["std_flag", "std_field1"]

    def test_should_trim_field_default_true(self):
        """Test field trimming with default behavior (trim=True)."""
        trim_config = {}
        result = FieldParser.should_trim_field("field1", trim_config)
        assert result is True

    def test_should_trim_field_explicit_true(self):
        """Test field trimming with explicit True configuration."""
        trim_config = {"field1": True}
        result = FieldParser.should_trim_field("field1", trim_config)
        assert result is True

    def test_should_trim_field_explicit_false(self):
        """Test field trimming with explicit False configuration."""
        trim_config = {"field1": False}
        result = FieldParser.should_trim_field("field1", trim_config)
        assert result is False

    def test_should_trim_field_mixed_config(self):
        """Test field trimming with mixed configuration."""
        trim_config = {"field1": True, "field2": False}

        assert FieldParser.should_trim_field("field1", trim_config) is True
        assert FieldParser.should_trim_field("field2", trim_config) is False
        assert FieldParser.should_trim_field("field3", trim_config) is True  # Default

    def test_get_null_values_global_only(self):
        """Test getting null values with only global configuration."""
        nulls_config = {"global": ["", "NULL", "N/A"]}

        result = FieldParser.get_null_values(None, nulls_config)
        assert result == ["", "NULL", "N/A"]

    def test_get_null_values_global_default(self):
        """Test getting null values with default global configuration."""
        nulls_config = {}

        result = FieldParser.get_null_values(None, nulls_config)
        assert result == [""]

    def test_get_null_values_per_column_exists(self):
        """Test getting null values for specific column that exists in config."""
        nulls_config = {
            "global": ["", "NULL"],
            "perColumn": {
                "field1": ["", "EMPTY", "NONE"],
                "field2": ["0", ""]
            }
        }

        result = FieldParser.get_null_values("field1", nulls_config)
        assert result == ["", "EMPTY", "NONE"]

    def test_get_null_values_per_column_missing(self):
        """Test getting null values for column not in per-column config."""
        nulls_config = {
            "global": ["", "NULL"],
            "perColumn": {
                "field1": ["", "EMPTY"]
            }
        }

        result = FieldParser.get_null_values("field2", nulls_config)
        assert result == ["", "NULL"]  # Falls back to global

    def test_get_null_values_no_per_column_config(self):
        """Test getting null values when no per-column config exists."""
        nulls_config = {"global": ["", "NULL", "N/A"]}

        result = FieldParser.get_null_values("field1", nulls_config)
        assert result == ["", "NULL", "N/A"]

    def test_get_null_values_empty_config(self):
        """Test getting null values with empty configuration."""
        nulls_config = {}

        result = FieldParser.get_null_values("field1", nulls_config)
        assert result == [""]  # Default global value
