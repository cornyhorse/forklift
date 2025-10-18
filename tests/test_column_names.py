"""Tests for column name processing utilities."""

from unittest.mock import patch

import pytest

from forklift.schema.fwf.utils.column_names import ColumnNameProcessor


class TestColumnNameProcessor:
    """Test cases for ColumnNameProcessor class."""

    def test_standardize_column_names_no_processing(self):
        """Test when no standardization or deduplication is requested."""
        column_names = ["Name", "Age", "Email"]
        result = ColumnNameProcessor.standardize_column_names(column_names)
        assert result == ["Name", "Age", "Email"]

    def test_standardize_column_names_none_methods(self):
        """Test when both methods are explicitly None."""
        column_names = ["Name", "Age", "Email"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method=None, dedupe_method=None
        )
        assert result == ["Name", "Age", "Email"]

    def test_standardize_column_names_postgres_only(self):
        """Test postgres standardization without deduplication."""
        column_names = ["User Name", "Email Address", "Phone Number"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method="postgres"
        )
        expected = ["user_name", "email_address", "phone_number"]
        assert result == expected

    def test_standardize_column_names_dedupe_only(self):
        """Test deduplication without standardization."""
        column_names = ["name", "age", "name", "email"]
        result = ColumnNameProcessor.standardize_column_names(column_names, dedupe_method="suffix")
        expected = ["name", "age", "name_1", "email"]
        assert result == expected

    def test_standardize_column_names_both_postgres_and_suffix(self):
        """Test both postgres standardization and suffix deduplication."""
        column_names = ["User Name", "Email Address", "User Name"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method="postgres", dedupe_method="suffix"
        )
        expected = ["user_name", "email_address", "user_name_1"]
        assert result == expected

    def test_standardize_column_names_both_postgres_and_prefix(self):
        """Test both postgres standardization and prefix deduplication."""
        column_names = ["User Name", "Email Address", "User Name"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method="postgres", dedupe_method="prefix"
        )
        expected = ["user_name", "email_address", "1_user_name"]
        assert result == expected

    def test_standardize_column_names_both_postgres_and_error(self):
        """Test both postgres standardization and error deduplication."""
        column_names = ["User Name", "Email Address", "User Name"]
        with pytest.raises(ValueError, match="Duplicate column name detected: user_name"):
            ColumnNameProcessor.standardize_column_names(
                column_names, standardize_method="postgres", dedupe_method="error"
            )

    def test_standardize_column_names_unknown_standardize_method(self):
        """Test with unknown standardization method (should be ignored)."""
        column_names = ["User Name", "Email Address"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method="unknown_method"
        )
        # Should return original names since unknown method is not handled
        assert result == ["User Name", "Email Address"]

    def test_standardize_column_names_empty_list(self):
        """Test with empty column names list."""
        result = ColumnNameProcessor.standardize_column_names(
            [], standardize_method="postgres", dedupe_method="suffix"
        )
        assert result == []

    def test_standardize_column_names_single_column(self):
        """Test with single column name."""
        result = ColumnNameProcessor.standardize_column_names(
            ["User Name"], standardize_method="postgres", dedupe_method="suffix"
        )
        assert result == ["user_name"]

    @patch("forklift.schema.fwf.utils.column_names.standardize_postgres_column_name")
    @patch("forklift.schema.fwf.utils.column_names.dedupe_column_names")
    def test_standardize_column_names_calls_utilities(self, mock_dedupe, mock_standardize):
        """Test that the utility functions are called correctly."""
        mock_standardize.side_effect = lambda x: f"std_{x}"
        mock_dedupe.return_value = ["deduped_1", "deduped_2"]

        column_names = ["Name1", "Name2"]
        result = ColumnNameProcessor.standardize_column_names(
            column_names, standardize_method="postgres", dedupe_method="suffix"
        )

        # Check that standardize_postgres_column_name was called for each name
        assert mock_standardize.call_count == 2
        mock_standardize.assert_any_call("Name1")
        mock_standardize.assert_any_call("Name2")

        # Check that dedupe_column_names was called with standardized names
        mock_dedupe.assert_called_once_with(["std_Name1", "std_Name2"], "suffix")

        assert result == ["deduped_1", "deduped_2"]
