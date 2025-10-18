"""Comprehensive tests for forklift.api module to achieve 100% code coverage."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from forklift.api import (
    generate_and_copy_schema,
    generate_and_save_schema,
    generate_schema_from_csv,
    generate_schema_from_excel,
    generate_schema_from_parquet,
)


class TestAPIValidationCoverage:
    """Test suite specifically targeting the missing validation coverage."""

    def setup_method(self):
        """Set up test fixtures."""
        # Mock schema that would be returned by SchemaGenerator
        self.mock_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Forklift Schema - Generated",
            "type": "object",
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
            "required": ["id"],
        }

    # Test coverage for generate_schema_from_excel validation (lines 108, 110)
    def test_generate_schema_from_excel_none_input_path(self):
        """Test that generate_schema_from_excel raises ValueError for None input_path."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_excel(None)

    def test_generate_schema_from_excel_empty_string_input_path(self):
        """Test that generate_schema_from_excel raises ValueError for empty string input_path."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_excel("")

    def test_generate_schema_from_excel_whitespace_only_input_path(self):
        """Test that generate_schema_from_excel raises ValueError for whitespace-only input_path."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_excel("   ")

    # Test coverage for generate_schema_from_parquet validation (lines 162, 164)
    def test_generate_schema_from_parquet_none_input_path(self):
        """Test that generate_schema_from_parquet raises ValueError for None input_path."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_parquet(None)

    def test_generate_schema_from_parquet_empty_string_input_path(self):
        """Test that generate_schema_from_parquet raises ValueError for empty string input_path."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_parquet("")

    def test_generate_schema_from_parquet_whitespace_only_input_path(self):
        """Test that generate_schema_from_parquet raises ValueError for whitespace-only input_path."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_parquet("   ")

    # Additional tests to ensure we have complete coverage
    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_excel_successful_execution(self, mock_schema_generator):
        """Test successful execution of generate_schema_from_excel to ensure normal path works."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with valid input
        result = generate_schema_from_excel("test.xlsx")

        assert result == self.mock_schema
        mock_schema_generator.assert_called_once()

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_parquet_successful_execution(self, mock_schema_generator):
        """Test successful execution of generate_schema_from_parquet to ensure normal path works."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with valid input
        result = generate_schema_from_parquet("test.parquet")

        assert result == self.mock_schema
        mock_schema_generator.assert_called_once()

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_csv_validation_already_covered(self, mock_schema_generator):
        """Verify CSV function validation is already tested (should already have coverage)."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # These should already be covered, but let's ensure they work
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_csv(None)

        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_csv("")

    # Tests for generate_and_save_schema function (lines 204-215)
    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_save_schema_csv(self, mock_schema_generator):
        """Test generate_and_save_schema with CSV file type."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test the function
        generate_and_save_schema(
            input_path="test.csv", output_path="schema.json", file_type="csv", nrows=500
        )

        # Verify SchemaGenerator was called correctly
        mock_schema_generator.assert_called_once()
        call_args = mock_schema_generator.call_args[0][0]  # Get the config object

        assert call_args.input_path == "test.csv"
        assert call_args.output_path == "schema.json"
        assert call_args.file_type.value == "csv"
        assert call_args.nrows == 500
        assert call_args.output_target.value == "file"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_save_schema_excel(self, mock_schema_generator):
        """Test generate_and_save_schema with Excel file type."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test the function
        generate_and_save_schema(
            input_path="test.xlsx",
            output_path="schema.json",
            file_type="excel",
            sheet_name="Sheet1",
        )

        # Verify SchemaGenerator was called correctly
        mock_schema_generator.assert_called_once()
        call_args = mock_schema_generator.call_args[0][0]  # Get the config object

        assert call_args.input_path == "test.xlsx"
        assert call_args.output_path == "schema.json"
        assert call_args.file_type.value == "excel"
        assert call_args.sheet_name == "Sheet1"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)

    # Tests for generate_and_copy_schema function (lines 239-250)
    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_copy_schema_csv(self, mock_schema_generator):
        """Test generate_and_copy_schema with CSV file type."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test the function
        result = generate_and_copy_schema(input_path="test.csv", file_type="csv", nrows=1000)

        # Verify return value
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        mock_schema_generator.assert_called_once()
        call_args = mock_schema_generator.call_args[0][0]  # Get the config object

        assert call_args.input_path == "test.csv"
        assert call_args.file_type.value == "csv"
        assert call_args.nrows == 1000
        assert call_args.output_target.value == "clipboard"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_copy_schema_parquet(self, mock_schema_generator):
        """Test generate_and_copy_schema with Parquet file type."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test the function
        result = generate_and_copy_schema(
            input_path="test.parquet", file_type="parquet", include_sample_data=True
        )

        # Verify return value
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        mock_schema_generator.assert_called_once()
        call_args = mock_schema_generator.call_args[0][0]  # Get the config object

        assert call_args.input_path == "test.parquet"
        assert call_args.file_type.value == "parquet"
        assert call_args.include_sample_data is True
        assert call_args.output_target.value == "clipboard"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)
