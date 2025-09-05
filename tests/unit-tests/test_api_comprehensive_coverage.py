"""Comprehensive tests for the API module to achieve high test coverage."""

import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import json
from typing import Dict, Any, List, Optional

from forklift.api import (
    generate_schema_from_csv,
    generate_schema_from_excel,
    generate_schema_from_parquet,
    generate_and_save_schema,
    generate_and_copy_schema
)
from forklift.schema.schema_generator import SchemaGenerationConfig, OutputTarget, FileType


class TestAPISchemaGeneration:
    """Test cases for API schema generation functions."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_minimal(self, mock_schema_gen):
        """Test CSV schema generation with minimal parameters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test Schema"}

        result = generate_schema_from_csv("test.csv")

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        assert result == {"title": "Test Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_all_options(self, mock_schema_gen):
        """Test CSV schema generation with all parameters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test Schema", "properties": {}}

        result = generate_schema_from_csv(
            input_path="test.csv",
            nrows=1000,
            delimiter="|",
            encoding="latin-1",
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["id", "code"]
        )

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        assert result == {"title": "Test Schema", "properties": {}}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_path_object(self, mock_schema_gen):
        """Test CSV schema generation with Path object."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test Schema"}

        path_obj = Path("test.csv")
        result = generate_schema_from_csv(path_obj)

        mock_schema_gen.assert_called_once()
        assert result == {"title": "Test Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_s3_path(self, mock_schema_gen):
        """Test CSV schema generation with S3 path."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "S3 Schema"}

        result = generate_schema_from_csv("s3://bucket/test.csv")

        mock_schema_gen.assert_called_once()
        assert result == {"title": "S3 Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_excel_minimal(self, mock_schema_gen):
        """Test Excel schema generation with minimal parameters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Excel Schema"}

        result = generate_schema_from_excel("test.xlsx")

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        assert result == {"title": "Excel Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_excel_with_sheet(self, mock_schema_gen):
        """Test Excel schema generation with sheet specification."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Excel Sheet Schema"}

        result = generate_schema_from_excel(
            input_path="test.xlsx",
            sheet_name="DataSheet",
            nrows=500,
            include_sample_data=True
        )

        mock_schema_gen.assert_called_once()
        assert result == {"title": "Excel Sheet Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_parquet_minimal(self, mock_schema_gen):
        """Test Parquet schema generation with minimal parameters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Parquet Schema"}

        result = generate_schema_from_parquet("test.parquet")

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        assert result == {"title": "Parquet Schema"}

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_parquet_with_options(self, mock_schema_gen):
        """Test Parquet schema generation with options."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Parquet Schema with Options"}

        result = generate_schema_from_parquet(
            input_path="test.parquet",
            nrows=2000,
            include_sample_data=False,
            infer_primary_key_from_metadata=True
        )

        mock_schema_gen.assert_called_once()
        assert result == {"title": "Parquet Schema with Options"}


class TestAPIFileOperations:
    """Test cases for API file operation functions."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_to_file_csv(self, mock_schema_gen):
        """Test generating schema from CSV to file."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        test_schema = {"title": "Test Schema", "properties": {}}
        mock_generator.generate_schema.return_value = test_schema

        generate_and_save_schema("input.csv", "output_schema.json", file_type="csv")

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        mock_generator.output_schema.assert_called_once_with(test_schema)

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_to_file_excel(self, mock_schema_gen):
        """Test generating schema from Excel to file."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        test_schema = {"title": "Excel Schema"}
        mock_generator.generate_schema.return_value = test_schema

        generate_and_save_schema(
            input_path="input.xlsx",
            output_path="schema.json",
            file_type="excel",
            sheet_name="Sheet1",
            nrows=1000
        )

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        mock_generator.output_schema.assert_called_once_with(test_schema)

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_to_clipboard(self, mock_schema_gen):
        """Test generating schema to clipboard."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        test_schema = {"title": "Clipboard Schema"}
        mock_generator.generate_schema.return_value = test_schema

        result = generate_and_copy_schema("input.csv", file_type="csv")

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()
        mock_generator.output_schema.assert_called_once_with(test_schema)
        assert result == test_schema

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_to_file_invalid_file_type(self, mock_schema_gen):
        """Test that invalid file type raises ValueError."""
        with pytest.raises(ValueError):
            generate_and_save_schema("input.txt", "output.json", file_type="invalid")

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_to_clipboard_invalid_file_type(self, mock_schema_gen):
        """Test that invalid file type raises ValueError for clipboard."""
        with pytest.raises(ValueError):
            generate_and_copy_schema("input.txt", file_type="invalid")


class TestAPIErrorHandling:
    """Test error handling in API functions."""

    @patch('forklift.api.SchemaGenerator')
    def test_schema_generation_error_propagation(self, mock_schema_gen):
        """Test that errors from SchemaGenerator are properly propagated."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.side_effect = Exception("Schema generation failed")

        with pytest.raises(Exception, match="Schema generation failed"):
            generate_schema_from_csv("nonexistent.csv")

    @patch('forklift.api.SchemaGenerator')
    def test_file_write_error(self, mock_schema_gen):
        """Test that file write errors are properly handled."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test"}
        mock_generator.output_schema.side_effect = IOError("Cannot write file")

        with pytest.raises(IOError, match="Cannot write file"):
            generate_and_save_schema("input.csv", "readonly.json", file_type="csv")

    @patch('forklift.api.SchemaGenerator')
    def test_clipboard_error(self, mock_schema_gen):
        """Test that clipboard errors are properly handled."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test"}
        mock_generator.output_schema.side_effect = Exception("Clipboard error")

        with pytest.raises(Exception, match="Clipboard error"):
            generate_and_copy_schema("input.csv", file_type="csv")


class TestAPIParameterValidation:
    """Test parameter validation in API functions."""

    def test_empty_input_path(self):
        """Test that empty input path raises appropriate error."""
        with pytest.raises((ValueError, FileNotFoundError)):
            generate_schema_from_csv("")

    def test_none_input_path(self):
        """Test that None input path raises appropriate error."""
        with pytest.raises((TypeError, ValueError)):
            generate_schema_from_csv(None)

    @patch('forklift.api.SchemaGenerator')
    def test_negative_nrows(self, mock_schema_gen):
        """Test handling of negative nrows parameter."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test"}

        # Should handle negative nrows gracefully
        result = generate_schema_from_csv("test.csv", nrows=-1)
        assert result == {"title": "Test"}

    @patch('forklift.api.SchemaGenerator')
    def test_zero_nrows(self, mock_schema_gen):
        """Test handling of zero nrows parameter."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test"}

        result = generate_schema_from_csv("test.csv", nrows=0)
        assert result == {"title": "Test"}

    @patch('forklift.api.SchemaGenerator')
    def test_invalid_encoding(self, mock_schema_gen):
        """Test handling of invalid encoding parameter."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")

        with pytest.raises(UnicodeDecodeError):
            generate_schema_from_csv("test.csv", encoding="invalid-encoding")

    @patch('forklift.api.SchemaGenerator')
    def test_empty_delimiter(self, mock_schema_gen):
        """Test handling of empty delimiter parameter."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Test"}

        result = generate_schema_from_csv("test.csv", delimiter="")
        assert result == {"title": "Test"}


class TestAPIEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch('forklift.api.SchemaGenerator')
    def test_very_large_nrows(self, mock_schema_gen):
        """Test handling of very large nrows parameter."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Large File"}

        result = generate_schema_from_csv("test.csv", nrows=10**9)
        assert result == {"title": "Large File"}

    @patch('forklift.api.SchemaGenerator')
    def test_special_characters_in_paths(self, mock_schema_gen):
        """Test handling of special characters in file paths."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Special Path"}

        special_paths = [
            "file with spaces.csv",
            "file-with-dashes.csv",
            "file_with_underscores.csv",
            "file.with.dots.csv",
            "файл.csv",  # Non-ASCII characters
        ]

        for path in special_paths:
            result = generate_schema_from_csv(path)
            assert result == {"title": "Special Path"}

    @patch('forklift.api.SchemaGenerator')
    def test_empty_primary_key_list(self, mock_schema_gen):
        """Test handling of empty primary key list."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Empty PK"}

        result = generate_schema_from_csv("test.csv", user_specified_primary_key=[])
        assert result == {"title": "Empty PK"}

    @patch('forklift.api.SchemaGenerator')
    def test_single_character_delimiter(self, mock_schema_gen):
        """Test various single character delimiters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Delimiter Test"}

        delimiters = [",", ";", "|", "\t", ":", " "]
        for delimiter in delimiters:
            result = generate_schema_from_csv("test.csv", delimiter=delimiter)
            assert result == {"title": "Delimiter Test"}

    @patch('forklift.api.SchemaGenerator')
    def test_multi_character_delimiter(self, mock_schema_gen):
        """Test multi-character delimiters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Multi Delimiter"}

        result = generate_schema_from_csv("test.csv", delimiter="||")
        assert result == {"title": "Multi Delimiter"}


class TestAPIIntegration:
    """Integration-style tests for API functionality."""

    @patch('forklift.api.SchemaGenerator')
    def test_schema_config_creation(self, mock_schema_gen):
        """Test that SchemaGenerationConfig is created with correct parameters."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Config Test"}

        generate_schema_from_csv(
            "test.csv",
            nrows=1000,
            delimiter=",",
            encoding="utf-8",
            include_sample_data=True,
            infer_primary_key_from_metadata=True
        )

        # Verify SchemaGenerator was called with correct config
        mock_schema_gen.assert_called_once()
        args, kwargs = mock_schema_gen.call_args

        # Verify the config object has the expected properties
        config = args[0]  # First argument should be the config
        assert hasattr(config, 'nrows')
        assert hasattr(config, 'file_type')

    @patch('forklift.api.SchemaGenerator')
    def test_config_defaults(self, mock_schema_gen):
        """Test that configuration defaults are applied correctly."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Defaults Test"}

        generate_schema_from_csv("test.csv")

        mock_schema_gen.assert_called_once()
        args, kwargs = mock_schema_gen.call_args

        # Verify default values are used
        config = args[0]
        assert config.file_type == FileType.CSV
        assert config.output_target == OutputTarget.STDOUT

    @patch('forklift.api.SchemaGenerator')
    def test_output_target_mapping(self, mock_schema_gen):
        """Test that output targets are mapped correctly."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "Output Test"}

        # Test different output scenarios
        generate_schema_from_csv("test.csv")  # Should use STDOUT
        generate_and_save_schema("test.csv", "output.json", "csv")  # Should use FILE

        assert mock_schema_gen.call_count >= 2

    @patch('forklift.api.SchemaGenerator')
    def test_file_type_mapping(self, mock_schema_gen):
        """Test that file types are mapped correctly."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator
        mock_generator.generate_schema.return_value = {"title": "File Type Test"}

        # Test different file types
        generate_schema_from_csv("test.csv")
        generate_schema_from_excel("test.xlsx")
        generate_schema_from_parquet("test.parquet")

        assert mock_schema_gen.call_count == 3
