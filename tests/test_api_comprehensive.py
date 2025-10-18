"""Comprehensive tests for forklift.api module to improve code coverage."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from forklift.api import (generate_and_copy_schema, generate_and_save_schema,
                          generate_schema_from_csv, generate_schema_from_excel,
                          generate_schema_from_parquet)


class TestAPIFunctions:
    """Test suite for API functions with comprehensive coverage."""

    def setup_method(self):
        """Set up test fixtures."""
        # Mock schema that would be returned by SchemaGenerator
        self.mock_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Forklift Schema - Generated",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "email": {"type": "string"},
            },
            "required": ["id", "name"],
        }

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_csv_with_defaults(self, mock_schema_generator):
        """Test CSV schema generation with default parameters."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with minimal parameters
        result = generate_schema_from_csv("test.csv")

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        mock_schema_generator.assert_called_once()
        call_args = mock_schema_generator.call_args[0][0]  # Get the config object

        assert call_args.input_path == "test.csv"
        assert call_args.file_type.value == "csv"
        assert call_args.nrows is None
        assert call_args.delimiter == ","
        assert call_args.encoding == "utf-8"
        assert call_args.include_sample_data is False
        assert call_args.infer_primary_key_from_metadata is False
        assert call_args.user_specified_primary_key is None

        # Verify generate_schema was called
        mock_generator_instance.generate_schema.assert_called_once()

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_csv_with_all_parameters(self, mock_schema_generator):
        """Test CSV schema generation with all parameters specified."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with all parameters
        result = generate_schema_from_csv(
            input_path="data/test.csv",
            nrows=1000,
            delimiter=";",
            encoding="utf-16",
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["user_id", "record_id"],
        )

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called with correct config
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "data/test.csv"
        assert call_args.nrows == 1000
        assert call_args.delimiter == ";"
        assert call_args.encoding == "utf-16"
        assert call_args.include_sample_data is True
        assert call_args.infer_primary_key_from_metadata is True
        assert call_args.user_specified_primary_key == ["user_id", "record_id"]

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_csv_with_pathlib_path(self, mock_schema_generator):
        """Test CSV schema generation with pathlib.Path input."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with Path object
        input_path = Path("/data/test.csv")
        result = generate_schema_from_csv(input_path)

        # Verify the result
        assert result == self.mock_schema

        # Verify path was passed correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == input_path

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_excel_with_defaults(self, mock_schema_generator):
        """Test Excel schema generation with default parameters."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with minimal parameters
        result = generate_schema_from_excel("test.xlsx")

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "test.xlsx"
        assert call_args.file_type.value == "excel"
        assert call_args.nrows == 1000  # Excel default
        assert call_args.sheet_name is None
        assert call_args.include_sample_data is False
        assert call_args.infer_primary_key_from_metadata is False
        assert call_args.user_specified_primary_key is None

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_excel_with_all_parameters(self, mock_schema_generator):
        """Test Excel schema generation with all parameters specified."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with all parameters
        result = generate_schema_from_excel(
            input_path="workbook.xlsx",
            nrows=500,
            sheet_name="Sheet2",
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["id"],
        )

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called with correct config
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "workbook.xlsx"
        assert call_args.nrows == 500
        assert call_args.sheet_name == "Sheet2"
        assert call_args.include_sample_data is True
        assert call_args.infer_primary_key_from_metadata is True
        assert call_args.user_specified_primary_key == ["id"]

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_parquet_with_defaults(self, mock_schema_generator):
        """Test Parquet schema generation with default parameters."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with minimal parameters
        result = generate_schema_from_parquet("data.parquet")

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "data.parquet"
        assert call_args.file_type.value == "parquet"
        assert call_args.nrows is None  # Parquet default
        assert call_args.include_sample_data is False
        assert call_args.infer_primary_key_from_metadata is False
        assert call_args.user_specified_primary_key is None

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_from_parquet_with_all_parameters(self, mock_schema_generator):
        """Test Parquet schema generation with all parameters specified."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        # Test with all parameters
        result = generate_schema_from_parquet(
            input_path="large_data.parquet",
            nrows=2000,
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["primary_key"],
        )

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called with correct config
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "large_data.parquet"
        assert call_args.nrows == 2000
        assert call_args.include_sample_data is True
        assert call_args.infer_primary_key_from_metadata is True
        assert call_args.user_specified_primary_key == ["primary_key"]

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_save_schema_csv(self, mock_schema_generator):
        """Test schema generation and saving to file for CSV."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with CSV file type
        generate_and_save_schema(
            input_path="input.csv", output_path="output.json", file_type="csv", nrows=1000
        )

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "input.csv"
        assert call_args.output_path == "output.json"
        assert call_args.file_type.value == "csv"
        assert call_args.nrows == 1000
        assert call_args.output_target.value == "file"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_save_schema_excel_with_kwargs(self, mock_schema_generator):
        """Test schema generation and saving to file for Excel with additional kwargs."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with Excel file type and additional kwargs
        generate_and_save_schema(
            input_path="workbook.xlsx",
            output_path="schema.json",
            file_type="excel",
            nrows=500,
            sheet_name="Data",
            include_sample_data=True,
        )

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "workbook.xlsx"
        assert call_args.output_path == "schema.json"
        assert call_args.file_type.value == "excel"
        assert call_args.nrows == 500
        assert call_args.sheet_name == "Data"
        assert call_args.include_sample_data is True

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_save_schema_parquet(self, mock_schema_generator):
        """Test schema generation and saving to file for Parquet."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with Parquet file type
        generate_and_save_schema(
            input_path=Path("data.parquet"), output_path=Path("schema.json"), file_type="parquet"
        )

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == Path("data.parquet")
        assert call_args.output_path == Path("schema.json")
        assert call_args.file_type.value == "parquet"
        assert call_args.nrows is None

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_copy_schema_csv(self, mock_schema_generator):
        """Test schema generation and copying to clipboard for CSV."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with CSV file type
        result = generate_and_copy_schema(input_path="data.csv", file_type="csv", nrows=1000)

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "data.csv"
        assert call_args.file_type.value == "csv"
        assert call_args.nrows == 1000
        assert call_args.output_target.value == "clipboard"

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(self.mock_schema)

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_copy_schema_excel_with_kwargs(self, mock_schema_generator):
        """Test schema generation and copying to clipboard for Excel with kwargs."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with Excel file type and additional kwargs
        result = generate_and_copy_schema(
            input_path="workbook.xlsx",
            file_type="excel",
            sheet_name="Sheet1",
            delimiter=",",  # This would be ignored for Excel but tests kwargs passing
            include_sample_data=True,
        )

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "workbook.xlsx"
        assert call_args.file_type.value == "excel"
        assert call_args.sheet_name == "Sheet1"
        assert call_args.include_sample_data is True

    @patch("forklift.api.SchemaGenerator")
    def test_generate_and_copy_schema_parquet_no_nrows(self, mock_schema_generator):
        """Test schema generation and copying to clipboard for Parquet with no nrows."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        # Test with Parquet file type and no nrows
        result = generate_and_copy_schema(input_path="big_data.parquet", file_type="parquet")

        # Verify the result
        assert result == self.mock_schema

        # Verify SchemaGenerator was called correctly
        call_args = mock_schema_generator.call_args[0][0]
        assert call_args.input_path == "big_data.parquet"
        assert call_args.file_type.value == "parquet"
        assert call_args.nrows is None

    @patch("forklift.api.SchemaGenerator")
    def test_schema_generator_exception_handling(self, mock_schema_generator):
        """Test that exceptions from SchemaGenerator are properly propagated."""
        # Setup mock to raise an exception
        mock_schema_generator.side_effect = ValueError("Invalid file format")

        # Test that the exception is propagated
        with pytest.raises(ValueError, match="Invalid file format"):
            generate_schema_from_csv("invalid.csv")

    @patch("forklift.api.SchemaGenerator")
    def test_generate_schema_exception_handling(self, mock_schema_generator):
        """Test that exceptions from generate_schema are properly propagated."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.side_effect = FileNotFoundError("File not found")
        mock_schema_generator.return_value = mock_generator_instance

        # Test that the exception is propagated
        with pytest.raises(FileNotFoundError, match="File not found"):
            generate_schema_from_parquet("nonexistent.parquet")

    @patch("forklift.api.SchemaGenerator")
    def test_output_schema_exception_handling(self, mock_schema_generator):
        """Test that exceptions from output_schema are properly propagated."""
        # Setup mock
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema.side_effect = PermissionError("Cannot write to file")
        mock_schema_generator.return_value = mock_generator_instance

        # Test that the exception is propagated
        with pytest.raises(PermissionError, match="Cannot write to file"):
            generate_and_save_schema("data.csv", "readonly.json", "csv")

    @patch("forklift.api.FileType")
    @patch("forklift.api.SchemaGenerator")
    def test_filetype_enum_conversion(self, mock_schema_generator, mock_filetype):
        """Test that string file types are properly converted to FileType enum."""
        # Setup mocks
        mock_generator_instance = Mock()
        mock_generator_instance.generate_schema.return_value = self.mock_schema
        mock_generator_instance.output_schema = Mock()
        mock_schema_generator.return_value = mock_generator_instance

        mock_filetype_instance = Mock()
        mock_filetype.return_value = mock_filetype_instance

        # Test FileType enum conversion in generate_and_save_schema
        generate_and_save_schema("test.csv", "output.json", "csv")

        # Verify FileType was called with the string
        mock_filetype.assert_called_with("csv")

    def test_api_functions_docstrings(self):
        """Test that all API functions have proper docstrings."""
        functions = [
            generate_schema_from_csv,
            generate_schema_from_excel,
            generate_schema_from_parquet,
            generate_and_save_schema,
            generate_and_copy_schema,
        ]

        for func in functions:
            assert func.__doc__ is not None, f"{func.__name__} is missing a docstring"
            assert len(func.__doc__.strip()) > 50, f"{func.__name__} has a very short docstring"
            assert "Args:" in func.__doc__, f"{func.__name__} docstring is missing Args section"
            assert (
                "Returns:" in func.__doc__ or "Example:" in func.__doc__
            ), f"{func.__name__} docstring is missing Returns or Example section"

    def test_api_functions_type_annotations(self):
        """Test that all API functions have proper type annotations."""
        import inspect

        functions = [
            generate_schema_from_csv,
            generate_schema_from_excel,
            generate_schema_from_parquet,
            generate_and_save_schema,
            generate_and_copy_schema,
        ]

        for func in functions:
            sig = inspect.signature(func)

            # Check that return type is annotated
            assert (
                sig.return_annotation != inspect.Signature.empty
            ), f"{func.__name__} is missing return type annotation"

            # Check that parameters have type annotations
            for param_name, param in sig.parameters.items():
                if param_name != "kwargs":  # kwargs don't need type annotations
                    assert (
                        param.annotation != inspect.Parameter.empty
                    ), f"{func.__name__} parameter '{param_name}' is missing type annotation"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
