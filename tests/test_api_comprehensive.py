"""Comprehensive tests for API module."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from forklift.api import (
    generate_schema_from_csv,
    generate_schema_from_excel,
    generate_schema_from_parquet,
    generate_and_save_schema,
    generate_and_copy_schema
)


class TestGenerateSchemaFromCSV:
    """Test generate_schema_from_csv function."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_basic(self, mock_generator_class):
        """Test basic CSV schema generation."""
        # Mock the generator instance and its generate_schema method
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Test Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_csv("test.csv")

        # Verify the result
        assert result == {"title": "Test Schema"}

        # Verify SchemaGenerator was instantiated and called correctly
        mock_generator_class.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_with_all_options(self, mock_generator_class):
        """Test CSV schema generation with all options."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Full Options Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_csv(
            input_path="data.csv",
            nrows=1000,
            delimiter="|",
            encoding="utf-16",
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["id", "uuid"]
        )

        assert result == {"title": "Full Options Schema"}
        mock_generator_class.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_csv_with_path_object(self, mock_generator_class):
        """Test CSV schema generation with Path object."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Path Schema"}
        mock_generator_class.return_value = mock_generator

        path_obj = Path("test.csv")
        result = generate_schema_from_csv(path_obj)

        assert result == {"title": "Path Schema"}
        mock_generator_class.assert_called_once()

    def test_generate_schema_from_csv_none_input(self):
        """Test CSV schema generation with None input."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_csv(None)

    def test_generate_schema_from_csv_empty_string(self):
        """Test CSV schema generation with empty string input."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_csv("")

    def test_generate_schema_from_csv_whitespace_string(self):
        """Test CSV schema generation with whitespace-only string input."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_csv("   ")


class TestGenerateSchemaFromExcel:
    """Test generate_schema_from_excel function."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_excel_basic(self, mock_generator_class):
        """Test basic Excel schema generation."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Excel Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_excel("test.xlsx")

        assert result == {"title": "Excel Schema"}
        mock_generator_class.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_excel_with_options(self, mock_generator_class):
        """Test Excel schema generation with options."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Excel Options Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_excel(
            input_path="workbook.xlsx",
            nrows=500,
            sheet_name="DataSheet",
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["record_id"]
        )

        assert result == {"title": "Excel Options Schema"}
        mock_generator_class.assert_called_once()

    def test_generate_schema_from_excel_none_input(self):
        """Test Excel schema generation with None input."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_excel(None)

    def test_generate_schema_from_excel_empty_string(self):
        """Test Excel schema generation with empty string input."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_excel("")


class TestGenerateSchemaFromParquet:
    """Test generate_schema_from_parquet function."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_parquet_basic(self, mock_generator_class):
        """Test basic Parquet schema generation."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Parquet Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_parquet("test.parquet")

        assert result == {"title": "Parquet Schema"}
        mock_generator_class.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_schema_from_parquet_with_options(self, mock_generator_class):
        """Test Parquet schema generation with options."""
        mock_generator = MagicMock()
        mock_generator.generate_schema.return_value = {"title": "Parquet Options Schema"}
        mock_generator_class.return_value = mock_generator

        result = generate_schema_from_parquet(
            input_path="data.parquet",
            nrows=2000,
            include_sample_data=True,
            infer_primary_key_from_metadata=True,
            user_specified_primary_key=["primary_key"]
        )

        assert result == {"title": "Parquet Options Schema"}
        mock_generator_class.assert_called_once()

    def test_generate_schema_from_parquet_none_input(self):
        """Test Parquet schema generation with None input."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_schema_from_parquet(None)

    def test_generate_schema_from_parquet_empty_string(self):
        """Test Parquet schema generation with empty string input."""
        with pytest.raises(ValueError, match="input_path cannot be empty"):
            generate_schema_from_parquet("")


class TestGenerateAndSaveSchema:
    """Test generate_and_save_schema function."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_and_save_schema_csv(self, mock_generator_class):
        """Test generating and saving schema for CSV."""
        mock_generator = MagicMock()
        mock_generator.generate_and_save_schema.return_value = None
        mock_generator_class.return_value = mock_generator

        generate_and_save_schema(
            input_path="test.csv",
            output_path="schema.json",
            file_type="csv"
        )

        mock_generator_class.assert_called_once()
        mock_generator.generate_and_save_schema.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_and_save_schema_excel(self, mock_generator_class):
        """Test generating and saving schema for Excel."""
        mock_generator = MagicMock()
        mock_generator.generate_and_save_schema.return_value = None
        mock_generator_class.return_value = mock_generator

        generate_and_save_schema(
            input_path="workbook.xlsx",
            output_path="excel_schema.json",
            file_type="excel",
            sheet_name="Sheet1",
            nrows=1000
        )

        mock_generator_class.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_and_save_schema_parquet(self, mock_generator_class):
        """Test generating and saving schema for Parquet."""
        mock_generator = MagicMock()
        mock_generator.generate_and_save_schema.return_value = None
        mock_generator_class.return_value = mock_generator

        generate_and_save_schema(
            input_path="data.parquet",
            output_path="parquet_schema.json",
            file_type="parquet",
            nrows=500
        )

        mock_generator_class.assert_called_once()

    def test_generate_and_save_schema_none_input(self):
        """Test generate_and_save_schema with None input."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_and_save_schema(None, "output.json", "csv")

    def test_generate_and_save_schema_none_output(self):
        """Test generate_and_save_schema with None output."""
        with pytest.raises(ValueError, match="output_path cannot be None"):
            generate_and_save_schema("input.csv", None, "csv")

    def test_generate_and_save_schema_invalid_file_type(self):
        """Test generate_and_save_schema with invalid file type."""
        with pytest.raises(ValueError, match="file_type must be one of"):
            generate_and_save_schema("input.txt", "output.json", "invalid")


class TestGenerateAndCopySchema:
    """Test generate_and_copy_schema function."""

    @patch('forklift.api.SchemaGenerator')
    def test_generate_and_copy_schema_csv(self, mock_generator_class):
        """Test generating and copying schema for CSV."""
        mock_generator = MagicMock()
        mock_generator.generate_and_copy_to_clipboard.return_value = None
        mock_generator_class.return_value = mock_generator

        generate_and_copy_schema(
            input_path="test.csv",
            file_type="csv"
        )

        mock_generator_class.assert_called_once()
        mock_generator.generate_and_copy_to_clipboard.assert_called_once()

    @patch('forklift.api.SchemaGenerator')
    def test_generate_and_copy_schema_with_options(self, mock_generator_class):
        """Test generating and copying schema with options."""
        mock_generator = MagicMock()
        mock_generator.generate_and_copy_to_clipboard.return_value = None
        mock_generator_class.return_value = mock_generator

        generate_and_copy_schema(
            input_path="data.xlsx",
            file_type="excel",
            sheet_name="Data",
            nrows=750,
            delimiter=";",
            encoding="utf-16"
        )

        mock_generator_class.assert_called_once()

    def test_generate_and_copy_schema_none_input(self):
        """Test generate_and_copy_schema with None input."""
        with pytest.raises(ValueError, match="input_path cannot be None"):
            generate_and_copy_schema(None, "csv")

    def test_generate_and_copy_schema_invalid_file_type(self):
        """Test generate_and_copy_schema with invalid file type."""
        with pytest.raises(ValueError, match="file_type must be one of"):
            generate_and_copy_schema("input.txt", "invalid")


class TestAPIModuleIntegration:
    """Test API module integration and edge cases."""

    def test_all_functions_importable(self):
        """Test that all API functions can be imported."""
        from forklift.api import (
            generate_schema_from_csv,
            generate_schema_from_excel,
            generate_schema_from_parquet,
            generate_and_save_schema,
            generate_and_copy_schema
        )

        # Verify all functions are callable
        assert callable(generate_schema_from_csv)
        assert callable(generate_schema_from_excel)
        assert callable(generate_schema_from_parquet)
        assert callable(generate_and_save_schema)
        assert callable(generate_and_copy_schema)

    def test_api_module_docstring(self):
        """Test that the API module has proper documentation."""
        import forklift.api as api_module

        assert api_module.__doc__ is not None
        assert "API functions for Forklift schema generation" in api_module.__doc__
        assert "programmatic access" in api_module.__doc__
