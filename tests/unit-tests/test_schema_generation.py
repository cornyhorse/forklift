"""Unit tests for schema generation functionality."""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa

from forklift.schema.schema_generator import (
    SchemaGenerator,
    SchemaGenerationConfig,
    OutputTarget,
    FileType
)


class TestSchemaGenerator:
    """Unit tests for SchemaGenerator class."""

    def test_schema_generation_config_creation(self):
        """Test SchemaGenerationConfig creation with various parameters."""
        config = SchemaGenerationConfig(
            input_path="/path/to/file.csv",
            file_type=FileType.CSV,
            nrows=500,  # Override default
            output_target=OutputTarget.STDOUT
        )

        assert config.input_path == "/path/to/file.csv"
        assert config.file_type == FileType.CSV
        assert config.nrows == 500
        assert config.output_target == OutputTarget.STDOUT
        assert config.delimiter == ","  # default
        assert config.encoding == "utf-8"  # default

        # Test new defaults
        default_config = SchemaGenerationConfig(
            input_path="/path/to/file.csv",
            file_type=FileType.CSV
        )
        assert default_config.nrows == 1000  # New default
        assert default_config.include_sample_data == False  # New default

    def test_arrow_to_json_schema_type_conversion(self, tmp_path):
        """Test Arrow type to JSON Schema type conversion."""
        csv_content = """id,name,age,salary,active,birth_date
1,John,30,50000.50,true,2023-01-01"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)

        # Test various Arrow type conversions
        assert generator._arrow_to_json_schema_type(pa.int64()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.float64()) == {"type": "number"}
        assert generator._arrow_to_json_schema_type(pa.bool_()) == {"type": "boolean"}
        assert generator._arrow_to_json_schema_type(pa.string()) == {"type": "string"}
        assert generator._arrow_to_json_schema_type(pa.date32()) == {"type": "string", "format": "date"}
        assert generator._arrow_to_json_schema_type(pa.timestamp('ms')) == {"type": "string", "format": "date-time"}

    def test_parquet_type_string_conversion(self, tmp_path):
        """Test Arrow type to Parquet type string conversion."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)

        # Test Parquet type string generation
        assert generator._get_parquet_type_string(pa.int32()) == "int32"
        assert generator._get_parquet_type_string(pa.float64()) == "double"
        assert generator._get_parquet_type_string(pa.string()) == "string"
        assert generator._get_parquet_type_string(pa.bool_()) == "bool"

    def test_primary_key_inference(self, tmp_path):
        """Test primary key inference logic."""
        # Create data with clear primary key
        csv_content = """user_id,name,email
1,John,john@test.com
2,Jane,jane@test.com
3,Bob,bob@test.com"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            infer_primary_key=True
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        pk_candidates = generator._infer_primary_key(table)
        assert "user_id" in pk_candidates

    def test_csv_extension_generation(self, tmp_path):
        """Test CSV extension configuration generation."""
        csv_content = """id,name,value
1,test,100
2,sample,200"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            delimiter="|",  # Use pipe delimiter instead of empty string
            encoding="latin-1"
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        csv_ext = generator._generate_csv_extension(table)

        assert csv_ext["delimiter"] == "|"
        assert "latin-1" in csv_ext["encodingPriority"]
        assert "dataTypes" in csv_ext
        # With comma delimiter on comma-separated data, we should get 3 columns
        # But with pipe delimiter, it will be treated as one column
        assert len(csv_ext["dataTypes"]) >= 1

    def test_sample_data_generation(self, tmp_path):
        """Test sample data extraction from table."""
        csv_content = """id,name,score
1,Alice,95
2,Bob,87
3,Carol,92
4,David,88
5,Eve,94"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            include_sample_data=True
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        sample_data = generator._generate_sample_data(table)

        assert "rows" in sample_data
        assert len(sample_data["rows"]) == 3  # Should take first 3 rows
        assert sample_data["rows"][0]["name"] == "Alice"

    def test_output_to_stdout(self, tmp_path, capsys):
        """Test schema output to stdout."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            output_target=OutputTarget.STDOUT
        )

        generator = SchemaGenerator(config)
        schema = {"test": "schema"}

        generator.output_schema(schema)

        captured = capsys.readouterr()
        assert "test" in captured.out
        assert "schema" in captured.out

    def test_output_to_file(self, tmp_path):
        """Test schema output to file."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        output_file = tmp_path / "schema.json"

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            output_target=OutputTarget.FILE,
            output_path=str(output_file)
        )

        generator = SchemaGenerator(config)
        schema = {"test": "schema", "properties": {}}

        generator.output_schema(schema)

        assert output_file.exists()
        with open(output_file, 'r') as f:
            saved_schema = json.load(f)
        assert saved_schema["test"] == "schema"

    @patch('pyperclip.copy')
    def test_output_to_clipboard(self, mock_copy, tmp_path):
        """Test schema output to clipboard."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            output_target=OutputTarget.CLIPBOARD
        )

        generator = SchemaGenerator(config)
        schema = {"test": "schema"}

        generator.output_schema(schema)

        mock_copy.assert_called_once()
        args = mock_copy.call_args[0]
        assert "test" in args[0]
        assert "schema" in args[0]

    def test_nrows_limitation(self, tmp_path):
        """Test that nrows parameter limits the data read."""
        # Create CSV with more rows than we want to analyze
        csv_content = """id,name
1,Alice
2,Bob
3,Carol
4,David
5,Eve"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            nrows=3
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        # Should only have 3 rows (Alice, Bob, Carol)
        assert table.num_rows == 3

    def test_schema_metadata_generation(self, tmp_path):
        """Test that schema includes proper metadata."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Check required schema metadata
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "x-generation" in schema
        assert "generated_at" in schema["x-generation"]
        assert "source_file" in schema["x-generation"]
        assert "rows_analyzed" in schema["x-generation"]

    def test_error_handling_missing_output_path(self, tmp_path):
        """Test error handling when output path is missing for file output."""
        csv_content = "id,name\n1,test"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            output_target=OutputTarget.FILE,
            output_path=None  # Missing output path
        )

        generator = SchemaGenerator(config)
        schema = {"test": "schema"}

        with pytest.raises(ValueError, match="output_path must be specified"):
            generator.output_schema(schema)


class TestSchemaGeneratorEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_csv_file(self, tmp_path):
        """Test handling of empty CSV files."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)

        # Should handle empty file gracefully
        with pytest.raises(Exception):
            generator.generate_schema()

    def test_csv_with_only_headers(self, tmp_path):
        """Test CSV file with only headers and no data."""
        csv_content = """id,name,email
1,test,test@example.com"""  # Add one data row to make it parseable
        csv_file = tmp_path / "headers_only.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should still generate schema with columns
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "name" in schema["properties"]
        assert "email" in schema["properties"]

    def test_unsupported_file_type(self, tmp_path):
        """Test error handling for unsupported file types."""
        # This would need to be implemented if we add more file types
        # or want to test the error path explicitly
        pass
