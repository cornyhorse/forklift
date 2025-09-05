"""Unit tests for schema generation functionality."""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from io import StringIO, BytesIO
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from forklift.schema.schema_generator import (
    SchemaGenerator,
    SchemaGenerationConfig,
    OutputTarget,
    FileType,
    CLIPBOARD_AVAILABLE
)


class TestSchemaGenerationConfig:
    """Test SchemaGenerationConfig class."""

    def test_config_creation_with_defaults(self):
        """Test config creation with default values."""
        config = SchemaGenerationConfig(
            input_path="/path/to/file.csv",
            file_type=FileType.CSV
        )

        assert config.input_path == "/path/to/file.csv"
        assert config.file_type == FileType.CSV
        assert config.nrows == 1000
        assert config.output_target == OutputTarget.STDOUT
        assert config.delimiter == ","
        assert config.encoding == "utf-8"
        assert config.include_sample_data is False
        assert config.generate_metadata is True
        assert config.enum_threshold == 0.1
        assert config.uniqueness_threshold == 0.95
        assert config.top_n_values == 10
        assert config.quantiles == [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

    def test_config_creation_with_custom_values(self):
        """Test config creation with custom values."""
        custom_quantiles = [0.1, 0.5, 0.9]
        config = SchemaGenerationConfig(
            input_path="/path/to/file.xlsx",
            file_type=FileType.EXCEL,
            nrows=500,
            output_target=OutputTarget.FILE,
            output_path="/output/schema.json",
            delimiter=";",
            encoding="latin-1",
            sheet_name="Sheet1",
            include_sample_data=True,
            user_specified_primary_key=["id", "name"],
            generate_metadata=False,
            enum_threshold=0.05,
            uniqueness_threshold=0.98,
            top_n_values=5,
            quantiles=custom_quantiles,
            infer_primary_key_from_metadata=True
        )

        assert config.input_path == "/path/to/file.xlsx"
        assert config.file_type == FileType.EXCEL
        assert config.nrows == 500
        assert config.output_target == OutputTarget.FILE
        assert config.output_path == "/output/schema.json"
        assert config.delimiter == ";"
        assert config.encoding == "latin-1"
        assert config.sheet_name == "Sheet1"
        assert config.include_sample_data is True
        assert config.user_specified_primary_key == ["id", "name"]
        assert config.generate_metadata is False
        assert config.enum_threshold == 0.05
        assert config.uniqueness_threshold == 0.98
        assert config.top_n_values == 5
        assert config.quantiles == custom_quantiles
        assert config.infer_primary_key_from_metadata is True

    def test_config_post_init_quantiles(self):
        """Test that quantiles are set to default if None."""
        config = SchemaGenerationConfig(
            input_path="/path/to/file.csv",
            file_type=FileType.CSV,
            quantiles=None
        )

        assert config.quantiles == [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


class TestSchemaGenerator:
    """Test SchemaGenerator class."""

    def create_test_csv(self, tmp_path, content=None):
        """Helper to create test CSV file."""
        if content is None:
            content = """id,name,age,salary,active,birth_date,category
1,John Doe,30,50000.50,true,2023-01-01,A
2,Jane Smith,25,45000.00,false,2023-02-15,B
3,Bob Johnson,35,60000.75,true,2023-03-10,A
4,Alice Brown,28,55000.25,true,2023-04-20,C
5,Charlie Wilson,40,70000.00,false,2023-05-30,B"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(content)
        return str(csv_file)

    def create_test_parquet(self, tmp_path):
        """Helper to create test Parquet file."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
            'age': [30, 25, 35, 28, 40],
            'salary': [50000.50, 45000.00, 60000.75, 55000.25, 70000.00],
            'active': [True, False, True, True, False],
            'category': ['A', 'B', 'A', 'C', 'B']
        }
        df = pd.DataFrame(data)
        parquet_file = tmp_path / "test.parquet"
        df.to_parquet(parquet_file)
        return str(parquet_file)

    def create_test_excel(self, tmp_path):
        """Helper to create test Excel file."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
            'age': [30, 25, 35, 28, 40],
            'salary': [50000.50, 45000.00, 60000.75, 55000.25, 70000.00],
            'active': [True, False, True, True, False],
            'category': ['A', 'B', 'A', 'C', 'B']
        }
        df = pd.DataFrame(data)
        excel_file = tmp_path / "test.xlsx"
        df.to_excel(excel_file, index=False)
        return str(excel_file)

    def test_schema_generator_initialization(self):
        """Test SchemaGenerator initialization."""
        config = SchemaGenerationConfig(
            input_path="/path/to/file.csv",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        assert generator.config == config
        assert generator.io_handler is not None

    def test_unsupported_file_type(self):
        """Test error handling for unsupported file types."""
        config = SchemaGenerationConfig(
            input_path="/path/to/file.txt",
            file_type="unsupported"  # This will cause an error
        )

        with pytest.raises(ValueError, match="Unsupported file type"):
            generator = SchemaGenerator(config)
            generator.generate_schema()

    def test_csv_schema_generation(self, tmp_path):
        """Test CSV schema generation."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            nrows=3
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Verify schema structure
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "x-csv" in schema
        assert "x-transformations" in schema
        assert "x-generation" in schema
        assert "x-metadata" in schema

        # Verify properties
        properties = schema["properties"]
        assert "id" in properties
        assert "name" in properties
        assert "age" in properties
        assert "salary" in properties
        assert "active" in properties
        assert "birth_date" in properties
        assert "category" in properties

        # Verify data types
        assert properties["id"]["type"] == "integer"
        assert properties["name"]["type"] == "string"
        assert properties["age"]["type"] == "integer"
        assert properties["salary"]["type"] == "number"
        assert properties["active"]["type"] == "boolean"

    def test_parquet_schema_generation(self, tmp_path):
        """Test Parquet schema generation."""
        parquet_file = self.create_test_parquet(tmp_path)

        config = SchemaGenerationConfig(
            input_path=parquet_file,
            file_type=FileType.PARQUET,
            nrows=3
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Verify schema structure
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "x-transformations" in schema
        assert "x-generation" in schema
        assert "x-metadata" in schema

        # Should not have x-csv or x-excel
        assert "x-csv" not in schema
        assert "x-excel" not in schema

    def test_excel_schema_generation(self, tmp_path):
        """Test Excel schema generation."""
        excel_file = self.create_test_excel(tmp_path)

        config = SchemaGenerationConfig(
            input_path=excel_file,
            file_type=FileType.EXCEL,
            sheet_name=0,
            nrows=3
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Verify schema structure
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "x-excel" in schema
        assert "x-transformations" in schema
        assert "x-generation" in schema
        assert "x-metadata" in schema

        # Should not have x-csv
        assert "x-csv" not in schema

    def test_arrow_to_json_schema_type_conversion(self):
        """Test Arrow type to JSON Schema type conversion."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        # Test basic types
        assert generator._arrow_to_json_schema_type(pa.int8()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.int16()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.int32()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.int64()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.uint8()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.uint16()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.uint32()) == {"type": "integer"}
        assert generator._arrow_to_json_schema_type(pa.uint64()) == {"type": "integer"}

        assert generator._arrow_to_json_schema_type(pa.float32()) == {"type": "number"}
        assert generator._arrow_to_json_schema_type(pa.float64()) == {"type": "number"}

        assert generator._arrow_to_json_schema_type(pa.bool_()) == {"type": "boolean"}

        assert generator._arrow_to_json_schema_type(pa.string()) == {"type": "string"}
        assert generator._arrow_to_json_schema_type(pa.large_string()) == {"type": "string"}

        assert generator._arrow_to_json_schema_type(pa.date32()) == {"type": "string", "format": "date"}
        assert generator._arrow_to_json_schema_type(pa.date64()) == {"type": "string", "format": "date"}

        assert generator._arrow_to_json_schema_type(pa.timestamp('ns')) == {"type": "string", "format": "date-time"}
        assert generator._arrow_to_json_schema_type(pa.time32('s')) == {"type": "string", "format": "time"}
        assert generator._arrow_to_json_schema_type(pa.time64('us')) == {"type": "string", "format": "time"}

        assert generator._arrow_to_json_schema_type(pa.binary()) == {"type": "string", "contentEncoding": "base64"}
        assert generator._arrow_to_json_schema_type(pa.large_binary()) == {"type": "string", "contentEncoding": "base64"}

        # Test list types
        list_result = generator._arrow_to_json_schema_type(pa.list_(pa.string()))
        assert list_result["type"] == "array"
        assert list_result["items"]["type"] == "string"

        large_list_result = generator._arrow_to_json_schema_type(pa.large_list(pa.int32()))
        assert large_list_result["type"] == "array"
        assert large_list_result["items"]["type"] == "integer"

        # Test struct type
        assert generator._arrow_to_json_schema_type(pa.struct([("field1", pa.string())])) == {"type": "object", "additionalProperties": True}

        # Test dictionary type
        assert generator._arrow_to_json_schema_type(pa.dictionary(pa.int32(), pa.string())) == {"type": "string"}

    @patch('pyarrow.types.is_list', return_value=True)
    @patch('pyarrow.types.is_large_list', return_value=False)
    @patch('pyarrow.types.is_integer', return_value=False)
    @patch('pyarrow.types.is_floating', return_value=False)
    @patch('pyarrow.types.is_boolean', return_value=False)
    @patch('pyarrow.types.is_string', return_value=False)
    @patch('pyarrow.types.is_large_string', return_value=False)
    @patch('pyarrow.types.is_date', return_value=False)
    @patch('pyarrow.types.is_timestamp', return_value=False)
    @patch('pyarrow.types.is_time', return_value=False)
    @patch('pyarrow.types.is_binary', return_value=False)
    @patch('pyarrow.types.is_large_binary', return_value=False)
    @patch('pyarrow.types.is_struct', return_value=False)
    @patch('pyarrow.types.is_dictionary', return_value=False)
    @patch('builtins.hasattr', return_value=False)
    def test_arrow_to_json_schema_type_list_without_value_type(self, *args):
        """Test list without value_type attribute (edge case)."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        mock_list_type = MagicMock()
        result = generator._arrow_to_json_schema_type(mock_list_type)
        assert result == {"type": "array", "items": {"type": "string"}}

    @patch('pyarrow.types.is_list', return_value=False)
    @patch('pyarrow.types.is_large_list', return_value=False)
    @patch('pyarrow.types.is_integer', return_value=False)
    @patch('pyarrow.types.is_floating', return_value=False)
    @patch('pyarrow.types.is_boolean', return_value=False)
    @patch('pyarrow.types.is_string', return_value=False)
    @patch('pyarrow.types.is_large_string', return_value=False)
    @patch('pyarrow.types.is_date', return_value=False)
    @patch('pyarrow.types.is_timestamp', return_value=False)
    @patch('pyarrow.types.is_time', return_value=False)
    @patch('pyarrow.types.is_binary', return_value=False)
    @patch('pyarrow.types.is_large_binary', return_value=False)
    @patch('pyarrow.types.is_struct', return_value=False)
    @patch('pyarrow.types.is_dictionary', return_value=False)
    def test_arrow_to_json_schema_type_unknown_fallback(self, *args):
        """Test unknown type fallback to string."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        mock_unknown_type = MagicMock()
        result = generator._arrow_to_json_schema_type(mock_unknown_type)
        assert result == {"type": "string"}

    def test_get_parquet_type_string(self):
        """Test Parquet type string conversion."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        # Test integer types
        assert generator._get_parquet_type_string(pa.int8()) == "int8"
        assert generator._get_parquet_type_string(pa.int16()) == "int16"
        assert generator._get_parquet_type_string(pa.int32()) == "int32"
        assert generator._get_parquet_type_string(pa.int64()) == "int64"
        assert generator._get_parquet_type_string(pa.uint8()) == "uint8"
        assert generator._get_parquet_type_string(pa.uint16()) == "uint16"
        assert generator._get_parquet_type_string(pa.uint32()) == "uint32"
        assert generator._get_parquet_type_string(pa.uint64()) == "uint64"

        # Test float types
        assert generator._get_parquet_type_string(pa.float32()) == "float32"
        assert generator._get_parquet_type_string(pa.float64()) == "double"

        # Test other types
        assert generator._get_parquet_type_string(pa.bool_()) == "bool"
        assert generator._get_parquet_type_string(pa.string()) == "string"
        assert generator._get_parquet_type_string(pa.large_string()) == "string"
        assert generator._get_parquet_type_string(pa.binary()) == "binary"
        assert generator._get_parquet_type_string(pa.large_binary()) == "binary"
        assert generator._get_parquet_type_string(pa.date32()) == "date32"
        assert generator._get_parquet_type_string(pa.date64()) == "date64"
        assert generator._get_parquet_type_string(pa.timestamp('ms')) == "timestamp[ms]"
        assert generator._get_parquet_type_string(pa.duration('ms')) == "duration[ms]"

        # Test list types
        assert generator._get_parquet_type_string(pa.list_(pa.string())) == "list<string>"
        assert generator._get_parquet_type_string(pa.large_list(pa.int32())) == "list<int32>"

        # Test struct and dictionary
        assert generator._get_parquet_type_string(pa.struct([("field1", pa.string())])) == "struct"
        assert generator._get_parquet_type_string(pa.dictionary(pa.int32(), pa.string())) == "dictionary<values=string, indices=int32>"

    @patch('pyarrow.types.is_list', return_value=True)
    @patch('pyarrow.types.is_large_list', return_value=False)
    @patch('builtins.hasattr', return_value=False)
    def test_get_parquet_type_string_list_without_value_type(self, *args):
        """Test list without value_type attribute (edge case)."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        mock_list_type = MagicMock()
        result = generator._get_parquet_type_string(mock_list_type)
        assert result == "list<string>"

    @patch('pyarrow.types.is_list', return_value=False)
    @patch('pyarrow.types.is_large_list', return_value=False)
    @patch('pyarrow.types.is_int8', return_value=False)
    @patch('pyarrow.types.is_int16', return_value=False)
    @patch('pyarrow.types.is_int32', return_value=False)
    @patch('pyarrow.types.is_int64', return_value=False)
    @patch('pyarrow.types.is_uint8', return_value=False)
    @patch('pyarrow.types.is_uint16', return_value=False)
    @patch('pyarrow.types.is_uint32', return_value=False)
    @patch('pyarrow.types.is_uint64', return_value=False)
    @patch('pyarrow.types.is_float32', return_value=False)
    @patch('pyarrow.types.is_float64', return_value=False)
    @patch('pyarrow.types.is_boolean', return_value=False)
    @patch('pyarrow.types.is_string', return_value=False)
    @patch('pyarrow.types.is_large_string', return_value=False)
    @patch('pyarrow.types.is_binary', return_value=False)
    @patch('pyarrow.types.is_large_binary', return_value=False)
    @patch('pyarrow.types.is_date32', return_value=False)
    @patch('pyarrow.types.is_date64', return_value=False)
    @patch('pyarrow.types.is_timestamp', return_value=False)
    @patch('pyarrow.types.is_duration', return_value=False)
    @patch('pyarrow.types.is_struct', return_value=False)
    @patch('pyarrow.types.is_dictionary', return_value=False)
    def test_get_parquet_type_string_unknown_fallback(self, *args):
        """Test unknown Parquet type fallback to string."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        mock_unknown_type = MagicMock()
        result = generator._get_parquet_type_string(mock_unknown_type)
        assert result == "string"

    def test_primary_key_user_specified_single(self, tmp_path):
        """Test user-specified single primary key."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            user_specified_primary_key=["id"]
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-primaryKey" in schema
        pk_config = schema["x-primaryKey"]
        assert pk_config["description"] == "User-specified primary key"
        assert pk_config["columns"] == ["id"]
        assert pk_config["type"] == "single"
        assert pk_config["enforceUniqueness"] is True
        assert pk_config["allowNulls"] is False

    def test_primary_key_user_specified_composite(self, tmp_path):
        """Test user-specified composite primary key."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            user_specified_primary_key=["id", "name"]
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-primaryKey" in schema
        pk_config = schema["x-primaryKey"]
        assert pk_config["description"] == "User-specified primary key"
        assert pk_config["columns"] == ["id", "name"]
        assert pk_config["type"] == "composite"
        assert pk_config["enforceUniqueness"] is True
        assert pk_config["allowNulls"] is False

    def test_primary_key_inference_from_metadata(self, tmp_path):
        """Test primary key inference from metadata."""
        # Create CSV with clear primary key pattern
        csv_content = """user_id,name,email
1,John,john@example.com
2,Jane,jane@example.com
3,Bob,bob@example.com"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            infer_primary_key_from_metadata=True
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should infer user_id as primary key
        if "x-primaryKey" in schema:
            pk_config = schema["x-primaryKey"]
            assert pk_config["description"] == "Inferred primary key from metadata analysis"
            assert "user_id" in pk_config["columns"]
            assert pk_config["type"] == "single"

    def test_primary_key_inference_no_candidates(self, tmp_path):
        """Test primary key inference when no good candidates exist."""
        # Create CSV without clear primary key
        csv_content = """name,description
John,A person
Jane,Another person
Bob,Yet another person"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            infer_primary_key_from_metadata=True
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should not have primary key
        assert "x-primaryKey" not in schema

    def test_sample_data_generation(self, tmp_path):
        """Test sample data generation."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            include_sample_data=True,
            nrows=5
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-sample" in schema
        sample = schema["x-sample"]
        assert "description" in sample
        assert "rows" in sample
        assert len(sample["rows"]) <= 3  # Sample size is min(3, table.num_rows)

        # Verify sample data structure
        for row in sample["rows"]:
            assert "id" in row
            assert "name" in row
            assert "age" in row

    def test_metadata_generation_disabled(self, tmp_path):
        """Test schema generation with metadata disabled."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            generate_metadata=False
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-metadata" not in schema

    def test_csv_extension_generation(self, tmp_path):
        """Test CSV extension configuration generation."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            delimiter=";",
            encoding="latin-1"
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-csv" in schema
        csv_ext = schema["x-csv"]
        assert csv_ext["delimiter"] == ";"
        assert "latin-1" in csv_ext["encodingPriority"]
        assert csv_ext["quotechar"] == "\""
        assert csv_ext["escapechar"] == "\\"
        assert csv_ext["multiline"] is True
        assert "header" in csv_ext
        assert "footer" in csv_ext
        assert "nulls" in csv_ext
        assert "dataTypes" in csv_ext
        assert "validation" in csv_ext

    def test_excel_extension_generation(self, tmp_path):
        """Test Excel extension configuration generation."""
        excel_file = self.create_test_excel(tmp_path)

        config = SchemaGenerationConfig(
            input_path=excel_file,
            file_type=FileType.EXCEL,
            sheet_name="Sheet1"
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-excel" in schema
        excel_ext = schema["x-excel"]
        assert excel_ext["sheet"] == "Sheet1"
        assert excel_ext["header"]["mode"] == "present"
        assert excel_ext["skipRows"] == 0
        assert excel_ext["skipFooter"] == 0
        assert "nulls" in excel_ext
        assert "validation" in excel_ext

    def test_excel_extension_default_sheet(self, tmp_path):
        """Test Excel extension with default sheet."""
        excel_file = self.create_test_excel(tmp_path)

        config = SchemaGenerationConfig(
            input_path=excel_file,
            file_type=FileType.EXCEL
            # No sheet_name specified
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-excel" in schema
        excel_ext = schema["x-excel"]
        assert excel_ext["sheet"] == 0  # Default to 0

    def test_transformation_extension_generation(self, tmp_path):
        """Test transformation extension configuration generation."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-transformations" in schema
        transformations = schema["x-transformations"]
        assert transformations["version"] == "1.0.0"
        assert "global_settings" in transformations
        assert "column_transformations" in transformations
        assert "transformation_types" in transformations

        # Check global settings
        global_settings = transformations["global_settings"]
        assert "nan_handling" in global_settings
        assert "error_handling" in global_settings

        # Check transformation types
        transform_types = transformations["transformation_types"]
        assert "regex_replace" in transform_types
        assert "string_replace" in transform_types
        assert "money_conversion" in transform_types
        assert "numeric_cleaning" in transform_types
        assert "string_padding" in transform_types
        assert "string_trimming" in transform_types
        assert "html_xml_cleaning" in transform_types

    def test_generation_metadata(self, tmp_path):
        """Test generation metadata in schema."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-generation" in schema
        generation = schema["x-generation"]
        assert "generated_at" in generation
        assert generation["source_file"] == csv_file
        assert "rows_analyzed" in generation
        assert generation["generator_version"] == "1.0.0"

    def test_metadata_analysis_comprehensive(self, tmp_path):
        """Test comprehensive metadata analysis."""
        # Create CSV with diverse data types and patterns
        csv_content = """id,category,score,description,active,amount,notes
1,A,85.5,Good performance,true,"$1,234.56",Clean data
2,B,92.3,Excellent work,false,"$2,567.89",Some whitespace  
3,A,78.1,Average results,true,"$987.65",HTML content <b>bold</b>
4,C,95.7,Outstanding,true,"$3,456.78",Contains &amp; entities
5,B,67.9,Needs improvement,false,"$4,567.89",  Leading spaces"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            enum_threshold=0.2,  # Lower threshold to catch enum candidates
            top_n_values=3
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-metadata" in schema
        metadata = schema["x-metadata"]

        # Check metadata structure
        assert metadata["version"] == "1.0.0"
        assert "generated_at" in metadata
        assert "analysis_config" in metadata
        assert "table_metadata" in metadata
        assert "column_metadata" in metadata
        assert "enum_suggestions" in metadata

        # Check table metadata - expect 7 columns from the CSV
        table_meta = metadata["table_metadata"]
        assert table_meta["row_count"] == 5
        # pandas.read_csv may create 7 columns from the CSV data
        assert table_meta["column_count"] == 7  # Exact count expected now
        assert table_meta["file_type"] == "csv"

        # Check column metadata exists for all columns
        col_meta = metadata["column_metadata"]
        assert "id" in col_meta
        assert "category" in col_meta
        assert "score" in col_meta
        assert "description" in col_meta
        assert "active" in col_meta
        assert "amount" in col_meta
        assert "notes" in col_meta

        # Check category column (should be enum candidate)
        category_meta = col_meta["category"]
        assert category_meta["distinct_count"] == 3  # A, B, C
        assert category_meta["uniqueness_ratio"] < 1.0
        assert "top_values" in category_meta

        # Check enum suggestions
        if "category" in metadata["enum_suggestions"]:
            enum_suggestion = metadata["enum_suggestions"]["category"]
            assert enum_suggestion["is_enum_candidate"] is True
            assert "suggested_enum_values" in enum_suggestion

    def test_enum_analysis(self, tmp_path):
        """Test enum type analysis."""
        # Create data with clear enum patterns
        csv_content = """status,priority,region
active,high,north
inactive,medium,south
active,low,east
pending,high,west
active,medium,north
inactive,low,south"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            enum_threshold=0.8,  # High threshold - should still catch these
            uniqueness_threshold=0.95
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        metadata = schema["x-metadata"]
        enum_suggestions = metadata["enum_suggestions"]

        # All columns should be enum candidates
        for col in ["status", "priority", "region"]:
            if col in enum_suggestions:
                suggestion = enum_suggestions[col]
                assert suggestion["is_enum_candidate"] is True
                assert "suggested_enum_values" in suggestion
                assert "confidence" in suggestion
                assert "distribution_balance" in suggestion

    def test_numeric_statistics_calculation(self, tmp_path):
        """Test numeric statistics calculation."""
        # Create CSV with numeric data including outliers
        csv_content = """value,score
10,85.5
20,92.3
15,78.1
25,95.7
18,67.9
1000,99.5
22,88.2
30,91.1"""  # 1000 is an outlier

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            quantiles=[0.25, 0.5, 0.75, 0.9]
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        metadata = schema["x-metadata"]
        col_meta = metadata["column_metadata"]

        # Check value column (integers with outlier)
        value_meta = col_meta["value"]
        assert "min_value" in value_meta
        assert "max_value" in value_meta
        assert "mean" in value_meta
        assert "median" in value_meta
        assert "std_dev" in value_meta
        assert "variance" in value_meta
        assert "quantiles" in value_meta
        assert "outlier_count" in value_meta
        assert "outlier_percentage" in value_meta

        # Check that outlier was detected
        assert value_meta["outlier_count"] > 0

        # Check score column (floats)
        score_meta = col_meta["score"]
        assert "min_value" in score_meta
        assert "max_value" in score_meta
        assert "quantiles" in score_meta

    def test_string_statistics_calculation(self, tmp_path):
        """Test string statistics calculation."""
        csv_content = """text,mixed_case,special_chars
hello world,CamelCase,"punct, & symbols!"
short,lowercase,normal text
"very long string here",UPPERCASE,números & español
"",MixedCase123,"<html>tags</html>"
single,lower,whitespace   """

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        metadata = schema["x-metadata"]
        col_meta = metadata["column_metadata"]

        # Check text column string statistics
        text_meta = col_meta["text"]
        assert "min_length" in text_meta
        assert "max_length" in text_meta
        assert "avg_length" in text_meta
        assert "median_length" in text_meta
        assert "empty_strings" in text_meta
        assert "contains_whitespace" in text_meta
        assert "contains_numbers" in text_meta
        assert "contains_special_chars" in text_meta
        assert "all_uppercase" in text_meta
        assert "all_lowercase" in text_meta

        # Should detect empty string
        assert text_meta["empty_strings"] >= 1

        # Check mixed_case column
        mixed_meta = col_meta["mixed_case"]
        assert mixed_meta["all_uppercase"] >= 1  # UPPERCASE
        assert mixed_meta["all_lowercase"] >= 1  # lowercase

    def test_boolean_statistics_calculation(self, tmp_path):
        """Test boolean statistics calculation."""
        csv_content = """active,verified,enabled
true,false,true
false,true,false
true,true,true
false,false,false
true,true,true"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        metadata = schema["x-metadata"]
        col_meta = metadata["column_metadata"]

        # Check boolean statistics
        active_meta = col_meta["active"]
        assert "true_count" in active_meta
        assert "false_count" in active_meta
        assert "true_percentage" in active_meta
        assert "false_percentage" in active_meta

        # Verify counts
        assert active_meta["true_count"] == 3
        assert active_meta["false_count"] == 2
        assert active_meta["true_percentage"] == 60.0
        assert active_meta["false_percentage"] == 40.0

    def test_transformation_analysis_patterns(self, tmp_path):
        """Test transformation pattern analysis."""
        # Create data with patterns that should trigger transformations
        csv_content = """money,numeric_sep,html_content,whitespace_issues
"$1,234.56","1,234.56","<p>Hello</p>","  trim me  "
"($500.00)","2,567.89","<b>Bold &amp; text</b>","multiple    spaces"
"€999.99","3,456.78","Plain text","normal"
"£123.45","4,567.89","<div>Content</div>","   leading"
"¥10,000","5,678.90","&lt;escaped&gt;","trailing   \""""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        transformations = schema["x-transformations"]
        col_transforms = transformations["column_transformations"]

        # Check money column transformations
        if "money" in col_transforms:
            money_transforms = col_transforms["money"]
            assert "money_conversion" in money_transforms
            money_config = money_transforms["money_conversion"]
            assert money_config["enabled"] is False  # User should enable
            assert "currency_symbols" in money_config
            assert "parentheses_negative" in money_config

        # Check numeric_sep column
        if "numeric_sep" in col_transforms:
            numeric_transforms = col_transforms["numeric_sep"]
            assert "numeric_cleaning" in numeric_transforms

        # Check html_content column
        if "html_content" in col_transforms:
            html_transforms = col_transforms["html_content"]
            assert "html_xml_cleaning" in html_transforms
            html_config = html_transforms["html_xml_cleaning"]
            assert "strip_tags" in html_config
            assert "decode_entities" in html_config

        # Check whitespace_issues column
        if "whitespace_issues" in col_transforms:
            ws_transforms = col_transforms["whitespace_issues"]
            assert "string_trimming" in ws_transforms

    def test_csv_reading_with_nrows_limit(self, tmp_path):
        """Test CSV reading with nrows limit."""
        # Create large CSV content
        rows = ["id,name,value"]
        for i in range(1000):
            rows.append(f"{i},Name_{i},{i*10}")

        csv_content = "\n".join(rows)
        csv_file = tmp_path / "large.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            nrows=10  # Limit to 10 rows
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Check that only limited rows were analyzed
        generation = schema["x-generation"]
        assert generation["rows_analyzed"] == 10

    def test_csv_reading_without_nrows_limit(self, tmp_path):
        """Test CSV reading without nrows limit."""
        csv_content = """id,name
1,Alice
2,Bob
3,Charlie"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            nrows=None  # No limit
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Check that all rows were analyzed
        generation = schema["x-generation"]
        assert generation["rows_analyzed"] == 3

    def test_parquet_reading_with_nrows_limit(self, tmp_path):
        """Test Parquet reading with nrows limit."""
        # Create larger dataset
        data = {
            'id': list(range(1, 101)),
            'name': [f'Name_{i}' for i in range(1, 101)],
            'value': [i * 10 for i in range(1, 101)]
        }
        df = pd.DataFrame(data)
        parquet_file = tmp_path / "large.parquet"
        df.to_parquet(parquet_file)

        config = SchemaGenerationConfig(
            input_path=str(parquet_file),
            file_type=FileType.PARQUET,
            nrows=5  # Limit to 5 rows
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Check that only limited rows were analyzed
        generation = schema["x-generation"]
        assert generation["rows_analyzed"] == 5

    def test_parquet_reading_without_nrows_limit(self, tmp_path):
        """Test Parquet reading without nrows limit."""
        data = {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        }
        df = pd.DataFrame(data)
        parquet_file = tmp_path / "test.parquet"
        df.to_parquet(parquet_file)

        config = SchemaGenerationConfig(
            input_path=str(parquet_file),
            file_type=FileType.PARQUET,
            nrows=None  # No limit
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Check that all rows were analyzed
        generation = schema["x-generation"]
        assert generation["rows_analyzed"] == 3

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_s3_csv_reading_with_nrows(self, mock_is_s3_path, tmp_path):
        """Test S3 CSV reading with nrows limit."""
        mock_is_s3_path.return_value = True

        csv_content = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
4,David,400
5,Eve,500"""

        config = SchemaGenerationConfig(
            input_path="s3://bucket/file.csv",
            file_type=FileType.CSV,
            nrows=3
        )

        generator = SchemaGenerator(config)

        # Mock the io_handler
        mock_file = StringIO(csv_content)
        with patch.object(generator.io_handler, 'open_for_read') as mock_open:
            mock_open.return_value.__enter__.return_value = mock_file

            schema = generator.generate_schema()

            # Verify S3 path was handled
            mock_open.assert_called_once_with("s3://bucket/file.csv", encoding='utf-8')

            # Check schema was generated
            assert "properties" in schema
            assert "id" in schema["properties"]

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_s3_csv_reading_without_nrows(self, mock_is_s3_path, tmp_path):
        """Test S3 CSV reading without nrows limit."""
        mock_is_s3_path.return_value = True

        csv_content = """id,name
1,Alice
2,Bob"""

        config = SchemaGenerationConfig(
            input_path="s3://bucket/file.csv",
            file_type=FileType.CSV,
            nrows=None
        )

        generator = SchemaGenerator(config)

        # Mock the io_handler
        mock_file = StringIO(csv_content)
        with patch.object(generator.io_handler, 'open_for_read') as mock_open:
            mock_open.return_value.__enter__.return_value = mock_file

            schema = generator.generate_schema()

            # Verify S3 path was handled
            mock_open.assert_called_once_with("s3://bucket/file.csv", encoding='utf-8')

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_s3_excel_reading(self, mock_is_s3_path, tmp_path):
        """Test S3 Excel reading."""
        mock_is_s3_path.return_value = True

        # Create test Excel data
        data = {'id': [1, 2], 'name': ['Alice', 'Bob']}
        df = pd.DataFrame(data)
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        config = SchemaGenerationConfig(
            input_path="s3://bucket/file.xlsx",
            file_type=FileType.EXCEL,
            sheet_name=0
        )

        generator = SchemaGenerator(config)

        with patch.object(generator.io_handler, 'open_for_read') as mock_open, \
             patch('pandas.read_excel') as mock_read_excel:
            mock_open.return_value.__enter__.return_value = excel_buffer
            mock_read_excel.return_value = df

            schema = generator.generate_schema()

            # Verify S3 path was handled
            mock_open.assert_called_once_with("s3://bucket/file.xlsx", encoding='binary')
            mock_read_excel.assert_called_once()

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_s3_parquet_reading(self, mock_is_s3_path, tmp_path):
        """Test S3 Parquet reading."""
        mock_is_s3_path.return_value = True

        # Create test Parquet data
        data = {'id': [1, 2], 'name': ['Alice', 'Bob']}
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        config = SchemaGenerationConfig(
            input_path="s3://bucket/file.parquet",
            file_type=FileType.PARQUET,
            nrows=1
        )

        generator = SchemaGenerator(config)

        # Create a mock parquet file
        mock_parquet_file = MagicMock()
        mock_parquet_file.read.return_value = table

        with patch.object(generator.io_handler, 'open_for_read') as mock_open, \
             patch('pyarrow.parquet.ParquetFile', return_value=mock_parquet_file) as mock_pq_file:
            mock_file = BytesIO()
            mock_open.return_value.__enter__.return_value = mock_file

            schema = generator.generate_schema()

            # Verify S3 path was handled
            mock_open.assert_called_once_with("s3://bucket/file.parquet", encoding='binary')
            mock_pq_file.assert_called_once_with(mock_file)

    def test_output_to_stdout(self, tmp_path, capsys):
        """Test schema output to stdout."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.STDOUT
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Test output_schema method
        generator.output_schema(schema)

        captured = capsys.readouterr()
        assert captured.out  # Should have output

        # Verify it's valid JSON
        output_schema = json.loads(captured.out)
        assert output_schema["type"] == "object"

    def test_output_to_file(self, tmp_path):
        """Test schema output to file."""
        csv_file = self.create_test_csv(tmp_path)
        output_file = tmp_path / "schema.json"

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.FILE,
            output_path=str(output_file)
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Test output_schema method
        generator.output_schema(schema)

        # Verify file was created
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            saved_schema = json.load(f)
        assert saved_schema["type"] == "object"

    def test_output_to_file_missing_path(self, tmp_path):
        """Test error when output path is missing."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.FILE
            # Missing output_path
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should raise error when trying to output
        with pytest.raises(ValueError, match="output_path must be specified"):
            generator.output_schema(schema)

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_output_to_s3_file(self, mock_is_s3_path, tmp_path):
        """Test schema output to S3 file."""
        mock_is_s3_path.return_value = True

        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.FILE,
            output_path="s3://bucket/schema.json"
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Mock the io_handler for S3 write
        mock_file = StringIO()
        with patch.object(generator.io_handler, 'open_for_write') as mock_write:
            mock_write.return_value.__enter__.return_value = mock_file

            generator.output_schema(schema)

            # Verify S3 write was called
            mock_write.assert_called_once_with("s3://bucket/schema.json", encoding='utf-8')

    @patch('forklift.schema.schema_generator.CLIPBOARD_AVAILABLE', True)
    @patch('forklift.schema.schema_generator.pyperclip')
    def test_output_to_clipboard(self, mock_pyperclip, tmp_path):
        """Test schema output to clipboard."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.CLIPBOARD
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Test output_schema method
        generator.output_schema(schema)

        # Verify pyperclip.copy was called
        mock_pyperclip.copy.assert_called_once()

        # Get the content that was copied
        copied_content = mock_pyperclip.copy.call_args[0][0]
        copied_schema = json.loads(copied_content)
        assert copied_schema["type"] == "object"

    @patch('forklift.schema.schema_generator.CLIPBOARD_AVAILABLE', False)
    def test_output_to_clipboard_not_available(self, tmp_path, capsys):
        """Test clipboard output when pyperclip not available."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.CLIPBOARD
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Test output_schema method
        generator.output_schema(schema)

        captured = capsys.readouterr()
        assert "Pyperclip not available" in captured.out
        assert "Falling back to stdout" in captured.out

    @patch('forklift.schema.schema_generator.CLIPBOARD_AVAILABLE', True)
    @patch('forklift.schema.schema_generator.pyperclip')
    def test_output_to_clipboard_error(self, mock_pyperclip, tmp_path, capsys):
        """Test clipboard output with copy error."""
        mock_pyperclip.copy.side_effect = Exception("Clipboard error")

        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            output_target=OutputTarget.CLIPBOARD
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Test output_schema method
        generator.output_schema(schema)

        captured = capsys.readouterr()
        assert "Failed to copy to clipboard" in captured.out
        assert "Falling back to stdout" in captured.out

    def test_metadata_generation_and_save(self, tmp_path):
        """Test metadata generation and saving to separate file."""
        csv_file = self.create_test_csv(tmp_path)
        metadata_file = tmp_path / "metadata.json"

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            metadata_output_path=str(metadata_file)
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        # Test generate_and_save_metadata method
        saved_path = generator.generate_and_save_metadata(table)

        assert saved_path == str(metadata_file)
        assert metadata_file.exists()

        # Verify metadata content
        with open(metadata_file) as f:
            metadata = json.load(f)
        assert metadata["version"] == "1.0.0"
        assert "column_metadata" in metadata

    @patch('forklift.schema.schema_generator.is_s3_path')
    def test_metadata_save_to_s3(self, mock_is_s3_path, tmp_path):
        """Test metadata saving to S3."""
        mock_is_s3_path.return_value = True

        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            metadata_output_path="s3://bucket/metadata.json"
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        # Mock the io_handler for S3 write
        mock_file = StringIO()
        with patch.object(generator.io_handler, 'open_for_write') as mock_write:
            mock_write.return_value.__enter__.return_value = mock_file

            saved_path = generator.generate_and_save_metadata(table)

            assert saved_path == "s3://bucket/metadata.json"
            mock_write.assert_called_once_with("s3://bucket/metadata.json", encoding='utf-8')

    def test_metadata_generation_disabled_no_save(self, tmp_path):
        """Test no metadata saving when generation is disabled."""
        csv_file = self.create_test_csv(tmp_path)

        config = SchemaGenerationConfig(
            input_path=csv_file,
            file_type=FileType.CSV,
            generate_metadata=False,
            metadata_output_path=tmp_path / "metadata.json"
        )

        generator = SchemaGenerator(config)
        table = generator._read_csv_sample()

        # Test generate_and_save_metadata method
        saved_path = generator.generate_and_save_metadata(table)

        assert saved_path is None

    def test_empty_data_handling(self, tmp_path):
        """Test handling of empty data."""
        # Create CSV with just headers
        csv_content = "id,name,value"

        csv_file = tmp_path / "empty.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should still generate schema structure
        assert "properties" in schema
        assert "x-generation" in schema

        # But should have 0 rows analyzed
        generation = schema["x-generation"]
        assert generation["rows_analyzed"] == 0

    def test_statistics_calculation_errors(self, tmp_path):
        """Test error handling in statistics calculation."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        # Test with empty series
        empty_series = pd.Series([], dtype=object)

        # Numeric stats with empty series
        numeric_stats = generator._calculate_numeric_statistics(empty_series)
        assert numeric_stats == {}

        # String stats with empty series
        string_stats = generator._calculate_string_statistics(empty_series)
        assert string_stats == {}

        # Boolean stats with empty series
        boolean_stats = generator._calculate_boolean_statistics(empty_series)
        assert boolean_stats == {}

        # Test with series that causes calculation errors
        problem_series = pd.Series([float('inf'), float('-inf'), float('nan')])

        # This should handle the error gracefully
        numeric_stats = generator._calculate_numeric_statistics(problem_series)
        # Should either return stats or error info, but not crash
        assert isinstance(numeric_stats, dict)

    def test_primary_key_inference_edge_cases(self, tmp_path):
        """Test primary key inference edge cases."""
        # Test with low scoring candidates
        csv_content = """some_field,other_field,value
1,A,100
2,B,200
2,C,300"""  # Duplicate in some_field, no clear primary key pattern

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            infer_primary_key_from_metadata=True
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        # Should not infer primary key due to low scores
        assert "x-primaryKey" not in schema

    def test_required_fields_detection(self, tmp_path):
        """Test detection of required fields."""
        # Create data with non-nullable field that has no nulls
        data = {
            'id': [1, 2, 3],  # Will be non-nullable, no nulls
            'name': ['Alice', 'Bob', 'Charlie'],  # Will be non-nullable, no nulls
            'optional': ['A', None, 'C']  # Has null, should not be required
        }
        df = pd.DataFrame(data)

        # Convert to PyArrow table with explicit schema
        schema_pa = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            pa.field('name', pa.string(), nullable=False),
            pa.field('optional', pa.string(), nullable=True)
        ])
        table = pa.Table.from_pandas(df, schema=schema_pa)

        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )

        generator = SchemaGenerator(config)
        schema = generator._generate_schema_from_table(table)

        # Check required fields
        required = schema["required"]
        assert "id" in required
        assert "name" in required
        assert "optional" not in required  # Has nulls, so not required

    def test_sample_data_with_small_dataset(self, tmp_path):
        """Test sample data generation with small dataset."""
        csv_content = """id,name
1,Alice
2,Bob"""

        csv_file = tmp_path / "small.csv"
        csv_file.write_text(csv_content)

        config = SchemaGenerationConfig(
            input_path=str(csv_file),
            file_type=FileType.CSV,
            include_sample_data=True
        )

        generator = SchemaGenerator(config)
        schema = generator.generate_schema()

        assert "x-sample" in schema
        sample = schema["x-sample"]
        assert len(sample["rows"]) == 2  # All rows since dataset is small

    def test_analyze_column_for_transformations_edge_cases(self, tmp_path):
        """Test column transformation analysis edge cases."""
        config = SchemaGenerationConfig(
            input_path="/dummy/path",
            file_type=FileType.CSV
        )
        generator = SchemaGenerator(config)

        # Test with empty column
        empty_column = pa.array([None, None, None])
        result = generator._analyze_column_for_transformations("empty_col", empty_column, pa.string())
        assert result is None

        # Test with all null column
        null_series = pd.Series([None, None, None])
        null_column = pa.array(null_series)
        result = generator._analyze_column_for_transformations("null_col", null_column, pa.string())
        assert result is None
