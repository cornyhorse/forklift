import json
import pytest
import tempfile
from pathlib import Path

from forklift.schema.csv_schema_importer import CsvSchemaImporter, SchemaValidationError


class TestCsvSchemaImporter:
    """Unit tests for CsvSchemaImporter class with comprehensive validation."""

    @property
    def valid_base_schema(self):
        """Valid base schema structure that meets JSON Schema 2020-12 requirements."""
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test-schema.json",
            "title": "Test Schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer", "minimum": 1},
                "name": {"type": "string", "minLength": 1}
            },
            "required": ["id"],
            "additionalProperties": False,
            "x-csv": {
                "delimiter": ",",
                "encodingPriority": ["utf-8"],
                "parquetTypeMapping": {
                    "id": "int64",
                    "name": "string"
                }
            }
        }

    def test_valid_schema_passes_validation(self):
        """Test that a valid schema passes comprehensive validation."""
        schema = self.valid_base_schema
        importer = CsvSchemaImporter(schema)

        assert importer.schema == schema
        assert importer.csv_ext == schema["x-csv"]
        assert importer.field_map == schema["properties"]
        assert importer.required == ["id"]
        assert importer.additional_properties is False

    def test_missing_schema_field_validation(self):
        """Test validation fails when $schema field is missing."""
        schema = {**self.valid_base_schema}
        del schema["$schema"]

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Missing required '$schema' field" in str(exc_info.value)

    def test_invalid_schema_version_validation(self):
        """Test validation fails with invalid schema version."""
        schema = {**self.valid_base_schema}
        schema["$schema"] = "https://json-schema.org/draft-07/schema"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Schema must reference JSON Schema 2020-12 standard" in str(exc_info.value)

    def test_missing_id_field_validation(self):
        """Test validation fails when $id field is missing."""
        schema = {**self.valid_base_schema}
        del schema["$id"]

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Missing required '$id' field" in str(exc_info.value)

    def test_invalid_id_pattern_validation(self):
        """Test validation fails with invalid $id pattern."""
        schema = {**self.valid_base_schema}
        schema["$id"] = "https://example.com/invalid-pattern.json"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Schema $id must follow the standard GitHub URL pattern" in str(exc_info.value)

    def test_missing_title_validation(self):
        """Test validation fails when title is missing."""
        schema = {**self.valid_base_schema}
        del schema["title"]

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Missing required 'title' field" in str(exc_info.value)

    def test_invalid_type_validation(self):
        """Test validation fails with invalid type."""
        schema = {**self.valid_base_schema}
        schema["type"] = "array"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Schema type must be 'object'" in str(exc_info.value)

    def test_missing_csv_extension_validation(self):
        """Test validation fails when x-csv extension is missing."""
        schema = {**self.valid_base_schema}
        del schema["x-csv"]

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Missing required 'x-csv' extension" in str(exc_info.value)

    def test_invalid_encoding_priority_validation(self):
        """Test validation fails with invalid encoding priority."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["encodingPriority"] = "utf-8"  # Should be list

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "x-csv.encodingPriority must be a list" in str(exc_info.value)

    def test_invalid_encoding_value_validation(self):
        """Test validation fails with invalid encoding value."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["encodingPriority"] = ["invalid-encoding"]

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid encoding 'invalid-encoding' in encodingPriority" in str(exc_info.value)

    def test_invalid_delimiter_validation(self):
        """Test validation fails with invalid delimiter."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["delimiter"] = "toolongdelimiter"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid delimiter specification" in str(exc_info.value)

    def test_invalid_quotechar_validation(self):
        """Test validation fails with invalid quote character."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["quotechar"] = "too_long"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "quotechar must be a single character" in str(exc_info.value)

    def test_invalid_parquet_type_validation(self):
        """Test validation fails with invalid Parquet type."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["parquetTypeMapping"]["id"] = "invalid_type"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid Parquet type 'invalid_type' for field 'id'" in str(exc_info.value)

    def test_parquet_mapping_unknown_field_validation(self):
        """Test validation fails with Parquet mapping for unknown field."""
        schema = {**self.valid_base_schema}
        schema["x-csv"]["parquetTypeMapping"]["unknown_field"] = "string"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Parquet type mapping for unknown field 'unknown_field'" in str(exc_info.value)

    def test_invalid_field_type_validation(self):
        """Test validation fails with invalid field type."""
        schema = {**self.valid_base_schema}
        schema["properties"]["name"]["type"] = "invalid_type"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid type 'invalid_type' for field 'name'" in str(exc_info.value)

    def test_invalid_integer_constraints_validation(self):
        """Test validation fails with invalid integer constraints."""
        schema = {**self.valid_base_schema}
        schema["properties"]["id"]["minimum"] = "not_a_number"

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid minimum value for integer field 'id'" in str(exc_info.value)

    def test_invalid_string_constraints_validation(self):
        """Test validation fails with invalid string constraints."""
        schema = {**self.valid_base_schema}
        schema["properties"]["name"]["minLength"] = -1

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid minLength for string field 'name'" in str(exc_info.value)

    def test_invalid_regex_pattern_validation(self):
        """Test validation fails with invalid regex pattern."""
        schema = {**self.valid_base_schema}
        schema["properties"]["name"]["pattern"] = "[invalid"  # Unclosed bracket

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        assert "Invalid regex pattern for field 'name'" in str(exc_info.value)

    def test_multiple_validation_errors(self):
        """Test that comprehensive validation reports multiple errors at once."""
        schema = {
            "$schema": "invalid-version",  # Error 1
            "$id": "invalid-pattern",      # Error 2
            "type": "array",               # Error 3
            "properties": {"test": {"type": "invalid"}},  # Error 4
            "x-csv": {
                "encodingPriority": "invalid",  # Error 5
                "delimiter": "toolongdelimiter"  # Error 6
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            CsvSchemaImporter(schema)

        error_msg = str(exc_info.value)
        # Should contain multiple errors, not just the first one
        assert "Schema must reference JSON Schema 2020-12 standard" in error_msg
        assert "Schema $id must follow the standard GitHub URL pattern" in error_msg
        assert "Schema type must be 'object'" in error_msg
        assert "Invalid type 'invalid' for field 'test'" in error_msg
        assert "x-csv.encodingPriority must be a list" in error_msg
        assert "Invalid delimiter specification" in error_msg

    def test_comprehensive_valid_schema(self):
        """Test a comprehensive valid schema with all supported features."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/comprehensive-test.json",
            "title": "Comprehensive Test Schema",
            "description": "A test schema with all supported features",
            "type": "object",
            "properties": {
                "id": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 999999
                },
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100,
                    "pattern": "^[A-Za-z\\s]+$"
                },
                "email": {
                    "type": "string",
                    "format": "email"
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "metadata": {
                    "type": "object",
                    "additionalProperties": True
                }
            },
            "required": ["id", "name"],
            "additionalProperties": False,
            "x-csv": {
                "encodingPriority": ["utf-8-sig", "utf-8", "latin-1"],
                "delimiter": ",",
                "quotechar": "\"",
                "escapechar": "\\",
                "header": {
                    "mode": "stability_scan",
                    "keywords": ["id", "name", "email"]
                },
                "footer": {
                    "mode": "regex",
                    "pattern": "^TOTAL"
                },
                "nulls": {
                    "global": ["", "NA", "NULL"],
                    "perColumn": {
                        "email": ["", "no-email"]
                    }
                },
                "case": {
                    "standardizeNames": "postgres",
                    "dedupeNames": "suffix"
                },
                "parquetTypeMapping": {
                    "id": "int64",
                    "name": "string",
                    "email": "string",
                    "tags": "list<string>",
                    "metadata": "struct"
                }
            }
        }

        # Should not raise any validation errors
        importer = CsvSchemaImporter(schema)
        assert importer.schema == schema

    def test_file_path_initialization(self):
        """Test initialization with file path using valid schema."""
        schema = self.valid_base_schema

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema, f)
            temp_path = f.name

        try:
            importer = CsvSchemaImporter(temp_path)
            assert importer.schema == schema
        finally:
            Path(temp_path).unlink()

    def test_pathlib_path_initialization(self):
        """Test initialization with pathlib.Path using valid schema."""
        schema = self.valid_base_schema

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema, f)
            temp_path = Path(f.name)

        try:
            importer = CsvSchemaImporter(temp_path)
            assert importer.schema == schema
        finally:
            temp_path.unlink()

    def test_invalid_input_type(self):
        """Test initialization with invalid input type."""
        with pytest.raises(TypeError, match="schema must be path-like or dict"):
            CsvSchemaImporter(123)

    def test_skip_validation(self):
        """Test that validation can be skipped during initialization."""
        invalid_schema = {"invalid": "schema"}

        # Should not raise error when validation is disabled
        importer = CsvSchemaImporter(invalid_schema, validate=False)
        assert importer.schema == invalid_schema

    def test_accessor_methods(self):
        """Test all accessor methods work correctly."""
        schema = self.valid_base_schema
        importer = CsvSchemaImporter(schema)

        # Test basic accessors
        assert importer.as_dict() == schema
        assert importer.get_field_map() == schema["properties"]
        assert importer.get_csv_extension() == schema["x-csv"]
        assert importer.get_parquet_type_mapping() == schema["x-csv"]["parquetTypeMapping"]
        assert importer.get_encoding_priority() == ["utf-8"]
        assert importer.get_delimiter() == ","
        assert importer.get_null_values() == [""]

    def test_column_name_standardization(self):
        """Test column name standardization functionality."""
        schema = {
            **self.valid_base_schema,
            "x-csv": {
                **self.valid_base_schema["x-csv"],
                "case": {
                    "standardizeNames": "postgres",
                    "dedupeNames": "suffix"
                }
            }
        }

        importer = CsvSchemaImporter(schema)

        # Test standardization
        test_names = ["User ID", "FIRST_NAME", "email-address"]
        result = importer.standardize_column_names(test_names)

        # Should be standardized to postgres naming convention
        expected = ["user_id", "first_name", "email_address"]
        assert result == expected

    def test_column_name_deduplication_modes(self):
        """Test column name deduplication with different modes."""
        schema = {
            **self.valid_base_schema,
            "x-csv": {
                **self.valid_base_schema["x-csv"],
                "case": {
                    "standardizeNames": "postgres",
                    "dedupeNames": "prefix"
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        # Test prefix mode
        test_names = ["id", "name", "name", "id"]
        result = importer.standardize_column_names(test_names)
        expected = ["id", "name", "1_name", "1_id"]
        assert result == expected

        # Test error mode
        schema["x-csv"]["case"]["dedupeNames"] = "error"
        importer = CsvSchemaImporter(schema)
        with pytest.raises(ValueError, match="Duplicate column name detected: name"):
            importer.standardize_column_names(test_names)


class TestSchemaValidationFeatures:
    """Test specific validation features and edge cases."""

    def test_valid_parquet_types(self):
        """Test validation of various valid Parquet types."""
        valid_types = [
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "double", "bool", "string", "binary",
            "date32", "date64",
            "timestamp[s]", "timestamp[ms]", "timestamp[us]", "timestamp[ns]",
            "duration[s]", "duration[ms]", "duration[us]", "duration[ns]",
            "decimal128(10,2)", "list<string>", "struct",
            "dictionary<values=string, indices=int32>"
        ]

        base_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
            "properties": {},
            "x-csv": {"parquetTypeMapping": {}}
        }

        for parquet_type in valid_types:
            schema = {
                **base_schema,
                "properties": {"test_field": {"type": "string"}},
                "x-csv": {
                    "parquetTypeMapping": {"test_field": parquet_type}
                }
            }

            # Should not raise validation error
            importer = CsvSchemaImporter(schema)
            assert importer.schema == schema

    def test_header_mode_validation(self):
        """Test validation of header modes."""
        valid_modes = ["present", "absent", "auto", "stability_scan"]

        base_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-csv": {}
        }

        for mode in valid_modes:
            schema = {
                **base_schema,
                "x-csv": {
                    "header": {"mode": mode}
                }
            }

            if mode == "stability_scan":
                schema["x-csv"]["header"]["keywords"] = ["id"]

            # Should not raise validation error
            importer = CsvSchemaImporter(schema)
            assert importer.schema == schema

    def test_case_configuration_validation(self):
        """Test validation of case configuration options."""
        valid_standardize = ["postgres", "snake_case", "camelCase"]
        valid_dedupe = ["suffix", "prefix", "error"]

        base_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-csv": {}
        }

        for standardize in valid_standardize:
            for dedupe in valid_dedupe:
                schema = {
                    **base_schema,
                    "x-csv": {
                        "case": {
                            "standardizeNames": standardize,
                            "dedupeNames": dedupe
                        }
                    }
                }

                # Should not raise validation error
                importer = CsvSchemaImporter(schema)
                assert importer.schema == schema


if __name__ == "__main__":
    pytest.main([__file__])
