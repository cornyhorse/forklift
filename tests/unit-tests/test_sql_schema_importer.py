"""Tests for SQL schema importer with explicit table specifications."""

import pytest
import json
from unittest.mock import Mock

from forklift.schema.sql_schema_importer import SqlSchemaImporter, SchemaValidationError


class TestSqlSchemaImporter:
    """Test cases for SqlSchemaImporter with explicit table specifications."""

    @pytest.fixture
    def valid_sql_schema(self):
        """Create a valid SQL schema for testing."""
        return {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test_sql.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test SQL Schema",
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer"},
                "customer_name": {"type": "string"},
                "created_date": {"type": "string", "format": "date"}
            },
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "sales",
                            "name": "customers"
                        },
                        "outputName": "customers"
                    },
                    {
                        "select": {
                            "schema": "inventory",
                            "name": "products"
                        },
                        "outputName": "products"
                    }
                ],
                "parquetTypeMapping": {
                    "customer_id": "int32",
                    "customer_name": "string"
                }
            }
        }

    def test_init_valid_schema(self, valid_sql_schema):
        """Test initialization with valid schema."""
        importer = SqlSchemaImporter(valid_sql_schema)

        assert importer.schema == valid_sql_schema
        assert len(importer.tables) == 2
        assert importer.tables[0]["select"]["schema"] == "sales"
        assert importer.tables[0]["select"]["name"] == "customers"
        assert importer.tables[0]["outputName"] == "customers"

    def test_get_table_list(self, valid_sql_schema):
        """Test getting table list from schema."""
        importer = SqlSchemaImporter(valid_sql_schema)

        table_list = importer.get_table_list()

        expected = [
            ("sales", "customers", "customers"),
            ("inventory", "products", "products")
        ]
        assert table_list == expected

    def test_get_table_list_with_default_schema(self):
        """Test table list when schema is not specified (uses default)."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "name": "users"
                        },
                        "outputName": "users_output"
                    }
                ]
            }
        }

        importer = SqlSchemaImporter(schema)
        table_list = importer.get_table_list()

        expected = [("default", "users", "users_output")]
        assert table_list == expected

    def test_get_table_list_without_output_name(self):
        """Test table list when outputName is not specified."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "public",
                            "name": "orders"
                        }
                    }
                ]
            }
        }

        importer = SqlSchemaImporter(schema)
        table_list = importer.get_table_list()

        expected = [("public", "orders", None)]
        assert table_list == expected

    def test_validation_missing_required_fields(self):
        """Test validation errors for missing required fields."""
        invalid_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}}
            # Missing $id, $schema, title
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Missing required '$id' field" in error_message
        assert "Missing required '$schema' field" in error_message
        assert "Missing required 'title' field" in error_message

    def test_validation_invalid_schema_url(self):
        """Test validation error for invalid $schema URL."""
        invalid_schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft-07/schema",  # Wrong version
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Schema must reference JSON Schema 2020-12 standard" in error_message

    def test_validation_disabled(self):
        """Test that validation can be disabled."""
        invalid_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}}
            # Missing required fields, but validation is disabled
        }

        # Should not raise an exception when validation is disabled
        importer = SqlSchemaImporter(invalid_schema, validate=False)
        assert importer.schema == invalid_schema

    def test_init_from_file_path(self, tmp_path):
        """Test initialization from file path."""
        schema_data = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {"tables": []}
        }

        schema_file = tmp_path / "test_schema.json"
        schema_file.write_text(json.dumps(schema_data))

        importer = SqlSchemaImporter(str(schema_file))
        assert importer.schema == schema_data

    def test_init_invalid_type(self):
        """Test initialization with invalid type."""
        with pytest.raises(TypeError) as exc_info:
            SqlSchemaImporter(123)  # Invalid type

        assert "schema must be path-like or dict" in str(exc_info.value)

    def test_validate_tables_invalid_types(self):
        """Test validation of tables with invalid types."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    "invalid_table",  # Should be dict
                    {
                        "select": "invalid_select"  # Should be dict
                    },
                    {
                        "select": {
                            "schema": 123,  # Should be string
                            "name": ["invalid"]  # Should be string
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 configuration must be an object" in error_message
        assert "Table 1 select must be an object" in error_message
        assert "Table 2 select.schema must be a string" in error_message
        assert "Table 2 select.name must be a string" in error_message

    def test_validate_table_patterns(self):
        """Test validation of table select patterns."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "pattern": 123  # Should be string
                        }
                    },
                    {
                        "select": {
                            "pattern": "invalid..pattern"  # Invalid pattern
                        }
                    },
                    {
                        "select": {
                            "pattern": "*.*"  # Valid pattern
                        }
                    },
                    {
                        "select": {
                            "pattern": "schema.*"  # Valid pattern
                        }
                    },
                    {
                        "select": {
                            "pattern": "schema.table"  # Valid pattern
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 select.pattern must be a string" in error_message
        assert "Table 1 invalid select.pattern 'invalid..pattern'" in error_message

    def test_validate_column_types(self):
        """Test validation of column type definitions."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {"name": "test_table"},
                        "columns": {
                            "invalid_col": "not_dict",  # Should be dict
                            "invalid_type_col": {
                                "type": "invalid_type"  # Invalid type
                            },
                            "invalid_parquet_col": {
                                "type": "string",
                                "parquetType": "invalid_parquet_type"
                            }
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 column 'invalid_col' must be an object" in error_message
        assert "Table 0 column 'invalid_type_col' invalid type 'invalid_type'" in error_message
        assert "Table 0 column 'invalid_parquet_col' invalid Parquet type 'invalid_parquet_type'" in error_message

    def test_validate_integer_constraints(self):
        """Test validation of integer column constraints."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {"name": "test_table"},
                        "columns": {
                            "invalid_min": {
                                "type": "integer",
                                "minimum": "not_number"
                            },
                            "invalid_max": {
                                "type": "integer",
                                "maximum": ["not_number"]
                            },
                            "valid_constraints": {
                                "type": "integer",
                                "minimum": 0,
                                "maximum": 100
                            }
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 column 'invalid_min' invalid minimum value" in error_message
        assert "Table 0 column 'invalid_max' invalid maximum value" in error_message

    def test_validate_string_constraints(self):
        """Test validation of string column constraints."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {"name": "test_table"},
                        "columns": {
                            "invalid_min_length": {
                                "type": "string",
                                "minLength": -1
                            },
                            "invalid_max_length": {
                                "type": "string",
                                "maxLength": "not_int"
                            },
                            "invalid_pattern": {
                                "type": "string",
                                "pattern": "[invalid regex"
                            },
                            "valid_string": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 100,
                                "pattern": "^[a-zA-Z]+$"
                            }
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 column 'invalid_min_length' invalid minLength" in error_message
        assert "Table 0 column 'invalid_max_length' invalid maxLength" in error_message
        assert "Table 0 column 'invalid_pattern' invalid regex pattern" in error_message

    def test_parquet_type_validation(self):
        """Test Parquet type validation functionality."""
        importer = SqlSchemaImporter({
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {"tables": []}
        }, validate=False)

        # Test valid Parquet types
        valid_types = ["int32", "string", "double", "bool", "timestamp[ms]"]
        for ptype in valid_types:
            assert importer._is_valid_parquet_type(ptype)

        # Test invalid Parquet types
        invalid_types = ["invalid_type", "int128", "timestamp", ""]
        for ptype in invalid_types:
            assert not importer._is_valid_parquet_type(ptype)

    def test_include_pattern_validation(self):
        """Test include pattern validation."""
        importer = SqlSchemaImporter({
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {"tables": []}
        }, validate=False)

        # Test valid patterns
        valid_patterns = ["*.*", "schema.*", "schema.table", "table_name"]
        for pattern in valid_patterns:
            assert importer._is_valid_include_pattern(pattern)

        # Test invalid patterns
        invalid_patterns = ["", "schema..table", "schema.table.extra", "123invalid"]
        for pattern in invalid_patterns:
            assert not importer._is_valid_include_pattern(pattern)

    def test_identifier_wildcard_validation(self):
        """Test SQL identifier and wildcard validation."""
        importer = SqlSchemaImporter({
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {"tables": []}
        }, validate=False)

        # Test valid identifiers and wildcards
        valid_names = ["*", "table_name", "schema1", "_valid", "CamelCase"]
        for name in valid_names:
            assert importer._is_valid_identifier_or_wildcard(name)

        # Test invalid identifiers
        invalid_names = ["", "123invalid", "table-name", "table name", "table.name"]
        for name in invalid_names:
            assert not importer._is_valid_identifier_or_wildcard(name)

    def test_missing_x_sql_extension(self):
        """Test handling when x-sql extension is missing."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
            # Missing x-sql extension
        }

        # When x-sql is missing, it defaults to empty dict, so no validation error
        # unless validation specifically checks for required x-sql
        importer = SqlSchemaImporter(schema, validate=False)
        assert importer.sql_ext == {}
        assert importer.tables == []

    def test_invalid_x_sql_type(self):
        """Test validation when x-sql is not a dict."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": "invalid_type"  # Should be dict
        }

        # This will cause an AttributeError when trying to call .get() on a string
        with pytest.raises(AttributeError):
            SqlSchemaImporter(schema, validate=False)

    def test_missing_tables_in_x_sql(self):
        """Test validation when tables are missing from x-sql."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {}  # Missing tables
        }

        # When tables are missing, it defaults to empty list
        importer = SqlSchemaImporter(schema, validate=False)
        assert importer.tables == []

    def test_parquet_type_mapping_validation(self):
        """Test validation of Parquet type mapping in x-sql."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {"name": "test_table"},
                        "columns": {
                            "invalid_parquet": {
                                "type": "string",
                                "parquetType": "invalid_type"
                            }
                        }
                    }
                ],
                "parquetTypeMapping": {
                    "valid_field": "int32",
                    "invalid_field": "invalid_parquet_type"
                }
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "invalid Parquet type" in error_message


if __name__ == "__main__":
    pytest.main([__file__])
