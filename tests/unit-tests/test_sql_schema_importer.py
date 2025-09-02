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

    def test_validation_invalid_id_pattern(self):
        """Test validation error for invalid $id pattern."""
        invalid_schema = {
            "$id": "https://example.com/invalid.json",  # Wrong GitHub pattern
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Schema $id must follow the standard GitHub URL pattern" in error_message

    def test_validation_missing_table_name(self):
        """Test validation error when table name is missing."""
        invalid_schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "public"
                            # Missing "name" field
                        }
                    }
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 select must have 'name', 'schema'+'name', or 'pattern'" in error_message

    def test_validation_invalid_table_structure(self):
        """Test validation error for invalid table structure."""
        invalid_schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": [
                    "invalid_table_format"  # Should be an object, not a string
                ]
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 configuration must be an object" in error_message

    def test_validation_tables_not_list(self):
        """Test validation error when tables is not a list."""
        invalid_schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": "not_a_list"  # Should be a list
            }
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "x-sql.tables must be an array" in error_message

    def test_validation_no_sql_extension(self):
        """Test behavior when x-sql extension is missing (it's optional)."""
        schema_without_sql = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
            # Missing x-sql extension - but this is valid
        }

        # Should not raise an exception since x-sql is optional
        importer = SqlSchemaImporter(schema_without_sql, validate=True)
        assert importer.sql_ext == {}
        assert importer.tables == []

    def test_empty_tables_list(self):
        """Test handling of empty tables list."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {
                "tables": []
            }
        }

        importer = SqlSchemaImporter(schema)
        table_list = importer.get_table_list()

        assert table_list == []

    def test_parquet_type_mapping(self, valid_sql_schema):
        """Test access to Parquet type mapping."""
        importer = SqlSchemaImporter(valid_sql_schema)

        expected_mapping = {
            "customer_id": "int32",
            "customer_name": "string"
        }
        assert importer.parquet_type_mapping == expected_mapping

    def test_no_validation_mode(self):
        """Test initialization without validation."""
        # This schema has errors but validation is disabled
        invalid_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}}
            # Missing required fields
        }

        # Should not raise an exception
        importer = SqlSchemaImporter(invalid_schema, validate=False)
        assert importer.schema == invalid_schema

    def test_complex_table_configuration(self):
        """Test complex table configuration with multiple options."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/complex.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Complex SQL Schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "amount": {"type": "number"}
            },
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "sales",
                            "name": "customers"
                        },
                        "outputName": "customers_cleaned"
                    },
                    {
                        "select": {
                            "schema": "finance",
                            "name": "transactions"
                        },
                        "outputName": "transactions_processed"
                    },
                    {
                        "select": {
                            "name": "logs"  # No schema specified
                        }
                        # No outputName specified
                    }
                ],
                "parquetTypeMapping": {
                    "id": "int64",
                    "amount": "decimal128"
                }
            }
        }

        importer = SqlSchemaImporter(schema)
        table_list = importer.get_table_list()

        expected = [
            ("sales", "customers", "customers_cleaned"),
            ("finance", "transactions", "transactions_processed"),
            ("default", "logs", None)
        ]
        assert table_list == expected


if __name__ == "__main__":
    pytest.main([__file__])
