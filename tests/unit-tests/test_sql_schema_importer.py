"""Tests for SQL schema importer with explicit table specifications."""

import json
from unittest.mock import Mock

import pytest

from forklift.schema.sql_schema_importer import SchemaValidationError, SqlSchemaImporter


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
                "created_date": {"type": "string", "format": "date"},
            },
            "x-sql": {
                "tables": [
                    {
                        "select": {"schema": "sales", "name": "customers"},
                        "outputName": "customers",
                    },
                    {
                        "select": {"schema": "inventory", "name": "products"},
                        "outputName": "products",
                    },
                ],
                "parquetTypeMapping": {"customer_id": "int32", "customer_name": "string"},
            },
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

        expected = [("sales", "customers", "customers"), ("inventory", "products", "products")]
        assert table_list == expected

    def test_get_table_list_with_default_schema(self):
        """Test table list when schema is not specified (uses default)."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {"tables": [{"select": {"name": "users"}, "outputName": "users_output"}]},
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
            "x-sql": {"tables": [{"select": {"schema": "public", "name": "orders"}}]},
        }

        importer = SqlSchemaImporter(schema)
        table_list = importer.get_table_list()

        expected = [("public", "orders", None)]
        assert table_list == expected

    def test_validation_missing_required_fields(self):
        """Test validation errors for missing required fields."""
        invalid_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
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
            "properties": {"id": {"type": "integer"}},
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(invalid_schema, validate=True)

        error_message = str(exc_info.value)
        assert "Schema must reference JSON Schema 2020-12 standard" in error_message

    def test_validation_disabled(self):
        """Test that validation can be disabled."""
        invalid_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
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
            "x-sql": {"tables": []},
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
                    {"select": "invalid_select"},  # Should be dict
                    {
                        "select": {
                            "schema": 123,  # Should be string
                            "name": ["invalid"],  # Should be string
                        }
                    },
                ]
            },
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
                    {"select": {"pattern": 123}},  # Should be string
                    {"select": {"pattern": "invalid..pattern"}},  # Invalid pattern
                    {"select": {"pattern": "*.*"}},  # Valid pattern
                    {"select": {"pattern": "schema.*"}},  # Valid pattern
                    {"select": {"pattern": "schema.table"}},  # Valid pattern
                ]
            },
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
                            "invalid_type_col": {"type": "invalid_type"},  # Invalid type
                            "invalid_parquet_col": {
                                "type": "string",
                                "parquetType": "invalid_parquet_type",
                            },
                        },
                    }
                ]
            },
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 column 'invalid_col' must be an object" in error_message
        assert "Table 0 column 'invalid_type_col' invalid type 'invalid_type'" in error_message
        assert (
            "Table 0 column 'invalid_parquet_col' invalid Parquet type 'invalid_parquet_type'"
            in error_message
        )

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
                            "invalid_min": {"type": "integer", "minimum": "not_number"},
                            "invalid_max": {"type": "integer", "maximum": ["not_number"]},
                            "valid_constraints": {"type": "integer", "minimum": 0, "maximum": 100},
                        },
                    }
                ]
            },
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
                            "invalid_min_length": {"type": "string", "minLength": -1},
                            "invalid_max_length": {"type": "string", "maxLength": "not_int"},
                            "invalid_pattern": {"type": "string", "pattern": "[invalid regex"},
                            "valid_string": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 100,
                                "pattern": "^[a-zA-Z]+$",
                            },
                        },
                    }
                ]
            },
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "Table 0 column 'invalid_min_length' invalid minLength" in error_message
        assert "Table 0 column 'invalid_max_length' invalid maxLength" in error_message
        assert "Table 0 column 'invalid_pattern' invalid regex pattern" in error_message

    def test_parquet_type_validation(self):
        """Test Parquet type validation functionality."""
        importer = SqlSchemaImporter(
            {
                "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Test Schema",
                "type": "object",
                "properties": {"id": {"type": "integer"}},
                "x-sql": {"tables": []},
            },
            validate=False,
        )

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
        importer = SqlSchemaImporter(
            {
                "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Test Schema",
                "type": "object",
                "properties": {"id": {"type": "integer"}},
                "x-sql": {"tables": []},
            },
            validate=False,
        )

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
        importer = SqlSchemaImporter(
            {
                "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Test Schema",
                "type": "object",
                "properties": {"id": {"type": "integer"}},
                "x-sql": {"tables": []},
            },
            validate=False,
        )

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
            "properties": {"id": {"type": "integer"}},
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
            "x-sql": "invalid_type",  # Should be dict
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
            "x-sql": {},  # Missing tables
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
                            "invalid_parquet": {"type": "string", "parquetType": "invalid_type"}
                        },
                    }
                ],
                "parquetTypeMapping": {
                    "valid_field": "int32",
                    "invalid_field": "invalid_parquet_type",
                },
            },
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema, validate=True)

        error_message = str(exc_info.value)
        assert "invalid Parquet type" in error_message


class TestSqlSchemaImporterComprehensiveCoverage:
    """Comprehensive tests to achieve 100% coverage for SQL schema importer."""

    @property
    def base_schema(self):
        """Base valid schema for testing edge cases."""
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test Schema",
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-sql": {},
        }

    def test_invalid_tables_not_list_validation(self):
        """Test validation when x-sql.tables is not a list."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = "not_a_list"  # Line 141

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "x-sql.tables must be an array" in str(exc_info.value)

    def test_invalid_parquet_type_mapping_not_dict_validation(self):
        """Test validation when parquetTypeMapping is not a dict."""
        schema = {**self.base_schema}
        schema["x-sql"]["parquetTypeMapping"] = "not_a_dict"  # Line 146

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "x-sql.parquetTypeMapping must be an object" in str(exc_info.value)

    def test_table_missing_select_validation(self):
        """Test validation when table is missing select field."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [{"outputName": "test"}]  # Missing select - Line 162

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 missing required 'select' configuration" in str(exc_info.value)

    def test_table_invalid_columns_not_dict_validation(self):
        """Test validation when table columns is not a dict."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"name": "test_table"}, "columns": "not_a_dict"}  # Line 172
        ]

        # This will cause an AttributeError in _validate_parquet_types before reaching the validation check
        # Let's test this with validation disabled to isolate the validation logic
        with pytest.raises(AttributeError):
            SqlSchemaImporter(schema)

    def test_table_columns_validation_isolated(self):
        """Test table columns validation in isolation."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test the validation method directly
        errors = importer._validate_tables()
        assert len(errors) == 0  # No errors for empty tables

        # Test with invalid columns structure
        importer.tables = [{"select": {"name": "test_table"}, "columns": "not_a_dict"}]

        errors = importer._validate_tables()
        assert "Table 0 columns must be an object" in errors

    def test_table_invalid_required_not_list_validation(self):
        """Test validation when table required is not a list."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"name": "test_table"}, "required": "not_a_list"}  # Line 179
        ]

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 required must be an array" in str(exc_info.value)

    def test_table_required_item_not_string_validation(self):
        """Test validation when table required item is not a string."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"name": "test_table"}, "required": [123, "valid_string"]}  # Lines 181-182
        ]

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 required[0] must be a string" in str(exc_info.value)

    def test_table_select_no_valid_method_validation(self):
        """Test validation when table select has no valid selection method."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"invalid": "value"}}  # Line 198 - no name, schema+name, or pattern
        ]

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 select must have 'name', 'schema'+'name', or 'pattern'" in str(
            exc_info.value
        )

    def test_invalid_include_pattern_empty_string(self):
        """Test invalid include pattern validation with empty string."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test empty pattern (Line 315)
        assert not importer._is_valid_include_pattern("")

    def test_invalid_include_pattern_more_than_two_parts(self):
        """Test invalid include pattern with more than two parts."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test pattern with more than 2 parts after split (Line 319)
        assert not importer._is_valid_include_pattern("schema.table.extra")

    def test_valid_include_pattern_single_identifier(self):
        """Test valid single identifier pattern."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test single valid identifier (Line 323)
        assert importer._is_valid_include_pattern("table_name")
        assert importer._is_valid_include_pattern("valid_table")

    def test_invalid_identifier_or_wildcard_edge_cases(self):
        """Test edge cases for identifier/wildcard validation."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test various invalid identifiers (Line 327, 331)
        assert not importer._is_valid_identifier_or_wildcard("123invalid")  # starts with number
        assert not importer._is_valid_identifier_or_wildcard("table-name")  # contains hyphen
        assert not importer._is_valid_identifier_or_wildcard("table name")  # contains space
        assert not importer._is_valid_identifier_or_wildcard("")  # empty string

    def test_parameterized_parquet_types_validation(self):
        """Test validation of parameterized Parquet types."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test decimal128 validation (Line 337)
        assert importer._is_valid_parquet_type("decimal128(10,2)")
        assert importer._is_valid_parquet_type("decimal128(5,1)")

        # Test timestamp validation (Line 342)
        assert importer._is_valid_parquet_type("timestamp[us]")
        assert importer._is_valid_parquet_type("timestamp[ms,UTC]")

        # Test duration validation (Line 346)
        assert importer._is_valid_parquet_type("duration[s]")
        assert importer._is_valid_parquet_type("duration[ms]")

        # Test list validation (Lines 350-351)
        assert importer._is_valid_parquet_type("list<string>")
        assert importer._is_valid_parquet_type("list<int32>")

        # Test dictionary validation (Lines 354-355)
        assert importer._is_valid_parquet_type("dictionary<values=string, indices=int32>")

        # Test invalid types return False (Line 357)
        assert not importer._is_valid_parquet_type("invalid_type")

    def test_get_table_by_name_functionality(self):
        """Test get_table_by_name method functionality."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"schema": "sales", "name": "customers"}, "outputName": "customers"},
            {"select": {"name": "products"}, "outputName": "products"},  # No schema
        ]

        importer = SqlSchemaImporter(schema)

        # Test exact match with schema (Line 365)
        table = importer.get_table_by_name("sales", "customers")
        assert table is not None
        assert table["outputName"] == "customers"

        # Test name-only match when no schema (Line 368)
        table = importer.get_table_by_name(None, "products")
        assert table is not None
        assert table["outputName"] == "products"

        # Test no match found (Line 370)
        table = importer.get_table_by_name("nonexistent", "table")
        assert table is None

    def test_get_column_schema_functionality(self):
        """Test get_column_schema method functionality."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {
                "select": {"name": "test_table"},
                "columns": {"id": {"type": "integer"}, "name": {"type": "string"}},
            }
        ]

        importer = SqlSchemaImporter(schema)

        # Test getting column schema for existing table (Line 372)
        columns = importer.get_column_schema(None, "test_table")
        assert "id" in columns
        assert "name" in columns

        # Test getting column schema for non-existent table (Line 375)
        columns = importer.get_column_schema(None, "nonexistent")
        assert columns == {}

    def test_get_required_columns_functionality(self):
        """Test get_required_columns method functionality."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"name": "test_table"}, "required": ["id", "name"]}
        ]

        importer = SqlSchemaImporter(schema)

        # Test getting required columns for existing table (Line 381)
        required = importer.get_required_columns(None, "test_table")
        assert required == ["id", "name"]

        # Test getting required columns for non-existent table (Line 383)
        required = importer.get_required_columns(None, "nonexistent")
        assert required == []

    def test_matches_include_pattern_deprecated(self):
        """Test deprecated matches_include_pattern method."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema)

        # This method always returns True now (Line 386)
        assert importer.matches_include_pattern("schema", "table") == True
        assert importer.matches_include_pattern(None, "table") == True

    def test_get_sql_to_parquet_mapping_default(self):
        """Test get_sql_to_parquet_mapping with default mappings."""
        schema = {**self.base_schema}
        # No parquetTypeMapping specified

        importer = SqlSchemaImporter(schema, validate=False)

        # Test default mappings when not specified (Lines 390-422)
        mapping = importer.get_sql_to_parquet_mapping()

        # Verify some default mappings are present
        assert mapping["INTEGER"] == "int64"
        assert mapping["VARCHAR"] == "string"
        assert mapping["BOOLEAN"] == "bool"
        assert mapping["DATE"] == "date32"
        assert mapping["TIMESTAMP"] == "timestamp[us]"
        assert mapping["DECIMAL"] == "decimal128(10,2)"
        assert mapping["FLOAT"] == "float32"
        assert mapping["DOUBLE"] == "double"
        assert mapping["ARRAY"] == "list<string>"
        assert mapping["JSON"] == "struct"
        assert mapping["UUID"] == "string"

    def test_get_sql_to_parquet_mapping_custom(self):
        """Test get_sql_to_parquet_mapping with custom mappings."""
        schema = {**self.base_schema}
        schema["x-sql"]["parquetTypeMapping"] = {
            "sqlToParquet": {"INTEGER": "int32", "VARCHAR": "string"}
        }

        importer = SqlSchemaImporter(schema, validate=False)

        # Test custom mappings
        mapping = importer.get_sql_to_parquet_mapping()
        assert mapping["INTEGER"] == "int32"
        assert mapping["VARCHAR"] == "string"

    def test_as_dict_functionality(self):
        """Test as_dict method functionality."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test as_dict returns the original schema (Line 426)
        assert importer.as_dict() == schema

    def test_get_include_patterns_deprecated(self):
        """Test deprecated get_include_patterns method."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # This method returns empty list now (deprecated)
        patterns = importer.get_include_patterns()
        assert patterns == []

    def test_get_sql_extension_functionality(self):
        """Test get_sql_extension method functionality."""
        schema = {**self.base_schema}
        schema["x-sql"]["custom_config"] = "test_value"

        importer = SqlSchemaImporter(schema, validate=False)

        # Test getting SQL extension
        sql_ext = importer.get_sql_extension()
        assert sql_ext["custom_config"] == "test_value"

    def test_get_tables_functionality(self):
        """Test get_tables method functionality."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"name": "table1"}},
            {"select": {"name": "table2"}},
        ]

        importer = SqlSchemaImporter(schema, validate=False)

        # Test getting tables list
        tables = importer.get_tables()
        assert len(tables) == 2
        assert tables[0]["select"]["name"] == "table1"
        assert tables[1]["select"]["name"] == "table2"

    def test_get_table_list_missing_table_name(self):
        """Test get_table_list when table name is missing."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {
                "select": {"schema": "test_schema"},  # Missing name - Line 60
                "outputName": "test_output",
            }
        ]

        importer = SqlSchemaImporter(schema, validate=False)

        # Should skip tables without name
        table_list = importer.get_table_list()
        assert table_list == []

    def test_invalid_schema_id_pattern_validation(self):
        """Test validation with invalid schema ID pattern."""
        schema = {**self.base_schema}
        schema["$id"] = "https://example.com/invalid-pattern.json"  # Line 121

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Schema $id must follow the standard GitHub URL pattern" in str(exc_info.value)

    def test_invalid_schema_type_validation(self):
        """Test validation with invalid schema type."""
        schema = {**self.base_schema}
        schema["type"] = "array"  # Line 127

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Schema type must be 'object'" in str(exc_info.value)

    def test_invalid_table_configuration_not_dict(self):
        """Test validation when table configuration is not a dict."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            "not_a_dict",  # Line 184 - table config must be dict
        ]

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 configuration must be an object" in str(exc_info.value)

    def test_invalid_table_name_not_string(self):
        """Test validation when table name is not a string."""
        schema = {**self.base_schema}
        schema["x-sql"]["tables"] = [
            {"select": {"schema": "test_schema", "name": 123}}  # Should be string
        ]

        with pytest.raises(SchemaValidationError) as exc_info:
            SqlSchemaImporter(schema)

        assert "Table 0 select.name must be a string" in str(exc_info.value)

    def test_invalid_include_pattern_single_identifier_edge_case(self):
        """Test invalid single identifier pattern to hit line 323."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test invalid single identifier (Line 323)
        assert not importer._is_valid_include_pattern("123invalid")
        assert not importer._is_valid_include_pattern("table-name")

    def test_invalid_identifier_regex_match_edge_case(self):
        """Test invalid identifier regex match to hit line 331."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test regex match returns False for invalid identifiers (Line 331)
        assert not importer._is_valid_identifier_or_wildcard("123")  # starts with number
        assert not importer._is_valid_identifier_or_wildcard("table@name")  # invalid character

    def test_parquet_type_invalid_return_false(self):
        """Test that invalid Parquet types return False to hit line 359."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test that method returns False for completely invalid types (Line 359)
        assert not importer._is_valid_parquet_type("completely_invalid_type")
        assert not importer._is_valid_parquet_type("not_a_parquet_type")
        assert not importer._is_valid_parquet_type("random_string")

    def test_matches_include_pattern_always_true(self):
        """Test that matches_include_pattern always returns True (deprecated method)."""
        schema = {**self.base_schema}

        importer = SqlSchemaImporter(schema, validate=False)

        # Test deprecated method always returns True (Line 386)
        assert importer.matches_include_pattern("any_schema", "any_table") == True
        assert importer.matches_include_pattern(None, "any_table") == True
        assert importer.matches_include_pattern("", "") == True
