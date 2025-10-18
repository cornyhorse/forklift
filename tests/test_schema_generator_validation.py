"""Tests for schema generator validation functionality."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from forklift.schema.generator.validation import SchemaValidator


class TestSchemaValidator:
    """Test cases for SchemaValidator class."""

    def test_validate_schema_structure_valid(self):
        """Test validation with valid schema structure."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
            "required": ["id", "name"],
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == True
        assert errors == []

    def test_validate_schema_structure_missing_required_fields(self):
        """Test validation with missing required top-level fields."""
        schema = {
            "type": "object"
            # Missing $schema and properties
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Missing required field: $schema" in errors
        assert "Missing required field: properties" in errors

    def test_validate_schema_structure_invalid_properties_type(self):
        """Test validation with invalid properties type."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": "invalid_properties",  # Should be dict
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Properties must be a dictionary" in errors

    def test_validate_schema_structure_invalid_property_definition(self):
        """Test validation with invalid property definition."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": "invalid_property_def",  # Should be dict
                "name": {"type": "string"},
            },
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Property 'id' must be a dictionary" in errors

    def test_validate_schema_structure_missing_property_type(self):
        """Test validation with missing property type."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {}, "name": {"type": "string"}},  # Missing type
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Property 'id' missing type definition" in errors

    def test_validate_schema_structure_invalid_required_type(self):
        """Test validation with invalid required field type."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": "invalid_required",  # Should be list
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Required field must be a list" in errors

    def test_validate_schema_structure_required_field_not_in_properties(self):
        """Test validation with required field not defined in properties."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id", "name"],  # 'name' not in properties
        }

        is_valid, errors = SchemaValidator.validate_schema_structure(schema)

        assert is_valid == False
        assert "Required field 'name' not defined in properties" in errors

    def test_validate_data_compatibility_valid(self):
        """Test data compatibility validation with compatible data."""
        schema = {
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "age": {"type": "integer"},
            }
        }

        # Create compatible PyArrow table
        table = pa.table(
            {"id": ["1", "2", "3"], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

        is_compatible, issues = SchemaValidator.validate_data_compatibility(schema, table)

        assert is_compatible == True
        assert issues == []

    def test_validate_data_compatibility_no_properties(self):
        """Test data compatibility validation with schema having no properties."""
        schema = {}  # No properties
        table = pa.table({"id": [1, 2, 3]})

        is_compatible, issues = SchemaValidator.validate_data_compatibility(schema, table)

        assert is_compatible == False
        assert "Schema has no properties defined" in issues

    def test_validate_data_compatibility_missing_columns(self):
        """Test data compatibility validation with missing columns in data."""
        schema = {
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string"},
            }
        }

        # Table missing 'email' column
        table = pa.table({"id": ["1", "2"], "name": ["Alice", "Bob"]})

        is_compatible, issues = SchemaValidator.validate_data_compatibility(schema, table)

        assert is_compatible == False
        assert any("Columns missing in data: email" in issue for issue in issues)

    def test_validate_data_compatibility_extra_columns(self):
        """Test data compatibility validation with extra columns in data."""
        schema = {"properties": {"id": {"type": "string"}, "name": {"type": "string"}}}

        # Table has extra 'age' column
        table = pa.table({"id": ["1", "2"], "name": ["Alice", "Bob"], "age": [25, 30]})

        is_compatible, issues = SchemaValidator.validate_data_compatibility(schema, table)

        assert is_compatible == False
        assert any("Extra columns in data: age" in issue for issue in issues)

    def test_validate_data_compatibility_required_field_with_nulls(self):
        """Test data compatibility validation with nulls in required fields."""
        schema = {
            "properties": {"id": {"type": "string"}, "name": {"type": "string"}},
            "required": ["id", "name"],
        }

        # Table with nulls in required field
        table = pa.table({"id": ["1", None, "3"], "name": ["Alice", "Bob", "Charlie"]})

        is_compatible, issues = SchemaValidator.validate_data_compatibility(schema, table)

        assert is_compatible == False
        assert any("Required field 'id' has null values" in issue for issue in issues)

    def test_validate_transformation_config_valid(self):
        """Test transformation config validation with valid config."""
        transform_config = {
            "global_settings": {"trim_whitespace": True, "case_conversion": "lower"},
            "column_transformations": {
                "name": {"uppercase": {"enabled": True}, "trim": {"enabled": False}},
                "email": {"validate_email": {"enabled": True}},
            },
        }

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == True
        assert errors == []

    def test_validate_transformation_config_invalid_type(self):
        """Test transformation config validation with invalid config type."""
        transform_config = "invalid_config"  # Should be dict

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "Transformation config must be a dictionary" in errors

    def test_validate_transformation_config_invalid_global_settings(self):
        """Test transformation config validation with invalid global settings."""
        transform_config = {"global_settings": "invalid_settings"}  # Should be dict

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "global_settings must be a dictionary" in errors

    def test_validate_transformation_config_invalid_column_transformations(self):
        """Test transformation config validation with invalid column transformations."""
        transform_config = {"column_transformations": "invalid_transforms"}  # Should be dict

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "column_transformations must be a dictionary" in errors

    def test_validate_transformation_config_invalid_column_transform_def(self):
        """Test transformation config validation with invalid column transform definition."""
        transform_config = {
            "column_transformations": {"name": "invalid_transform_def"}  # Should be dict
        }

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "Transformations for column 'name' must be a dictionary" in errors

    def test_validate_transformation_config_invalid_transform_definition(self):
        """Test transformation config validation with invalid individual transform definition."""
        transform_config = {
            "column_transformations": {"name": {"uppercase": "invalid_def"}}  # Should be dict
        }

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "Transform 'uppercase' for column 'name' must be a dictionary" in errors

    def test_validate_transformation_config_missing_enabled_field(self):
        """Test transformation config validation with missing enabled field."""
        transform_config = {
            "column_transformations": {"name": {"uppercase": {}}}  # Missing 'enabled' field
        }

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == False
        assert "Transform 'uppercase' for column 'name' missing 'enabled' field" in errors

    def test_validate_transformation_config_empty(self):
        """Test transformation config validation with empty config."""
        transform_config = {}

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == True
        assert errors == []

    def test_validate_transformation_config_partial(self):
        """Test transformation config validation with partial config."""
        transform_config = {
            "global_settings": {"trim_whitespace": True}
            # No column_transformations
        }

        is_valid, errors = SchemaValidator.validate_transformation_config(transform_config)

        assert is_valid == True
        assert errors == []
