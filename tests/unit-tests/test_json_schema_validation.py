"""Tests for JSON Schema validation functionality."""

import pytest

from forklift.schema.fwf.validation.json_schema import JsonSchemaValidator


class TestJsonSchemaValidator:
    """Test cases for JsonSchemaValidator class."""

    def test_validate_empty_schema(self):
        """Test validation with empty schema."""
        errors = JsonSchemaValidator.validate({})

        assert len(errors) == 4
        assert "Missing required '$schema' field" in errors
        assert "Missing required '$id' field" in errors
        assert "Missing required 'title' field" in errors
        assert "Schema type must be 'object'" in errors

    def test_validate_valid_minimal_schema(self):
        """Test validation with minimal valid schema."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test-schema.json",
            "title": "Test Schema",
            "type": "object",
            "properties": {},
        }

        errors = JsonSchemaValidator.validate(schema)
        assert errors == []

    def test_validate_missing_schema_field(self):
        """Test validation with missing $schema field."""
        schema = {
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Missing required '$schema' field" in errors

    def test_validate_invalid_schema_version(self):
        """Test validation with invalid schema version."""
        schema = {
            "$schema": "https://json-schema.org/draft-07/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Schema must reference JSON Schema 2020-12 standard" in errors

    def test_validate_missing_id_field(self):
        """Test validation with missing $id field."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Missing required '$id' field" in errors

    def test_validate_invalid_id_pattern(self):
        """Test validation with invalid $id pattern."""
        invalid_ids = [
            "https://example.com/schema.json",
            "https://github.com/other/repo/schema.json",
            "http://github.com/cornyhorse/forklift/schema-standards/test.json",
            "github.com/cornyhorse/forklift/schema-standards/test.json",
            "https://github.com/cornyhorse/forklift/other-path/test.json",
        ]

        for invalid_id in invalid_ids:
            schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": invalid_id,
                "title": "Test",
                "type": "object",
            }

            errors = JsonSchemaValidator.validate(schema)
            assert len(errors) == 1
            assert "Schema $id must follow the standard GitHub URL pattern" in errors[0]

    def test_validate_valid_id_patterns(self):
        """Test validation with valid $id patterns."""
        valid_ids = [
            "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "https://github.com/cornyhorse/forklift/schema-standards/fwf/employee.json",
            "https://github.com/cornyhorse/forklift/schema-standards/subdir/complex-schema.json",
        ]

        for valid_id in valid_ids:
            schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": valid_id,
                "title": "Test",
                "type": "object",
                "properties": {},
            }

            errors = JsonSchemaValidator.validate(schema)
            assert errors == [], f"Valid ID {valid_id} should not produce errors"

    def test_validate_missing_title_field(self):
        """Test validation with missing title field."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Missing required 'title' field" in errors

    def test_validate_empty_title_field(self):
        """Test validation with empty title field."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Missing required 'title' field" in errors

    def test_validate_invalid_type(self):
        """Test validation with invalid type."""
        invalid_types = ["array", "string", "number", "boolean", "null"]

        for invalid_type in invalid_types:
            schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
                "title": "Test",
                "type": invalid_type,
            }

            errors = JsonSchemaValidator.validate(schema)
            assert len(errors) == 1
            assert "Schema type must be 'object'" in errors[0]

    def test_validate_missing_type(self):
        """Test validation with missing type field."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 1
        assert "Schema type must be 'object'" in errors

    def test_validate_properties_not_dict(self):
        """Test validation with properties that is not a dictionary."""
        invalid_properties = ["string", 123, [], True, None]

        for invalid_prop in invalid_properties:
            schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
                "title": "Test",
                "type": "object",
                "properties": invalid_prop,
            }

            errors = JsonSchemaValidator.validate(schema)
            assert len(errors) == 1
            assert "Properties must be a dictionary" in errors[0]

    def test_validate_valid_properties(self):
        """Test validation with valid properties."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test Schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "active": {"type": "boolean"},
            },
        }

        errors = JsonSchemaValidator.validate(schema)
        assert errors == []

    def test_validate_missing_properties(self):
        """Test validation with missing properties field."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
        }

        errors = JsonSchemaValidator.validate(schema)
        assert errors == []  # properties is optional

    def test_validate_multiple_errors(self):
        """Test validation with multiple errors."""
        schema = {
            "$schema": "https://json-schema.org/draft-07/schema",  # Wrong version
            "$id": "https://example.com/invalid.json",  # Wrong pattern
            "type": "array",  # Wrong type
            "properties": "invalid",  # Not a dict
        }

        errors = JsonSchemaValidator.validate(schema)
        assert len(errors) == 5

        error_text = " ".join(errors)
        assert "Schema must reference JSON Schema 2020-12 standard" in error_text
        assert "Schema $id must follow the standard GitHub URL pattern" in error_text
        assert "Missing required 'title' field" in error_text
        assert "Schema type must be 'object'" in error_text
        assert "Properties must be a dictionary" in error_text

    def test_validate_comprehensive_valid_schema(self):
        """Test validation with comprehensive valid schema."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/employee-records.json",
            "title": "Employee Records Schema",
            "description": "Schema for employee record data",
            "type": "object",
            "properties": {
                "employee_id": {"type": "string", "pattern": "^EMP[0-9]{6}$"},
                "full_name": {"type": "string", "minLength": 1},
                "department": {"type": "string", "enum": ["HR", "IT", "Finance", "Operations"]},
                "salary": {"type": "number", "minimum": 0},
                "hire_date": {"type": "string", "format": "date"},
                "is_active": {"type": "boolean"},
            },
            "required": ["employee_id", "full_name", "department"],
            "additionalProperties": False,
        }

        errors = JsonSchemaValidator.validate(schema)
        assert errors == [], "Comprehensive valid schema should not produce any errors"

    def test_validate_edge_cases(self):
        """Test validation with edge cases."""
        # Schema with extra fields (should be allowed)
        schema_with_extras = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://github.com/cornyhorse/forklift/schema-standards/test.json",
            "title": "Test",
            "type": "object",
            "properties": {},
            "description": "Extra field",
            "version": "1.0",
            "custom_field": {"any": "value"},
        }

        errors = JsonSchemaValidator.validate(schema_with_extras)
        assert errors == []

    def test_validate_none_schema(self):
        """Test validation with None schema."""
        with pytest.raises(AttributeError):
            JsonSchemaValidator.validate(None)
