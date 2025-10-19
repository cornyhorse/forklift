"""Tests for schema validator processor backward compatibility."""

from unittest.mock import Mock

import pytest

from forklift.processors import schema_validator


class TestSchemaValidatorModule:
    """Test cases for the schema validator processor module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(schema_validator, "SchemaValidator")
        assert hasattr(schema_validator, "SchemaValidatorConfig")
        assert hasattr(schema_validator, "SchemaValidationMode")
        assert hasattr(schema_validator, "NullabilityMode")
        assert hasattr(schema_validator, "ColumnSchema")
        assert hasattr(schema_validator, "create_schema_validator_from_json")
        assert hasattr(schema_validator, "create_schema_from_batch")

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        expected_exports = [
            "SchemaValidator",
            "SchemaValidatorConfig",
            "SchemaValidationMode",
            "NullabilityMode",
            "ColumnSchema",
            "create_schema_validator_from_json",
            "create_schema_from_batch",
        ]

        assert schema_validator.__all__ == expected_exports

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.processors.schema_validator import (
            ColumnSchema,
            NullabilityMode,
            SchemaValidationMode,
            SchemaValidator,
            SchemaValidatorConfig,
            create_schema_from_batch,
            create_schema_validator_from_json,
        )

        # Verify they are the same as the module attributes
        assert SchemaValidator is schema_validator.SchemaValidator
        assert SchemaValidatorConfig is schema_validator.SchemaValidatorConfig
        assert SchemaValidationMode is schema_validator.SchemaValidationMode
        assert NullabilityMode is schema_validator.NullabilityMode
        assert ColumnSchema is schema_validator.ColumnSchema
        assert (
            create_schema_validator_from_json is schema_validator.create_schema_validator_from_json
        )
        assert create_schema_from_batch is schema_validator.create_schema_from_batch

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert schema_validator.__doc__ is not None
        assert "Schema validation package" in schema_validator.__doc__
        assert "validating data against schema definitions" in schema_validator.__doc__
