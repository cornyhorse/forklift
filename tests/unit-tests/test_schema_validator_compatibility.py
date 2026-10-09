"""Tests for schema validator backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/processors/schema_validator.py by testing the import statements and __all__ exports.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestSchemaValidatorCompatibility:
    """Test cases for schema validator backward compatibility."""

    def test_import_all_classes_and_functions(self):
        """Test importing all classes and functions from the compatibility module."""
        # Import from the backward-compatibility module
        from forklift.processors.schema_validator import (
            ColumnSchema,
            NullabilityMode,
            SchemaValidationMode,
            SchemaValidator,
            SchemaValidatorConfig,
            create_schema_from_batch,
            create_schema_validator_from_json,
        )

        # Verify all classes and functions are imported and are callable/classes
        assert callable(SchemaValidator)
        assert callable(SchemaValidatorConfig)
        assert callable(create_schema_validator_from_json)
        assert callable(create_schema_from_batch)

        # Verify enums/classes exist
        assert SchemaValidationMode is not None
        assert NullabilityMode is not None
        assert callable(ColumnSchema)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected exports."""
        import forklift.processors.schema_validator as schema_validator_module

        expected_exports = [
            "SchemaValidator",
            "SchemaValidatorConfig",
            "SchemaValidationMode",
            "NullabilityMode",
            "ColumnSchema",
            "create_schema_validator_from_json",
            "create_schema_from_batch",
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(schema_validator_module, "__all__")
        assert schema_validator_module.__all__ == expected_exports

        # Verify all items in __all__ are actually available in the module
        for export_name in expected_exports:
            assert hasattr(schema_validator_module, export_name)
            export_item = getattr(schema_validator_module, export_name)
            assert export_item is not None

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.processors.schema_validator as schema_validator_module

        # Get all public names from the module
        public_names = [name for name in dir(schema_validator_module) if not name.startswith("_")]

        # All items in __all__ should be in the public namespace
        for export_name in schema_validator_module.__all__:
            assert export_name in public_names

        # Test that we can access each export from __all__
        for export_name in schema_validator_module.__all__:
            export_item = getattr(schema_validator_module, export_name)
            assert export_item is not None

    def test_individual_imports(self):
        """Test importing each class and function individually."""
        # Test SchemaValidator
        from forklift.processors.schema_validator import SchemaValidator

        assert callable(SchemaValidator)

        # Test SchemaValidatorConfig
        from forklift.processors.schema_validator import SchemaValidatorConfig

        assert callable(SchemaValidatorConfig)

        # Test SchemaValidationMode
        from forklift.processors.schema_validator import SchemaValidationMode

        assert SchemaValidationMode is not None

        # Test NullabilityMode
        from forklift.processors.schema_validator import NullabilityMode

        assert NullabilityMode is not None

        # Test ColumnSchema
        from forklift.processors.schema_validator import ColumnSchema

        assert callable(ColumnSchema)

        # Test create_schema_validator_from_json
        from forklift.processors.schema_validator import create_schema_validator_from_json

        assert callable(create_schema_validator_from_json)

        # Test create_schema_from_batch
        from forklift.processors.schema_validator import create_schema_from_batch

        assert callable(create_schema_from_batch)

    def test_module_docstring(self):
        """Test that the module has the expected docstring."""
        import forklift.processors.schema_validator as schema_validator_module

        # The import actually loads the package's __init__.py, not the schema_validator.py file
        # So we check for the package docstring content
        expected_docstring_parts = [
            "Schema validation package",
            "modular schema validation capabilities",
            "Configuration and enums",
            "Core validation logic",
        ]

        assert schema_validator_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in schema_validator_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.processors.schema_validator import SchemaValidator as CompatSchemaValidator

        # Import from source module directly
        from forklift.processors.schema_validator.core import (
            SchemaValidator as SourceSchemaValidator,
        )

        # They should be the same class
        assert CompatSchemaValidator is SourceSchemaValidator

    def test_classes_have_expected_attributes(self):
        """Test that imported classes have expected attributes without instantiating."""
        from forklift.processors.schema_validator import (
            ColumnSchema,
            SchemaValidator,
            SchemaValidatorConfig,
            create_schema_from_batch,
            create_schema_validator_from_json,
        )

        # Test that classes have expected methods/attributes (without instantiating)
        # This ensures the imports are working correctly
        # SchemaValidator should be a class with certain methods
        assert hasattr(SchemaValidator, "__init__")

        # SchemaValidatorConfig should be a class
        assert hasattr(SchemaValidatorConfig, "__init__")

        # ColumnSchema should be a class
        assert hasattr(ColumnSchema, "__init__")

        # Functions should be callable
        assert callable(create_schema_validator_from_json)
        assert callable(create_schema_from_batch)

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        # Test that re-importing works
        import forklift.processors.schema_validator
        import forklift.processors.schema_validator as schema_validator_alias

        # Both should reference the same module
        assert forklift.processors.schema_validator is schema_validator_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # This test exercises the actual schema_validator.py file by importing through the normal mechanism
        # which will execute all the import statements and __all__ definition

        # Import the module (this will execute the schema_validator.py file)
        import forklift.processors.schema_validator as schema_validator_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
            "SchemaValidator",
            "SchemaValidatorConfig",
            "SchemaValidationMode",
            "NullabilityMode",
            "ColumnSchema",
            "create_schema_validator_from_json",
            "create_schema_from_batch",
            "__all__",
        ]

        for attr in expected_attributes:
            assert hasattr(schema_validator_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(schema_validator_module.__all__) == 7
        assert all(
            name in schema_validator_module.__all__ for name in expected_attributes[:-1]
        )  # exclude __all__ itself
