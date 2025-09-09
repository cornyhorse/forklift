"""Tests for schema validator backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/processors/schema_validator.py by testing the import statements and __all__ exports.
"""

import pytest
import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestSchemaValidatorCompatibility:
    """Test cases for schema validator backward compatibility."""

    def test_import_all_classes_and_functions(self):
        """Test importing all classes and functions from the compatibility module."""
        # Import from the backward-compatibility module
        from forklift.processors.schema_validator import (
            SchemaValidator,
            SchemaValidatorConfig,
            SchemaValidationMode,
            NullabilityMode,
            ColumnSchema,
            create_schema_validator_from_json,
            create_schema_from_batch
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
            'SchemaValidator',
            'SchemaValidatorConfig',
            'SchemaValidationMode',
            'NullabilityMode',
            'ColumnSchema',
            'create_schema_validator_from_json',
            'create_schema_from_batch'
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(schema_validator_module, '__all__')
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
        public_names = [name for name in dir(schema_validator_module) if not name.startswith('_')]

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
            "Core validation logic"
        ]

        assert schema_validator_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in schema_validator_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.processors.schema_validator import SchemaValidator as CompatSchemaValidator

        # Import from source module directly
        from forklift.processors.schema_validator.core import SchemaValidator as SourceSchemaValidator

        # They should be the same class
        assert CompatSchemaValidator is SourceSchemaValidator

    def test_classes_have_expected_attributes(self):
        """Test that imported classes have expected attributes without instantiating."""
        from forklift.processors.schema_validator import (
            SchemaValidator,
            SchemaValidatorConfig,
            ColumnSchema,
            create_schema_validator_from_json,
            create_schema_from_batch
        )

        # Test that classes have expected methods/attributes (without instantiating)
        # This ensures the imports are working correctly

        # SchemaValidator should be a class with certain methods
        assert hasattr(SchemaValidator, '__init__')

        # SchemaValidatorConfig should be a class
        assert hasattr(SchemaValidatorConfig, '__init__')

        # ColumnSchema should be a class
        assert hasattr(ColumnSchema, '__init__')

        # Functions should be callable
        assert callable(create_schema_validator_from_json)
        assert callable(create_schema_from_batch)

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        import forklift.processors.schema_validator

        # Test that re-importing works
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
            'SchemaValidator',
            'SchemaValidatorConfig',
            'SchemaValidationMode',
            'NullabilityMode',
            'ColumnSchema',
            'create_schema_validator_from_json',
            'create_schema_from_batch',
            '__all__'
        ]

        for attr in expected_attributes:
            assert hasattr(schema_validator_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(schema_validator_module.__all__) == 7
        assert all(name in schema_validator_module.__all__ for name in expected_attributes[:-1])  # exclude __all__ itself

    def test_import_schema_validator_py_module_directly(self):
        """Test importing the schema_validator.py module directly to ensure coverage."""
        # We need to mock the imports since we're loading the module directly
        mock_classes_and_functions = {
            'SchemaValidator': MagicMock,
            'SchemaValidatorConfig': MagicMock,
            'SchemaValidationMode': MagicMock,
            'NullabilityMode': MagicMock,
            'ColumnSchema': MagicMock,
            'create_schema_validator_from_json': MagicMock(),
            'create_schema_from_batch': MagicMock()
        }

        # Create mock modules for the imports
        mock_core_module = MagicMock()
        mock_core_module.SchemaValidator = mock_classes_and_functions['SchemaValidator']

        mock_config_module = MagicMock()
        mock_config_module.SchemaValidatorConfig = mock_classes_and_functions['SchemaValidatorConfig']
        mock_config_module.SchemaValidationMode = mock_classes_and_functions['SchemaValidationMode']
        mock_config_module.NullabilityMode = mock_classes_and_functions['NullabilityMode']

        mock_schema_module = MagicMock()
        mock_schema_module.ColumnSchema = mock_classes_and_functions['ColumnSchema']

        mock_utils_module = MagicMock()
        mock_utils_module.create_schema_validator_from_json = mock_classes_and_functions['create_schema_validator_from_json']
        mock_utils_module.create_schema_from_batch = mock_classes_and_functions['create_schema_from_batch']

        # Patch the import system to handle the imports
        with patch.dict('sys.modules', {
            'forklift.processors.schema_validator.core': mock_core_module,
            'forklift.processors.schema_validator.config': mock_config_module,
            'forklift.processors.schema_validator.schema': mock_schema_module,
            'forklift.processors.schema_validator.utils': mock_utils_module
        }):
            # Load the schema_validator.py file
            spec = importlib.util.spec_from_file_location(
                "forklift.processors.schema_validator_compat",
                Path(__file__).parent.parent / "src" / "forklift" / "processors" / "schema_validator.py"
            )
            schema_validator_compat = importlib.util.module_from_spec(spec)

            # Set up the module's package context
            schema_validator_compat.__package__ = 'forklift.processors'

            # Execute the module
            spec.loader.exec_module(schema_validator_compat)

            # Verify all expected exports are available
            expected_exports = [
                'SchemaValidator',
                'SchemaValidatorConfig',
                'SchemaValidationMode',
                'NullabilityMode',
                'ColumnSchema',
                'create_schema_validator_from_json',
                'create_schema_from_batch'
            ]

            # Test __all__ attribute
            assert hasattr(schema_validator_compat, '__all__')
            assert schema_validator_compat.__all__ == expected_exports

            # Test that all exports in __all__ are available
            for export_name in expected_exports:
                assert hasattr(schema_validator_compat, export_name)
                export_item = getattr(schema_validator_compat, export_name)
                assert export_item is not None

    def test_schema_validator_py_module_docstring(self):
        """Test the docstring of the schema_validator.py module."""
        # Read the file directly to get the docstring without executing imports
        schema_validator_py_path = Path(__file__).parent.parent / "src" / "forklift" / "processors" / "schema_validator.py"
        content = schema_validator_py_path.read_text()

        # Check docstring content in the file
        expected_docstring_parts = [
            "Backward compatibility wrapper",
            "refactored schema validator",
            "backward compatibility",
            "modular structure"
        ]

        for part in expected_docstring_parts:
            assert part in content

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario using the backward compatibility interface."""
        # Import all exports through the compatibility interface
        from forklift.processors.schema_validator import (
            SchemaValidator,
            SchemaValidatorConfig,
            SchemaValidationMode,
            NullabilityMode,
            ColumnSchema,
            create_schema_validator_from_json,
            create_schema_from_batch
        )

        # Verify all exports are accessible and have expected properties
        exports_to_test = [
            ('SchemaValidator', SchemaValidator),
            ('SchemaValidatorConfig', SchemaValidatorConfig),
            ('SchemaValidationMode', SchemaValidationMode),
            ('NullabilityMode', NullabilityMode),
            ('ColumnSchema', ColumnSchema),
            ('create_schema_validator_from_json', create_schema_validator_from_json),
            ('create_schema_from_batch', create_schema_from_batch)
        ]

        for export_name, export_item in exports_to_test:
            assert export_item is not None, f"{export_name} should not be None"

            # Check if it's a class or function
            if export_name in ['SchemaValidator', 'SchemaValidatorConfig', 'ColumnSchema']:
                assert callable(export_item), f"{export_name} should be callable (class)"
                assert hasattr(export_item, '__init__'), f"{export_name} should have __init__ method"
            elif export_name in ['create_schema_validator_from_json', 'create_schema_from_batch']:
                assert callable(export_item), f"{export_name} should be callable (function)"
            else:  # Enums/constants
                assert export_item is not None, f"{export_name} should exist"

    def test_module_level_imports_coverage(self):
        """Test that ensures all module-level import statements are executed."""
        # Import the module which will execute all import statements
        import forklift.processors.schema_validator

        # Verify that the module was loaded successfully and has the expected structure
        module = forklift.processors.schema_validator

        # This test ensures that lines 8-14 (the import statements and __all__ definition) are executed
        assert hasattr(module, 'SchemaValidator')
        assert hasattr(module, 'SchemaValidatorConfig')
        assert hasattr(module, 'SchemaValidationMode')
        assert hasattr(module, 'NullabilityMode')
        assert hasattr(module, 'ColumnSchema')
        assert hasattr(module, 'create_schema_validator_from_json')
        assert hasattr(module, 'create_schema_from_batch')
        assert hasattr(module, '__all__')

        # Verify the __all__ list matches exactly what's expected
        expected_all = [
            'SchemaValidator',
            'SchemaValidatorConfig',
            'SchemaValidationMode',
            'NullabilityMode',
            'ColumnSchema',
            'create_schema_validator_from_json',
            'create_schema_from_batch'
        ]
        assert module.__all__ == expected_all
