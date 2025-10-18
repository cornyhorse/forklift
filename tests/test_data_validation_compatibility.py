"""Tests for data validation processor backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/processors/data_validation.py by testing the import statements and __all__ exports.
"""

import pytest


class TestDataValidationCompatibility:
    """Test cases for data validation processor backward compatibility."""

    def test_import_all_classes_and_functions(self):
        """Test importing all classes and functions from the data validation compatibility module."""
        # Import from the backward-compatibility module
        from forklift.processors.data_validation import (
            BadRowsConfig, BadRowsHandler, DataValidationProcessor,
            DateValidation, EnumValidation, FieldValidationRule,
            RangeValidation, StringValidation, ValidationConfig,
            ValidationRules)

        # Verify all classes are imported and are callable
        assert callable(RangeValidation)
        assert callable(StringValidation)
        assert callable(EnumValidation)
        assert callable(DateValidation)
        assert callable(FieldValidationRule)
        assert callable(BadRowsConfig)
        assert callable(ValidationConfig)
        assert callable(ValidationRules)
        assert callable(BadRowsHandler)
        assert callable(DataValidationProcessor)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected exports."""
        import forklift.processors.data_validation as validation_module

        expected_exports = [
            "RangeValidation",
            "StringValidation",
            "EnumValidation",
            "DateValidation",
            "FieldValidationRule",
            "BadRowsConfig",
            "ValidationConfig",
            "ValidationRules",
            "BadRowsHandler",
            "DataValidationProcessor",
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(validation_module, "__all__")
        assert validation_module.__all__ == expected_exports

        # Verify all items in __all__ are actually available in the module
        for export_name in expected_exports:
            assert hasattr(validation_module, export_name)
            export_item = getattr(validation_module, export_name)
            assert export_item is not None

    def test_individual_imports(self):
        """Test importing each class individually."""
        # Test validation config classes
        from forklift.processors.data_validation import RangeValidation

        assert callable(RangeValidation)

        from forklift.processors.data_validation import StringValidation

        assert callable(StringValidation)

        from forklift.processors.data_validation import EnumValidation

        assert callable(EnumValidation)

        from forklift.processors.data_validation import DateValidation

        assert callable(DateValidation)

        from forklift.processors.data_validation import FieldValidationRule

        assert callable(FieldValidationRule)

        from forklift.processors.data_validation import BadRowsConfig

        assert callable(BadRowsConfig)

        from forklift.processors.data_validation import ValidationConfig

        assert callable(ValidationConfig)

        # Test core classes
        from forklift.processors.data_validation import ValidationRules

        assert callable(ValidationRules)

        from forklift.processors.data_validation import BadRowsHandler

        assert callable(BadRowsHandler)

        from forklift.processors.data_validation import DataValidationProcessor

        assert callable(DataValidationProcessor)

    def test_module_docstring(self):
        """Test that the module has the expected docstring."""
        import forklift.processors.data_validation as validation_module

        expected_docstring_parts = [
            "Data validation package",
            "comprehensive data validation",
            "Field validation",
            "Bad rows handling",
        ]

        assert validation_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in validation_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.processors.data_validation import \
            DataValidationProcessor as CompatProcessor
        # Import from source module directly
        from forklift.processors.data_validation.data_validation_processor import \
            DataValidationProcessor as SourceProcessor

        # They should be the same class
        assert CompatProcessor is SourceProcessor

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.processors.data_validation as validation_module

        # Get all public names from the module
        public_names = [name for name in dir(validation_module) if not name.startswith("_")]

        # All items in __all__ should be in the public namespace
        for export_name in validation_module.__all__:
            assert export_name in public_names

        # Test that we can access each export from __all__
        for export_name in validation_module.__all__:
            export_item = getattr(validation_module, export_name)
            assert export_item is not None

    def test_classes_have_expected_attributes(self):
        """Test that imported classes have expected attributes without instantiating."""
        from forklift.processors.data_validation import (
            BadRowsConfig, BadRowsHandler, DataValidationProcessor,
            DateValidation, EnumValidation, FieldValidationRule,
            RangeValidation, StringValidation, ValidationConfig,
            ValidationRules)

        # Test that classes have expected methods/attributes (without instantiating)
        validation_classes = [
            RangeValidation,
            StringValidation,
            EnumValidation,
            DateValidation,
            FieldValidationRule,
            BadRowsConfig,
            ValidationConfig,
            ValidationRules,
            BadRowsHandler,
            DataValidationProcessor,
        ]

        for cls in validation_classes:
            assert hasattr(cls, "__init__"), f"{cls.__name__} should have __init__ method"

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        # Test that re-importing works
        import forklift.processors.data_validation
        import forklift.processors.data_validation as validation_alias

        # Both should reference the same module
        assert forklift.processors.data_validation is validation_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # Import the module
        import forklift.processors.data_validation as validation_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
            "RangeValidation",
            "StringValidation",
            "EnumValidation",
            "DateValidation",
            "FieldValidationRule",
            "BadRowsConfig",
            "ValidationConfig",
            "ValidationRules",
            "BadRowsHandler",
            "DataValidationProcessor",
            "__all__",
        ]

        for attr in expected_attributes:
            assert hasattr(validation_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(validation_module.__all__) == 10
        assert all(
            name in validation_module.__all__ for name in expected_attributes[:-1]
        )  # exclude __all__ itself

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario using the backward compatibility interface."""
        # Import all exports through the compatibility interface
        from forklift.processors.data_validation import (
            BadRowsConfig, BadRowsHandler, DataValidationProcessor,
            DateValidation, EnumValidation, FieldValidationRule,
            RangeValidation, StringValidation, ValidationConfig,
            ValidationRules)

        # Verify all exports are accessible and have expected properties
        exports_to_test = [
            ("RangeValidation", RangeValidation),
            ("StringValidation", StringValidation),
            ("EnumValidation", EnumValidation),
            ("DateValidation", DateValidation),
            ("FieldValidationRule", FieldValidationRule),
            ("BadRowsConfig", BadRowsConfig),
            ("ValidationConfig", ValidationConfig),
            ("ValidationRules", ValidationRules),
            ("BadRowsHandler", BadRowsHandler),
            ("DataValidationProcessor", DataValidationProcessor),
        ]

        for export_name, export_item in exports_to_test:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (class)"
            assert hasattr(export_item, "__init__"), f"{export_name} should have __init__ method"

    def test_module_level_imports_coverage(self):
        """Test that ensures all module-level import statements are executed."""
        # Import the module which will execute all import statements
        import forklift.processors.data_validation

        # Verify that the module was loaded successfully and has the expected structure
        module = forklift.processors.data_validation

        # This test ensures that all import statements and __all__ definition are executed
        assert hasattr(module, "RangeValidation")
        assert hasattr(module, "StringValidation")
        assert hasattr(module, "EnumValidation")
        assert hasattr(module, "DateValidation")
        assert hasattr(module, "FieldValidationRule")
        assert hasattr(module, "BadRowsConfig")
        assert hasattr(module, "ValidationConfig")
        assert hasattr(module, "ValidationRules")
        assert hasattr(module, "BadRowsHandler")
        assert hasattr(module, "DataValidationProcessor")
        assert hasattr(module, "__all__")

        # Verify the __all__ list matches exactly what's expected
        expected_all = [
            "RangeValidation",
            "StringValidation",
            "EnumValidation",
            "DateValidation",
            "FieldValidationRule",
            "BadRowsConfig",
            "ValidationConfig",
            "ValidationRules",
            "BadRowsHandler",
            "DataValidationProcessor",
        ]
        assert module.__all__ == expected_all

    def test_validation_config_imports_coverage(self):
        """Test that the validation config imports are executed."""
        import forklift.processors.data_validation as validation_module

        # Verify that all validation config classes are available
        config_classes = [
            "RangeValidation",
            "StringValidation",
            "EnumValidation",
            "DateValidation",
            "FieldValidationRule",
            "BadRowsConfig",
            "ValidationConfig",
        ]

        for class_name in config_classes:
            assert hasattr(validation_module, class_name)
            cls = getattr(validation_module, class_name)
            assert cls is not None
            assert callable(cls)

    def test_core_classes_imports_coverage(self):
        """Test that the core classes imports are executed."""
        import forklift.processors.data_validation as validation_module

        # Verify that all core classes are available
        core_classes = ["ValidationRules", "BadRowsHandler", "DataValidationProcessor"]

        for class_name in core_classes:
            assert hasattr(validation_module, class_name)
            cls = getattr(validation_module, class_name)
            assert cls is not None
            assert callable(cls)
