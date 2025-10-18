"""Tests for data validation backward compatibility wrapper file.

This test file ensures 100% coverage of the single file
src/forklift/processors/data_validation.py which provides backward compatibility.
"""

import pytest


class TestDataValidationBackwardCompatibilityFile:
    """Test cases for the data validation backward compatibility single file."""

    def test_import_from_file_works(self):
        """Test that importing from the file works correctly."""
        from forklift.processors.data_validation import (
            BadRowsConfig,
            BadRowsHandler,
            DataValidationProcessor,
            DateValidation,
            EnumValidation,
            FieldValidationRule,
            RangeValidation,
            StringValidation,
            ValidationConfig,
            ValidationRules,
        )

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

    def test_file_docstring(self):
        """Test that the file has the expected docstring."""
        import forklift.processors.data_validation as validation_file

        expected_docstring_parts = [
            "Data validation package",
            "comprehensive data validation",
            "Field validation",
            "Bad rows handling",
        ]

        assert validation_file.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in validation_file.__doc__

    def test_file_all_attribute(self):
        """Test that the __all__ attribute is correctly defined."""
        import forklift.processors.data_validation as validation_file

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

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(validation_file, "__all__")
        assert validation_file.__all__ == expected_all

    def test_validation_config_imports_coverage(self):
        """Test that the validation config imports are executed."""
        import forklift.processors.data_validation as validation_file

        # This tests lines 7-15 (validation config imports)
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
            assert hasattr(validation_file, class_name)
            cls = getattr(validation_file, class_name)
            assert cls is not None
            assert callable(cls)

    def test_validation_rules_import_coverage(self):
        """Test that the validation rules import is executed."""
        import forklift.processors.data_validation as validation_file

        # This tests line 18 (validation rules import)
        assert hasattr(validation_file, "ValidationRules")
        assert callable(validation_file.ValidationRules)

    def test_bad_rows_handler_import_coverage(self):
        """Test that the bad rows handler import is executed."""
        import forklift.processors.data_validation as validation_file

        # This tests line 21 (bad rows handler import)
        assert hasattr(validation_file, "BadRowsHandler")
        assert callable(validation_file.BadRowsHandler)

    def test_data_validation_processor_import_coverage(self):
        """Test that the data validation processor import is executed."""
        import forklift.processors.data_validation as validation_file

        # This tests line 24 (data validation processor import)
        assert hasattr(validation_file, "DataValidationProcessor")
        assert callable(validation_file.DataValidationProcessor)

    def test_backward_compatibility_maintained(self):
        """Test that backward compatibility is maintained."""
        # Import from the compatibility file
        from forklift.processors.data_validation import DataValidationProcessor as FileProcessor

        # Import from the package directly
        from forklift.processors.data_validation.data_validation_processor import (
            DataValidationProcessor as PackageProcessor,
        )

        # They should be the same class
        assert FileProcessor is PackageProcessor

    def test_complete_file_coverage(self):
        """Test to ensure complete coverage of all lines in the file."""
        # Import the module to execute all lines
        import forklift.processors.data_validation

        # Access all attributes to ensure imports are executed
        module = forklift.processors.data_validation

        # Verify docstring (lines 1-6)
        assert module.__doc__ is not None

        # Verify all imports by checking available attributes
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

        # Verify __all__ definition (lines 26-37)
        assert hasattr(module, "__all__")
        assert len(module.__all__) == 10

    def test_all_items_in_all_are_accessible(self):
        """Test that all items listed in __all__ are accessible."""
        import forklift.processors.data_validation as validation_file

        # Test that we can access each export from __all__
        for export_name in validation_file.__all__:
            assert hasattr(validation_file, export_name)
            export_item = getattr(validation_file, export_name)
            assert export_item is not None
            assert callable(export_item)

    def test_module_structure_after_import(self):
        """Test that the module has the expected structure after import."""
        import forklift.processors.data_validation as validation_file

        # Check that the module has the expected attributes
        required_attrs = ["__doc__", "__all__"] + validation_file.__all__

        for attr in required_attrs:
            assert hasattr(validation_file, attr), f"Missing attribute: {attr}"

        # Verify the module structure matches expectations
        assert isinstance(validation_file.__all__, list)
        assert len(validation_file.__all__) == 10
