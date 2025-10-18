"""Tests for calculated columns backward compatibility wrapper file.

This test file ensures 100% coverage of the single file
src/forklift/processors/calculated_columns.py which provides backward compatibility.
"""

import pytest


class TestCalculatedColumnsBackwardCompatibilityFile:
    """Test cases for the calculated columns backward compatibility single file."""

    def test_wildcard_import_coverage(self):
        """Test that the wildcard import from calculated_columns package works."""
        # Import the module to execute the wildcard import
        import forklift.processors.calculated_columns as calc_file

        # Verify that all expected items are available after wildcard import
        expected_items = [
            "CalculatedColumn",
            "ConstantColumn",
            "ExpressionColumn",
            "CalculatedColumnsConfig",
            "CalculatedColumnsProcessor",
            "ExpressionEvaluator",
            "get_available_functions",
            "get_constants",
        ]

        for item_name in expected_items:
            assert hasattr(calc_file, item_name)
            item = getattr(calc_file, item_name)
            assert item is not None
            assert callable(item)

    def test_file_docstring(self):
        """Test that the file has the expected docstring."""
        import forklift.processors.calculated_columns as calc_file

        expected_docstring_parts = [
            "Calculated columns package",
            "dynamic field generation",
            "computation",
        ]

        assert calc_file.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in calc_file.__doc__

    def test_file_all_attribute(self):
        """Test that the __all__ attribute is correctly defined."""
        import forklift.processors.calculated_columns as calc_file

        expected_all = [
            "CalculatedColumn",
            "ConstantColumn",
            "ExpressionColumn",
            "CalculatedColumnsConfig",
            "CalculatedColumnsProcessor",
            "ExpressionEvaluator",
            "get_available_functions",
            "get_constants",
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(calc_file, "__all__")
        assert calc_file.__all__ == expected_all

    def test_import_from_file_works(self):
        """Test that importing from the file works correctly."""
        from forklift.processors.calculated_columns import (
            CalculatedColumn, CalculatedColumnsConfig,
            CalculatedColumnsProcessor, ConstantColumn, ExpressionColumn)

        # Verify all classes are imported and are callable
        assert callable(CalculatedColumn)
        assert callable(ConstantColumn)
        assert callable(ExpressionColumn)
        assert callable(CalculatedColumnsConfig)
        assert callable(CalculatedColumnsProcessor)

    def test_backward_compatibility_maintained(self):
        """Test that backward compatibility is maintained."""
        # Import from the compatibility file
        from forklift.processors.calculated_columns import \
            CalculatedColumnsProcessor as FileProcessor
        # Import from the package directly
        from forklift.processors.calculated_columns.processor import \
            CalculatedColumnsProcessor as PackageProcessor

        # They should be the same class
        assert FileProcessor is PackageProcessor

    def test_complete_file_coverage(self):
        """Test to ensure complete coverage of all lines in the file."""
        # Import the module to execute all lines
        import forklift.processors.calculated_columns

        # Access all attributes to ensure wildcard import is executed
        module = forklift.processors.calculated_columns

        # Verify docstring (lines 1-5)
        assert module.__doc__ is not None

        # Verify wildcard import (line 8) by checking available attributes
        assert hasattr(module, "CalculatedColumn")
        assert hasattr(module, "ConstantColumn")
        assert hasattr(module, "ExpressionColumn")
        assert hasattr(module, "CalculatedColumnsConfig")
        assert hasattr(module, "CalculatedColumnsProcessor")
        assert hasattr(module, "ExpressionEvaluator")
        assert hasattr(module, "get_available_functions")
        assert hasattr(module, "get_constants")

        # Verify __all__ definition (lines 10-19)
        assert hasattr(module, "__all__")
        assert len(module.__all__) == 8

    def test_all_items_in_all_are_accessible(self):
        """Test that all items listed in __all__ are accessible."""
        import forklift.processors.calculated_columns as calc_file

        # Test that we can access each export from __all__
        for export_name in calc_file.__all__:
            assert hasattr(calc_file, export_name)
            export_item = getattr(calc_file, export_name)
            assert export_item is not None
            assert callable(export_item)

    def test_individual_class_access(self):
        """Test accessing individual classes to ensure import coverage."""
        import forklift.processors.calculated_columns as calc_file

        # Access each class individually to ensure wildcard import worked
        classes_and_functions = [
            "CalculatedColumn",
            "ConstantColumn",
            "ExpressionColumn",
            "CalculatedColumnsConfig",
            "CalculatedColumnsProcessor",
            "ExpressionEvaluator",
            "get_available_functions",
            "get_constants",
        ]

        for name in classes_and_functions:
            item = getattr(calc_file, name)
            assert item is not None
            assert callable(item)

    def test_module_structure_after_import(self):
        """Test that the module has the expected structure after import."""
        import forklift.processors.calculated_columns as calc_file

        # Check that the module has the expected attributes
        required_attrs = ["__doc__", "__all__"] + calc_file.__all__

        for attr in required_attrs:
            assert hasattr(calc_file, attr), f"Missing attribute: {attr}"

        # Verify the module structure matches expectations
        assert isinstance(calc_file.__all__, list)
        assert len(calc_file.__all__) == 8
