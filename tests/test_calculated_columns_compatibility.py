"""Tests for calculated columns processor backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/processors/calculated_columns.py by testing the import statements and __all__ exports.
"""

import pytest


class TestCalculatedColumnsCompatibility:
    """Test cases for calculated columns processor backward compatibility."""

    def test_import_all_classes(self):
        """Test importing all classes from the calculated columns compatibility module."""
        # Import from the backward-compatibility module
        from forklift.processors.calculated_columns import (
            CalculatedColumn,
            ConstantColumn,
            ExpressionColumn,
            CalculatedColumnsConfig,
            CalculatedColumnsProcessor,
            ExpressionEvaluator,
            get_available_functions,
            get_constants
        )

        # Verify all classes are imported and are callable
        assert callable(CalculatedColumn)
        assert callable(ConstantColumn)
        assert callable(ExpressionColumn)
        assert callable(CalculatedColumnsConfig)
        assert callable(CalculatedColumnsProcessor)
        assert callable(ExpressionEvaluator)
        assert callable(get_available_functions)
        assert callable(get_constants)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected exports."""
        import forklift.processors.calculated_columns as calc_module

        expected_exports = [
            'CalculatedColumn',
            'ConstantColumn',
            'ExpressionColumn',
            'CalculatedColumnsConfig',
            'CalculatedColumnsProcessor',
            'ExpressionEvaluator',
            'get_available_functions',
            'get_constants'
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(calc_module, '__all__')
        assert calc_module.__all__ == expected_exports

        # Verify all items in __all__ are actually available in the module
        for export_name in expected_exports:
            assert hasattr(calc_module, export_name)
            export_item = getattr(calc_module, export_name)
            assert export_item is not None

    def test_individual_imports(self):
        """Test importing each class individually."""
        # Test CalculatedColumn
        from forklift.processors.calculated_columns import CalculatedColumn
        assert callable(CalculatedColumn)

        # Test ConstantColumn
        from forklift.processors.calculated_columns import ConstantColumn
        assert callable(ConstantColumn)

        # Test ExpressionColumn
        from forklift.processors.calculated_columns import ExpressionColumn
        assert callable(ExpressionColumn)

        # Test CalculatedColumnsConfig
        from forklift.processors.calculated_columns import CalculatedColumnsConfig
        assert callable(CalculatedColumnsConfig)

        # Test CalculatedColumnsProcessor
        from forklift.processors.calculated_columns import CalculatedColumnsProcessor
        assert callable(CalculatedColumnsProcessor)

        # Test ExpressionEvaluator
        from forklift.processors.calculated_columns import ExpressionEvaluator
        assert callable(ExpressionEvaluator)

        # Test get_available_functions
        from forklift.processors.calculated_columns import get_available_functions
        assert callable(get_available_functions)

        # Test get_constants
        from forklift.processors.calculated_columns import get_constants
        assert callable(get_constants)

    def test_module_docstring(self):
        """Test that the module has the expected docstring."""
        import forklift.processors.calculated_columns as calc_module

        expected_docstring_parts = [
            "Calculated columns package",
            "dynamic field generation",
            "computation"
        ]

        assert calc_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in calc_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.processors.calculated_columns import CalculatedColumnsProcessor as CompatProcessor

        # Import from source module directly
        from forklift.processors.calculated_columns.processor import CalculatedColumnsProcessor as SourceProcessor

        # They should be the same class
        assert CompatProcessor is SourceProcessor

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.processors.calculated_columns as calc_module

        # Get all public names from the module
        public_names = [name for name in dir(calc_module) if not name.startswith('_')]

        # All items in __all__ should be in the public namespace
        for export_name in calc_module.__all__:
            assert export_name in public_names

        # Test that we can access each export from __all__
        for export_name in calc_module.__all__:
            export_item = getattr(calc_module, export_name)
            assert export_item is not None

    def test_classes_have_expected_attributes(self):
        """Test that imported classes have expected attributes without instantiating."""
        from forklift.processors.calculated_columns import (
            CalculatedColumn,
            ConstantColumn,
            ExpressionColumn,
            CalculatedColumnsConfig,
            CalculatedColumnsProcessor,
            ExpressionEvaluator
        )

        # Test that classes have expected methods/attributes (without instantiating)
        # This ensures the imports are working correctly

        # CalculatedColumn should be a class with certain methods
        assert hasattr(CalculatedColumn, '__init__')

        # ConstantColumn should be a class
        assert hasattr(ConstantColumn, '__init__')

        # ExpressionColumn should be a class
        assert hasattr(ExpressionColumn, '__init__')

        # CalculatedColumnsConfig should be a class
        assert hasattr(CalculatedColumnsConfig, '__init__')

        # CalculatedColumnsProcessor should be a class
        assert hasattr(CalculatedColumnsProcessor, '__init__')

        # ExpressionEvaluator should be a class
        assert hasattr(ExpressionEvaluator, '__init__')

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        import forklift.processors.calculated_columns

        # Test that re-importing works
        import forklift.processors.calculated_columns as calc_alias

        # Both should reference the same module
        assert forklift.processors.calculated_columns is calc_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # Import the module
        import forklift.processors.calculated_columns as calc_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
            'CalculatedColumn',
            'ConstantColumn',
            'ExpressionColumn',
            'CalculatedColumnsConfig',
            'CalculatedColumnsProcessor',
            'ExpressionEvaluator',
            'get_available_functions',
            'get_constants',
            '__all__'
        ]

        for attr in expected_attributes:
            assert hasattr(calc_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(calc_module.__all__) == 8
        assert all(name in calc_module.__all__ for name in expected_attributes[:-1])  # exclude __all__ itself

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario using the backward compatibility interface."""
        # Import all exports through the compatibility interface
        from forklift.processors.calculated_columns import (
            CalculatedColumn,
            ConstantColumn,
            ExpressionColumn,
            CalculatedColumnsConfig,
            CalculatedColumnsProcessor,
            ExpressionEvaluator,
            get_available_functions,
            get_constants
        )

        # Verify all exports are accessible and have expected properties
        class_exports = [
            ('CalculatedColumn', CalculatedColumn),
            ('ConstantColumn', ConstantColumn),
            ('ExpressionColumn', ExpressionColumn),
            ('CalculatedColumnsConfig', CalculatedColumnsConfig),
            ('CalculatedColumnsProcessor', CalculatedColumnsProcessor),
            ('ExpressionEvaluator', ExpressionEvaluator)
        ]

        function_exports = [
            ('get_available_functions', get_available_functions),
            ('get_constants', get_constants)
        ]

        # Test classes
        for export_name, export_item in class_exports:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (class)"
            assert hasattr(export_item, '__init__'), f"{export_name} should have __init__ method"

        # Test functions
        for export_name, export_item in function_exports:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (function)"

    def test_module_level_imports_coverage(self):
        """Test that ensures all module-level import statements are executed."""
        # Import the module which will execute all import statements
        import forklift.processors.calculated_columns

        # Verify that the module was loaded successfully and has the expected structure
        module = forklift.processors.calculated_columns

        # This test ensures that the import statement and __all__ definition are executed
        assert hasattr(module, 'CalculatedColumn')
        assert hasattr(module, 'ConstantColumn')
        assert hasattr(module, 'ExpressionColumn')
        assert hasattr(module, 'CalculatedColumnsConfig')
        assert hasattr(module, 'CalculatedColumnsProcessor')
        assert hasattr(module, 'ExpressionEvaluator')
        assert hasattr(module, 'get_available_functions')
        assert hasattr(module, 'get_constants')
        assert hasattr(module, '__all__')

        # Verify the __all__ list matches exactly what's expected
        expected_all = [
            'CalculatedColumn',
            'ConstantColumn',
            'ExpressionColumn',
            'CalculatedColumnsConfig',
            'CalculatedColumnsProcessor',
            'ExpressionEvaluator',
            'get_available_functions',
            'get_constants'
        ]
        assert module.__all__ == expected_all

