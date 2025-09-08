"""Tests for calculated columns processor backward compatibility."""

import pytest


class TestCalculatedColumnsBackwardCompatibility:
    """Test backward compatibility of calculated columns processor module."""

    def test_calculated_columns_imports(self):
        """Test that all calculated columns classes can be imported from the main module."""
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

        # Verify all classes and functions are available
        assert CalculatedColumn is not None
        assert ConstantColumn is not None
        assert ExpressionColumn is not None
        assert CalculatedColumnsConfig is not None
        assert CalculatedColumnsProcessor is not None
        assert ExpressionEvaluator is not None
        assert callable(get_available_functions)
        assert callable(get_constants)

    def test_calculated_columns_all_exports(self):
        """Test that __all__ contains expected exports."""
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

        assert hasattr(calc_module, '__all__')
        assert set(calc_module.__all__) == set(expected_exports)

    def test_calculated_columns_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.processors.calculated_columns as calc_module

        assert calc_module.__doc__ is not None
        assert "Calculated columns processor" in calc_module.__doc__
        assert "backward compatibility" in calc_module.__doc__

    def test_calculated_columns_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.processors.calculated_columns import (
            CalculatedColumn,
            ConstantColumn,
            ExpressionColumn,
            CalculatedColumnsConfig,
            CalculatedColumnsProcessor,
            ExpressionEvaluator
        )

        # Verify classes are callable (can be instantiated)
        assert callable(CalculatedColumn)
        assert callable(ConstantColumn)
        assert callable(ExpressionColumn)
        assert callable(CalculatedColumnsConfig)
        assert callable(CalculatedColumnsProcessor)
        assert callable(ExpressionEvaluator)

    def test_calculated_columns_functions(self):
        """Test that utility functions work properly."""
        from forklift.processors.calculated_columns import (
            get_available_functions,
            get_constants
        )

        # Test that functions return expected types
        functions = get_available_functions()
        constants = get_constants()

        assert isinstance(functions, dict)
        assert isinstance(constants, dict)
