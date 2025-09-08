"""Tests for calculated columns processor backward compatibility."""

import pytest
from unittest.mock import Mock

from forklift.processors import calculated_columns


class TestCalculatedColumnsModule:
    """Test cases for the calculated columns processor module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(calculated_columns, 'CalculatedColumn')
        assert hasattr(calculated_columns, 'ConstantColumn')
        assert hasattr(calculated_columns, 'ExpressionColumn')
        assert hasattr(calculated_columns, 'CalculatedColumnsConfig')
        assert hasattr(calculated_columns, 'CalculatedColumnsProcessor')

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
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

        assert calculated_columns.__all__ == expected_exports

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.processors.calculated_columns import CalculatedColumn
        from forklift.processors.calculated_columns import ConstantColumn
        from forklift.processors.calculated_columns import ExpressionColumn
        from forklift.processors.calculated_columns import CalculatedColumnsConfig
        from forklift.processors.calculated_columns import CalculatedColumnsProcessor
        from forklift.processors.calculated_columns import ExpressionEvaluator
        from forklift.processors.calculated_columns import get_available_functions
        from forklift.processors.calculated_columns import get_constants

        # Verify they are the same as the module attributes
        assert CalculatedColumn is calculated_columns.CalculatedColumn
        assert ConstantColumn is calculated_columns.ConstantColumn
        assert ExpressionColumn is calculated_columns.ExpressionColumn
        assert CalculatedColumnsConfig is calculated_columns.CalculatedColumnsConfig
        assert CalculatedColumnsProcessor is calculated_columns.CalculatedColumnsProcessor
        assert ExpressionEvaluator is calculated_columns.ExpressionEvaluator
        assert get_available_functions is calculated_columns.get_available_functions
        assert get_constants is calculated_columns.get_constants

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert calculated_columns.__doc__ is not None
        assert "Calculated columns package" in calculated_columns.__doc__
        assert "dynamic field generation" in calculated_columns.__doc__
