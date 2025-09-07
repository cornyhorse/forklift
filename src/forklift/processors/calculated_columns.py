"""Calculated columns processor for dynamic field generation and computation.

This module has been refactored into a package structure for better maintainability.
All original functionality is preserved through imports.
"""

# Import everything from the new package to maintain backward compatibility
from .calculated_columns import *

# Ensure backward compatibility by re-exporting all classes
__all__ = [
    'CalculatedColumn',
    'ConstantColumn',
    'ExpressionColumn',
    'CalculatedColumnsConfig',
    'CalculatedColumnsProcessor',
    'ExpressionEvaluator',
    'get_available_functions',
    'get_constants'
]
