"""Calculated columns package for dynamic field generation and computation."""

# Import all public classes and functions
from .models import (
    CalculatedColumn,
    ConstantColumn,
    ExpressionColumn,
    CalculatedColumnsConfig
)
from .processor import CalculatedColumnsProcessor
from .evaluator import ExpressionEvaluator
from .functions import get_available_functions, get_constants

# Maintain backward compatibility by exposing all classes at package level
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
