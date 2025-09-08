"""String transformation utilities package.

This package provides string cleaning, formatting, and case transformation capabilities.
The original StringTransformer class has been refactored into focused modules for better maintainability.
"""

from .core import StringTransformer
from .regex_operations import RegexOperations
from .padding_operations import PaddingOperations
from .cleaning_operations import CleaningOperations
from .case_operations import CaseOperations
from .normalization_operations import NormalizationOperations

__all__ = [
    'StringTransformer',
    'RegexOperations',
    'PaddingOperations',
    'CleaningOperations',
    'CaseOperations',
    'NormalizationOperations'
]
