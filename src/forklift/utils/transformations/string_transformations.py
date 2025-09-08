"""String transformation utilities.

This module provides string cleaning, formatting, and case transformation capabilities.

This module has been refactored into a package for better maintainability.
All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .string_transformations.core import StringTransformer
from .string_transformations.regex_operations import RegexOperations
from .string_transformations.padding_operations import PaddingOperations
from .string_transformations.cleaning_operations import CleaningOperations
from .string_transformations.case_operations import CaseOperations
from .string_transformations.normalization_operations import NormalizationOperations

__all__ = [
    'StringTransformer',
    'RegexOperations',
    'PaddingOperations',
    'CleaningOperations',
    'CaseOperations',
    'NormalizationOperations'
]
