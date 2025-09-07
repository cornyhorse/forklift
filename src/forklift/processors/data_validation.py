"""Data validation processor with bad rows handling for required, unique, and range validation.

This module has been refactored into a package for better organization.
All classes are re-exported here for backward compatibility.
"""

# Import everything from the new package structure
from .data_validation.validation_config import (
    RangeValidation,
    StringValidation,
    EnumValidation,
    DateValidation,
    FieldValidationRule,
    BadRowsConfig,
    ValidationConfig,
)
from .data_validation.validation_rules import ValidationRules
from .data_validation.bad_rows_handler import BadRowsHandler
from .data_validation.data_validation_processor import DataValidationProcessor

# Ensure backward compatibility by exposing all the same names
__all__ = [
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
