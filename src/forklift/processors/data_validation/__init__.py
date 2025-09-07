"""Data validation package for forklift processors.

This package provides comprehensive data validation functionality including:
- Field validation (required, unique, range, string, enum, date)
- Bad rows handling and collection
- Configurable validation rules
- Validation summary reporting
"""

# Import all configuration classes
from .validation_config import (
    RangeValidation,
    StringValidation,
    EnumValidation,
    DateValidation,
    FieldValidationRule,
    BadRowsConfig,
    ValidationConfig
)

# Import validation rules
from .validation_rules import ValidationRules

# Import bad rows handler
from .bad_rows_handler import BadRowsHandler

# Import main processor
from .data_validation_processor import DataValidationProcessor

# For backward compatibility, also export the main processor as the original name
__all__ = [
    # Configuration classes
    "RangeValidation",
    "StringValidation",
    "EnumValidation",
    "DateValidation",
    "FieldValidationRule",
    "BadRowsConfig",
    "ValidationConfig",

    # Core classes
    "ValidationRules",
    "BadRowsHandler",
    "DataValidationProcessor",
]
