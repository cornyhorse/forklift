"""Data validation package for forklift processors.

This package provides comprehensive data validation functionality including:
- Field validation (required, unique, range, string, enum, date)
- Bad rows handling and collection
- Configurable validation rules
- Validation summary reporting
"""

# Import bad rows handler
from .bad_rows_handler import BadRowsHandler

# Import main processor
from .data_validation_processor import DataValidationProcessor

# Import all configuration classes
from .validation_config import (
    BadRowsConfig,
    DateValidation,
    EnumValidation,
    FieldValidationRule,
    RangeValidation,
    StringValidation,
    ValidationConfig,
)

# Import validation rules
from .validation_rules import ValidationRules

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
