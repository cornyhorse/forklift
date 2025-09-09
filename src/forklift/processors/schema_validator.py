"""Backward compatibility wrapper for the refactored schema validator.

This module maintains backward compatibility by re-exporting the main classes
and functions from the new modular structure.
"""

# Import from the new modular structure
from .schema_validator.core import SchemaValidator
from .schema_validator.config import SchemaValidatorConfig, SchemaValidationMode, NullabilityMode
from .schema_validator.schema import ColumnSchema
from .schema_validator.utils import create_schema_validator_from_json, create_schema_from_batch

# Maintain backward compatibility
__all__ = [
    'SchemaValidator',
    'SchemaValidatorConfig',
    'SchemaValidationMode',
    'NullabilityMode',
    'ColumnSchema',
    'create_schema_validator_from_json',
    'create_schema_from_batch'
]
