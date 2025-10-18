"""Schema validation package for validating data against schema definitions.

This package provides modular schema validation capabilities with the following components:
- Configuration and enums for validation modes
- Schema definition classes
- Type conversion utilities
- Core validation logic
- Constraint validators
- Utility functions
"""

from .config import NullabilityMode, SchemaValidationMode, SchemaValidatorConfig

# Import main classes and functions for backward compatibility
from .core import SchemaValidator
from .schema import ColumnSchema
from .utils import create_schema_from_batch, create_schema_validator_from_json

__all__ = [
    "SchemaValidator",
    "SchemaValidatorConfig",
    "SchemaValidationMode",
    "NullabilityMode",
    "ColumnSchema",
    "create_schema_validator_from_json",
    "create_schema_from_batch",
]
