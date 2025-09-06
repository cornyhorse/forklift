"""Configuration classes and enums for schema validation."""

from enum import Enum
from dataclasses import dataclass
from typing import Optional


class SchemaValidationMode(Enum):
    """Schema validation modes."""
    STRICT = "strict"  # All columns must match schema exactly
    PERMISSIVE = "permissive"  # Allow extra columns not in schema
    COERCE = "coerce"  # Attempt to coerce types when possible


class NullabilityMode(Enum):
    """How to handle nullability violations."""
    ERROR = "error"  # Raise validation errors for null violations
    WARNING = "warning"  # Log warnings but continue processing
    IGNORE = "ignore"  # Ignore nullability constraints


@dataclass
class SchemaValidatorConfig:
    """Configuration for schema validation."""
    validation_mode: SchemaValidationMode = SchemaValidationMode.STRICT
    nullability_mode: NullabilityMode = NullabilityMode.ERROR
    allow_type_coercion: bool = False
    check_column_order: bool = False
    case_sensitive: bool = True
    extra_columns_allowed: bool = False

    # Validation thresholds
    max_null_percentage: Optional[float] = None
    min_row_count: Optional[int] = None
    max_row_count: Optional[int] = None
