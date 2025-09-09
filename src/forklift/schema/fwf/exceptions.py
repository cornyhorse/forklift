"""Custom exceptions for FWF schema handling."""

from __future__ import annotations


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


class FieldValidationError(SchemaValidationError):
    """Raised when field validation fails."""
    pass


class ConditionalSchemaError(SchemaValidationError):
    """Raised when conditional schema processing fails."""
    pass


class ParquetTypeError(SchemaValidationError):
    """Raised when Parquet type validation fails."""
    pass
