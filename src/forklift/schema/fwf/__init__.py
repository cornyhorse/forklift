"""FWF (Fixed Width File) schema handling package for Forklift.

This package provides comprehensive support for parsing and validating
Fixed Width File schemas with JSON Schema extensions.
"""

from .core import FwfSchemaImporter
from .exceptions import SchemaValidationError

__all__ = [
    'FwfSchemaImporter',
    'SchemaValidationError'
]
