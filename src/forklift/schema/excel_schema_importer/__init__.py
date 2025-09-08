"""Excel schema importer package for processing Excel schema definitions.

This module provides comprehensive schema import and validation capabilities
for Excel-based data schemas. The original file has been refactored into a package
for better maintainability. All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .core import ExcelSchemaImporter
from .exceptions import SchemaValidationError
from .validator import SchemaValidator
from .type_validator import ParquetTypeValidator
from .utils import SchemaDataExtractor

__all__ = [
    'ExcelSchemaImporter',
    'SchemaValidationError',
    'SchemaValidator',
    'ParquetTypeValidator',
    'SchemaDataExtractor'
]
