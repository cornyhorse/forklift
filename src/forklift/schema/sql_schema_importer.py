"""SQL Schema Importer for dynamic schema parsing and validation.

This module has been refactored into a package structure for better maintainability.
All original functionality is preserved through imports.
"""

# Import everything from the new package to maintain backward compatibility
from .sql_schema_importer import *

# Ensure backward compatibility by re-exporting all classes
__all__ = [
    'SqlSchemaImporter',
    'SchemaValidationError'
]
