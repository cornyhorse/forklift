"""SQL Schema Importer package for parsing and validating Forklift SQL schema files.

This package has been refactored from a single large module into smaller,
more focused modules for better maintainability and testability.
"""

# Import main classes for backward compatibility
from .importer import SqlSchemaImporter
from .exceptions import SchemaValidationError

# Re-export all public classes
__all__ = [
    'SqlSchemaImporter',
    'SchemaValidationError'
]
