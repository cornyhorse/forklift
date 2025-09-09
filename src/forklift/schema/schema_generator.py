"""Schema generation functionality for Forklift.

This module has been refactored into a modular structure for better maintainability.
The original functionality is preserved through imports from the new modules.
"""

# Import everything from the new modular structure to maintain backward compatibility
from .generator.core import (
    SchemaGenerator,
    SchemaGenerationConfig,
    OutputTarget,
    FileType
)

# For any code that might import directly from this file
__all__ = [
    'SchemaGenerator',
    'SchemaGenerationConfig',
    'OutputTarget',
    'FileType'
]
