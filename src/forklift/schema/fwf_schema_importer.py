"""Backward compatibility wrapper for the refactored FWF schema importer.

This module maintains backward compatibility by re-exporting the main classes
from the new modular structure.
"""

# Import from the new modular structure
from .fwf.core import FwfSchemaImporter
from .fwf.exceptions import SchemaValidationError

# Maintain backward compatibility
__all__ = [
    'FwfSchemaImporter',
    'SchemaValidationError'
]
