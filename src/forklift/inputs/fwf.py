"""Fixed-width file input handler for reading and processing FWF files.

This module has been refactored into a package structure for better maintainability.
This file now serves as a backward-compatible interface to the new package.
"""

# Import from the new package structure to maintain backward compatibility
from .fwf import (
    FwfInputHandler,
    FwfConfigValidator,
    FwfTypeConverter,
    FwfValueProcessor,
    FwfEncodingDetector,
    FwfSchemaDetector,
    FwfFieldExtractor,
    FwfLineParser,
)

# Re-export everything for backward compatibility
__all__ = [
    'FwfInputHandler',
    'FwfConfigValidator',
    'FwfTypeConverter',
    'FwfValueProcessor',
    'FwfEncodingDetector',
    'FwfSchemaDetector',
    'FwfFieldExtractor',
    'FwfLineParser',
]
