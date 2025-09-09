"""FWF (Fixed Width File) input processing package.

This package provides functionality for reading and processing fixed-width files
with support for conditional schemas, field validation, and data type conversion.

The package is organized into several modules:
- handlers: Main FwfInputHandler class
- validators: Configuration and field validation
- converters: Type conversion and value processing
- detectors: Encoding detection and schema detection
- parsers: Line parsing and field extraction
"""

from .handlers import FwfInputHandler
from .validators import FwfConfigValidator
from .converters import FwfTypeConverter, FwfValueProcessor
from .detectors import FwfEncodingDetector, FwfSchemaDetector
from .parsers import FwfFieldExtractor, FwfLineParser

# Maintain backward compatibility by exposing the main handler
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
