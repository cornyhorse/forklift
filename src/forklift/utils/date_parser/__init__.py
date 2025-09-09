"""Date parsing utilities package.

This package provides comprehensive date and datetime parsing functionality with support for:
- Multiple date/datetime formats
- Epoch timestamp parsing
- Schema token format normalization
- Fuzzy parsing fallback using dateutil

The package is organized into several modules:
- constants: Format lists and mappings
- epoch: Epoch timestamp handling
- format_utils: Format normalization and validation
- parsing: Core parsing utilities
- core: Main public API functions
"""

# Import the main public API functions
from .core import parse_date, coerce_date, coerce_datetime

# Import constants for backward compatibility with tests
from .constants import COMMON_DATE_FORMATS, COMMON_DATETIME_FORMATS, SCHEMA_TOKEN_MAP

# Make the main functions and constants available at package level for backward compatibility
__all__ = [
    'parse_date',
    'coerce_date',
    'coerce_datetime',
    'COMMON_DATE_FORMATS',
    'COMMON_DATETIME_FORMATS',
    'SCHEMA_TOKEN_MAP'
]
