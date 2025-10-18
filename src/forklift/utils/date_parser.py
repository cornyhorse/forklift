"""Backward compatibility wrapper for the refactored date parser.

This module maintains backward compatibility by re-exporting the main functions
from the new modular structure.
"""

from .date_parser.constants import COMMON_DATE_FORMATS, COMMON_DATETIME_FORMATS, SCHEMA_TOKEN_MAP

# Import from the new modular structure
from .date_parser.core import coerce_date, coerce_datetime, parse_date

# Maintain backward compatibility
__all__ = [
    "parse_date",
    "coerce_date",
    "coerce_datetime",
    "COMMON_DATE_FORMATS",
    "COMMON_DATETIME_FORMATS",
    "SCHEMA_TOKEN_MAP",
]
