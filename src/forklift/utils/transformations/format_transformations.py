"""Format transformation utilities for structured data types.

This module has been refactored into a modular package structure.
This file now serves as a backward compatibility layer.

For new code, consider importing directly from the format package:
- from forklift.utils.transformations.format import SSNFormatter
- from forklift.utils.transformations.format import ZipCodeFormatter
- etc.
"""

# Import the refactored FormatTransformer for backward compatibility
from .format.transformer import FormatTransformer

# Import individual formatters for direct use
from .format import (
    BaseFormatter,
    SSNFormatter,
    ZipCodeFormatter,
    PhoneNumberFormatter,
    EmailFormatter,
    IPAddressFormatter,
    MACAddressFormatter
)

__all__ = [
    'FormatTransformer',  # Backward compatibility
    'BaseFormatter',
    'SSNFormatter',
    'ZipCodeFormatter',
    'PhoneNumberFormatter',
    'EmailFormatter',
    'IPAddressFormatter',
    'MACAddressFormatter'
]
