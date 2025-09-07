"""Format transformation package for structured data types.

This package provides specialized formatting capabilities for various data types
including SSN, ZIP codes, phone numbers, email addresses, and network addresses.
"""

from .base import BaseFormatter
from .ssn import SSNFormatter
from .postal import ZipCodeFormatter
from .phone import PhoneNumberFormatter
from .email import EmailFormatter
from .network import IPAddressFormatter, MACAddressFormatter

# For backward compatibility, maintain the original FormatTransformer interface
from .transformer import FormatTransformer

__all__ = [
    'BaseFormatter',
    'SSNFormatter',
    'ZipCodeFormatter',
    'PhoneNumberFormatter',
    'EmailFormatter',
    'IPAddressFormatter',
    'MACAddressFormatter',
    'FormatTransformer'  # Backward compatibility
]
