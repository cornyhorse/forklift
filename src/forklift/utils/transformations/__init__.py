"""Data transformation utilities package.

This package provides comprehensive data transformation capabilities split into focused modules:
- base: Core transformation infrastructure and base classes
- configs: Configuration dataclasses for all transformations
- string_transformations: String cleaning, formatting, and case transformations
- numeric_transformations: Money, numeric cleaning, and padding operations
- datetime_transformations: Date/time parsing and formatting
- format_transformations: SSN, ZIP, phone, email, IP, MAC address formatting
- html_xml_transformations: HTML/XML tag removal and entity decoding
- factory: Factory functions for creating transformations from configuration
"""

# Import all transformation configs and utilities for backward compatibility
from .configs import *
from .base import DataTransformer
from .string_transformations import StringTransformer
from .numeric_transformations import NumericTransformer
from .datetime_transformations import DateTimeTransformer
from .format.transformer import FormatTransformer  # Updated import path
from .html_xml_transformations import HTMLXMLTransformer
from .factory import create_transformation_from_config

# Import date parser utilities for backward compatibility
from ..date_parser import coerce_datetime

# Main transformer class that combines all transformation capabilities
__all__ = [
    'DataTransformer',
    'StringTransformer',
    'NumericTransformer',
    'DateTimeTransformer',
    'FormatTransformer',
    'HTMLXMLTransformer',
    'create_transformation_from_config',
    'coerce_datetime',
    # Config classes will be imported via configs module
]
