"""Column transformation processor and common transformation functions.

This module has been refactored into a package for better organization.
All classes and functions are re-exported from their new locations to maintain
backward compatibility.
"""

# Re-export all components from the new package structure
from .transformations.column_transformer import ColumnTransformer
from .transformations.schema_transformer import SchemaBasedTransformer
from .transformations.common import (
    trim_whitespace,
    uppercase,
    lowercase
)
from .transformations.factories import (
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_replace,
    apply_html_xml_cleaning,
    apply_string_padding,
    apply_string_trimming
)

# Maintain backward compatibility
__all__ = [
    'ColumnTransformer',
    'SchemaBasedTransformer',
    'trim_whitespace',
    'uppercase',
    'lowercase',
    'apply_money_conversion',
    'apply_numeric_cleaning',
    'apply_regex_replace',
    'apply_string_replace',
    'apply_html_xml_cleaning',
    'apply_string_padding',
    'apply_string_trimming'
]
