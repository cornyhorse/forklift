"""Transformation processors package.

This package provides data transformation capabilities including:
- Basic column transformations
- Schema-driven transformations
- Common transformation functions
- Factory functions for creating transformations
"""

from .column_transformer import ColumnTransformer
from .schema_transformer import SchemaBasedTransformer
from .common import (
    trim_whitespace,
    uppercase,
    lowercase
)
from .factories import (
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_replace,
    apply_html_xml_cleaning,
    apply_string_padding,
    apply_string_trimming
)

# Re-export utilities for backward compatibility
from ...utils.transformations import (
    DataTransformer,
    create_transformation_from_config,
    # Configuration classes
    MoneyTypeConfig,
    NumericCleaningConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    HTMLXMLConfig,
    StringPaddingConfig,
    DateTimeTransformConfig,
    StringCleaningConfig
)

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
    'apply_string_trimming',
    'DataTransformer',
    'create_transformation_from_config',
    # Configuration classes
    'MoneyTypeConfig',
    'NumericCleaningConfig',
    'RegexReplaceConfig',
    'StringReplaceConfig',
    'HTMLXMLConfig',
    'StringPaddingConfig',
    'DateTimeTransformConfig',
    'StringCleaningConfig'
]