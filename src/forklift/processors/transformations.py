"""Column transformation processor and common transformation functions.

This module has been refactored into a package for better organization.
All classes and functions are re-exported from their new locations to maintain
backward compatibility.
"""

# Re-export all components from the new package structure
from .transformations.column_transformer import ColumnTransformer  # pragma: no cover
from .transformations.schema_transformer import SchemaBasedTransformer  # pragma: no cover
from .transformations.common import (  # pragma: no cover
    trim_whitespace,
    uppercase,
    lowercase
)
from .transformations.factories import (  # pragma: no cover
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_replace,
    apply_html_xml_cleaning,
    apply_string_padding,
    apply_string_trimming
)

# Re-export utilities for backward compatibility
from ...utils.transformations import (  # pragma: no cover
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

# Maintain backward compatibility
__all__ = [  # pragma: no cover
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
    'MoneyTypeConfig',
    'NumericCleaningConfig',
    'RegexReplaceConfig',
    'StringReplaceConfig',
    'HTMLXMLConfig',
    'StringPaddingConfig',
    'DateTimeTransformConfig',
    'StringCleaningConfig'
]
