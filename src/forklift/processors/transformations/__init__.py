"""Transformation processors package.

This package provides data transformation capabilities including:
- Basic column transformations
- Schema-driven transformations
- Common transformation functions
- Factory functions for creating transformations
"""

# Re-export utilities for backward compatibility
from ...utils.transformations import DataTransformer  # Configuration classes
from ...utils.transformations import (
    DateTimeTransformConfig,
    HTMLXMLConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    RegexReplaceConfig,
    StringCleaningConfig,
    StringPaddingConfig,
    StringReplaceConfig,
    create_transformation_from_config,
)
from .column_transformer import ColumnTransformer
from .common import lowercase, trim_whitespace, uppercase
from .factories import (
    apply_html_xml_cleaning,
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_padding,
    apply_string_replace,
    apply_string_trimming,
)
from .schema_transformer import SchemaBasedTransformer

__all__ = [
    "ColumnTransformer",
    "SchemaBasedTransformer",
    "trim_whitespace",
    "uppercase",
    "lowercase",
    "apply_money_conversion",
    "apply_numeric_cleaning",
    "apply_regex_replace",
    "apply_string_replace",
    "apply_html_xml_cleaning",
    "apply_string_padding",
    "apply_string_trimming",
    "DataTransformer",
    "create_transformation_from_config",
    # Configuration classes
    "MoneyTypeConfig",
    "NumericCleaningConfig",
    "RegexReplaceConfig",
    "StringReplaceConfig",
    "HTMLXMLConfig",
    "StringPaddingConfig",
    "DateTimeTransformConfig",
    "StringCleaningConfig",
]
