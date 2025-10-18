"""Column transformation processor and common transformation functions.

This module has been refactored into a package for better organization.
All classes and functions are re-exported from their new locations to maintain
backward compatibility.
"""

# Re-export utilities for backward compatibility
from ...utils.transformations import (  # pragma: no cover; Configuration classes
    DataTransformer,
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

# Re-export all components from the new package structure
from .transformations.column_transformer import ColumnTransformer  # pragma: no cover
from .transformations.common import lowercase, trim_whitespace, uppercase  # pragma: no cover
from .transformations.factories import (  # pragma: no cover
    apply_html_xml_cleaning,
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_padding,
    apply_string_replace,
    apply_string_trimming,
)
from .transformations.schema_transformer import SchemaBasedTransformer  # pragma: no cover

# Maintain backward compatibility
__all__ = [  # pragma: no cover
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
    "MoneyTypeConfig",
    "NumericCleaningConfig",
    "RegexReplaceConfig",
    "StringReplaceConfig",
    "HTMLXMLConfig",
    "StringPaddingConfig",
    "DateTimeTransformConfig",
    "StringCleaningConfig",
]
