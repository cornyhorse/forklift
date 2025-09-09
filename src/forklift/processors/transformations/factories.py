"""Factory functions for creating enhanced transformation functions."""

from __future__ import annotations
from typing import List, Callable, Optional

import pyarrow as pa

from ...utils.transformations import (
    DataTransformer,
    MoneyTypeConfig,
    NumericCleaningConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    HTMLXMLConfig,
    StringPaddingConfig
)


def apply_money_conversion(currency_symbols: List[str] = None,
                          thousands_separator: str = ",",
                          decimal_separator: str = ".",
                          parentheses_negative: bool = True) -> Callable[[pa.Array], pa.Array]:
    """Create a money conversion transformation function.

    Args:
        currency_symbols: List of currency symbols to remove
        thousands_separator: Thousands separator character
        decimal_separator: Decimal separator character
        parentheses_negative: Whether to treat parentheses as negative

    Returns:
        Transformation function for money conversion
    """
    config = MoneyTypeConfig(
        currency_symbols=currency_symbols,
        thousands_separator=thousands_separator,
        decimal_separator=decimal_separator,
        parentheses_negative=parentheses_negative
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_money_conversion(column, config)


def apply_numeric_cleaning(thousands_separator: str = ",",
                          decimal_separator: str = ".",
                          allow_nan: bool = True,
                          target_type: str = "double") -> Callable[[pa.Array], pa.Array]:
    """Create a numeric cleaning transformation function.

    Args:
        thousands_separator: Thousands separator to remove
        decimal_separator: Decimal separator to normalize
        allow_nan: Whether to allow NaN values instead of errors
        target_type: Target numeric type (int64, double, etc.)

    Returns:
        Transformation function for numeric cleaning
    """
    config = NumericCleaningConfig(
        thousands_separator=thousands_separator,
        decimal_separator=decimal_separator,
        allow_nan=allow_nan
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_numeric_cleaning(column, config, target_type)


def apply_regex_replace(pattern: str,
                       replacement: str,
                       flags: int = 0) -> Callable[[pa.Array], pa.Array]:
    """Create a regex replace transformation function.

    Args:
        pattern: Regex pattern to match
        replacement: Replacement string
        flags: Regex flags (re.IGNORECASE, etc.)

    Returns:
        Transformation function for regex replacement
    """
    config = RegexReplaceConfig(
        pattern=pattern,
        replacement=replacement,
        flags=flags
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_regex_replace(column, config)


def apply_string_replace(old: str,
                        new: str,
                        count: int = -1) -> Callable[[pa.Array], pa.Array]:
    """Create a string replace transformation function.

    Args:
        old: String to replace
        new: Replacement string
        count: Number of replacements (-1 for all)

    Returns:
        Transformation function for string replacement
    """
    config = StringReplaceConfig(
        old=old,
        new=new,
        count=count
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_replace(column, config)


def apply_html_xml_cleaning(strip_tags: bool = True,
                           decode_entities: bool = True,
                           preserve_whitespace: bool = False) -> Callable[[pa.Array], pa.Array]:
    """Create an HTML/XML cleaning transformation function.

    Args:
        strip_tags: Whether to remove HTML/XML tags
        decode_entities: Whether to decode HTML entities
        preserve_whitespace: Whether to preserve whitespace formatting

    Returns:
        Transformation function for HTML/XML cleaning
    """
    config = HTMLXMLConfig(
        strip_tags=strip_tags,
        decode_entities=decode_entities,
        preserve_whitespace=preserve_whitespace
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_html_xml_cleaning(column, config)


def apply_string_padding(width: int,
                        fillchar: str = " ",
                        side: str = "left") -> Callable[[pa.Array], pa.Array]:
    """Create a string padding transformation function.

    Args:
        width: Target width for padding
        fillchar: Character to use for padding
        side: Which side to pad ("left", "right", "both")

    Returns:
        Transformation function for string padding
    """
    config = StringPaddingConfig(
        width=width,
        fillchar=fillchar,
        side=side
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_padding(column, config)


def apply_string_trimming(side: str = "both",
                         chars: Optional[str] = None) -> Callable[[pa.Array], pa.Array]:
    """Create a string trimming transformation function.

    Args:
        side: Which side to trim ("left", "right", "both")
        chars: Characters to trim (None for whitespace)

    Returns:
        Transformation function for string trimming
    """
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_trimming(column, side, chars)
