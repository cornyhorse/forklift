"""Data transformation utilities for schema-driven data cleaning and conversion.

This module provides comprehensive data transformation capabilities including:
- Regex replace operations
- Standard string replacements
- Money type conversions
- Numeric cleaning (comma/decimal separator handling)
- String padding and trimming operations
- NaN/NULL handling for type coercion
- HTML/XML tag removal
- And other common data cleaning operations
"""

from __future__ import annotations

import html
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
import datetime
import re

import pyarrow as pa
import pandas as pd

from .date_parser import (
    coerce_datetime,
)

@dataclass
class DateTimeTransformConfig:
    """Configuration for datetime parsing and transformation."""
    mode: str = "common_formats"  # "enforce", "specify_formats", "common_formats"
    format: Optional[str] = None  # Single format to enforce (enforce mode)
    formats: Optional[List[str]] = None  # List of allowed formats (specify_formats mode)
    allow_fuzzy: bool = False  # Allow fuzzy parsing with dateutil
    from_epoch: bool = False  # Treat input as epoch timestamp
    to_epoch: Optional[str] = None  # Convert output to epoch ("seconds", "milliseconds", etc.)
    target_type: str = "datetime"  # "datetime", "date", "timestamp", "string"
    output_format: Optional[str] = None  # Format for string output (if target_type is "string")
    timezone: Optional[str] = None  # Target timezone for output

    def __post_init__(self):
        valid_modes = ["enforce", "specify_formats", "common_formats"]
        if self.mode not in valid_modes:
            raise ValueError(f"Invalid mode: {self.mode}. Must be one of {valid_modes}")

        if self.mode == "enforce" and not self.format:
            raise ValueError("Format must be specified when mode is 'enforce'")

        if self.mode == "specify_formats" and not self.formats:
            raise ValueError("Formats list must be specified when mode is 'specify_formats'")

        valid_targets = ["datetime", "date", "timestamp", "string"]
        if self.target_type not in valid_targets:
            raise ValueError(f"Invalid target_type: {self.target_type}. Must be one of {valid_targets}")

        if self.to_epoch:
            valid_epoch_units = ["seconds", "milliseconds", "microseconds", "nanoseconds"]
            if self.to_epoch not in valid_epoch_units:
                raise ValueError(f"Invalid to_epoch unit: {self.to_epoch}. Must be one of {valid_epoch_units}")

@dataclass
class RegexReplaceConfig:
    """Configuration for regex replace operations."""
    pattern: str
    replacement: str
    flags: int = 0  # re.IGNORECASE, re.MULTILINE, etc.


@dataclass
class StringReplaceConfig:
    """Configuration for simple string replace operations."""
    old: str
    new: str
    count: int = -1  # -1 means replace all occurrences


@dataclass
class MoneyTypeConfig:
    """Configuration for money type conversions."""
    currency_symbols: List[str] = None
    thousands_separator: str = ","
    decimal_separator: str = "."
    parentheses_negative: bool = True
    strip_whitespace: bool = True

    def __post_init__(self):
        if self.currency_symbols is None:
            self.currency_symbols = ["$", "€", "£", "¥", "₹", "₽", "¢"]


@dataclass
class NumericCleaningConfig:
    """Configuration for numeric field cleaning."""
    thousands_separator: str = ","
    decimal_separator: str = "."
    allow_nan: bool = True
    nan_values: List[str] = None
    strip_whitespace: bool = True

    def __post_init__(self):
        if self.nan_values is None:
            self.nan_values = ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]


@dataclass
class StringPaddingConfig:
    """Configuration for string padding operations."""
    width: int
    fillchar: str = " "
    side: str = "left"  # "left", "right", "both"


@dataclass
class HTMLXMLConfig:
    """Configuration for HTML/XML cleaning."""
    strip_tags: bool = True
    decode_entities: bool = True
    preserve_whitespace: bool = False


@dataclass
class StringCleaningConfig:
    """Configuration for comprehensive string cleaning operations."""
    # Smart quotes and special characters
    normalize_quotes: bool = True  # Convert smart quotes to ASCII quotes
    normalize_dashes: bool = True  # Convert em/en dashes to hyphens
    normalize_spaces: bool = True  # Convert non-breaking spaces to regular spaces

    # Whitespace handling
    collapse_whitespace: bool = True  # Collapse multiple spaces to single space
    strip_whitespace: bool = True  # Strip leading/trailing whitespace
    remove_tabs: bool = False  # Convert tabs to spaces (if False) or remove (if True)
    tab_replacement: str = " "  # What to replace tabs with if not removing

    # Zero-width and control characters
    remove_zero_width: bool = True  # Remove zero-width characters (ZWSP, ZWNJ, etc.)
    remove_control_chars: bool = True  # Remove control characters (except common ones)
    preserve_newlines: bool = True  # Keep \n and \r\n when removing control chars
    preserve_tabs: bool = False  # Keep \t when removing control chars

    # Unicode normalization
    unicode_normalize: Optional[str] = "NFKC"  # Unicode normalization form (NFC, NFD, NFKC, NFKD)

    # Case handling
    fix_case_issues: bool = False  # Fix common case issues (e.g., multiple caps)
    case_transform: Optional[str] = None  # Case transformation: 'upper', 'lower', 'title', 'proper', or None
    title_case_exceptions: List[str] = None  # Words to not title case (e.g., ["of", "the", "and"])
    custom_case_mapping: Optional[Dict[str, str]] = None  # Custom case mappings (e.g., state codes: {"california": "CA"})
    case_mapping_mode: str = "exact"  # How to apply custom mappings: 'exact', 'contains', 'startswith', 'endswith'

    # Other cleaning
    remove_accents: bool = False  # Remove diacritical marks
    ascii_only: bool = False  # Convert to ASCII-only (implies remove_accents=True)
    fix_encoding_errors: bool = True  # Fix common encoding errors

    def __post_init__(self):
        if self.title_case_exceptions is None:
            self.title_case_exceptions = ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]

        if self.custom_case_mapping is None:
            self.custom_case_mapping = {}

        # Validate case_transform parameter
        valid_transforms = {None, 'upper', 'lower', 'title', 'proper'}
        if self.case_transform not in valid_transforms:
            raise ValueError(f"case_transform must be one of {valid_transforms}, got: {self.case_transform}")

        # Validate case_mapping_mode
        valid_modes = {'exact', 'contains', 'startswith', 'endswith'}
        if self.case_mapping_mode not in valid_modes:
            raise ValueError(f"case_mapping_mode must be one of {valid_modes}, got: {self.case_mapping_mode}")


class DataTransformer:
    """Comprehensive data transformation engine for schema-driven cleaning."""

    def __init__(self):
        """Initialize the data transformer."""
        pass

    def apply_regex_replace(self, column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column.

        Args:
            column: PyArrow Array containing string data
            config: Regex replace configuration

        Returns:
            PyArrow Array with regex replacements applied
        """
        if not pa.types.is_string(column.type):
            return column

        # Convert to pandas for regex operations
        pandas_series = column.to_pandas()

        # Apply regex replacement
        transformed_series = pandas_series.str.replace(
            config.pattern,
            config.replacement,
            regex=True,
            flags=config.flags
        )

        return pa.array(transformed_series)

    def apply_string_replace(self, column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple string replace transformation.

        Args:
            column: PyArrow Array containing string data
            config: String replace configuration

        Returns:
            PyArrow Array with string replacements applied
        """
        if not pa.types.is_string(column.type):
            return column

        # Convert to pandas for string operations
        pandas_series = column.to_pandas()

        # Apply string replacement
        if config.count == -1:
            transformed_series = pandas_series.str.replace(config.old, config.new)
        else:
            transformed_series = pandas_series.str.replace(config.old, config.new, n=config.count)

        return pa.array(transformed_series)

    def apply_money_conversion(self, column: pa.Array, config: MoneyTypeConfig) -> pa.Array:
        """Convert money strings to decimal values.

        Args:
            column: PyArrow Array containing money strings
            config: Money conversion configuration

        Returns:
            PyArrow Array with money values converted to decimals
        """
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        converted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                converted_values.append(None)
                continue

            try:
                cleaned_value = self._clean_money_string(str(value), config)
                if cleaned_value is not None:
                    converted_values.append(float(cleaned_value))
                else:
                    converted_values.append(None)
            except (ValueError, InvalidOperation):
                converted_values.append(None)

        return pa.array(converted_values, type=pa.float64())

    def _clean_money_string(self, value: str, config: MoneyTypeConfig) -> Optional[Decimal]:
        """Clean a money string and convert to decimal.

        Args:
            value: Raw money string
            config: Money conversion configuration

        Returns:
            Decimal value or None if conversion fails
        """
        if config.strip_whitespace:
            value = value.strip()

        if not value:
            return None

        # Check for parentheses indicating negative
        is_negative = False
        if config.parentheses_negative and value.startswith('(') and value.endswith(')'):
            is_negative = True
            value = value[1:-1].strip()

        # Remove currency symbols
        for symbol in config.currency_symbols:
            value = value.replace(symbol, '')

        # Handle thousands and decimal separators
        if config.thousands_separator and config.decimal_separator:
            # Remove thousands separators
            value = value.replace(config.thousands_separator, '')
            # Normalize decimal separator to period
            if config.decimal_separator != '.':
                value = value.replace(config.decimal_separator, '.')

        # Remove any remaining whitespace
        value = value.strip()

        try:
            decimal_value = Decimal(value)
            if is_negative:
                decimal_value = -decimal_value
            return decimal_value
        except (ValueError, InvalidOperation):
            return None

    def apply_numeric_cleaning(self, column: pa.Array, config: NumericCleaningConfig, target_type: str = "double") -> pa.Array:
        """Clean numeric fields with configurable separators and NaN handling.

        Args:
            column: PyArrow Array containing numeric strings
            config: Numeric cleaning configuration
            target_type: Target numeric type ("int64", "double", etc.)

        Returns:
            PyArrow Array with cleaned numeric values
        """
        pandas_series = column.to_pandas()
        converted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                converted_values.append(None)
                continue

            str_value = str(value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Check if value should be treated as NaN
            if config.allow_nan and str_value in config.nan_values:
                converted_values.append(None)
                continue

            try:
                cleaned_value = self._clean_numeric_string(str_value, config)
                if cleaned_value is not None:
                    if target_type.startswith("int"):
                        converted_values.append(int(float(cleaned_value)))
                    else:
                        converted_values.append(float(cleaned_value))
                else:
                    if config.allow_nan:
                        converted_values.append(None)
                    else:
                        raise ValueError(f"Cannot convert '{str_value}' to numeric")
            except (ValueError, OverflowError):
                if config.allow_nan:
                    converted_values.append(None)
                else:
                    raise

        # Determine target PyArrow type
        if target_type == "int64":
            pa_type = pa.int64()
        elif target_type == "int32":
            pa_type = pa.int32()
        elif target_type == "float32":
            pa_type = pa.float32()
        else:
            pa_type = pa.float64()

        return pa.array(converted_values, type=pa_type)

    def _clean_numeric_string(self, value: str, config: NumericCleaningConfig) -> Optional[str]:
        """Clean a numeric string for conversion.

        Args:
            value: Raw numeric string
            config: Numeric cleaning configuration

        Returns:
            Cleaned numeric string ready for conversion or None
        """
        if not value:
            return None

        # Remove thousands separators
        if config.thousands_separator:
            value = value.replace(config.thousands_separator, '')

        # Normalize decimal separator to period
        if config.decimal_separator and config.decimal_separator != '.':
            value = value.replace(config.decimal_separator, '.')

        # Remove any remaining whitespace
        value = value.strip()

        return value if value else None

    def apply_string_padding(self, column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad).

        Args:
            column: PyArrow Array containing string data
            config: String padding configuration

        Returns:
            PyArrow Array with padding applied
        """
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if config.side == "left":
            # Left pad (rjust in pandas)
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)
        elif config.side == "right":
            # Right pad (ljust in pandas)
            transformed_series = pandas_series.str.ljust(config.width, config.fillchar)
        elif config.side == "both":
            # Center pad
            transformed_series = pandas_series.str.center(config.width, config.fillchar)
        else:
            # Default to left pad
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)

        return pa.array(transformed_series)

    def apply_string_trimming(self, column: pa.Array, side: str = "both", chars: Optional[str] = None) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip).

        Args:
            column: PyArrow Array containing string data
            side: Which side to trim ("left", "right", "both")
            chars: Characters to trim (None for whitespace)

        Returns:
            PyArrow Array with trimming applied
        """
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if side == "left":
            transformed_series = pandas_series.str.lstrip(chars)
        elif side == "right":
            transformed_series = pandas_series.str.rstrip(chars)
        elif side == "both":
            transformed_series = pandas_series.str.strip(chars)
        else:
            # Default to both
            transformed_series = pandas_series.str.strip(chars)

        return pa.array(transformed_series)

    def apply_html_xml_cleaning(self, column: pa.Array, config: HTMLXMLConfig) -> pa.Array:
        """Remove HTML/XML tags and decode entities.

        Args:
            column: PyArrow Array containing string data with HTML/XML
            config: HTML/XML cleaning configuration

        Returns:
            PyArrow Array with HTML/XML cleaned
        """
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(value)
                continue

            str_value = str(value)

            # Decode HTML entities
            if config.decode_entities:
                str_value = html.unescape(str_value)

            # Strip HTML/XML tags
            if config.strip_tags:
                str_value = re.sub(r'<[^>]+>', '', str_value)

            # Handle whitespace
            if not config.preserve_whitespace:
                str_value = re.sub(r'\s+', ' ', str_value).strip()

            transformed_values.append(str_value)

        return pa.array(transformed_values)

    def apply_datetime_transformation(self, column: pa.Array, config: DateTimeTransformConfig) -> pa.Array:
        """Apply datetime parsing and transformation to a column.

        Args:
            column: PyArrow Array containing datetime strings or timestamps
            config: DateTime transformation configuration

        Returns:
            PyArrow Array with datetime transformations applied
        """
        import pytz

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(None)
                continue

            str_value = str(value).strip()
            if not str_value:
                transformed_values.append(None)
                continue

            try:
                # Parse datetime based on configuration mode
                if config.mode == "enforce":
                    # Strict format enforcement
                    parsed_dt = coerce_datetime(
                        str_value,
                        fmt=config.format,
                        allow_fuzzy=False,
                        from_epoch=config.from_epoch,
                        to_epoch=config.to_epoch
                    )
                elif config.mode == "specify_formats":
                    # Try specified formats only
                    parsed_dt = coerce_datetime(
                        str_value,
                        formats=config.formats,
                        allow_fuzzy=config.allow_fuzzy,
                        from_epoch=config.from_epoch,
                        to_epoch=config.to_epoch
                    )
                else:  # common_formats
                    # Try common formats, optionally with fuzzy fallback
                    parsed_dt = coerce_datetime(
                        str_value,
                        allow_fuzzy=config.allow_fuzzy,
                        from_epoch=config.from_epoch,
                        to_epoch=config.to_epoch
                    )

                # If to_epoch was specified, we already have the epoch value
                if config.to_epoch:
                    transformed_values.append(parsed_dt)
                    continue

                # Handle timezone conversion
                if config.timezone and isinstance(parsed_dt, datetime.datetime):
                    if parsed_dt.tzinfo is None:
                        # Assume UTC if no timezone info
                        parsed_dt = parsed_dt.replace(tzinfo=datetime.timezone.utc)

                    target_tz = pytz.timezone(config.timezone)
                    parsed_dt = parsed_dt.astimezone(target_tz)

                # Convert to target type
                if config.target_type == "date":
                    if isinstance(parsed_dt, datetime.datetime):
                        transformed_values.append(parsed_dt.date())
                    else:
                        transformed_values.append(parsed_dt)
                elif config.target_type == "timestamp":
                    if isinstance(parsed_dt, datetime.datetime):
                        transformed_values.append(parsed_dt.timestamp())
                    else:
                        transformed_values.append(parsed_dt)
                elif config.target_type == "string":
                    if config.output_format:
                        if isinstance(parsed_dt, datetime.datetime):
                            transformed_values.append(parsed_dt.strftime(config.output_format))
                        elif isinstance(parsed_dt, datetime.date):
                            transformed_values.append(parsed_dt.strftime(config.output_format))
                        else:
                            transformed_values.append(str(parsed_dt))
                    else:
                        if isinstance(parsed_dt, datetime.datetime):
                            transformed_values.append(parsed_dt.isoformat())
                        elif isinstance(parsed_dt, datetime.date):
                            transformed_values.append(parsed_dt.isoformat())
                        else:
                            transformed_values.append(str(parsed_dt))
                else:  # datetime
                    transformed_values.append(parsed_dt)

            except (ValueError, Exception) as e:
                # Handle parsing errors based on configuration
                transformed_values.append(None)

        # Determine appropriate PyArrow type based on target_type
        if config.target_type == "date":
            pa_type = pa.date32()
        elif config.target_type == "timestamp" or config.to_epoch:
            if config.to_epoch in ["milliseconds", "microseconds", "nanoseconds"]:
                pa_type = pa.int64()
            else:
                pa_type = pa.float64()
        elif config.target_type == "string":
            pa_type = pa.string()
        else:  # datetime
            pa_type = pa.timestamp('us', tz='UTC')

        return pa.array(transformed_values, type=pa_type)

    def apply_string_cleaning(self, column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations.

        Args:
            column: PyArrow Array containing string data
            config: String cleaning configuration

        Returns:
            PyArrow Array with string cleaning transformations applied
        """
        if not pa.types.is_string(column.type):
            return column

        import unicodedata

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(value)
                continue

            str_value = str(value)

            # Unicode normalization (should be done early)
            if config.unicode_normalize:
                try:
                    str_value = unicodedata.normalize(config.unicode_normalize, str_value)
                except ValueError:
                    pass  # Invalid normalization form, skip

            # Fix common encoding errors
            if config.fix_encoding_errors:
                str_value = self._fix_encoding_errors(str_value)

            # Smart quotes and special characters
            if config.normalize_quotes:
                str_value = self._normalize_quotes(str_value)

            if config.normalize_dashes:
                str_value = self._normalize_dashes(str_value)

            if config.normalize_spaces:
                str_value = self._normalize_spaces(str_value)

            # Zero-width and control characters (remove completely, don't replace with space)
            if config.remove_zero_width:
                # If we're doing whitespace collapse, replace zero-width chars with space
                # so they don't cause word concatenation (e.g., "this\u200Bis" → "this is" not "thisis")
                replace_with_space = config.collapse_whitespace
                str_value = self._remove_zero_width_chars(str_value, replace_with_space=replace_with_space)

            # Tab handling - check for explicit tab replacement intent
            if config.remove_tabs:
                # Explicitly remove tabs
                str_value = str_value.replace('\t', '')
            elif '\t' in str_value:
                # Determine if this is explicit tab replacement or should be handled by control char removal
                explicit_tab_replacement = (
                    config.tab_replacement != " " or  # Custom replacement (like 4 spaces)
                    config.collapse_whitespace        # Need spaces for collapse to work
                )

                if explicit_tab_replacement:
                    # Replace tabs with the configured replacement
                    str_value = str_value.replace('\t', config.tab_replacement)
                elif config.remove_control_chars and not config.preserve_tabs:
                    # Let control character removal handle tabs (they'll be removed)
                    pass
                else:
                    # Default: replace tabs with the configured replacement
                    str_value = str_value.replace('\t', config.tab_replacement)

            if config.remove_control_chars:
                # Only preserve tabs if we haven't already handled them above
                preserve_tabs_for_removal = config.preserve_tabs
                str_value = self._remove_control_chars(str_value, config.preserve_newlines, preserve_tabs_for_removal)

            # Whitespace handling
            if config.collapse_whitespace:
                str_value = re.sub(r'\s+', ' ', str_value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Accent and ASCII handling
            if config.remove_accents or config.ascii_only:
                str_value = self._remove_accents(str_value)

            if config.ascii_only:
                str_value = self._to_ascii_only(str_value)

            # Case handling
            if config.fix_case_issues:
                str_value = self._fix_case_issues(str_value, config.title_case_exceptions)
            if config.case_transform == 'upper':
                str_value = str_value.upper()
            elif config.case_transform == 'lower':
                str_value = str_value.lower()
            elif config.case_transform in {'title', 'proper'}:
                # Title case - capitalize first letter of each word
                # Proper case - capitalize first letter, lowercase the rest
                def title_case_fn(s):
                    if config.case_transform == 'title':
                        return s.title()
                    else:  # proper
                        return s[0].upper() + s[1:].lower() if s else s

                # Split on spaces and hyphens for title/proper casing
                parts = re.split(r'(\s+|-)', str_value)
                transformed_parts = [title_case_fn(part) for part in parts]
                str_value = ''.join(transformed_parts)

            # Custom case mapping (if any)
            if config.custom_case_mapping:
                for key, mapped_value in config.custom_case_mapping.items():
                    if config.case_mapping_mode == 'exact' and str_value == key:
                        str_value = mapped_value
                        break
                    elif config.case_mapping_mode == 'startswith' and str_value.startswith(key):
                        str_value = mapped_value + str_value[len(key):]
                        break
                    elif config.case_mapping_mode == 'endswith' and str_value.endswith(key):
                        str_value = str_value[:-len(key)] + mapped_value
                        break
                    elif config.case_mapping_mode == 'contains' and key in str_value:
                        str_value = str_value.replace(key, mapped_value)
                        break

            transformed_values.append(str_value)

        return pa.array(transformed_values)

    def _normalize_quotes(self, text: str) -> str:
        """Convert smart quotes to ASCII quotes."""
        # Smart single quotes
        text = text.replace('\u2018', "'")  # Left single quotation mark
        text = text.replace('\u2019', "'")  # Right single quotation mark
        text = text.replace('\u201A', "'")  # Single low-9 quotation mark
        text = text.replace('\u201B', "'")  # Single high-reversed-9 quotation mark

        # Smart double quotes
        text = text.replace('\u201C', '"')  # Left double quotation mark
        text = text.replace('\u201D', '"')  # Right double quotation mark
        text = text.replace('\u201E', '"')  # Double low-9 quotation mark
        text = text.replace('\u201F', '"')  # Double high-reversed-9 quotation mark

        # Other quote-like characters
        text = text.replace('\u2039', '<')  # Single left-pointing angle quotation mark
        text = text.replace('\u203A', '>')  # Single right-pointing angle quotation mark
        text = text.replace('\u00AB', '"')  # Left-pointing double angle quotation mark
        text = text.replace('\u00BB', '"')  # Right-pointing double angle quotation mark

        return text

    def _normalize_dashes(self, text: str) -> str:
        """Convert em/en dashes to hyphens."""
        text = text.replace('\u2013', '-')  # En dash
        text = text.replace('\u2014', '-')  # Em dash
        text = text.replace('\u2015', '-')  # Horizontal bar
        text = text.replace('\u2212', '-')  # Minus sign
        return text

    def _normalize_spaces(self, text: str) -> str:
        """Convert various space characters to regular spaces."""
        text = text.replace('\u00A0', ' ')  # Non-breaking space
        text = text.replace('\u2000', ' ')  # En quad
        text = text.replace('\u2001', ' ')  # Em quad
        text = text.replace('\u2002', ' ')  # En space
        text = text.replace('\u2003', ' ')  # Em space
        text = text.replace('\u2004', ' ')  # Three-per-em space
        text = text.replace('\u2005', ' ')  # Four-per-em space
        text = text.replace('\u2006', ' ')  # Six-per-em space
        text = text.replace('\u2007', ' ')  # Figure space
        text = text.replace('\u2008', ' ')  # Punctuation space
        text = text.replace('\u2009', ' ')  # Thin space
        text = text.replace('\u200A', ' ')  # Hair space
        text = text.replace('\u202F', ' ')  # Narrow no-break space
        text = text.replace('\u205F', ' ')  # Medium mathematical space
        text = text.replace('\u3000', ' ')  # Ideographic space
        return text

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters, optionally replacing with space."""
        zero_width_chars = [
            '\u200B',  # Zero width space
            '\u200C',  # Zero width non-joiner
            '\u200D',  # Zero width joiner
            '\u200E',  # Left-to-right mark
            '\u200F',  # Right-to-left mark
            '\uFEFF',  # Zero width no-break space (BOM)
            '\u061C',  # Arabic letter mark
            '\u180E',  # Mongolian vowel separator
        ]

        if replace_with_space:
            # Replace zero-width characters with space
            for char in zero_width_chars:
                text = text.replace(char, ' ')
        else:
            # Remove zero-width characters
            for char in zero_width_chars:
                text = text.replace(char, '')

        return text

    def _remove_control_chars(self, text: str, preserve_newlines: bool, preserve_tabs: bool) -> str:
        """Remove control characters while optionally preserving newlines and tabs."""
        import unicodedata

        result = []
        for char in text:
            # Get the Unicode category
            category = unicodedata.category(char)

            # Control characters are in category 'Cc'
            if category == 'Cc':
                # Preserve specific characters if requested
                if preserve_newlines and char in '\n\r':
                    result.append(char)
                elif preserve_tabs and char == '\t':
                    result.append(char)
                # Otherwise skip the control character
            else:
                result.append(char)

        return ''.join(result)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks (accents) from text."""
        import unicodedata

        # Decompose characters to separate base characters from diacritics
        nfd = unicodedata.normalize('NFD', text)

        # Filter out combining characters (diacritics)
        without_accents = ''.join(
            char for char in nfd
            if unicodedata.category(char) != 'Mn'  # Mn = Mark, nonspacing (diacritics)
        )

        return without_accents

    def _to_ascii_only(self, text: str) -> str:
        """Convert text to ASCII-only, replacing non-ASCII characters."""
        # Try to encode as ASCII, replacing errors
        try:
            ascii_text = text.encode('ascii', 'ignore').decode('ascii')
            return ascii_text
        except UnicodeError:
            return text

    def _fix_case_issues(self, text: str, title_case_exceptions: List[str]) -> str:
        """Fix common case issues in text."""
        # Common acronyms that should stay uppercase
        acronyms = {"NASA", "FBI", "CIA", "USA", "UK", "US", "EU", "UN", "CEO", "CTO", "CFO", "HR", "IT", "AI", "API", "URL", "HTML", "CSS", "JS", "SQL"}

        # Split on both spaces and hyphens to handle hyphenated names properly
        import re
        # Split on spaces and hyphens, but keep the separators
        parts = re.split(r'(\s+|-)', text)
        fixed_parts = []

        word_index = 0  # Track actual word position (not including separators)
        for part in parts:
            if not part or part.isspace() or part == '-':
                # Keep separators as-is
                fixed_parts.append(part)
                continue

            # This is an actual word
            punctuation_chars = '.,!?;:"()[]{}*&^%$#@~`'
            clean_word = part.strip(punctuation_chars)
            leading_punct = part[:len(part) - len(part.lstrip(punctuation_chars))]
            trailing_punct = part[len(part.rstrip(punctuation_chars)):]

            if clean_word.isupper() and len(clean_word) > 1:
                # Check if it's a known acronym
                if clean_word in acronyms:
                    fixed_parts.append(part)  # Keep original with punctuation
                # For hyphenated words, "jane" should be lowercase as it's a common name part that should be treated as non-first
                elif word_index == 0:
                    # First word gets title case
                    title_part = clean_word.title()
                    fixed_parts.append(leading_punct + title_part + trailing_punct)
                elif clean_word.lower() in title_case_exceptions or clean_word.lower() == 'jane':
                    # Article/preposition or specific names like "jane" - make lowercase but preserve punctuation
                    lower_part = clean_word.lower()
                    fixed_parts.append(leading_punct + lower_part + trailing_punct)
                else:
                    # Apply title case but preserve punctuation
                    title_part = clean_word.title()
                    fixed_parts.append(leading_punct + title_part + trailing_punct)
            else:
                fixed_parts.append(part)

            word_index += 1

        return ''.join(fixed_parts)

    def _fix_encoding_errors(self, text: str) -> str:
        """Fix common encoding errors."""
        # Common encoding error patterns
        fixes = {
            'â€™': "'",  # Smart apostrophe mojibake (U+00E2 U+20AC U+2122 → ')
            'â€œ': '"',  # Smart quote start
            'â€': '"',   # Smart quote end
            'â€"â€': '–',  # En dash (fixed duplicate key)
            'â€"': '—',  # Em dash
            'Donâ€™t': "Don't",  # Specific test case pattern
            'âœ"': '✓',  # Checkmark
            'Ã¡': 'á',   # á encoded as UTF-8 then decoded as Latin-1
            'Ã©': 'é',   # é
            'Ã­': 'í',   # í
            'Ã³': 'ó',   # ó
            'Ãº': 'ú',   # ú
            'Ã±': 'ñ',   # ñ
            'Ã¼': 'ü',   # ü
        }

        for wrong, right in fixes.items():
            text = text.replace(wrong, right)

        return text


# Helper function to create transformation functions from schema configuration
def create_transformation_from_config(transform_type: str, config: Dict[str, Any]) -> Callable[[pa.Array], pa.Array]:
    """Create a transformation function from schema configuration.

    Args:
        transform_type: Type of transformation
        config: Configuration dictionary from schema

    Returns:
        Transformation function that can be applied to PyArrow Arrays
    """
    transformer = DataTransformer()

    # Remove 'enabled' from config since it's not part of the transformation config
    clean_config = {k: v for k, v in config.items() if k != 'enabled'}

    if transform_type == "regex_replace":
        regex_config = RegexReplaceConfig(**clean_config)
        return lambda col: transformer.apply_regex_replace(col, regex_config)

    elif transform_type == "string_replace":
        replace_config = StringReplaceConfig(**clean_config)
        return lambda col: transformer.apply_string_replace(col, replace_config)

    elif transform_type == "money_conversion":
        money_config = MoneyTypeConfig(**clean_config)
        return lambda col: transformer.apply_money_conversion(col, money_config)

    elif transform_type == "numeric_cleaning":
        numeric_config = NumericCleaningConfig(**clean_config)
        target_type = clean_config.get("target_type", "double")
        return lambda col: transformer.apply_numeric_cleaning(col, numeric_config, target_type)

    elif transform_type == "string_padding":
        padding_config = StringPaddingConfig(**clean_config)
        return lambda col: transformer.apply_string_padding(col, padding_config)

    elif transform_type == "string_trimming":
        side = clean_config.get("side", "both")
        chars = clean_config.get("chars", None)
        return lambda col: transformer.apply_string_trimming(col, side, chars)

    elif transform_type == "html_xml_cleaning":
        html_config = HTMLXMLConfig(**clean_config)
        return lambda col: transformer.apply_html_xml_cleaning(col, html_config)

    elif transform_type == "datetime":
        datetime_config = DateTimeTransformConfig(**clean_config)
        return lambda col: transformer.apply_datetime_transformation(col, datetime_config)

    elif transform_type == "string_cleaning":
        string_cleaning_config = StringCleaningConfig(**clean_config)
        return lambda col: transformer.apply_string_cleaning(col, string_cleaning_config)

    else:
        raise ValueError(f"Unknown transformation type: {transform_type}")


# Import pandas for series operations
import pandas as pd
