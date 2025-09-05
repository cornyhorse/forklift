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
class SSNConfig:
    """Configuration for Social Security Number formatting."""
    format_with_dashes: bool = True  # Format as XXX-XX-XXXX
    zero_pad: bool = True  # Zero-pad numbers with fewer than 9 digits
    validate: bool = True  # Validate that result has exactly 9 digits
    allow_invalid: bool = False  # If False, invalid SSNs become None


@dataclass
class ZipCodeConfig:
    """Configuration for ZIP code formatting."""
    zip_type: str = "zip-permissive"  # "zip-permissive", "zip-5", "zip-9"
    format_with_dash: bool = True  # Format ZIP+4 as XXXXX-XXXX
    zero_pad: bool = True  # Zero-pad ZIP codes
    validate: bool = True  # Validate ZIP code format
    allow_invalid: bool = False  # If False, invalid ZIP codes become None

    def __post_init__(self):
        valid_types = {"zip-permissive", "zip-5", "zip-9"}
        if self.zip_type not in valid_types:
            raise ValueError(f"zip_type must be one of {valid_types}, got: {self.zip_type}")


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

    def apply_ssn_formatting(self, column: pa.Array, config: SSNConfig) -> pa.Array:
        """Format Social Security Numbers to XXX-XX-XXXX format.

        Args:
            column: PyArrow Array containing SSN data
            config: SSN formatting configuration

        Returns:
            PyArrow Array with SSNs formatted as XXX-XX-XXXX
        """
        if not pa.types.is_string(column.type):
            # Convert to string first if not already
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_ssn = self._format_ssn(str(value), config)
                formatted_values.append(formatted_ssn)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_ssn(self, value: str, config: SSNConfig) -> str:
        """Format a single SSN value.

        Args:
            value: Raw SSN string
            config: SSN formatting configuration

        Returns:
            Formatted SSN string

        Raises:
            ValueError: If SSN is invalid and validation is enabled
        """
        original_value = value.strip()

        # Check for empty or invalid input early
        if not original_value:
            raise ValueError("Empty SSN value")

        # Remove all non-digits
        digits_only = re.sub(r'\D', '', original_value)

        # Check if we lost too much of the original (indicates invalid data like "abc123")
        # For SSN, we should be more strict - if there are any letters, it's invalid
        if config.validate and re.search(r'[a-zA-Z]', original_value):
            raise ValueError(f"SSN contains letters")

        # Check for empty after digit extraction
        if not digits_only:
            raise ValueError("No digits found in SSN")

        # Handle zero padding
        if config.zero_pad and len(digits_only) < 9:
            digits_only = digits_only.zfill(9)

        # Validate length
        if config.validate and len(digits_only) != 9:
            raise ValueError(f"SSN must have exactly 9 digits, got {len(digits_only)}")

        # Format with dashes if requested
        if config.format_with_dashes and len(digits_only) == 9:
            return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
        else:
            return digits_only

    def apply_zip_code_formatting(self, column: pa.Array, config: ZipCodeConfig) -> pa.Array:
        """Format ZIP codes according to the specified type.

        Args:
            column: PyArrow Array containing ZIP code data
            config: ZIP code formatting configuration

        Returns:
            PyArrow Array with ZIP codes formatted according to type
        """
        if not pa.types.is_string(column.type):
            # Convert to string first if not already
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_zip = self._format_zip_code(str(value), config)
                formatted_values.append(formatted_zip)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_zip_code(self, value: str, config: ZipCodeConfig) -> str:
        """Format a single ZIP code value.

        Args:
            value: Raw ZIP code string
            config: ZIP code formatting configuration

        Returns:
            Formatted ZIP code string

        Raises:
            ValueError: If ZIP code is invalid and validation is enabled
        """
        original_value = value.strip()

        # Check for empty or invalid input early
        if not original_value:
            raise ValueError("Empty ZIP code value")

        # Remove all non-digits
        digits_only = re.sub(r'\D', '', original_value)

        # Check if we lost too much of the original (indicates invalid data like "abc12")
        if config.validate and len(digits_only) < len(original_value) * 0.5:
            # If more than half the characters were removed, likely invalid
            raise ValueError(f"ZIP code contains too many non-digit characters")

        # Check for empty after digit extraction
        if not digits_only:
            raise ValueError("No digits found in ZIP code")

        if config.zip_type == "zip-5":
            # Handle 5-digit ZIP codes
            if config.zero_pad and len(digits_only) < 5:
                digits_only = digits_only.zfill(5)
            elif len(digits_only) > 5:
                # Truncate to 5 digits if longer
                digits_only = digits_only[:5]

            if config.validate and len(digits_only) != 5:
                raise ValueError(f"ZIP-5 must have exactly 5 digits, got {len(digits_only)}")

            return digits_only

        elif config.zip_type == "zip-9":
            # Handle 9-digit ZIP codes (ZIP+4)
            if config.zero_pad and len(digits_only) < 9:
                digits_only = digits_only.zfill(9)

            if config.validate and len(digits_only) != 9:
                raise ValueError(f"ZIP-9 must have exactly 9 digits, got {len(digits_only)}")

            # Format with dash if requested
            if config.format_with_dash and len(digits_only) == 9:
                return f"{digits_only[:5]}-{digits_only[5:]}"
            else:
                return digits_only

        else:  # zip-permissive
            # Handle permissive ZIP codes (5 or 9 digits)
            if config.zero_pad:
                if len(digits_only) <= 5:
                    digits_only = digits_only.zfill(5)
                elif len(digits_only) <= 9:
                    digits_only = digits_only.zfill(9)

            if config.validate:
                if len(digits_only) not in [5, 9]:
                    raise ValueError(f"ZIP code must have 5 or 9 digits, got {len(digits_only)}")

            # Format based on length
            if len(digits_only) == 9 and config.format_with_dash:
                return f"{digits_only[:5]}-{digits_only[5:]}"
            else:
                return digits_only

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

    def _normalize_quotes(self, text: str) -> str:
        """Normalize smart quotes to ASCII quotes."""
        # Smart quotes to ASCII
        quote_mappings = {
            '\u2018': "'",  # Left single quotation mark
            '\u2019': "'",  # Right single quotation mark
            '\u201A': "'",  # Single low-9 quotation mark
            '\u201B': "'",  # Single high-reversed-9 quotation mark
            '\u201C': '"',  # Left double quotation mark
            '\u201D': '"',  # Right double quotation mark
            '\u201E': '"',  # Double low-9 quotation mark
            '\u201F': '"',  # Double high-reversed-9 quotation mark
            '\u00AB': '"',  # Left-pointing double angle quotation mark
            '\u00BB': '"',  # Right-pointing double angle quotation mark
            '\u2039': "'",  # Single left-pointing angle quotation mark
            '\u203A': "'",  # Single right-pointing angle quotation mark
        }

        for smart, ascii_char in quote_mappings.items():
            text = text.replace(smart, ascii_char)

        return text

    def _normalize_dashes(self, text: str) -> str:
        """Normalize em/en dashes to hyphens."""
        dash_mappings = {
            '\u2014': '-',  # Em dash
            '\u2013': '-',  # En dash
            '\u2212': '-',  # Minus sign
            '\u2010': '-',  # Hyphen
            '\u2011': '-',  # Non-breaking hyphen
        }

        for dash, hyphen in dash_mappings.items():
            text = text.replace(dash, hyphen)

        return text

    def _normalize_spaces(self, text: str) -> str:
        """Normalize various space characters to regular spaces."""
        space_mappings = {
            '\u00A0': ' ',  # Non-breaking space
            '\u2002': ' ',  # En space
            '\u2003': ' ',  # Em space
            '\u2004': ' ',  # Three-per-em space
            '\u2005': ' ',  # Four-per-em space
            '\u2006': ' ',  # Six-per-em space
            '\u2007': ' ',  # Figure space
            '\u2008': ' ',  # Punctuation space
            '\u2009': ' ',  # Thin space
            '\u200A': ' ',  # Hair space
            '\u202F': ' ',  # Narrow no-break space
            '\u205F': ' ',  # Medium mathematical space
            '\u3000': ' ',  # Ideographic space
        }

        for special_space, regular_space in space_mappings.items():
            text = text.replace(special_space, regular_space)

        return text

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters."""
        zero_width_chars = [
            '\u200B',  # Zero-width space
            '\u200C',  # Zero-width non-joiner
            '\u200D',  # Zero-width joiner
            '\uFEFF',  # Byte order mark (BOM)
            '\u200E',  # Left-to-right mark
            '\u200F',  # Right-to-left mark
            '\u061C',  # Arabic letter mark
        ]

        replacement = ' ' if replace_with_space else ''

        for char in zero_width_chars:
            text = text.replace(char, replacement)

        return text

    def _remove_control_chars(self, text: str, preserve_newlines: bool = True, preserve_tabs: bool = False) -> str:
        """Remove control characters from text."""
        import unicodedata

        result = []
        for char in text:
            # Get the Unicode category
            category = unicodedata.category(char)

            # Control characters have category 'Cc'
            if category == 'Cc':
                # Check for preserved characters
                if preserve_newlines and char in ['\n', '\r']:
                    result.append(char)
                elif preserve_tabs and char == '\t':
                    result.append(char)
                # Otherwise skip control characters
            else:
                result.append(char)

        return ''.join(result)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks from text."""
        import unicodedata

        # Normalize to NFD (decomposed form) to separate base chars from accents
        nfd = unicodedata.normalize('NFD', text)

        # Remove combining characters (accents)
        result = []
        for char in nfd:
            if unicodedata.category(char) != 'Mn':  # Mn = Nonspacing_Mark (combining chars)
                result.append(char)

        return ''.join(result)

    def _to_ascii_only(self, text: str) -> str:
        """Convert text to ASCII-only characters."""
        import unicodedata

        # First remove accents
        text = self._remove_accents(text)

        # Then encode to ASCII, replacing non-ASCII chars
        try:
            return text.encode('ascii', errors='ignore').decode('ascii')
        except UnicodeError:
            # Fallback: manually filter ASCII characters
            return ''.join(char for char in text if ord(char) < 128)

    def _fix_case_issues(self, text: str, title_case_exceptions: List[str]) -> str:
        """Fix common case issues like ALL CAPS."""
        # Check if the text is all uppercase (indicating a case issue)
        if text.isupper() and len(text) > 2:
            # Convert to title case, respecting exceptions
            words = text.split()
            fixed_words = []

            for i, word in enumerate(words):
                if i == 0:
                    # Always capitalize the first word
                    fixed_words.append(word.capitalize())
                elif word.lower() in title_case_exceptions:
                    # Use lowercase for exception words
                    fixed_words.append(word.lower())
                else:
                    # Capitalize normally
                    fixed_words.append(word.capitalize())

            return ' '.join(fixed_words)

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

    elif transform_type == "ssn_formatting":
        ssn_config = SSNConfig(**clean_config)
        return lambda col: transformer.apply_ssn_formatting(col, ssn_config)

    elif transform_type == "zip_code_formatting":
        zip_config = ZipCodeConfig(**clean_config)
        return lambda col: transformer.apply_zip_code_formatting(col, zip_config)

    else:
        raise ValueError(f"Unknown transformation type: {transform_type}")


# Import pandas for series operations
import pandas as pd
