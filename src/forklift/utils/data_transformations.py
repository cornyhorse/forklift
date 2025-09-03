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

import re
import html
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
import datetime

import pyarrow as pa
import pyarrow.compute as pc

from .date_parser import (
    coerce_datetime,
    coerce_date,
    COMMON_DATE_FORMATS,
    COMMON_DATETIME_FORMATS
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

    else:
        raise ValueError(f"Unknown transformation type: {transform_type}")


# Import pandas for series operations
import pandas as pd
