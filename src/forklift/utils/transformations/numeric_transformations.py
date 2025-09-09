"""Numeric transformation utilities.

This module provides money conversion, numeric cleaning, and related operations.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import pyarrow as pa
import pandas as pd
from typing import Optional

from .configs import MoneyTypeConfig, NumericCleaningConfig


class NumericTransformer:
    """Specialized transformer for numeric operations."""

    def apply_money_conversion(self, column: pa.Array, config: MoneyTypeConfig) -> pa.Array:
        """Convert money strings to decimal values."""
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
        """Clean a money string and convert to decimal."""
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
            value = value.replace(config.thousands_separator, '')
            if config.decimal_separator != '.':
                value = value.replace(config.decimal_separator, '.')

        value = value.strip()

        try:
            decimal_value = Decimal(value)
            if is_negative:
                decimal_value = -decimal_value
            return decimal_value
        except (ValueError, InvalidOperation):
            return None

    def apply_numeric_cleaning(self, column: pa.Array, config: NumericCleaningConfig, target_type: str = "double") -> pa.Array:
        """Clean numeric fields with configurable separators and NaN handling."""
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
        """Clean a numeric string for conversion."""
        if not value:
            return None

        # Remove thousands separators
        if config.thousands_separator:
            value = value.replace(config.thousands_separator, '')

        # Normalize decimal separator to period
        if config.decimal_separator and config.decimal_separator != '.':
            value = value.replace(config.decimal_separator, '.')

        value = value.strip()
        return value if value else None
