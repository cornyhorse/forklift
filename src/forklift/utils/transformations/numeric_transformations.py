"""Numeric transformation utilities.

This module provides money conversion, numeric cleaning, and related operations.

Conversion rules:

* Values are parsed with :class:`decimal.Decimal`, so integers never pass through ``float``
  (``"9007199254740993"`` stays exact).
* Integer targets (``int8`` ... ``uint64``) only accept integral values: ``"3.0"`` and ``"1e3"``
  become 3 and 1000, ``"3.9"`` becomes NULL. Values outside the target's range are NULL.
* ``NaN`` / ``Infinity`` text and values that overflow the target type become NULL (they never
  abort the batch). With ``allow_nan=False`` every such value raises ``ValueError`` instead,
  naming the row but never the cell content.
"""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Optional

import pyarrow as pa

from ._arrow_utils import is_string_like
from .configs import MoneyTypeConfig, NumericCleaningConfig

_FLOAT32_MAX = 3.4028234663852886e38

_INTEGER_TYPES = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
}
_FLOAT_TYPES = {
    "float32": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "float": pa.float64(),
}
_TYPE_ALIASES = {"int": "int64", "integer": "int64", "bigint": "int64"}


def resolve_numeric_target_type(target_type: str) -> pa.DataType:
    """Map a ``target_type`` name (``int16``, ``double``...) to its Arrow type.

    Raises:
        ValueError: for unknown names (instead of silently producing doubles)
    """
    name = _TYPE_ALIASES.get(str(target_type).strip().lower(), str(target_type).strip().lower())
    if name in _INTEGER_TYPES:
        return _INTEGER_TYPES[name]
    if name in _FLOAT_TYPES:
        return _FLOAT_TYPES[name]
    supported = sorted(set(_INTEGER_TYPES) | set(_FLOAT_TYPES) | set(_TYPE_ALIASES))
    raise ValueError(f"Unsupported numeric target_type '{target_type}'. Use one of {supported}")


def _integer_bounds(arrow_type: pa.DataType):
    bits = arrow_type.bit_width
    if pa.types.is_unsigned_integer(arrow_type):
        return 0, 2**bits - 1
    return -(2 ** (bits - 1)), 2 ** (bits - 1) - 1


def _normalize_separators(value: str, thousands: Optional[str], decimal: Optional[str]):
    """Drop thousands separators and turn the decimal separator into ``.``.

    Each step is independent: a configuration with an empty thousands separator still converts
    its decimal separator. When the decimal separator is not ``.``, a literal ``.`` in the text is
    not a number and yields None (it would otherwise be silently read as a decimal point).
    """
    if thousands:
        value = value.replace(thousands, "")
    if decimal and decimal != ".":
        if "." in value:
            return None
        value = value.replace(decimal, ".")
    return value


class NumericTransformer:
    """Specialized transformer for numeric operations."""

    def apply_money_conversion(self, column: pa.Array, config: MoneyTypeConfig) -> pa.Array:
        """Convert money strings to decimal values (as float64; NULL for unparseable values)."""
        if not is_string_like(column.type):
            return column

        converted_values = []

        for value in column.to_pylist():
            if value is None:
                converted_values.append(None)
                continue

            # _clean_money_string turns every unparseable value into None; float() of the finite
            # Decimal it returns cannot fail (it is inf when out of float range)
            cleaned_value = self._clean_money_string(value, config)
            if cleaned_value is None:
                converted_values.append(None)
                continue
            number = float(cleaned_value)
            converted_values.append(number if math.isfinite(number) else None)

        return pa.array(converted_values, type=pa.float64())

    def _clean_money_string(self, value: str, config: MoneyTypeConfig) -> Optional[Decimal]:
        """Clean a money string and convert to decimal (None if it is not a finite number)."""
        if config.strip_whitespace:
            value = value.strip()

        if not value:
            return None

        # Check for parentheses indicating negative
        is_negative = False
        if config.parentheses_negative and value.startswith("(") and value.endswith(")"):
            is_negative = True
            value = value[1:-1].strip()

        # Remove currency symbols
        for symbol in config.currency_symbols:
            value = value.replace(symbol, "")

        # Handle thousands and decimal separators
        value = _normalize_separators(value, config.thousands_separator, config.decimal_separator)
        if value is None:
            return None

        value = value.strip()

        try:
            decimal_value = Decimal(value)
        except (ValueError, ArithmeticError):  # InvalidOperation is an ArithmeticError
            return None
        if not decimal_value.is_finite():  # "NaN", "Infinity", "sNaN"
            return None
        # copy_negate is exact; unary minus would round to the context precision (28 digits)
        return decimal_value.copy_negate() if is_negative else decimal_value

    def apply_numeric_cleaning(
        self, column: pa.Array, config: NumericCleaningConfig, target_type: str = "double"
    ) -> pa.Array:
        """Clean numeric fields with configurable separators and NaN handling.

        The result has exactly the Arrow type named by ``target_type`` (``int16`` gives int16,
        not int64 or double). See the module docstring for the conversion rules.
        """
        pa_type = resolve_numeric_target_type(target_type)
        converted_values = []

        for row, value in enumerate(column.to_pylist()):
            if value is None:
                converted_values.append(None)
                continue

            str_value = value if isinstance(value, str) else str(value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Check if value should be treated as NaN
            if config.allow_nan and str_value in config.nan_values:
                converted_values.append(None)
                continue

            try:
                cleaned_value = self._clean_numeric_string(str_value, config)
                if cleaned_value is None:
                    raise ValueError("empty or malformed number")
                converted_values.append(self._to_number(cleaned_value, pa_type))
            except (ValueError, ArithmeticError):  # ArithmeticError: InvalidOperation/Overflow
                if config.allow_nan:
                    converted_values.append(None)
                else:
                    # Never echo the cell content: it can be personal data
                    raise ValueError(
                        f"Cannot convert value at row {row} to {target_type}"
                    ) from None

        return pa.array(converted_values, type=pa_type)

    @staticmethod
    def _to_number(cleaned_value: str, pa_type: pa.DataType):
        """Convert cleaned text to a Python number for ``pa_type``; ValueError if impossible."""
        number = Decimal(cleaned_value)
        if not number.is_finite():
            raise ValueError("NaN or infinite value")

        if pa.types.is_integer(pa_type):
            if number != number.to_integral_value():
                raise ValueError("not an integer")
            if number.adjusted() > 19:  # also guards int() against absurd exponents (1e999999999)
                raise ValueError("integer out of range")
            integer = int(number)
            low, high = _integer_bounds(pa_type)
            if not low <= integer <= high:
                raise ValueError("integer out of range")
            return integer

        result = float(number)
        limit = _FLOAT32_MAX if pa.types.is_float32(pa_type) else math.inf
        if not math.isfinite(result) or abs(result) > limit:
            raise ValueError("number out of range")
        return result

    def _clean_numeric_string(self, value: str, config: NumericCleaningConfig) -> Optional[str]:
        """Clean a numeric string for conversion."""
        if not value:
            return None

        # Remove thousands separators and normalize the decimal separator to a period
        cleaned = _normalize_separators(
            value, config.thousands_separator, config.decimal_separator
        )
        if cleaned is None:
            return None

        cleaned = cleaned.strip()
        return cleaned if cleaned else None
