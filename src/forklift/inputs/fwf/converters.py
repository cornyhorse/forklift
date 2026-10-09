"""Type conversion and value processing for FWF fields."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pyarrow as pa

from ..config import FwfFieldSpec

# Tokens accepted for boolean fields (compared case-insensitively, after trimming).
# Anything else is an invalid value rather than a silent False.
TRUE_TOKENS = frozenset({"true", "1", "yes", "y", "t"})
FALSE_TOKENS = frozenset({"false", "0", "no", "n", "f"})

_INT_BOUNDS = {
    "int8": (-(2**7), 2**7 - 1),
    "int16": (-(2**15), 2**15 - 1),
    "int32": (-(2**31), 2**31 - 1),
    "int64": (-(2**63), 2**63 - 1),
    "uint8": (0, 2**8 - 1),
    "uint16": (0, 2**16 - 1),
    "uint32": (0, 2**32 - 1),
    "uint64": (0, 2**64 - 1),
}


class FwfTypeConverter:
    """Handles type conversion for FWF field values."""

    @staticmethod
    def get_arrow_type(parquet_type: str) -> pa.DataType:
        """Convert parquet type string to PyArrow type.

        Args:
            parquet_type: String representation of the type

        Returns:
            PyArrow DataType object
        """
        type_mapping = {
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            "double": pa.float64(),  # Add double as alias for float64
            "bool": pa.bool_(),
            "string": pa.string(),
            "utf8": pa.string(),
            "binary": pa.binary(),
            "date32": pa.date32(),
            "date64": pa.date64(),
            "timestamp": pa.timestamp("ns"),
        }

        # Handle timestamp types with specific units
        if parquet_type.startswith("timestamp[") and parquet_type.endswith("]"):
            unit = parquet_type[10:-1]  # Extract unit between brackets
            return pa.timestamp(unit)

        # Handle duration types with specific units
        if parquet_type.startswith("duration[") and parquet_type.endswith("]"):
            unit = parquet_type[9:-1]  # Extract unit between brackets
            return pa.duration(unit)

        # Handle list types
        if parquet_type.startswith("list<") and parquet_type.endswith(">"):
            inner_type = parquet_type[5:-1]  # Extract inner type
            inner_arrow_type = FwfTypeConverter.get_arrow_type(inner_type)
            return pa.list_(inner_arrow_type)

        # Handle decimal types
        if parquet_type.startswith("decimal"):
            # Extract precision and scale if specified
            if "(" in parquet_type:
                params = parquet_type[parquet_type.find("(") + 1 : parquet_type.find(")")]
                if "," in params:
                    precision, scale = map(int, params.split(","))
                    return pa.decimal128(precision, scale)
                else:
                    precision = int(params)
                    return pa.decimal128(precision, 2)
            return pa.decimal128(10, 2)

        return type_mapping.get(parquet_type, pa.string())

    @staticmethod
    def convert_value_checked(value: str, parquet_type: str) -> Tuple[Any, bool]:
        """Convert a string value and report whether the conversion was valid.

        Args:
            value: String value to convert
            parquet_type: Target Parquet data type

        Returns:
            ``(converted, ok)``. Blank input is ``(None, True)``; a value that is not valid
            for the type (``12A`` as an int, ``maybe`` as a bool, an out-of-range integer)
            is ``(None, False)``.
        """
        if not value:
            return None, True

        try:
            if parquet_type in _INT_BOUNDS:
                number = int(value)
                low, high = _INT_BOUNDS[parquet_type]
                if not low <= number <= high:
                    return None, False
                return number, True
            elif parquet_type in ["float32", "float64", "double"]:  # Add double support
                return float(value), True
            elif parquet_type.startswith("decimal"):
                return float(value), True  # Convert to float for testing purposes
            elif parquet_type == "bool":
                token = value.strip().lower()
                if token in TRUE_TOKENS:
                    return True, True
                if token in FALSE_TOKENS:
                    return False, True
                return None, False
            else:  # string or other types
                return value, True
        except (ValueError, TypeError):
            return None, False

    @staticmethod
    def convert_value(value: str, parquet_type: str) -> Any:
        """Convert string value to appropriate Python type.

        Args:
            value: String value to convert
            parquet_type: Target Parquet data type

        Returns:
            Converted value, or None for blank or invalid input (use
            :meth:`convert_value_checked` to tell those apart)
        """
        return FwfTypeConverter.convert_value_checked(value, parquet_type)[0]

    @staticmethod
    def convert_field_value(value: str, field: FwfFieldSpec) -> Any:
        """Convert field value to appropriate type based on field specification.

        Args:
            value: String value to convert
            field: Field specification

        Returns:
            Converted value
        """
        return FwfTypeConverter.convert_value(value, field.parquet_type)


class FwfValueProcessor:
    """Handles value processing including null value handling."""

    @staticmethod
    def process_null_values(
        value: str, field_name: str, null_values: Optional[Dict] = None
    ) -> Optional[str]:
        """Process null values according to configuration.

        Args:
            value: The field value to check
            field_name: Name of the field
            null_values: Null values configuration

        Returns:
            None if value should be treated as null, otherwise the original value
        """
        if not null_values:
            # Default behavior: empty strings are treated as None
            return None if value == "" else value

        # Check global null values
        global_nulls = null_values.get("global", [])
        if value in global_nulls:
            return None

        # Check per-column null values
        per_column = null_values.get("perColumn", {})
        field_nulls = per_column.get(field_name, [])
        if value in field_nulls:
            return None

        return value
