"""Type conversion utilities for schema validation."""

import re
from datetime import datetime
from typing import Any, Dict, Optional

import pyarrow as pa

_SIMPLE_TYPES = {
    "int": pa.int64,
    "integer": pa.int64,
    "long": pa.int64,
    "bigint": pa.int64,
    "int64": pa.int64,
    "int32": pa.int32,
    "int16": pa.int16,
    "smallint": pa.int16,
    "int8": pa.int8,
    "tinyint": pa.int8,
    "uint64": pa.uint64,
    "uint32": pa.uint32,
    "uint16": pa.uint16,
    "uint8": pa.uint8,
    "float": pa.float64,
    "number": pa.float64,
    "numeric": pa.float64,
    "double": pa.float64,
    "float64": pa.float64,
    "real": pa.float32,
    "float32": pa.float32,
    "float16": pa.float16,
    "halffloat": pa.float16,
    "string": pa.string,
    "str": pa.string,
    "text": pa.string,
    "utf8": pa.string,
    "varchar": pa.string,
    "char": pa.string,
    "large_string": pa.large_string,
    "large_utf8": pa.large_string,
    "binary": pa.binary,
    "large_binary": pa.large_binary,
    "bool": pa.bool_,
    "bool_": pa.bool_,
    "boolean": pa.bool_,
    "date": pa.date32,
    "date32": pa.date32,
    "date32[day]": pa.date32,
    "date64": pa.date64,
    "date64[ms]": pa.date64,
    "datetime": lambda: pa.timestamp("us"),
    "timestamp": lambda: pa.timestamp("us"),
    "null": pa.null,
}

_DECIMAL_RE = re.compile(
    r"(?:decimal|decimal128|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", re.IGNORECASE
)
_SIZED_STRING_RE = re.compile(r"(?:varchar|char|string|text)\s*\(\s*\d+\s*\)", re.IGNORECASE)
_TIMESTAMP_RE = re.compile(r"timestamp\[(s|ms|us|ns)(?:\s*,\s*tz=([^\]\s]+))?\]", re.IGNORECASE)
_LIST_RE = re.compile(r"(large_)?list<(.*)>", re.IGNORECASE | re.DOTALL)
_TIME_RE = re.compile(r"time(32|64)\[(s|ms|us|ns)\]", re.IGNORECASE)


def parse_arrow_type(type_str: str) -> pa.DataType:
    """Parse a schema type string (``int``, ``decimal(18,2)``, ``timestamp[us]``, ...).

    Accepts the common SQL/JSON-schema aliases as well as the strings PyArrow itself prints
    for a type (``str(pa.int16())``, ``decimal128(18, 2)``, ``list<item: string>``).

    Raises:
        ValueError: If the string is not a recognised type. Unknown types are never silently
            mapped to string.
    """
    if not isinstance(type_str, str):
        raise ValueError(f"Data type must be a string, got {type(type_str).__name__}")

    text = type_str.strip()
    if not text:
        raise ValueError("Data type is empty")

    lowered = text.lower()
    if lowered in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[lowered]()

    if _SIZED_STRING_RE.fullmatch(text):
        return pa.string()

    match = _DECIMAL_RE.fullmatch(text)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2)) if match.group(2) is not None else 0
        try:
            return pa.decimal128(precision, scale)
        except (pa.ArrowException, ValueError, OverflowError):
            raise ValueError(f"Invalid decimal precision/scale in data type '{text}'") from None

    match = _TIMESTAMP_RE.fullmatch(text)
    if match:
        return pa.timestamp(match.group(1).lower(), tz=match.group(2))

    match = _TIME_RE.fullmatch(text)
    if match:
        try:
            factory = pa.time32 if match.group(1) == "32" else pa.time64
            return factory(match.group(2).lower())
        except (pa.ArrowException, ValueError):
            raise ValueError(f"Invalid time unit in data type '{text}'") from None

    match = _LIST_RE.fullmatch(text)
    if match:
        inner = match.group(2).strip()
        if inner.lower().startswith("item:"):
            inner = inner[5:].strip()
        inner_type = parse_arrow_type(inner)
        return pa.large_list(inner_type) if match.group(1) else pa.list_(inner_type)

    raise ValueError(f"Unknown data type '{text}'")


class TypeConverter:
    """Handles conversion between different type representations."""

    @staticmethod
    def string_to_arrow_type(type_str: str) -> pa.DataType:
        """Convert string type to PyArrow type.

        Raises:
            ValueError: If the type string is not recognised (it is never silently
                mapped to string).
        """
        return parse_arrow_type(type_str)

    @staticmethod
    def convert_arrow_schema_to_dict(schema: pa.Schema) -> Dict[str, Any]:
        """Convert PyArrow schema to internal dictionary format."""
        columns = []
        for field in schema:
            column_def = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
                "constraints": {},
            }
            columns.append(column_def)

        return {
            "columns": columns,
            "metadata": {
                "converted_from_arrow_schema": True,
                "creation_timestamp": datetime.now().isoformat(),
            },
        }

    @staticmethod
    def convert_dict_to_arrow_schema(schema_dict: Dict[str, Any]) -> Optional[pa.Schema]:
        """Convert internal dictionary format to PyArrow schema."""
        if "columns" not in schema_dict:
            return None

        fields = []
        for col_def in schema_dict["columns"]:
            if isinstance(col_def, dict):
                name = col_def.get("name", "")
                type_str = col_def.get("type", "string")
                nullable = col_def.get("nullable", True)

                # Convert type string to PyArrow type
                pa_type = TypeConverter.string_to_arrow_type(type_str)
                fields.append(pa.field(name, pa_type, nullable=nullable))

        return pa.schema(fields) if fields else None

    @staticmethod
    def is_numeric_type(data_type: pa.DataType) -> bool:
        """Check if a PyArrow data type is numeric."""
        return (
            pa.types.is_integer(data_type)
            or pa.types.is_floating(data_type)
            or pa.types.is_decimal(data_type)
        )

    @staticmethod
    def is_type_compatible(actual_type: pa.DataType, expected_type_str: str) -> bool:
        """Check if actual type is compatible with expected type.

        Family names (``int``, ``float``, ``string``, ``number``, ``date``, ...) accept any
        member of the family; specific names (``int16``, ``decimal(18,2)``,
        ``timestamp[ms]``, ...) must match exactly.
        """
        expected_type_str = expected_type_str.strip().lower()

        # Exact string matches
        if str(actual_type).lower() == expected_type_str:
            return True

        # Numeric type compatibility
        if expected_type_str in ["int", "integer", "int64"]:
            return pa.types.is_integer(actual_type)
        elif expected_type_str in ["float", "double", "float64"]:
            return pa.types.is_floating(actual_type)
        elif expected_type_str in ["number", "numeric"]:
            return TypeConverter.is_numeric_type(actual_type)

        # String type compatibility
        elif expected_type_str in ["string", "str", "text"]:
            return pa.types.is_string(actual_type) or pa.types.is_large_string(actual_type)

        # Boolean type compatibility
        elif expected_type_str in ["bool", "boolean"]:
            return pa.types.is_boolean(actual_type)

        # Date/time type compatibility
        elif expected_type_str in ["date", "datetime", "timestamp"]:
            return pa.types.is_temporal(actual_type)

        # Specific types (int16, decimal(18,2), timestamp[ms], list<string>, ...)
        try:
            expected_type = parse_arrow_type(expected_type_str)
        except ValueError:
            return False
        return actual_type.equals(expected_type)

    @staticmethod
    def can_coerce_type(from_type: pa.DataType, to_type_str: str) -> bool:
        """Check if a cast from ``from_type`` to the named type is worth attempting.

        The cast itself is performed (safely, per value) by the schema validator; values that
        cannot be converted are reported as violations.
        """
        try:
            to_type = parse_arrow_type(to_type_str)
        except ValueError:
            return False

        to_string = pa.types.is_string(to_type) or pa.types.is_large_string(to_type)

        if pa.types.is_string(from_type) or pa.types.is_large_string(from_type):
            return (
                TypeConverter.is_numeric_type(to_type)
                or pa.types.is_boolean(to_type)
                or pa.types.is_temporal(to_type)
                or to_string
            )

        if TypeConverter.is_numeric_type(from_type):
            return to_string or TypeConverter.is_numeric_type(to_type)

        return False
