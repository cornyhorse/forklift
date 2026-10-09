"""Common helper functions and exceptions for schema generation."""

import base64
import math
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import PureWindowsPath
from typing import Any, Dict, List, Optional

import pyarrow as pa

# Quantiles reported for numeric columns when the caller does not configure any.
DEFAULT_QUANTILES = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


class SchemaValidationError(Exception):
    """Exception raised when schema validation fails."""

    pass


def validate_schema_structure(schema: Dict[str, Any]) -> bool:
    """Validate that a schema has the required structure.

    Args:
        schema: Schema dictionary to validate

    Returns:
        bool: True if valid

    Raises:
        SchemaValidationError: If schema structure is invalid
    """
    required_fields = ["$schema", "type", "properties"]

    for field in required_fields:
        if field not in schema:
            raise SchemaValidationError(f"Missing required field: {field}")

    if not isinstance(schema.get("properties"), dict):
        raise SchemaValidationError("Properties must be a dictionary")

    return True


def get_parquet_type_string(arrow_type: pa.DataType) -> str:
    """Convert Arrow type to the Parquet type string used in Forklift schemas.

    The result is a type string the schema importers accept (see
    ``ParquetTypeValidator``): precision, scale, time unit and time zone are kept
    (``decimal128(18,4)``, ``timestamp[us, tz=UTC]``, ``duration[ns]``). Arrow types that
    have no importer-accepted spelling are mapped to the closest accepted type:

    * ``float16`` -> ``float32`` (lossless widening)
    * ``fixed_size_list<T>`` / ``large_list<T>`` / ``list_view<T>`` -> ``list<T>``
    * ``map`` -> ``list<struct>`` (Arrow's own physical layout of a map)
    * ``time32`` / ``time64`` / ``decimal256`` / interval / null / union -> ``string``

    Args:
        arrow_type: PyArrow data type

    Returns:
        str: Parquet type string representation
    """
    if isinstance(arrow_type, pa.BaseExtensionType):
        return get_parquet_type_string(arrow_type.storage_type)

    if pa.types.is_int8(arrow_type):
        return "int8"
    elif pa.types.is_int16(arrow_type):
        return "int16"
    elif pa.types.is_int32(arrow_type):
        return "int32"
    elif pa.types.is_int64(arrow_type):
        return "int64"
    elif pa.types.is_uint8(arrow_type):
        return "uint8"
    elif pa.types.is_uint16(arrow_type):
        return "uint16"
    elif pa.types.is_uint32(arrow_type):
        return "uint32"
    elif pa.types.is_uint64(arrow_type):
        return "uint64"
    elif pa.types.is_float16(arrow_type):
        return "float32"
    elif pa.types.is_float32(arrow_type):
        return "float32"
    elif pa.types.is_float64(arrow_type):
        return "double"
    elif pa.types.is_boolean(arrow_type):
        return "bool"
    elif _is_string_like(arrow_type):
        return "string"
    elif _is_binary_like(arrow_type):
        return "binary"
    elif pa.types.is_date32(arrow_type):
        return "date32"
    elif pa.types.is_date64(arrow_type):
        return "date64"
    elif pa.types.is_timestamp(arrow_type):
        if getattr(arrow_type, "tz", None):
            return f"timestamp[{arrow_type.unit}, tz={arrow_type.tz}]"
        return f"timestamp[{arrow_type.unit}]"
    elif pa.types.is_duration(arrow_type):
        return f"duration[{arrow_type.unit}]"
    elif pa.types.is_decimal128(arrow_type):
        return f"decimal128({arrow_type.precision},{arrow_type.scale})"
    elif _is_list_like(arrow_type):
        if hasattr(arrow_type, "value_type"):
            return f"list<{get_parquet_type_string(arrow_type.value_type)}>"
        else:
            return "list<string>"
    elif pa.types.is_map(arrow_type):
        return "list<struct>"
    elif pa.types.is_struct(arrow_type):
        return "struct"
    elif pa.types.is_dictionary(arrow_type):
        values = get_parquet_type_string(arrow_type.value_type)
        indices = get_parquet_type_string(arrow_type.index_type)
        return f"dictionary<values={values}, indices={indices}>"
    else:
        return "string"


def _is_string_like(arrow_type: pa.DataType) -> bool:
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return True
    return bool(getattr(pa.types, "is_string_view", lambda _t: False)(arrow_type))


def _is_binary_like(arrow_type: pa.DataType) -> bool:
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return True
    if pa.types.is_fixed_size_binary(arrow_type):
        return True
    return bool(getattr(pa.types, "is_binary_view", lambda _t: False)(arrow_type))


def _is_list_like(arrow_type: pa.DataType) -> bool:
    # Arrow's map type is a list subtype in some releases; it is handled separately.
    if pa.types.is_map(arrow_type):
        return False
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return True
    if pa.types.is_fixed_size_list(arrow_type):
        return True
    return bool(getattr(pa.types, "is_list_view", lambda _t: False)(arrow_type)) or bool(
        getattr(pa.types, "is_large_list_view", lambda _t: False)(arrow_type)
    )


_SIMPLE_PARQUET_TYPES = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "string": pa.string(),
    "binary": pa.binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
}


def parquet_type_string_to_arrow(type_string: str) -> pa.DataType:
    """Parse a type string produced by :func:`get_parquet_type_string` back to Arrow.

    This is the inverse used for round-trip checks of generated schemas. ``struct`` has no
    field information in its string form and comes back as an empty struct.

    Args:
        type_string: Parquet type string, e.g. ``decimal128(18,4)`` or ``timestamp[ms, tz=UTC]``

    Returns:
        pa.DataType: The corresponding Arrow type

    Raises:
        ValueError: If the string is not a recognised type string
    """
    text = type_string.strip()

    if text in _SIMPLE_PARQUET_TYPES:
        return _SIMPLE_PARQUET_TYPES[text]
    if text == "struct":
        return pa.struct([])

    match = re.fullmatch(r"timestamp\[(s|ms|us|ns)(?:,\s*tz=(.+))?\]", text)
    if match:
        return pa.timestamp(match.group(1), tz=match.group(2))

    match = re.fullmatch(r"duration\[(s|ms|us|ns)\]", text)
    if match:
        return pa.duration(match.group(1))

    match = re.fullmatch(r"decimal128\((\d+),\s*(-?\d+)\)", text)
    if match:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))

    if text.startswith("list<") and text.endswith(">"):
        return pa.list_(parquet_type_string_to_arrow(text[5:-1]))

    match = re.fullmatch(r"dictionary<values=(.+), indices=([a-z0-9]+)>", text)
    if match:
        return pa.dictionary(
            parquet_type_string_to_arrow(match.group(2)),
            parquet_type_string_to_arrow(match.group(1)),
        )

    raise ValueError(f"Unrecognised Parquet type string: {type_string!r}")


def validate_quantiles(quantiles: Optional[List[float]]) -> List[float]:
    """Validate configured quantiles.

    Args:
        quantiles: Quantile fractions, each within ``0 <= q <= 1``. ``None`` selects the
            default list.

    Returns:
        List[float]: The validated quantiles as floats

    Raises:
        ValueError: If a quantile is not a finite number within [0, 1]
    """
    if quantiles is None:
        return list(DEFAULT_QUANTILES)
    if isinstance(quantiles, (str, bytes)) or not hasattr(quantiles, "__iter__"):
        raise ValueError("quantiles must be a list of numbers between 0 and 1")

    validated = []
    for q in quantiles:
        if isinstance(q, bool) or not isinstance(q, (int, float, Decimal)):
            raise ValueError(f"Invalid quantile {q!r}: quantiles must be numbers between 0 and 1")
        q_float = float(q)
        if not math.isfinite(q_float) or not 0.0 <= q_float <= 1.0:
            raise ValueError(f"Invalid quantile {q!r}: quantiles must be between 0 and 1")
        validated.append(q_float)
    return validated


def quantile_label(q: float) -> str:
    """Return the percent label used in quantile keys (``0.29`` -> ``"29"``).

    Labels are computed with decimal arithmetic so float representation error does not
    shift them (``int(0.29 * 100)`` is 28). Fractional percentiles keep their fraction with
    ``_`` as separator (``0.995`` -> ``"99_5"``).
    """
    percent = (Decimal(repr(float(q))) * 100).normalize()
    return format(percent, "f").replace(".", "_")


_CAMEL_BOUNDARY = re.compile(
    r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=[A-Za-z])(?=[0-9])|(?<=[0-9])(?=[A-Za-z])"
)


def split_name_tokens(name: str) -> List[str]:
    """Split a column name into lower-case word tokens.

    Splits on any non-alphanumeric character and on camelCase / digit boundaries:
    ``user_id`` -> ``["user", "id"]``, ``userId`` -> ``["user", "id"]``,
    ``clientIPAddress`` -> ``["client", "ip", "address"]``, ``width`` -> ``["width"]``.
    """
    tokens = []
    for chunk in re.split(r"[\W_]+", str(name)):
        if chunk:
            tokens.extend(part.lower() for part in _CAMEL_BOUNDARY.sub(" ", chunk).split())
    return tokens


def source_basename(path: Any) -> str:
    """Return only the file name of ``path`` (local, Windows or ``s3://``) for reporting.

    Generated schemas record the file name, never the absolute path, so directory layouts
    and user names do not leak into shared schema files.
    """
    name = PureWindowsPath(str(path)).name
    return name or "unknown"


def to_json_safe(value: Any) -> Any:
    """Convert ``value`` to something ``json.dumps(..., allow_nan=False)`` accepts.

    * ``NaN`` / ``inf`` -> ``None``
    * ``Decimal`` -> ``str`` (exact), ``date`` / ``datetime`` / ``time`` -> ISO string
    * ``timedelta`` -> ``str``, ``bytes`` -> base64 string
    * containers are converted recursively, unknown objects fall back to ``str``
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, dict):
        return {(k if isinstance(k, str) else str(k)): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_json_safe(v) for v in value]
    # numpy scalars and other number-like objects
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return to_json_safe(item())
        except Exception:  # pragma: no cover - defensive
            pass
    return str(value)
