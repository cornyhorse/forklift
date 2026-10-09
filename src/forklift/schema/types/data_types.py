"""Data type conversion utilities.

Besides the Arrow -> JSON Schema mapping this module owns the one strict parser/validator for the
``parquetType`` strings used by the CSV, Excel, SQL and FWF schema importers (``int32``,
``timestamp[us, tz=UTC]``, ``decimal128(10,2)``, ``list<string>`` ...).
"""

import re
from typing import Any, Callable, Dict, List, Optional, Sequence

import pyarrow as pa

_MAX_TYPE_LENGTH = 256
_MAX_NESTING = 8
_UNIT_RANK = {"s": 0, "ms": 1, "us": 2, "ns": 3}

_SCALAR_TYPES: Dict[str, pa.DataType] = {
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
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
}
_INDEX_TYPES = {
    name: _SCALAR_TYPES[name]
    for name in (
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    )
}

_TIME_RE = re.compile(r"time(32|64)\[(s|ms|us|ns)\]")
_TIMESTAMP_RE = re.compile(r"timestamp\[(s|ms|us|ns)(?:\s*,\s*tz=([A-Za-z0-9_+\-/:]{1,64}))?\]")
_DURATION_RE = re.compile(r"duration\[(s|ms|us|ns)\]")
_DECIMAL_RE = re.compile(r"decimal(128|256)\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)")
_LIST_RE = re.compile(r"(large_)?list<(?:item:\s*)?(.+)>", re.DOTALL)
_DICTIONARY_RE = re.compile(
    r"dictionary<\s*values=(.+?)\s*,\s*indices=([a-z0-9]+)\s*(?:,\s*ordered=[01]\s*)?>",
    re.DOTALL,
)


def _parse_parquet_type(text: str, depth: int) -> pa.DataType:
    if depth > _MAX_NESTING:
        raise ValueError("type is nested too deeply")
    text = text.strip()
    if text in _SCALAR_TYPES:
        return _SCALAR_TYPES[text]
    if text == "struct":
        return pa.struct([])  # placeholder: the schema standard only knows an opaque struct

    match = _TIME_RE.fullmatch(text)
    if match:
        width, unit = match.groups()
        if width == "32" and unit in ("s", "ms"):
            return pa.time32(unit)
        if width == "64" and unit in ("us", "ns"):
            return pa.time64(unit)
        raise ValueError(f"time{width} does not support unit '{unit}'")

    match = _TIMESTAMP_RE.fullmatch(text)
    if match:
        return pa.timestamp(match.group(1), tz=match.group(2))

    match = _DURATION_RE.fullmatch(text)
    if match:
        return pa.duration(match.group(1))

    match = _DECIMAL_RE.fullmatch(text)
    if match:
        width, precision, scale = match.group(1), int(match.group(2)), int(match.group(3))
        max_precision = 38 if width == "128" else 76
        if not 1 <= precision <= max_precision:
            raise ValueError(f"decimal{width} precision must be between 1 and {max_precision}")
        if not 0 <= scale <= precision:
            raise ValueError("decimal scale must be between 0 and the precision")
        return (
            pa.decimal128(precision, scale) if width == "128" else pa.decimal256(precision, scale)
        )

    match = _LIST_RE.fullmatch(text)
    if match:
        inner = _parse_parquet_type(match.group(2), depth + 1)
        return pa.large_list(inner) if match.group(1) else pa.list_(inner)

    match = _DICTIONARY_RE.fullmatch(text)
    if match:
        index_type = _INDEX_TYPES.get(match.group(2))
        if index_type is None:
            raise ValueError("dictionary indices must be an integer type")
        value_type = _parse_parquet_type(match.group(1), depth + 1)
        if pa.types.is_dictionary(value_type) or pa.types.is_nested(value_type):
            raise ValueError("dictionary values must be a scalar type")
        return pa.dictionary(index_type, value_type)

    raise ValueError("unknown or malformed type")


def parse_parquet_type(parquet_type: Any) -> pa.DataType:
    """Strictly parse a ``parquetType`` string into a PyArrow type.

    Supported: ``int8..int64``, ``uint8..uint64``, ``float32``, ``double``, ``bool``, ``string``,
    ``large_string``, ``binary``, ``large_binary``, ``date32``, ``date64``,
    ``time32[s|ms]``, ``time64[us|ns]``, ``timestamp[unit]``, ``timestamp[unit, tz=ZONE]``,
    ``duration[unit]``, ``decimal128(p,s)``, ``decimal256(p,s)``, ``list<T>``, ``large_list<T>``,
    ``dictionary<values=T, indices=INT>`` and the opaque ``struct``. Units are ``s|ms|us|ns``.

    Raises:
        ValueError: if the string is not a well-formed, in-range type (the message never contains
            the offending text, so it is safe to wrap in a schema error message).
    """
    if not isinstance(parquet_type, str):
        raise ValueError("Parquet type must be a string")
    if not parquet_type.strip() or len(parquet_type) > _MAX_TYPE_LENGTH:
        raise ValueError("Parquet type is empty or too long")
    return _parse_parquet_type(parquet_type, 0)


def is_valid_parquet_type(parquet_type: Any) -> bool:
    """Return True if ``parquet_type`` is a well-formed Parquet type string (see
    :func:`parse_parquet_type`). Non-string input is simply invalid."""
    try:
        parse_parquet_type(parquet_type)
    except ValueError:
        return False
    return True


def arrow_to_parquet_type_string(arrow_type: pa.DataType) -> str:
    """Render a PyArrow type as a lossless ``parquetType`` string that
    :func:`parse_parquet_type` reads back (decimal precision/scale, timestamp unit and timezone
    are preserved, unlike the coarse JSON Schema mapping).

    Raises:
        ValueError: if the type has no ``parquetType`` representation (e.g. float16, union).
    """
    for name, scalar in _SCALAR_TYPES.items():
        if arrow_type == scalar:
            return name
    if pa.types.is_timestamp(arrow_type):
        tz = f", tz={arrow_type.tz}" if arrow_type.tz else ""
        return f"timestamp[{arrow_type.unit}{tz}]"
    if pa.types.is_duration(arrow_type):
        return f"duration[{arrow_type.unit}]"
    if pa.types.is_time32(arrow_type):
        return f"time32[{arrow_type.unit}]"
    if pa.types.is_time64(arrow_type):
        return f"time64[{arrow_type.unit}]"
    if pa.types.is_decimal(arrow_type):
        width = 128 if pa.types.is_decimal128(arrow_type) else 256
        return f"decimal{width}({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_large_list(arrow_type):
        return f"large_list<{arrow_to_parquet_type_string(arrow_type.value_type)}>"
    if pa.types.is_list(arrow_type):
        return f"list<{arrow_to_parquet_type_string(arrow_type.value_type)}>"
    if pa.types.is_dictionary(arrow_type):
        values = arrow_to_parquet_type_string(arrow_type.value_type)
        indices = arrow_to_parquet_type_string(arrow_type.index_type)
        return f"dictionary<values={values}, indices={indices}>"
    if pa.types.is_struct(arrow_type):
        return "struct"
    raise ValueError(f"No parquetType representation for Arrow type {arrow_type}")


def _unify_numeric(parsed: List[pa.DataType]) -> pa.DataType:
    if any(pa.types.is_floating(t) for t in parsed):
        # float32 holds int8/16 and uint8/16 exactly; anything wider needs a double
        narrow = all(
            pa.types.is_float32(t) or (pa.types.is_integer(t) and t.bit_width <= 16)
            for t in parsed
        )
        return pa.float32() if narrow else pa.float64()
    signed = [t for t in parsed if pa.types.is_signed_integer(t)]
    unsigned = [t for t in parsed if pa.types.is_unsigned_integer(t)]
    if not unsigned:
        return max(signed, key=lambda t: t.bit_width)
    if not signed:
        return max(unsigned, key=lambda t: t.bit_width)
    needed = max(max(t.bit_width for t in signed), 2 * max(t.bit_width for t in unsigned))
    return {16: pa.int16(), 32: pa.int32(), 64: pa.int64()}.get(needed, pa.float64())


def _finest_unit(parsed: List[pa.DataType]) -> str:
    return max((t.unit for t in parsed), key=_UNIT_RANK.__getitem__)


def unify_parquet_types(types: Sequence[Any]) -> Optional[str]:
    """Return the narrowest ``parquetType`` that can hold every type in ``types``, or None when
    the types are not compatible (e.g. ``string`` vs ``int32``, differing time zones).

    Identical strings unify to themselves (without being parsed). Otherwise: integers widen
    (signed/unsigned mixes widen to the next signed width, or ``double`` past 64 bits), ints with
    floats widen to ``double`` (``float32`` when exact), decimals widen to hold the largest integer
    part and scale, dates/timestamps widen to the finest timestamp unit, durations to the finest
    unit and string/binary to binary.
    """
    if not types or not all(isinstance(t, str) for t in types):
        return None
    unique = list(dict.fromkeys(t.strip() for t in types))
    if len(unique) == 1:
        return unique[0]
    try:
        parsed = [parse_parquet_type(t) for t in unique]
    except ValueError:
        return None

    def all_of(predicate: Callable[[pa.DataType], bool]) -> bool:
        return all(predicate(t) for t in parsed)

    result: Optional[pa.DataType] = None
    if all_of(lambda t: pa.types.is_integer(t) or pa.types.is_floating(t)):
        result = _unify_numeric(parsed)
    elif all_of(pa.types.is_decimal):
        scale = max(t.scale for t in parsed)
        precision = max(t.precision - t.scale for t in parsed) + scale
        result = (
            pa.decimal128(precision, scale) if precision <= 38 else pa.decimal256(precision, scale)
        )
    elif all_of(lambda t: pa.types.is_date(t) or pa.types.is_timestamp(t)):
        stamps = [t for t in parsed if pa.types.is_timestamp(t)]
        if not stamps:
            result = pa.date64() if any(pa.types.is_date64(t) for t in parsed) else pa.date32()
        elif len({t.tz for t in stamps}) == 1:
            result = pa.timestamp(_finest_unit(stamps), tz=stamps[0].tz)
        # timestamps with different time zones (or only some zoned) are not interchangeable
    elif all_of(pa.types.is_duration):
        result = pa.duration(_finest_unit(parsed))
    elif all_of(
        lambda t: pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
    ):
        large = any(pa.types.is_large_string(t) or pa.types.is_large_binary(t) for t in parsed)
        binary = any(pa.types.is_binary(t) or pa.types.is_large_binary(t) for t in parsed)
        result = {
            (False, False): pa.string(),
            (True, False): pa.large_string(),
            (False, True): pa.binary(),
            (True, True): pa.large_binary(),
        }[(large, binary)]

    return arrow_to_parquet_type_string(result) if result is not None else None


class DataTypeConverter:
    """Handles conversion between different data type representations."""

    @staticmethod
    def arrow_to_json_schema_type(arrow_type: pa.DataType) -> Dict[str, Any]:
        """Convert PyArrow type to JSON Schema type definition.

        Args:
            arrow_type: PyArrow data type

        Returns:
            Dict: JSON Schema type definition
        """
        if pa.types.is_integer(arrow_type):
            return {"type": "integer"}
        elif pa.types.is_floating(arrow_type):
            return {"type": "number"}
        elif isinstance(arrow_type, pa.DataType) and pa.types.is_decimal(arrow_type):
            # Decimals are numbers; the exact precision/scale lives in the lossless parquetType
            # (see arrow_to_parquet_type_string) instead of degrading the column to a string.
            return {"type": "number"}
        elif pa.types.is_boolean(arrow_type):
            return {"type": "boolean"}
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return {"type": "string"}
        elif pa.types.is_date(arrow_type):
            return {"type": "string", "format": "date"}
        elif pa.types.is_timestamp(arrow_type):
            return {"type": "string", "format": "date-time"}
        elif pa.types.is_time(arrow_type):
            return {"type": "string", "format": "time"}
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return {"type": "string", "contentEncoding": "base64"}
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            if hasattr(arrow_type, "value_type"):
                value_type = DataTypeConverter.arrow_to_json_schema_type(arrow_type.value_type)
            else:
                value_type = {"type": "string"}
            return {"type": "array", "items": value_type}
        elif pa.types.is_struct(arrow_type):
            return {"type": "object", "additionalProperties": True}
        elif pa.types.is_dictionary(arrow_type):
            return {"type": "string"}
        else:
            return {"type": "string"}

    @staticmethod
    def arrow_to_parquet_type_string(arrow_type: pa.DataType) -> str:
        """Lossless Arrow -> ``parquetType`` string (see :func:`arrow_to_parquet_type_string`)."""
        return arrow_to_parquet_type_string(arrow_type)

    @staticmethod
    def parquet_type_string_to_arrow(parquet_type: str) -> pa.DataType:
        """Strict ``parquetType`` string -> Arrow type (see :func:`parse_parquet_type`)."""
        return parse_parquet_type(parquet_type)

    @staticmethod
    def is_valid_parquet_type(parquet_type: Any) -> bool:
        """Strict ``parquetType`` validation (see :func:`is_valid_parquet_type`)."""
        return is_valid_parquet_type(parquet_type)

    @staticmethod
    def unify_parquet_types(types: Sequence[Any]) -> Optional[str]:
        """Common ``parquetType`` for several variants of one field (see
        :func:`unify_parquet_types`)."""
        return unify_parquet_types(types)

    @staticmethod
    def detect_numeric_patterns(sample_values: list) -> Dict[str, bool]:
        """Detect numeric patterns in string data.

        Args:
            sample_values: List of sample string values

        Returns:
            Dict: Pattern detection results
        """
        patterns = {
            "has_thousands_separator": False,
            "has_decimal_separator": False,
            "has_currency_symbols": False,
            "has_parentheses_negative": False,
        }

        currency_pattern = r"[\$€£¥₹₽¢]"
        thousands_pattern = r"\d+,\d+"
        decimal_pattern = r"\d+\.\d+"
        parentheses_pattern = r"\(.*\)"

        for value in sample_values[:10]:  # Check first 10 values
            str_val = str(value)
            if re.search(currency_pattern, str_val):
                patterns["has_currency_symbols"] = True
            if re.search(thousands_pattern, str_val):
                patterns["has_thousands_separator"] = True
            if re.search(decimal_pattern, str_val):
                patterns["has_decimal_separator"] = True
            if re.search(parentheses_pattern, str_val):
                patterns["has_parentheses_negative"] = True

        return patterns
