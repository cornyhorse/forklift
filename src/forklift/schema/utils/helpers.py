"""Common helper functions and exceptions for schema generation."""

from typing import Any, Dict
import pyarrow as pa


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
    """Convert Arrow type to Parquet type string.

    Args:
        arrow_type: PyArrow data type

    Returns:
        str: Parquet type string representation
    """
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
    elif pa.types.is_float32(arrow_type):
        return "float32"
    elif pa.types.is_float64(arrow_type):
        return "double"
    elif pa.types.is_boolean(arrow_type):
        return "bool"
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "binary"
    elif pa.types.is_date32(arrow_type):
        return "date32"
    elif pa.types.is_date64(arrow_type):
        return "date64"
    elif pa.types.is_timestamp(arrow_type):
        return "timestamp[ms]"
    elif pa.types.is_duration(arrow_type):
        return "duration[ms]"
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        if hasattr(arrow_type, 'value_type'):
            return f"list<{get_parquet_type_string(arrow_type.value_type)}>"
        else:
            return "list<string>"
    elif pa.types.is_struct(arrow_type):
        return "struct"
    elif pa.types.is_dictionary(arrow_type):
        return "dictionary<values=string, indices=int32>"
    else:
        return "string"
