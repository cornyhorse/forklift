"""Type conversion utilities for schema validation."""

from typing import Dict, Any, Optional
import pyarrow as pa
from datetime import datetime


class TypeConverter:
    """Handles conversion between different type representations."""

    @staticmethod
    def string_to_arrow_type(type_str: str) -> pa.DataType:
        """Convert string type to PyArrow type."""
        type_str = type_str.lower()

        type_mapping = {
            "int": pa.int64(),
            "integer": pa.int64(),
            "int64": pa.int64(),
            "int32": pa.int32(),
            "float": pa.float64(),
            "double": pa.float64(),
            "float64": pa.float64(),
            "float32": pa.float32(),
            "string": pa.string(),
            "str": pa.string(),
            "text": pa.string(),
            "bool": pa.bool_(),
            "boolean": pa.bool_(),
            "date": pa.date32(),
            "datetime": pa.timestamp('us'),
            "timestamp": pa.timestamp('us')
        }

        return type_mapping.get(type_str, pa.string())

    @staticmethod
    def convert_arrow_schema_to_dict(schema: pa.Schema) -> Dict[str, Any]:
        """Convert PyArrow schema to internal dictionary format."""
        columns = []
        for field in schema:
            column_def = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
                "constraints": {}
            }
            columns.append(column_def)

        return {
            "columns": columns,
            "metadata": {
                "converted_from_arrow_schema": True,
                "creation_timestamp": datetime.now().isoformat()
            }
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
        return (pa.types.is_integer(data_type) or
                pa.types.is_floating(data_type) or
                pa.types.is_decimal(data_type))

    @staticmethod
    def is_type_compatible(actual_type: pa.DataType, expected_type_str: str) -> bool:
        """Check if actual type is compatible with expected type."""
        expected_type_str = expected_type_str.lower()

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
            return pa.types.is_string(actual_type)

        # Boolean type compatibility
        elif expected_type_str in ["bool", "boolean"]:
            return pa.types.is_boolean(actual_type)

        # Date/time type compatibility
        elif expected_type_str in ["date", "datetime", "timestamp"]:
            return pa.types.is_temporal(actual_type)

        return False

    @staticmethod
    def can_coerce_type(from_type: pa.DataType, to_type_str: str) -> bool:
        """Check if type coercion is possible."""
        # Simple coercion rules
        if pa.types.is_string(from_type):
            return True  # Strings can usually be coerced to other types

        if TypeConverter.is_numeric_type(from_type) and to_type_str.lower() in ["string", "str", "text"]:
            return True  # Numbers can be converted to strings

        return False
