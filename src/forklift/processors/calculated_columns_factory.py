"""Factory functions for creating calculated columns processors from schema configurations."""

from __future__ import annotations
from typing import Dict, Any, Optional
import pyarrow as pa

from .calculated_columns import (
    CalculatedColumnsProcessor,
    CalculatedColumnsConfig,
    ConstantColumn,
    ExpressionColumn,
    CalculatedColumn
)


def create_calculated_columns_processor_from_schema(
    schema_config: Dict[str, Any]
) -> Optional[CalculatedColumnsProcessor]:
    """Create a CalculatedColumnsProcessor from schema configuration.

    Args:
        schema_config: Dictionary containing the x-calculatedColumns configuration

    Returns:
        CalculatedColumnsProcessor instance or None if no configuration found
    """
    if not schema_config:
        return None

    # Parse constants
    constants = []
    for const_def in schema_config.get("constants", []):
        constant = ConstantColumn(
            name=const_def["name"],
            value=const_def["value"],
            data_type=_parse_data_type(const_def.get("dataType")),
            description=const_def.get("description")
        )
        constants.append(constant)

    # Parse expressions
    expressions = []
    for expr_def in schema_config.get("expressions", []):
        expression = ExpressionColumn(
            name=expr_def["name"],
            expression=expr_def["expression"],
            data_type=_parse_data_type(expr_def.get("dataType")),
            description=expr_def.get("description"),
            dependencies=expr_def.get("dependencies", [])
        )
        expressions.append(expression)

    # Parse calculated columns
    calculated = []
    for calc_def in schema_config.get("calculated", []):
        calculated_col = CalculatedColumn(
            name=calc_def["name"],
            function=calc_def["function"],
            dependencies=calc_def["dependencies"],
            data_type=_parse_data_type(calc_def.get("dataType")),
            description=calc_def.get("description")
        )
        calculated.append(calculated_col)

    # Create configuration
    config = CalculatedColumnsConfig(
        constants=constants,
        expressions=expressions,
        calculated=calculated,
        partition_columns=schema_config.get("partitionColumns", [])
    )

    return CalculatedColumnsProcessor(config)


def _parse_data_type(data_type_str: Optional[str]) -> Optional[pa.DataType]:
    """Parse a data type string into PyArrow DataType.

    Args:
        data_type_str: String representation of data type

    Returns:
        PyArrow DataType or None if not specified
    """
    if not data_type_str:
        return None

    type_mapping = {
        "string": pa.string(),
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
        "double": pa.float64(),
        "bool": pa.bool_(),
        "boolean": pa.bool_(),
        "date32": pa.date32(),
        "date64": pa.date64(),
        "timestamp": pa.timestamp('us'),
        "binary": pa.binary(),
    }

    # Handle simple types
    if data_type_str in type_mapping:
        return type_mapping[data_type_str]

    # Handle complex types
    if data_type_str.startswith("timestamp["):
        # Extract unit from timestamp[unit]
        unit = data_type_str[10:-1]  # Remove "timestamp[" and "]"
        return pa.timestamp(unit)

    elif data_type_str.startswith("decimal128("):
        # Extract precision and scale from decimal128(precision,scale)
        params = data_type_str[11:-1]  # Remove "decimal128(" and ")"
        precision, scale = map(int, params.split(","))
        return pa.decimal128(precision, scale)

    elif data_type_str.startswith("list<"):
        # Extract inner type from list<type>
        inner_type_str = data_type_str[5:-1]  # Remove "list<" and ">"
        inner_type = _parse_data_type(inner_type_str)
        return pa.list_(inner_type)

    # Default to string if unknown
    return pa.string()
