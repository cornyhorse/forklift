"""Factory functions for creating calculated columns processors from schema configurations."""

from __future__ import annotations
from typing import Dict, Any, Optional, List
import pyarrow as pa

from .calculated_columns import (
    CalculatedColumnsProcessor,
    CalculatedColumnsConfig,
    ConstantColumn,
    ExpressionColumn,
    CalculatedColumn
)


def _parse_data_type(data_type_str: Optional[str]) -> Optional[pa.DataType]:
    """Parse data type string to PyArrow data type.

    Args:
        data_type_str: String representation of data type

    Returns:
        PyArrow DataType or None if input is None/empty
    """
    if data_type_str is None:
        return None
    
    if data_type_str == "":
        return None
    
    # For whitespace-only strings, strip first then check if empty
    stripped = data_type_str.strip()
    if not stripped:
        return pa.string()

    data_type_str = stripped.lower()

    # Handle simple types
    type_mapping = {
        'string': pa.string(),
        'int64': pa.int64(),
        'int32': pa.int32(),
        'float64': pa.float64(),
        'float32': pa.float32(),
        'double': pa.float64(),  # alias for float64
        'bool': pa.bool_(),
        'boolean': pa.bool_(),
        'date32': pa.date32(),
        'date64': pa.date64(),
        'timestamp': pa.timestamp('ns'),
        'binary': pa.binary(),
    }

    # Handle simple mapped types first
    if data_type_str in type_mapping:
        return type_mapping[data_type_str]

    # Handle complex types with parameters
    if data_type_str.startswith('timestamp[') and data_type_str.endswith(']'):
        # Extract unit from timestamp[unit]
        unit = data_type_str[10:-1]  # Remove 'timestamp[' and ']'
        return pa.timestamp(unit)

    if data_type_str.startswith('decimal128(') and data_type_str.endswith(')'):
        # Extract precision and scale from decimal128(precision,scale)
        params = data_type_str[11:-1]  # Remove 'decimal128(' and ')'
        try:
            precision, scale = map(int, params.split(','))
            return pa.decimal128(precision, scale)
        except (ValueError, TypeError):
            pass

    if data_type_str.startswith('list<') and data_type_str.endswith('>'):
        # Extract inner type from list<type>
        inner_type_str = data_type_str[5:-1]  # Remove 'list<' and '>'
        inner_type = _parse_data_type(inner_type_str)
        if inner_type:
            return pa.list_(inner_type)

    # Default to string for unknown types
    return pa.string()


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

    all_columns = []
    constants_list = []
    expressions_list = []
    calculated_list = []

    # Parse constants and convert to CalculatedColumns
    for const_def in schema_config.get("constants", []):
        # Only pass data_type if dataType is provided in schema
        const_kwargs = {
            "name": const_def["name"],
            "value": const_def["value"],
            "description": const_def.get("description")
        }
        if "dataType" in const_def:
            const_kwargs["data_type"] = _parse_data_type(const_def["dataType"])

        constant = ConstantColumn(**const_kwargs)
        constants_list.append(constant)
        all_columns.append(constant.to_calculated_column())

    # Parse expressions and convert to CalculatedColumns
    for expr_def in schema_config.get("expressions", []):
        # Only pass data_type if dataType is provided in schema
        expr_kwargs = {
            "name": expr_def["name"],
            "expression": expr_def["expression"],
            "description": expr_def.get("description"),
            "dependencies": expr_def.get("dependencies", [])
        }
        if "dataType" in expr_def:
            expr_kwargs["data_type"] = _parse_data_type(expr_def["dataType"])

        expression = ExpressionColumn(**expr_kwargs)
        expressions_list.append(expression)
        all_columns.append(expression.to_calculated_column())

    # Parse calculated columns (treat 'function' as 'expression' for backward compatibility)
    for calc_def in schema_config.get("calculated", []):
        # Handle both 'function' and 'expression' keys for backward compatibility
        expression_value = calc_def.get("expression", calc_def.get("function", ""))

        # Only pass data_type if dataType is provided in schema
        calc_kwargs = {
            "name": calc_def["name"],
            "expression": expression_value,
            "dependencies": calc_def.get("dependencies", []),
            "description": calc_def.get("description")
        }
        if "dataType" in calc_def:
            calc_kwargs["data_type"] = _parse_data_type(calc_def["dataType"])

        calculated_col = CalculatedColumn(**calc_kwargs)
        # Add function attribute for backward compatibility
        calculated_col.function = calc_def.get("function", expression_value)
        calculated_list.append(calculated_col)
        all_columns.append(calculated_col)

    # Handle partition columns (just store them, don't process as calculated columns)
    partition_columns = schema_config.get("partitionColumns", [])

    # If no columns were defined and no partition columns, return None
    if not all_columns and not partition_columns:
        return None

    # Create configuration with our actual interface plus compatibility attributes
    config = CalculatedColumnsConfig(
        columns=all_columns,
        fail_on_error=schema_config.get("failOnError", True),
        add_metadata=schema_config.get("addMetadata", False),
        validate_dependencies=schema_config.get("validateDependencies", True),
        constants=constants_list,
        expressions=expressions_list,
        calculated=calculated_list,
        partition_columns=partition_columns
    )

    return CalculatedColumnsProcessor(config)


def create_calculated_columns_processor_from_metadata(
    metadata: Dict[str, Any]
) -> Optional[CalculatedColumnsProcessor]:
    """Create a CalculatedColumnsProcessor from metadata configuration.

    Args:
        metadata: Dictionary containing calculated columns metadata

    Returns:
        CalculatedColumnsProcessor instance or None if no configuration found
    """
    calculated_columns_config = metadata.get("x-calculatedColumns")
    if not calculated_columns_config:
        return None

    return create_calculated_columns_processor_from_schema(calculated_columns_config)


def validate_calculated_columns_schema(schema_config: Dict[str, Any]) -> List[str]:
    """Validate calculated columns schema configuration.

    Args:
        schema_config: Dictionary containing the x-calculatedColumns configuration

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    if not isinstance(schema_config, dict):
        errors.append("Schema configuration must be a dictionary")
        return errors

    # Validate constants
    for i, const_def in enumerate(schema_config.get("constants", [])):
        if not isinstance(const_def, dict):
            errors.append(f"Constant at index {i} must be a dictionary")
            continue

        if "name" not in const_def:
            errors.append(f"Constant at index {i} missing required 'name' field")
        if "value" not in const_def:
            errors.append(f"Constant at index {i} missing required 'value' field")

    # Validate expressions
    for i, expr_def in enumerate(schema_config.get("expressions", [])):
        if not isinstance(expr_def, dict):
            errors.append(f"Expression at index {i} must be a dictionary")
            continue

        if "name" not in expr_def:
            errors.append(f"Expression at index {i} missing required 'name' field")
        if "expression" not in expr_def:
            errors.append(f"Expression at index {i} missing required 'expression' field")

    # Validate calculated columns
    for i, calc_def in enumerate(schema_config.get("calculated", [])):
        if not isinstance(calc_def, dict):
            errors.append(f"Calculated column at index {i} must be a dictionary")
            continue

        if "name" not in calc_def:
            errors.append(f"Calculated column at index {i} missing required 'name' field")
        if "function" not in calc_def and "expression" not in calc_def:
            errors.append(f"Calculated column at index {i} missing required 'function' or 'expression' field")

    return errors
