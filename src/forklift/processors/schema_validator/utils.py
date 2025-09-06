"""Utility functions for schema validation."""

from typing import Dict, Any, Optional
import pyarrow as pa
from datetime import datetime

from .core import SchemaValidator
from .config import SchemaValidatorConfig
from .type_converter import TypeConverter


def create_schema_validator_from_json(schema_json: Dict[str, Any], config: Optional[SchemaValidatorConfig] = None) -> SchemaValidator:
    """Create a schema validator from a JSON schema definition.

    Args:
        schema_json: JSON schema definition
        config: Optional validation configuration

    Returns:
        SchemaValidator instance
    """
    return SchemaValidator(schema_json, config)


def create_schema_from_batch(batch: pa.RecordBatch, include_nullability: bool = True) -> Dict[str, Any]:
    """Create a schema definition from a PyArrow RecordBatch.

    Args:
        batch: PyArrow RecordBatch to analyze
        include_nullability: Whether to include nullability information

    Returns:
        Schema definition dictionary
    """
    columns = []

    for i, field in enumerate(batch.schema):
        column_def = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable if include_nullability else True
        }

        # Add basic constraints based on data type
        if TypeConverter.is_numeric_type(field.type):
            column_def["constraints"] = {}

        columns.append(column_def)

    return {
        "columns": columns,
        "metadata": {
            "created_from_batch": True,
            "creation_timestamp": datetime.now().isoformat(),
            "total_columns": len(columns)
        }
    }
