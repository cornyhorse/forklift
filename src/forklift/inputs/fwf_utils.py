"""Utility functions for creating FWF configurations from schema files."""

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from .config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


def create_fwf_config_from_schema(schema_path: Path) -> FwfInputConfig:
    """Create FWF input configuration from a schema standard file.

    Args:
        schema_path: Path to the JSON schema file

    Returns:
        Configured FwfInputConfig instance

    Raises:
        FileNotFoundError: If schema file doesn't exist
        ValueError: If schema format is invalid
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    if 'x-fwf' not in schema:
        raise ValueError("Schema file does not contain x-fwf configuration")

    fwf_config = schema['x-fwf']

    # Extract basic configuration
    encoding = fwf_config.get('encoding', 'utf-8')
    trim_whitespace = fwf_config.get('trim', {}).get('rstrip', True)

    # Handle null values
    null_values = fwf_config.get('nulls')

    # Handle footer detection
    footer_detection = fwf_config.get('footer')

    # Check for conditional schemas
    conditional_config = fwf_config.get('conditionalSchemas')

    if conditional_config:
        # Handle conditional FWF
        flag_column_spec = conditional_config.get('flagColumn')
        if not flag_column_spec:
            raise ValueError("Conditional schemas require flagColumn specification")

        flag_column = FwfFieldSpec(
            name=flag_column_spec['name'],
            start=flag_column_spec['start'],
            length=flag_column_spec['length'],
            parquet_type=flag_column_spec.get('parquetType', 'string')
        )

        conditional_schemas = []
        for schema_spec in conditional_config.get('schemas', []):
            fields = []
            for field_spec in schema_spec.get('fields', []):
                field = FwfFieldSpec(
                    name=field_spec['name'],
                    start=field_spec['start'],
                    length=field_spec['length'],
                    align=field_spec.get('align', 'left'),
                    pad=field_spec.get('pad', ' '),
                    parquet_type=field_spec.get('parquetType', 'string'),
                    trim=field_spec.get('trim', True)
                )
                fields.append(field)

            conditional_schema = FwfConditionalSchema(
                flag_value=schema_spec['flagValue'],
                description=schema_spec.get('description', ''),
                fields=fields
            )
            conditional_schemas.append(conditional_schema)

        return FwfInputConfig(
            encoding=encoding,
            conditional_schemas=conditional_schemas,
            flag_column=flag_column,
            trim_whitespace=trim_whitespace,
            null_values=null_values,
            footer_detection=footer_detection
        )

    else:
        # Handle standard FWF
        fields = []
        for field_spec in fwf_config.get('fields', []):
            field = FwfFieldSpec(
                name=field_spec['name'],
                start=field_spec['start'],
                length=field_spec['length'],
                align=field_spec.get('align', 'left'),
                pad=field_spec.get('pad', ' '),
                parquet_type=field_spec.get('parquetType', 'string'),
                required=field_spec.get('required', False),
                trim=field_spec.get('trim', True)
            )
            fields.append(field)

        return FwfInputConfig(
            encoding=encoding,
            fields=fields,
            trim_whitespace=trim_whitespace,
            null_values=null_values,
            footer_detection=footer_detection
        )


def create_simple_fwf_config(field_specs: List[Dict[str, Any]], **kwargs) -> FwfInputConfig:
    """Create a simple FWF configuration from field specifications.

    Args:
        field_specs: List of field specification dictionaries
        **kwargs: Additional configuration options

    Returns:
        Configured FwfInputConfig instance

    Example:
        config = create_simple_fwf_config([
            {'name': 'id', 'start': 1, 'length': 10, 'parquet_type': 'int64'},
            {'name': 'name', 'start': 11, 'length': 30, 'parquet_type': 'string'},
        ])
    """
    fields = []
    for spec in field_specs:
        field = FwfFieldSpec(
            name=spec['name'],
            start=spec['start'],
            length=spec['length'],
            align=spec.get('align', 'left'),
            pad=spec.get('pad', ' '),
            parquet_type=spec.get('parquet_type', 'string'),
            required=spec.get('required', False),
            trim=spec.get('trim', True)
        )
        fields.append(field)

    return FwfInputConfig(
        fields=fields,
        encoding=kwargs.get('encoding', 'utf-8'),
        trim_whitespace=kwargs.get('trim_whitespace', True),
        skip_blank_lines=kwargs.get('skip_blank_lines', True),
        null_values=kwargs.get('null_values'),
        footer_detection=kwargs.get('footer_detection')
    )
