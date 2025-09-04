"""Factory functions for creating row hash processors from schema configurations."""

from __future__ import annotations
from typing import Dict, Any, Optional

from .row_hash import RowHashProcessor, RowHashConfig


def create_row_hash_processor_from_schema(
    schema_config: Dict[str, Any]
) -> Optional[RowHashProcessor]:
    """Create a RowHashProcessor from schema configuration.

    Args:
        schema_config: Dictionary containing the x-rowHash configuration

    Returns:
        RowHashProcessor instance or None if disabled or no configuration found
    """
    if not schema_config:
        return None

    # Create configuration from schema
    config = RowHashConfig(
        enabled=schema_config.get("enabled", False),
        column_name=schema_config.get("columnName", "row_hash"),
        algorithm=schema_config.get("algorithm", "sha256"),
        include_columns=schema_config.get("includeColumns"),
        exclude_columns=schema_config.get("excludeColumns", []),
        null_value=schema_config.get("nullValue", "NULL"),
        separator=schema_config.get("separator", "||"),

        # New metadata options
        input_hash_enabled=schema_config.get("inputHashEnabled", False),
        input_hash_column_name=schema_config.get("inputHashColumnName", "_input_hash"),
        source_uri_enabled=schema_config.get("sourceUriEnabled", False),
        source_uri_column_name=schema_config.get("sourceUriColumnName", "_source_uri"),
        ingested_at_enabled=schema_config.get("ingestedAtEnabled", False),
        ingested_at_column_name=schema_config.get("ingestedAtColumnName", "_ingested_at_utc"),
        row_number_enabled=schema_config.get("rowNumberEnabled", False),
        source_row_number_column_name=schema_config.get("sourceRowNumberColumnName", "_rownum_in_source_file"),
        processing_row_number_column_name=schema_config.get("processingRowNumberColumnName", "_rownum")
    )

    # Only create processor if at least one feature is enabled
    if not (config.enabled or config.input_hash_enabled or config.source_uri_enabled or
            config.ingested_at_enabled or config.row_number_enabled):
        return None

    return RowHashProcessor(config)
