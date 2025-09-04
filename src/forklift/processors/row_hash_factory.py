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
        separator=schema_config.get("separator", "||")
    )

    # Only create processor if enabled
    if not config.enabled:
        return None

    return RowHashProcessor(config)
