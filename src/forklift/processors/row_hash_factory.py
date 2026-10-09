"""Factory functions for creating row hash processors from schema configurations."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .row_hash import (
    HASH_VERSION_CURRENT,
    HASH_VERSION_LEGACY,
    RowHashConfig,
    RowHashProcessor,
)


def _config_from_schema(schema_config: Dict[str, Any]) -> RowHashConfig:
    """Build (and validate) the RowHashConfig for an x-rowHash dictionary."""
    # "hashVersion" selects the encoding: 2 = injective (default), 1 = legacy (verify old hashes)
    legacy_encoding = bool(schema_config.get("legacyEncoding", False))
    hash_version = schema_config.get("hashVersion")
    if hash_version is not None:
        if hash_version not in (HASH_VERSION_LEGACY, HASH_VERSION_CURRENT):
            raise ValueError(
                f"Unsupported hashVersion: {hash_version!r} "
                f"(supported: {HASH_VERSION_LEGACY}, {HASH_VERSION_CURRENT})"
            )
        if "legacyEncoding" in schema_config and legacy_encoding != (
            hash_version == HASH_VERSION_LEGACY
        ):
            raise ValueError("hashVersion and legacyEncoding contradict each other")
        legacy_encoding = hash_version == HASH_VERSION_LEGACY

    return RowHashConfig(
        enabled=schema_config.get("enabled", False),
        column_name=schema_config.get("columnName", "row_hash"),
        algorithm=schema_config.get("algorithm", "sha256"),
        include_columns=schema_config.get("includeColumns"),
        exclude_columns=schema_config.get("excludeColumns", []),
        null_value=schema_config.get("nullValue", "NULL"),
        separator=schema_config.get("separator", "||"),
        allow_weak_hash=schema_config.get("allowWeakHash", False),
        legacy_encoding=legacy_encoding,
        # New metadata options
        input_hash_enabled=schema_config.get("inputHashEnabled", False),
        input_hash_column_name=schema_config.get("inputHashColumnName", "_input_hash"),
        source_uri_enabled=schema_config.get("sourceUriEnabled", False),
        source_uri_column_name=schema_config.get("sourceUriColumnName", "_source_uri"),
        ingested_at_enabled=schema_config.get("ingestedAtEnabled", False),
        ingested_at_column_name=schema_config.get("ingestedAtColumnName", "_ingested_at_utc"),
        row_number_enabled=schema_config.get("rowNumberEnabled", False),
        source_row_number_column_name=schema_config.get(
            "sourceRowNumberColumnName", "_rownum_in_source_file"
        ),
        processing_row_number_column_name=schema_config.get(
            "processingRowNumberColumnName", "_rownum"
        ),
    )


def _any_feature_enabled(config: RowHashConfig) -> bool:
    return bool(
        config.enabled
        or config.input_hash_enabled
        or config.source_uri_enabled
        or config.ingested_at_enabled
        or config.row_number_enabled
    )


def create_row_hash_processor_from_schema(
    schema_config: Dict[str, Any],
) -> Optional[RowHashProcessor]:
    """Create a RowHashProcessor from schema configuration.

    Args:
        schema_config: Dictionary containing the x-rowHash configuration

    Returns:
        RowHashProcessor instance or None if disabled or no configuration found
    """
    if not schema_config:
        return None

    config = _config_from_schema(schema_config)

    # Only create processor if at least one feature is enabled
    if not _any_feature_enabled(config):
        return None

    return RowHashProcessor(config)


def row_hash_output_columns(schema_config: Optional[Dict[str, Any]]) -> List[str]:
    """Names of the columns the row hash processor adds, in the order it adds them.

    Lets a caller compute the final output schema, and detect name collisions with the data
    columns, before any batch is processed. ``schema_config`` is the inner ``x-rowHash``
    dictionary (camelCase keys, defaults as in ``create_row_hash_processor_from_schema``); the
    result is exactly the columns ``process_batch`` of the processor created from the same
    dictionary appends: the output hash (``columnName``, default ``row_hash``), the input hash
    (``inputHashColumnName``), the source URI (``sourceUriColumnName``), the ingestion timestamp
    (``ingestedAtColumnName``) and, with ``rowNumberEnabled``, the source row number
    (``sourceRowNumberColumnName``) followed by the processing row number
    (``processingRowNumberColumnName``). Empty if no processor would be created.

    Raises:
        ValueError: For the same invalid configuration the factory rejects.
    """
    if not schema_config:
        return []

    config = _config_from_schema(schema_config)
    if not _any_feature_enabled(config):
        return []
    return config.output_column_names()
