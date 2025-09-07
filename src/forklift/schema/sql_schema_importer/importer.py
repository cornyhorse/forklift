"""Main SQL Schema Importer class that orchestrates all components."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .core_validator import CoreSchemaValidator
from .exceptions import SchemaValidationError
from .parquet_types import ParquetTypeHandler
from .sql_validator import SqlValidator
from .table_manager import TableManager


class SqlSchemaImporter:
    """Parse a Forklift SQL schema JSON file/dict and expose derived options.

    The schema is expected to follow the internal extension structure present in
    ``schema-standards/20250826-sql.json`` (``x-sql`` root key extension). This class
    performs comprehensive validation to ensure schemas conform to the standard
    and provides complete Parquet data type mapping support.

    Provided conveniences:
      * Access to the raw schema dict (``.schema``)
      * Extraction of Forklift SQL extension (``.sql_ext``)
      * Comprehensive schema validation with detailed error reporting
      * Table/schema pattern validation and resolution
      * Parquet data type mapping and validation
      * SQL-specific configuration validation (connection, query patterns)
    """

    # Expose SUPPORTED_PARQUET_TYPES for backward compatibility
    SUPPORTED_PARQUET_TYPES = ParquetTypeHandler.SUPPORTED_PARQUET_TYPES

    def __init__(self, schema: Union[str, Path, Dict[str, Any]], validate: bool = True):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
        elif isinstance(schema, dict):
            self.schema = schema
        else:
            raise TypeError("schema must be path-like or dict")

        # Extract core schema components
        self.sql_ext: Dict[str, Any] = self.schema.get("x-sql", {})

        # Extract SQL-specific configurations with type safety
        tables_raw = self.sql_ext.get("tables", [])
        if isinstance(tables_raw, list):
            self.tables: List[Dict[str, Any]] = tables_raw
        else:
            # Invalid type - will be caught during validation
            self.tables = []

        self.parquet_type_mapping: Dict[str, Any] = self.sql_ext.get("parquetTypeMapping", {})

        # Initialize component handlers
        self.core_validator = CoreSchemaValidator(self.schema)
        self.parquet_handler = ParquetTypeHandler(self.parquet_type_mapping)
        self.sql_validator = SqlValidator(self.sql_ext, self.tables, self.parquet_handler)
        self.table_manager = TableManager(self.tables)

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self.core_validator.validate_json_schema_structure())

        # Validate SQL-specific extension
        errors.extend(self.sql_validator.validate_sql_extension())

        # Validate table configurations
        errors.extend(self.sql_validator.validate_tables())

        # Validate Parquet type mappings
        errors.extend(self.parquet_handler.validate_parquet_types(self.tables))

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(f"  - {err}" for err in errors)
            raise SchemaValidationError(error_msg)

    def get_table_list(self) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema configuration."""
        return self.table_manager.get_table_list()

    def get_sql_extension(self) -> Dict[str, Any]:
        """Get the SQL-specific extension configuration."""
        return self.sql_ext

    def get_include_patterns(self) -> List[str]:
        """Get all resolved include patterns - deprecated, returns empty list."""
        return self.table_manager.get_include_patterns()

    def get_tables(self) -> List[Dict[str, Any]]:
        """Get the table configurations."""
        return self.tables

    def get_table_by_name(self, schema_name: Optional[str], table_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific table configuration by schema and table name."""
        return self.table_manager.get_table_by_name(schema_name, table_name)

    def get_column_schema(self, schema_name: Optional[str], table_name: str) -> Dict[str, Any]:
        """Get column schema for a specific table."""
        return self.table_manager.get_column_schema(schema_name, table_name)

    def get_required_columns(self, schema_name: Optional[str], table_name: str) -> List[str]:
        """Get required columns for a specific table."""
        return self.table_manager.get_required_columns(schema_name, table_name)

    def matches_include_pattern(self, schema_name: Optional[str], table_name: str) -> bool:
        """Check if a schema/table matches any include pattern - deprecated."""
        return self.table_manager.matches_include_pattern(schema_name, table_name)

    def get_sql_to_parquet_mapping(self) -> Dict[str, str]:
        """Get the SQL to Parquet type mapping."""
        return self.parquet_handler.get_sql_to_parquet_mapping()

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema

    # Backward compatibility methods for tests
    def _is_valid_parquet_type(self, parquet_type: str) -> bool:
        """Check if a Parquet type is valid - backward compatibility method."""
        return self.parquet_handler.is_valid_parquet_type(parquet_type)

    def _is_valid_include_pattern(self, pattern: str) -> bool:
        """Validate an include pattern format - backward compatibility method."""
        return TableManager.is_valid_include_pattern(pattern)

    def _is_valid_identifier_or_wildcard(self, name: str) -> bool:
        """Check if a name is a valid SQL identifier or wildcard - backward compatibility method."""
        return TableManager._is_valid_identifier_or_wildcard(name)

    def _validate_tables(self) -> List[str]:
        """Validate table configurations - backward compatibility method."""
        # Create a fresh validator with current tables state for testing
        fresh_validator = SqlValidator(self.sql_ext, self.tables, self.parquet_handler)
        return fresh_validator.validate_tables()
