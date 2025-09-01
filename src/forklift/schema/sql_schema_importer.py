from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


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

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES = {
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "double", "bool", "string", "binary",
        "date32", "date64", "timestamp[s]", "timestamp[ms]",
        "timestamp[us]", "timestamp[ns]", "duration[s]", "duration[ms]",
        "duration[us]", "duration[ns]", "decimal128(10,2)",
        "list<string>", "struct", "dictionary<values=string, indices=int32>"
    }

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
        self.include_patterns: List[str] = self.schema.get("include", [])

        # Extract SQL-specific configurations
        self.sql_include: List[str] = self.sql_ext.get("include", [])
        self.tables: List[Dict[str, Any]] = self.sql_ext.get("tables", [])
        self.parquet_type_mapping: Dict[str, Any] = self.sql_ext.get("parquetTypeMapping", {})

        # Merge include patterns
        self.all_include_patterns = self._merge_include_patterns()

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    def _merge_include_patterns(self) -> List[str]:
        """Merge root-level and x-sql include patterns, removing duplicates while preserving order."""
        merged = []
        seen = set()

        # Add root-level includes first
        for pattern in self.include_patterns:
            if pattern not in seen:
                merged.append(pattern)
                seen.add(pattern)

        # Add x-sql includes
        for pattern in self.sql_include:
            if pattern not in seen:
                merged.append(pattern)
                seen.add(pattern)

        # Add patterns derived from table configurations
        for table in self.tables:
            pattern = self._derive_table_pattern(table)
            if pattern and pattern not in seen:
                merged.append(pattern)
                seen.add(pattern)

        # Default to all tables if no patterns specified
        if not merged:
            merged = ["*.*"]

        return merged

    def _derive_table_pattern(self, table: Dict[str, Any]) -> Optional[str]:
        """Derive an include pattern from a table configuration."""
        select = table.get("select", {})

        # Check for explicit pattern override
        if "pattern" in select:
            return select["pattern"]

        # Derive from schema and name
        schema = select.get("schema")
        name = select.get("name")

        if schema and name:
            return f"{schema}.{name}"
        elif name:
            return name

        return None

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self._validate_json_schema_structure())

        # Validate SQL-specific extension
        errors.extend(self._validate_sql_extension())

        # Validate include patterns
        errors.extend(self._validate_include_patterns())

        # Validate table configurations
        errors.extend(self._validate_tables())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(f"  - {err}" for err in errors)
            raise SchemaValidationError(error_msg)

    def _validate_json_schema_structure(self) -> List[str]:
        """Validate basic JSON Schema 2020-12 structure."""
        errors = []

        # Required JSON Schema fields
        if not self.schema.get("$schema"):
            errors.append("Missing required '$schema' field")
        elif self.schema["$schema"] != "https://json-schema.org/draft/2020-12/schema":
            errors.append("Schema must reference JSON Schema 2020-12 standard")

        if not self.schema.get("$id"):
            errors.append("Missing required '$id' field")
        elif not self.schema["$id"].startswith("https://github.com/cornyhorse/forklift/schema-standards/"):
            errors.append("Schema $id must follow the standard GitHub URL pattern")

        if not self.schema.get("title"):
            errors.append("Missing required 'title' field")

        if self.schema.get("type") != "object":
            errors.append("Schema type must be 'object'")

        return errors

    def _validate_sql_extension(self) -> List[str]:
        """Validate x-sql extension structure and values."""
        errors = []

        # x-sql extension is optional, but if present must be valid
        if self.sql_ext:
            # Validate include array
            if "include" in self.sql_ext:
                if not isinstance(self.sql_include, list):
                    errors.append("x-sql.include must be an array")
                else:
                    for i, pattern in enumerate(self.sql_include):
                        if not isinstance(pattern, str):
                            errors.append(f"x-sql.include[{i}] must be a string")

            # Validate tables array
            if "tables" in self.sql_ext:
                if not isinstance(self.tables, list):
                    errors.append("x-sql.tables must be an array")

            # Validate parquetTypeMapping
            if "parquetTypeMapping" in self.sql_ext:
                if not isinstance(self.parquet_type_mapping, dict):
                    errors.append("x-sql.parquetTypeMapping must be an object")

        return errors

    def _validate_include_patterns(self) -> List[str]:
        """Validate include pattern formats."""
        errors = []

        # Validate root-level include patterns
        if self.include_patterns:
            if not isinstance(self.include_patterns, list):
                errors.append("include must be an array")
            else:
                for i, pattern in enumerate(self.include_patterns):
                    if not isinstance(pattern, str):
                        errors.append(f"include[{i}] must be a string")
                    elif not self._is_valid_include_pattern(pattern):
                        errors.append(f"Invalid include pattern '{pattern}' at index {i}")

        return errors

    def _validate_tables(self) -> List[str]:
        """Validate table configurations."""
        errors = []

        for i, table in enumerate(self.tables):
            if not isinstance(table, dict):
                errors.append(f"Table {i} configuration must be an object")
                continue

            # Validate required select field
            select = table.get("select")
            if not select:
                errors.append(f"Table {i} missing required 'select' configuration")
            elif not isinstance(select, dict):
                errors.append(f"Table {i} select must be an object")
            else:
                errors.extend(self._validate_table_select(select, i))

            # Validate optional columns field
            columns = table.get("columns")
            if columns:
                if not isinstance(columns, dict):
                    errors.append(f"Table {i} columns must be an object")
                else:
                    errors.extend(self._validate_table_columns(columns, i))

            # Validate optional required field
            required = table.get("required")
            if required:
                if not isinstance(required, list):
                    errors.append(f"Table {i} required must be an array")
                else:
                    for j, req_col in enumerate(required):
                        if not isinstance(req_col, str):
                            errors.append(f"Table {i} required[{j}] must be a string")

        return errors

    def _validate_table_select(self, select: Dict[str, Any], table_index: int) -> List[str]:
        """Validate table select configuration."""
        errors = []

        # Must have at least one selection method
        has_schema_name = "schema" in select and "name" in select
        has_name_only = "name" in select and "schema" not in select
        has_pattern = "pattern" in select

        if not (has_schema_name or has_name_only or has_pattern):
            errors.append(f"Table {table_index} select must have 'name', 'schema'+'name', or 'pattern'")

        # Validate individual fields
        if "schema" in select and not isinstance(select["schema"], str):
            errors.append(f"Table {table_index} select.schema must be a string")

        if "name" in select and not isinstance(select["name"], str):
            errors.append(f"Table {table_index} select.name must be a string")

        if "pattern" in select:
            pattern = select["pattern"]
            if not isinstance(pattern, str):
                errors.append(f"Table {table_index} select.pattern must be a string")
            elif not self._is_valid_include_pattern(pattern):
                errors.append(f"Table {table_index} invalid select.pattern '{pattern}'")

        return errors

    def _validate_table_columns(self, columns: Dict[str, Any], table_index: int) -> List[str]:
        """Validate table column configurations."""
        errors = []

        for col_name, col_def in columns.items():
            if not isinstance(col_def, dict):
                errors.append(f"Table {table_index} column '{col_name}' must be an object")
                continue

            # Validate column type
            col_type = col_def.get("type")
            if col_type:
                valid_types = {"string", "integer", "number", "boolean", "array", "object"}
                if col_type not in valid_types:
                    errors.append(f"Table {table_index} column '{col_name}' invalid type '{col_type}'")

            # Validate Parquet type
            parquet_type = col_def.get("parquetType")
            if parquet_type and not self._is_valid_parquet_type(parquet_type):
                errors.append(f"Table {table_index} column '{col_name}' invalid Parquet type '{parquet_type}'")

            # Validate constraints based on type
            if col_type == "integer":
                minimum = col_def.get("minimum")
                maximum = col_def.get("maximum")
                if minimum is not None and not isinstance(minimum, (int, float)):
                    errors.append(f"Table {table_index} column '{col_name}' invalid minimum value")
                if maximum is not None and not isinstance(maximum, (int, float)):
                    errors.append(f"Table {table_index} column '{col_name}' invalid maximum value")

            elif col_type == "string":
                min_length = col_def.get("minLength")
                max_length = col_def.get("maxLength")
                pattern = col_def.get("pattern")

                if min_length is not None and (not isinstance(min_length, int) or min_length < 0):
                    errors.append(f"Table {table_index} column '{col_name}' invalid minLength")
                if max_length is not None and (not isinstance(max_length, int) or max_length < 0):
                    errors.append(f"Table {table_index} column '{col_name}' invalid maxLength")
                if pattern is not None:
                    try:
                        re.compile(pattern)
                    except re.error:
                        errors.append(f"Table {table_index} column '{col_name}' invalid regex pattern")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in table columns."""
        errors = []

        for i, table in enumerate(self.tables):
            columns = table.get("columns", {})
            for col_name, col_def in columns.items():
                if isinstance(col_def, dict):
                    parquet_type = col_def.get("parquetType")
                    if parquet_type and not self._is_valid_parquet_type(parquet_type):
                        errors.append(f"Table {i} column '{col_name}' invalid Parquet type '{parquet_type}'")

        return errors

    def _is_valid_include_pattern(self, pattern: str) -> bool:
        """Validate an include pattern format."""
        if not pattern:
            return False

        # Valid patterns: *.*, schema.*, schema.table, table_name
        if pattern == "*.*":
            return True

        if "." in pattern:
            parts = pattern.split(".")
            if len(parts) == 2:
                schema_part, table_part = parts
                # Both parts must be valid identifiers or wildcards
                return self._is_valid_identifier_or_wildcard(schema_part) and self._is_valid_identifier_or_wildcard(table_part)

        # Single identifier (table name)
        return self._is_valid_identifier_or_wildcard(pattern)

    def _is_valid_identifier_or_wildcard(self, name: str) -> bool:
        """Check if a name is a valid SQL identifier or wildcard."""
        if name == "*":
            return True

        # Basic SQL identifier validation (simplified)
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

    def _is_valid_parquet_type(self, parquet_type: str) -> bool:
        """Check if a Parquet type is valid."""
        if parquet_type in self.SUPPORTED_PARQUET_TYPES:
            return True

        # Check for parameterized types like decimal128(precision,scale)
        if parquet_type.startswith("decimal128(") and parquet_type.endswith(")"):
            return True

        # Check for timestamp with timezone
        if parquet_type.startswith("timestamp[") and parquet_type.endswith("]"):
            return True

        # Check for duration types
        if parquet_type.startswith("duration[") and parquet_type.endswith("]"):
            return True

        # Check for list types
        if parquet_type.startswith("list<") and parquet_type.endswith(">"):
            return True

        # Check for dictionary types
        if parquet_type.startswith("dictionary<") and parquet_type.endswith(">"):
            return True

        return False

    def get_sql_extension(self) -> Dict[str, Any]:
        """Get the SQL-specific extension configuration."""
        return self.sql_ext

    def get_include_patterns(self) -> List[str]:
        """Get all resolved include patterns."""
        return self.all_include_patterns

    def get_tables(self) -> List[Dict[str, Any]]:
        """Get the table configurations."""
        return self.tables

    def get_table_by_name(self, schema_name: Optional[str], table_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific table configuration by schema and table name."""
        for table in self.tables:
            select = table.get("select", {})

            # Check for exact match
            if select.get("schema") == schema_name and select.get("name") == table_name:
                return table

            # Check for name-only match when no schema specified
            if not schema_name and select.get("name") == table_name and "schema" not in select:
                return table

        return None

    def get_column_schema(self, schema_name: Optional[str], table_name: str) -> Dict[str, Any]:
        """Get column schema for a specific table."""
        table = self.get_table_by_name(schema_name, table_name)
        if table:
            return table.get("columns", {})
        return {}

    def get_required_columns(self, schema_name: Optional[str], table_name: str) -> List[str]:
        """Get required columns for a specific table."""
        table = self.get_table_by_name(schema_name, table_name)
        if table:
            return table.get("required", [])
        return []

    def matches_include_pattern(self, schema_name: Optional[str], table_name: str) -> bool:
        """Check if a schema/table matches any include pattern."""
        full_name = f"{schema_name}.{table_name}" if schema_name else table_name

        for pattern in self.all_include_patterns:
            if self._matches_pattern(full_name, pattern, schema_name, table_name):
                return True

        return False

    def _matches_pattern(self, full_name: str, pattern: str, schema_name: Optional[str], table_name: str) -> bool:
        """Check if a table matches a specific pattern."""
        if pattern == "*.*":
            return True

        if pattern == table_name:
            return True

        if "." in pattern:
            pattern_schema, pattern_table = pattern.split(".", 1)

            if pattern_schema == "*":
                return pattern_table == "*" or pattern_table == table_name

            if pattern_table == "*":
                return pattern_schema == schema_name

            return pattern_schema == schema_name and pattern_table == table_name

        return pattern == table_name

    def get_sql_to_parquet_mapping(self) -> Dict[str, str]:
        """Get the SQL to Parquet type mapping."""
        mapping = self.parquet_type_mapping.get("sqlToParquet", {})

        # Provide default mappings if not specified
        if not mapping:
            mapping = {
                "INTEGER": "int64",
                "BIGINT": "int64",
                "SMALLINT": "int32",
                "TINYINT": "int8",
                "DECIMAL": "decimal128(10,2)",
                "NUMERIC": "decimal128(10,2)",
                "FLOAT": "float32",
                "DOUBLE": "double",
                "REAL": "float32",
                "BOOLEAN": "bool",
                "VARCHAR": "string",
                "TEXT": "string",
                "CHAR": "string",
                "DATE": "date32",
                "TIMESTAMP": "timestamp[us]",
                "DATETIME": "timestamp[us]",
                "TIME": "duration[us]",
                "INTERVAL": "duration[us]",
                "BINARY": "binary",
                "VARBINARY": "binary",
                "BLOB": "binary",
                "ARRAY": "list<string>",
                "JSON": "struct",
                "JSONB": "struct",
                "UUID": "string"
            }

        return mapping

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema


__all__ = ["SqlSchemaImporter", "SchemaValidationError"]
