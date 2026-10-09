from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .types.data_types import is_valid_parquet_type
from .validation_utils import bounds_inverted, regex_error, resolve_json_types


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""

    pass


# Schema/table names end up in SQL text (quoted by the input layer), so they are validated here, at
# schema-load time, with an allow-list: unicode letters/digits/underscore, plus space (legal in
# quoted identifiers such as "Order Details") and . - $ # @. Quotes, semicolons, backslashes,
# comment markers and control characters are never acceptable.
_IDENTIFIER_RE = re.compile(r"[\w#@][\w #@$.\-]*")
_MAX_IDENTIFIER_LENGTH = 128
_FORBIDDEN_CATEGORIES = {"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"}
_FORBIDDEN_IDENTIFIER_CHARS = set(";'\"`\\")
_COMMENT_MARKERS = ("--", "/*", "*/")

# outputName becomes a file name inside the output directory: a plain stem, no separators.
_OUTPUT_NAME_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


def sql_identifier_problem(value: Any) -> Optional[str]:
    """Return why ``value`` is not an acceptable schema/table name, or None if it is fine."""
    if not isinstance(value, str):
        return "must be a string"
    if not value:
        return "must not be empty"
    if len(value) > _MAX_IDENTIFIER_LENGTH:
        return f"is longer than {_MAX_IDENTIFIER_LENGTH} characters"
    if value != value.strip():
        return "must not start or end with whitespace"
    if any(unicodedata.category(char) in _FORBIDDEN_CATEGORIES for char in value):
        return "contains control or non-printing characters"
    if any(marker in value for marker in _COMMENT_MARKERS):
        return "contains an SQL comment marker"
    if any(char in _FORBIDDEN_IDENTIFIER_CHARS for char in value):
        return "contains a quote, semicolon or backslash"
    if not _IDENTIFIER_RE.fullmatch(value):
        return (
            "is not a plausible identifier (unicode letters, digits, '_', space, '.', '-', '$', "
            "'#' and '@' only)"
        )
    return None


def output_name_problem(value: Any) -> Optional[str]:
    """Return why ``value`` is not a plain output file stem, or None if it is fine."""
    if not isinstance(value, str):
        return "must be a string"
    if not value:
        return "must not be empty"
    if len(value) > _MAX_IDENTIFIER_LENGTH:
        return f"is longer than {_MAX_IDENTIFIER_LENGTH} characters"
    if ".." in value or value.endswith("."):
        return "must not contain '..' or end with '.'"
    if not _OUTPUT_NAME_RE.fullmatch(value):
        return (
            "must be a plain file name stem (letters, digits, '_', '-' and '.' only; "
            "no path separators)"
        )
    return None


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
      * Table selection by explicit schema/name (pattern-based selection is rejected)
      * Parquet data type mapping and validation
      * SQL-specific configuration validation (connection, query patterns)
    """

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES = {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "double",
        "bool",
        "string",
        "binary",
        "date32",
        "date64",
        "timestamp[s]",
        "timestamp[ms]",
        "timestamp[us]",
        "timestamp[ns]",
        "duration[s]",
        "duration[ms]",
        "duration[us]",
        "duration[ns]",
        "decimal128(10,2)",
        "list<string>",
        "struct",
        "dictionary<values=string, indices=int32>",
    }

    # Default SQL type -> Parquet type mapping. A user ``x-sql.parquetTypeMapping.sqlToParquet`` is
    # merged over these (entries not mentioned keep their default).
    #  * FLOAT is a double precision float on every supported database; REAL is single precision.
    #  * DECIMAL/NUMERIC cannot be mapped correctly from the type name alone. The value here is
    #    only the fallback for when the column's real precision/scale is unknown (it is wide on
    #    purpose); ``get_parquet_type_for_sql_type`` builds ``decimal128(p,s)`` from the real
    #    precision/scale, and a user override of DECIMAL/NUMERIC always wins.
    DEFAULT_SQL_TO_PARQUET_MAPPING: Dict[str, str] = {
        "INTEGER": "int64",
        "BIGINT": "int64",
        "SMALLINT": "int16",
        "TINYINT": "int8",
        "DECIMAL": "decimal128(38,9)",
        "NUMERIC": "decimal128(38,9)",
        "FLOAT": "double",
        "DOUBLE": "double",
        "REAL": "float32",
        "BOOLEAN": "bool",
        "VARCHAR": "string",
        "TEXT": "string",
        "CHAR": "string",
        "DATE": "date32",
        "TIMESTAMP": "timestamp[us]",
        "DATETIME": "timestamp[us]",
        "TIME": "time64[us]",
        "INTERVAL": "duration[us]",
        "BINARY": "binary",
        "VARBINARY": "binary",
        "BLOB": "binary",
        "ARRAY": "list<string>",
        "JSON": "struct",
        "JSONB": "struct",
        "UUID": "string",
    }

    def __init__(self, schema: Union[str, Path, Dict[str, Any]], validate: bool = True):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
            if not isinstance(self.schema, dict):
                raise SchemaValidationError(f"{schema}: schema root must be a JSON object")
        elif isinstance(schema, dict):
            self.schema = schema
        else:
            raise TypeError("schema must be path-like or dict")

        # Extract core schema components (malformed values are replaced by empty ones here and
        # reported by validation)
        sql_ext = self.schema.get("x-sql", {})
        self.sql_ext: Dict[str, Any] = sql_ext if isinstance(sql_ext, dict) else {}

        # Extract SQL-specific configurations with type safety
        tables_raw = self.sql_ext.get("tables", [])
        if isinstance(tables_raw, list):
            self.tables: List[Dict[str, Any]] = tables_raw
        else:
            # Invalid type - will be caught during validation
            self.tables = []

        self.parquet_type_mapping: Dict[str, Any] = self.sql_ext.get("parquetTypeMapping", {})

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    def get_table_list(self) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema configuration.

        Returns:
            List of tuples (schema_name, table_name, output_name)
        """
        table_list = []
        for table in self.tables:
            if not isinstance(table, dict) or not isinstance(table.get("select"), dict):
                continue
            select = table["select"]
            schema_name = select.get("schema", "default")
            table_name = select.get("name")
            output_name = table.get("outputName")

            if table_name:
                table_list.append((schema_name, table_name, output_name))

        return table_list

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self._validate_json_schema_structure())

        # Validate SQL-specific extension
        errors.extend(self._validate_sql_extension())

        # Validate table configurations
        errors.extend(self._validate_tables())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(
                f"  - {err}" for err in errors
            )
            raise SchemaValidationError(error_msg)

    def _validate_json_schema_structure(self) -> List[str]:
        """Validate basic JSON Schema 2020-12 structure."""
        errors = []

        # Required JSON Schema fields
        if not self.schema.get("$schema"):
            errors.append("Missing required '$schema' field")
        elif self.schema["$schema"] != "https://json-schema.org/draft/2020-12/schema":
            errors.append("Schema must reference JSON Schema 2020-12 standard")

        schema_id = self.schema.get("$id")
        if not schema_id:
            errors.append("Missing required '$id' field")
        elif not isinstance(schema_id, str):
            errors.append("'$id' must be a string")
        elif not schema_id.startswith("https://github.com/cornyhorse/forklift/schema-standards/"):
            errors.append("Schema $id must follow the standard GitHub URL pattern")

        if not self.schema.get("title"):
            errors.append("Missing required 'title' field")

        if self.schema.get("type") != "object":
            errors.append("Schema type must be 'object'")

        return errors

    def _validate_sql_extension(self) -> List[str]:
        """Validate x-sql extension structure and values."""
        errors = []

        if "x-sql" in self.schema and not isinstance(self.schema["x-sql"], dict):
            errors.append("x-sql must be an object")
            return errors

        # x-sql extension is optional, but if present must be valid
        if self.sql_ext:
            # Validate tables array - check the original raw value, not the processed self.tables
            if "tables" in self.sql_ext:
                tables_raw = self.sql_ext["tables"]
                if not isinstance(tables_raw, list):
                    errors.append("x-sql.tables must be an array")

            # Validate parquetTypeMapping
            if "parquetTypeMapping" in self.sql_ext:
                if not isinstance(self.parquet_type_mapping, dict):
                    errors.append("x-sql.parquetTypeMapping must be an object")
                else:
                    errors.extend(self._validate_sql_to_parquet_mapping())

        return errors

    def _validate_sql_to_parquet_mapping(self) -> List[str]:
        """Validate ``x-sql.parquetTypeMapping.sqlToParquet`` (SQL type name -> Parquet type)."""
        mapping = self.parquet_type_mapping.get("sqlToParquet")
        if mapping is None:
            return []
        if not isinstance(mapping, dict):
            return ["x-sql.parquetTypeMapping.sqlToParquet must be an object"]

        errors = []
        for sql_type, parquet_type in mapping.items():
            if not isinstance(sql_type, str) or not sql_type.strip():
                errors.append("x-sql.parquetTypeMapping.sqlToParquet keys must be SQL type names")
            elif not self._is_valid_parquet_type(parquet_type):
                errors.append(
                    f"x-sql.parquetTypeMapping.sqlToParquet['{sql_type}']"
                    f" invalid Parquet type '{parquet_type}'"
                )
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

            # outputName becomes a file name in the output directory
            if table.get("outputName") is not None:
                problem = output_name_problem(table["outputName"])
                if problem:
                    errors.append(f"Table {i} outputName {problem}")

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
            errors.append(
                f"Table {table_index} select must have 'name', 'schema'+'name', or 'pattern'"
            )

        # Validate individual fields. Schema and table names are interpolated (quoted) into SQL
        # later on, so they must look like identifiers, not like SQL.
        for key in ("schema", "name"):
            if key not in select:
                continue
            if not isinstance(select[key], str):
                errors.append(f"Table {table_index} select.{key} must be a string")
                continue
            problem = sql_identifier_problem(select[key])
            if problem:
                errors.append(f"Table {table_index} select.{key} {problem}")

        if "pattern" in select:
            pattern = select["pattern"]
            if not isinstance(pattern, str):
                errors.append(f"Table {table_index} select.pattern must be a string")
            elif not self._is_valid_include_pattern(pattern):
                errors.append(f"Table {table_index} invalid select.pattern '{pattern}'")
            if "name" not in select:
                # get_table_list only returns explicitly named tables, so a pattern-only entry
                # would be silently dropped
                errors.append(
                    f"Table {table_index} select.pattern: pattern-based table selection is not"
                    " supported; list each table explicitly with 'name'"
                )

        return errors

    def _validate_table_columns(self, columns: Dict[str, Any], table_index: int) -> List[str]:
        """Validate table column configurations."""
        errors = []

        for col_name, col_def in columns.items():
            if not isinstance(col_def, dict):
                errors.append(f"Table {table_index} column '{col_name}' must be an object")
                continue

            # Validate column type (string, nullable array such as ["string", "null"], anyOf)
            col_types: List[str] = []
            if col_def.get("type") or "anyOf" in col_def or "oneOf" in col_def:
                col_types, invalid_types, problems = resolve_json_types(col_def)
                for col_type in invalid_types:
                    errors.append(
                        f"Table {table_index} column '{col_name}' invalid type '{col_type}'"
                    )
                for problem in problems:
                    errors.append(f"Table {table_index} column '{col_name}': {problem}")

            # Validate Parquet type
            parquet_type = col_def.get("parquetType")
            if parquet_type and not self._is_valid_parquet_type(parquet_type):
                errors.append(
                    f"Table {table_index} column '{col_name}'"
                    f" invalid Parquet type '{parquet_type}'"
                )

            # Validate constraints based on type
            if "integer" in col_types or "number" in col_types:
                minimum = col_def.get("minimum")
                maximum = col_def.get("maximum")
                if minimum is not None and not isinstance(minimum, (int, float)):
                    errors.append(f"Table {table_index} column '{col_name}' invalid minimum value")
                if maximum is not None and not isinstance(maximum, (int, float)):
                    errors.append(f"Table {table_index} column '{col_name}' invalid maximum value")
                if bounds_inverted(minimum, maximum):
                    errors.append(
                        f"Table {table_index} column '{col_name}' minimum exceeds maximum"
                    )

            if "string" in col_types:
                min_length = col_def.get("minLength")
                max_length = col_def.get("maxLength")
                pattern = col_def.get("pattern")

                if min_length is not None and (not isinstance(min_length, int) or min_length < 0):
                    errors.append(f"Table {table_index} column '{col_name}' invalid minLength")
                if max_length is not None and (not isinstance(max_length, int) or max_length < 0):
                    errors.append(f"Table {table_index} column '{col_name}' invalid maxLength")
                if (
                    isinstance(min_length, int)
                    and isinstance(max_length, int)
                    and min_length > max_length
                ):
                    errors.append(
                        f"Table {table_index} column '{col_name}' minLength exceeds maxLength"
                    )
                if pattern is not None and regex_error(pattern):
                    errors.append(f"Table {table_index} column '{col_name}' invalid regex pattern")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in table columns."""
        errors = []

        for i, table in enumerate(self.tables):
            # Skip invalid table entries (they'll be caught by _validate_tables)
            if not isinstance(table, dict):
                continue

            columns = table.get("columns", {})
            if not isinstance(columns, dict):
                continue  # reported by _validate_tables
            for col_name, col_def in columns.items():
                if isinstance(col_def, dict):
                    parquet_type = col_def.get("parquetType")
                    if parquet_type and not self._is_valid_parquet_type(parquet_type):
                        errors.append(
                            f"Table {i} column '{col_name}' invalid Parquet type '{parquet_type}'"
                        )

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
                return self._is_valid_identifier_or_wildcard(
                    schema_part
                ) and self._is_valid_identifier_or_wildcard(table_part)

        # Single identifier (table name)
        return self._is_valid_identifier_or_wildcard(pattern)

    def _is_valid_identifier_or_wildcard(self, name: str) -> bool:
        """Check if a name is a valid SQL identifier or wildcard."""
        if name == "*":
            return True

        # Basic SQL identifier validation (simplified)
        return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))

    def _is_valid_parquet_type(self, parquet_type: Any) -> bool:
        """Check if a Parquet type is valid (strict: units and parameters are parsed)."""
        return is_valid_parquet_type(parquet_type)

    def get_sql_extension(self) -> Dict[str, Any]:
        """Get the SQL-specific extension configuration."""
        return self.sql_ext

    def get_include_patterns(self) -> List[str]:
        """Get all resolved include patterns - deprecated, returns empty list."""
        # No longer used since we use explicit table lists instead of glob patterns
        return []

    def get_tables(self) -> List[Dict[str, Any]]:
        """Get the table configurations."""
        return self.tables

    def get_table_by_name(
        self, schema_name: Optional[str], table_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get a specific table configuration by schema and table name."""
        for table in self.tables:
            select = table.get("select", {}) if isinstance(table, dict) else {}
            if not isinstance(select, dict):
                continue

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
        """Check if a schema/table matches any include pattern - deprecated."""
        # Since we use explicit table lists now, this always returns True
        # Individual tables are explicitly listed in the schema
        return True

    def _matches_pattern(
        self, full_name: str, pattern: str, schema_name: Optional[str], table_name: str
    ) -> bool:
        """Check if a table matches a specific pattern - deprecated."""
        # No longer used since we use explicit table lists instead of glob patterns
        return True

    def _user_sql_to_parquet_mapping(self) -> Dict[str, str]:
        """The user's ``sqlToParquet`` entries, keyed by upper-cased SQL type name."""
        mapping = (
            self.parquet_type_mapping.get("sqlToParquet")
            if isinstance(self.parquet_type_mapping, dict)
            else None
        )
        if not isinstance(mapping, dict):
            return {}
        return {
            key.strip().upper(): value
            for key, value in mapping.items()
            if isinstance(key, str) and key.strip() and isinstance(value, str)
        }

    def get_sql_to_parquet_mapping(self) -> Dict[str, str]:
        """Get the SQL to Parquet type mapping.

        The user's ``x-sql.parquetTypeMapping.sqlToParquet`` entries are merged over the defaults
        (``DEFAULT_SQL_TO_PARQUET_MAPPING``), so overriding one type keeps all the others.
        """
        return {**self.DEFAULT_SQL_TO_PARQUET_MAPPING, **self._user_sql_to_parquet_mapping()}

    def get_parquet_type_for_sql_type(
        self, sql_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> Optional[str]:
        """Parquet type for a SQL column type such as ``"VARCHAR"`` or ``"DECIMAL(12,4)"``.

        DECIMAL/NUMERIC resolve to ``decimal128(p,s)`` (``decimal256`` above 38 digits) from the
        column's real precision/scale (given here or parsed from ``sql_type``) so the values are
        not forced into one fixed precision. A user override of the type in ``sqlToParquet``
        always wins, and the mapping default is only used when the precision is unknown.

        Returns:
            The Parquet type string, or None if the SQL type is unknown.
        """
        match = re.fullmatch(
            r"\s*([A-Za-z_][A-Za-z_ ]*?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*", sql_type
        )
        if not match:
            return None
        base = match.group(1).upper()
        if precision is None and match.group(2) is not None:
            precision = int(match.group(2))
            scale = int(match.group(3)) if match.group(3) is not None else scale

        mapping = self.get_sql_to_parquet_mapping()
        if base in ("DECIMAL", "NUMERIC") and base not in self._user_sql_to_parquet_mapping():
            scale = 0 if scale is None else scale
            if precision is not None and 1 <= precision <= 76 and 0 <= scale <= precision:
                width = 128 if precision <= 38 else 256
                return f"decimal{width}({precision},{scale})"
        return mapping.get(base)

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema


__all__ = [
    "SqlSchemaImporter",
    "SchemaValidationError",
    "sql_identifier_problem",
    "output_name_problem",
]
