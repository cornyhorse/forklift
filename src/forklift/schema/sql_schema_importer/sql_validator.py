"""SQL-specific validation for tables and columns."""

import re
from typing import Any, Dict, List

from .table_manager import TableManager


class SqlValidator:
    """Validates SQL-specific schema components."""

    def __init__(self, sql_ext: Dict[str, Any], tables: List[Dict[str, Any]], parquet_handler):
        """Initialize with SQL extension data and dependencies."""
        self.sql_ext = sql_ext
        self.tables = tables
        self.parquet_handler = parquet_handler

    def validate_sql_extension(self) -> List[str]:
        """Validate x-sql extension structure and values."""
        errors = []

        # x-sql extension is optional, but if present must be valid
        if self.sql_ext:
            # Validate tables array - check the original raw value, not the processed self.tables
            if "tables" in self.sql_ext:
                tables_raw = self.sql_ext["tables"]
                if not isinstance(tables_raw, list):
                    errors.append("x-sql.tables must be an array")

            # Validate parquetTypeMapping
            if "parquetTypeMapping" in self.sql_ext:
                parquet_type_mapping = self.sql_ext["parquetTypeMapping"]
                if not isinstance(parquet_type_mapping, dict):
                    errors.append("x-sql.parquetTypeMapping must be an object")

        return errors

    def validate_tables(self) -> List[str]:
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
            elif not TableManager.is_valid_include_pattern(pattern):
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
            if parquet_type and not self.parquet_handler.is_valid_parquet_type(parquet_type):
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
