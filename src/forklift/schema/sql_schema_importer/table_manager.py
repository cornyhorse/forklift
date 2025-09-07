"""Table management and pattern matching for SQL Schema Importer."""

import re
from typing import Any, Dict, List, Optional, Tuple


class TableManager:
    """Manages table configurations and selection logic."""

    def __init__(self, tables: List[Dict[str, Any]]):
        """Initialize with table configurations."""
        self.tables = tables if isinstance(tables, list) else []

    def get_table_list(self) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema configuration.

        Returns:
            List of tuples (schema_name, table_name, output_name)
        """
        table_list = []
        for table in self.tables:
            select = table.get("select", {})
            schema_name = select.get("schema", "default")
            table_name = select.get("name")
            output_name = table.get("outputName")

            if table_name:
                table_list.append((schema_name, table_name, output_name))

        return table_list

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
        """Check if a schema/table matches any include pattern - deprecated."""
        # Since we use explicit table lists now, this always returns True
        # Individual tables are explicitly listed in the schema
        return True

    def get_include_patterns(self) -> List[str]:
        """Get all resolved include patterns - deprecated, returns empty list."""
        # No longer used since we use explicit table lists instead of glob patterns
        return []

    @classmethod
    def is_valid_include_pattern(cls, pattern: str) -> bool:
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
                return (cls._is_valid_identifier_or_wildcard(schema_part) and
                       cls._is_valid_identifier_or_wildcard(table_part))

        # Single identifier (table name)
        return cls._is_valid_identifier_or_wildcard(pattern)

    @classmethod
    def _is_valid_identifier_or_wildcard(cls, name: str) -> bool:
        """Check if a name is a valid SQL identifier or wildcard."""
        if name == "*":
            return True

        # Basic SQL identifier validation (simplified)
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))
