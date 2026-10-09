"""Schema discovery and management for SQL databases."""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Tuple

import pyarrow as pa

from ..config import SqlInputConfig
from .connection import SqlConnectionManager
from .types import SqlTypeConverter

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA = "default"


def _is_default_schema(schema_name: Optional[str]) -> bool:
    """True when ``schema_name`` means "no schema qualifier"."""
    return schema_name in (None, "", DEFAULT_SCHEMA)


def _check_identifier(value: object, kind: str) -> str:
    """Reject values that cannot be a catalog identifier before they get near SQL text."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"Invalid {kind} name")
    return value


def match_catalog_entries(
    available: List[Tuple[str, str]], schema_name: Optional[str], table_name: str
) -> List[Tuple[str, str]]:
    """Return the catalog entries that match a (schema, table) request.

    Names are compared exactly first; only when nothing matches exactly are they compared
    case-insensitively (databases such as SQL Server or PostgreSQL resolve unquoted names that
    way). The entries returned carry the catalog's own spelling, which is what callers must use
    from then on.

    A request without a schema (``None``/``""``/``"default"``) matches the table in any
    schema, preferring a schema-less catalog entry. A request with a schema matches that
    schema; only when the driver reports no schemas at all (entries are ``"default"``) is the
    table matched by name alone.
    """
    for fold in (False, True):

        def norm(value: str) -> str:
            return value.casefold() if fold else value

        by_name = [(s, t) for s, t in available if norm(t) == norm(table_name)]
        if _is_default_schema(schema_name):
            matches = [m for m in by_name if m[0] == DEFAULT_SCHEMA] or by_name
        else:
            matches = [m for m in by_name if norm(m[0]) == norm(schema_name)]
            matches = matches or [m for m in by_name if m[0] == DEFAULT_SCHEMA]
        if matches:
            return matches
    return []


def resolve_specified_tables(
    available: List[Tuple[str, str]],
    table_specifications: List[str],
    parse: Callable[[str], Tuple[str, str]],
) -> List[Tuple[str, str]]:
    """Resolve table specifications against the catalog listing.

    Unknown tables are skipped with a warning. A bare table name that exists in several
    schemas is ambiguous and raises instead of silently picking one.

    Raises:
        ValueError: If a specification matches tables in more than one schema
    """
    specified_tables = []
    for spec in table_specifications:
        schema_name, table_name = parse(spec)

        if (schema_name, table_name) in available:
            specified_tables.append((schema_name, table_name))
            continue

        matches = match_catalog_entries(available, schema_name, table_name)
        if len(matches) > 1:
            schemas = ", ".join(sorted(s for s, _ in matches))
            raise ValueError(
                f"Table specification '{spec}' is ambiguous: found in schemas {schemas}; "
                "qualify it as schema.table"
            )
        if matches:
            specified_tables.append(matches[0])
            logger.info(f"Using {matches[0]} for specification '{spec}'")
        else:
            logger.warning(f"Table not found: {spec}")

    return specified_tables


class SqlSchemaManager:
    """Manages database schema discovery and PyArrow schema generation."""

    def __init__(self, config: SqlInputConfig, connection_manager: SqlConnectionManager):
        """Initialize the schema manager.

        Args:
            config: SQL input configuration
            connection_manager: Database connection manager
        """
        self.config = config
        self.connection_manager = connection_manager
        self.type_converter = SqlTypeConverter()
        self._connection_override = None  # For test mocking support

    def _get_connection(self):
        """Get the database connection, with override support for testing."""
        if self._connection_override is not None:
            return self._connection_override
        return self.connection_manager.get_connection()

    def set_schema_importer(self, schema_importer) -> None:
        """Set the schema importer for validation and type mapping.

        Args:
            schema_importer: SQL schema importer instance
        """
        self.type_converter.schema_importer = schema_importer

    def get_table_list(self) -> List[Tuple[str, str]]:
        """Get list of available tables and views.

        Returns:
            List of tuples (schema_name, table_name)

        Raises:
            ConnectionError: If not connected to database
            RuntimeError: If the catalog cannot be read at all
        """
        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            return self._list_tables(cursor)
        finally:
            cursor.close()

    def _list_tables(self, cursor, table: Optional[str] = None) -> List[Tuple[str, str]]:
        """List catalog tables/views, optionally narrowing the driver query by table name.

        ``table`` is an ODBC search pattern (``_`` and ``%`` are wildcards), so a narrowed
        result is a superset: callers must still compare names exactly. Schemas are never
        sent to the driver because some report none (SQLite) and the caller matches them.
        """
        tables = []

        try:
            # Get tables - this works for most ODBC drivers
            for row in cursor.tables(**({"table": table} if table else {})):
                schema_name = row.table_schem or DEFAULT_SCHEMA
                table_name = row.table_name
                table_type = row.table_type

                # Include both tables and views
                if table_type in ("TABLE", "VIEW"):
                    tables.append((schema_name, table_name))
            return tables

        except Exception as e:
            logger.warning(f"Could not retrieve table list via ODBC: {e}")
            # Fallback for databases that don't support tables() method
            try:
                # Try SQLite-style system tables
                cursor.execute("""
                    SELECT 'main' as schema_name, name as table_name
                    FROM sqlite_master
                    WHERE type IN ('table', 'view')
                """)
                return [(row[0], row[1]) for row in cursor.fetchall()]
            except Exception as fallback_error:
                logger.error("Could not retrieve table list using fallback method")
                raise RuntimeError(
                    "Could not retrieve the table list from the database catalog"
                ) from fallback_error

    def resolve_table(
        self, schema_name: Optional[str], table_name: str
    ) -> Tuple[Optional[str], str]:
        """Validate a requested table against the catalog and return its catalog names.

        This is the gate every query-building path goes through: names that are not in the
        catalog never reach SQL text.

        Args:
            schema_name: Requested schema (``None``/``""``/``"default"`` for no schema)
            table_name: Requested table or view name (exact match preferred, otherwise a
                unique case-insensitive match)

        Returns:
            ``(schema, table)`` as reported by the catalog; ``schema`` is ``None`` for
            schema-less databases

        Raises:
            ValueError: If the table is unknown or the request matches several schemas
            ConnectionError: If not connected to database
        """
        _check_identifier(table_name, "table")
        default_schema = _is_default_schema(schema_name)
        if not default_schema:
            _check_identifier(schema_name, "schema")

        connection = self._get_connection()
        cursor = connection.cursor()
        try:
            matches = match_catalog_entries(
                self._list_tables(cursor, table=table_name), schema_name, table_name
            )
            if not matches:
                # The driver's pattern match can be case-sensitive; look at the full listing
                matches = match_catalog_entries(self._list_tables(cursor), schema_name, table_name)
        finally:
            cursor.close()

        label = table_name if default_schema else f"{schema_name}.{table_name}"
        if not matches:
            raise ValueError(f"Table '{label}' was not found in the database catalog")
        if len(matches) > 1:
            schemas = ", ".join(sorted(s for s, _ in matches))
            raise ValueError(
                f"Table '{label}' is ambiguous: found in schemas {schemas}; specify the schema"
            )
        resolved_schema, resolved_table = matches[0]
        return (None if resolved_schema == DEFAULT_SCHEMA else resolved_schema), resolved_table

    def get_specified_tables(self, table_specifications: List[str]) -> List[Tuple[str, str]]:
        """Get tables based on explicit specifications.

        Args:
            table_specifications: List of table specifications in format:
                - "table_name" (uses default schema)
                - "schema.table_name" (fully qualified)
                - For SQLite: "table_name" only
                - For MySQL: "database.table_name" where database acts as schema

        Returns:
            List of validated (schema_name, table_name) tuples

        Raises:
            ValueError: If a bare table name exists in several schemas (ambiguous)
        """
        return resolve_specified_tables(
            self.get_table_list(), table_specifications, self._parse_table_specification
        )

    def _parse_table_specification(self, spec: str) -> Tuple[str, str]:
        """Parse a table specification into schema and table name.

        Args:
            spec: Table specification string

        Returns:
            Tuple of (schema_name, table_name)
        """
        if "." in spec:
            parts = spec.split(".", 1)
            schema_name = parts[0].strip()
            table_name = parts[1].strip()
        else:
            schema_name = DEFAULT_SCHEMA  # Will be resolved to actual default schema
            table_name = spec.strip()

        return schema_name, table_name

    def get_table_schema(self, schema_name: str, table_name: str) -> pa.Schema:
        """Get PyArrow schema for a table.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            PyArrow schema with appropriate data types

        Raises:
            ConnectionError: If not connected to database
            ValueError: If the table is not in the catalog or reports no columns
            RuntimeError: If the column metadata cannot be determined
        """
        resolved_schema, resolved_table = self.resolve_table(schema_name, table_name)

        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            # Use ODBC standard columns() method when possible
            try:
                columns_info = self._catalog_columns(cursor, resolved_schema, resolved_table)
            except Exception as e:
                logger.info(f"columns() not available, inferring schema from a query: {e}")
                columns_info = self._query_columns(
                    cursor, resolved_schema, resolved_table, schema_name, table_name
                )

            if not columns_info:
                raise ValueError(
                    f"Table '{resolved_table}' reports no columns in the database catalog"
                )

            # Convert to PyArrow schema
            fields = []
            for col_info in columns_info:
                pa_type = self.type_converter.sql_type_to_pyarrow(
                    col_info["data_type"],
                    col_info.get("column_size"),
                    col_info.get("decimal_digits"),
                )

                field = pa.field(
                    col_info["column_name"], pa_type, nullable=col_info.get("nullable", True)
                )
                fields.append(field)

            return pa.schema(fields)

        finally:
            cursor.close()

    def _catalog_columns(self, cursor, schema_name: Optional[str], table_name: str) -> List[Dict]:
        """Read column metadata through ODBC ``columns()``, keeping exact table matches only.

        ``columns(table=..., schema=...)`` treats ``_`` and ``%`` as search patterns, so
        ``my_table`` also returns the columns of ``myXtable``; rows of other tables are dropped.
        """
        kwargs = {"table": table_name}
        if schema_name:
            kwargs["schema"] = schema_name

        columns_info = []
        for row in cursor.columns(**kwargs):
            if getattr(row, "table_name", None) != table_name:
                continue
            row_schema = getattr(row, "table_schem", None)
            if schema_name and row_schema is not None and row_schema != schema_name:
                continue
            columns_info.append(
                {
                    "column_name": row.column_name,
                    "data_type": row.type_name,
                    "column_size": getattr(row, "column_size", None),
                    "decimal_digits": getattr(row, "decimal_digits", None),
                    "nullable": getattr(row, "nullable", True),
                }
            )
        return columns_info

    def _query_columns(
        self,
        cursor,
        resolved_schema: Optional[str],
        resolved_table: str,
        schema_name: str,
        table_name: str,
    ) -> List[Dict]:
        """Infer column metadata from a zero-row query (drivers without ``columns()``).

        The table name was validated against the catalog by the caller and is quoted here.
        """
        try:
            qualified = self.qualified_table_name(resolved_schema, resolved_table)
            # WHERE 1=0 is valid on every dialect (LIMIT is not valid on T-SQL/Oracle)
            cursor.execute(f"SELECT * FROM {qualified} WHERE 1=0")

            columns_info = []
            # DB-API description: (name, type_code, display_size, internal_size,
            # precision, scale, null_ok); pyodbc's type_code is a Python type.
            for desc in cursor.description:
                type_code = desc[1]
                if isinstance(type_code, int) and not isinstance(type_code, bool):
                    data_type = self.type_converter.odbc_type_to_string(type_code)
                else:
                    data_type = self.type_converter.python_type_to_string(type_code)
                size = next((desc[i] for i in (4, 3, 2) if len(desc) > i and desc[i]), None)
                null_ok = desc[6] if len(desc) > 6 else None
                columns_info.append(
                    {
                        "column_name": desc[0],
                        "data_type": data_type,
                        "column_size": size,
                        "decimal_digits": desc[5] if len(desc) > 5 else None,
                        "nullable": True if null_ok is None else bool(null_ok),
                    }
                )
            return columns_info
        except Exception as e:
            raise RuntimeError(
                f"Could not determine schema for {schema_name}.{table_name}: {e}"
            ) from e

    def _quote_char(self) -> str:
        """Identifier quote character reported by the driver (``"`` when unknown)."""
        connection = self._connection_override
        if connection is None:
            connection = self.connection_manager.connection
        if connection is not None:
            try:
                import pyodbc

                quote = connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
                # ODBC reports a single space when the driver has no quote character
                if isinstance(quote, str) and len(quote.strip()) == 1:
                    return quote.strip()
            except Exception:
                pass
        return '"'

    def _quote_identifier(self, identifier: str) -> str:
        """Quote a database identifier, doubling any embedded quote character.

        Identifiers are always quoted: they end up in SQL text, and an unescaped quote in a
        name would otherwise let it break out of the identifier. ``use_quoted_identifiers``
        is accepted for backward compatibility but no longer switches quoting off.

        Args:
            identifier: Database identifier (table/column name)

        Returns:
            Quoted identifier

        Raises:
            ValueError: If the identifier is empty, not a string, or contains a NUL character
        """
        _check_identifier(identifier, "identifier")
        quote = self._quote_char()
        return f"{quote}{identifier.replace(quote, quote * 2)}{quote}"

    def qualified_table_name(self, schema_name: Optional[str], table_name: str) -> str:
        """Quoted ``schema.table`` (or just ``table`` for schema-less names) for SQL text."""
        quoted_table = self._quote_identifier(table_name)
        if _is_default_schema(schema_name):
            return quoted_table
        return f"{self._quote_identifier(schema_name)}.{quoted_table}"

    def get_tables_to_process(self, schema_importer=None) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema or config.

        Args:
            schema_importer: Optional schema importer for table discovery

        Returns:
            List of tuples (schema_name, table_name, output_name)
        """
        if schema_importer:
            # Use explicit table list from schema
            return schema_importer.get_table_list()
        else:
            # No specific tables configured - discover all tables
            return [(schema, table, None) for schema, table in self.get_table_list()]
