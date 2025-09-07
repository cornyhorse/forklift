"""Schema discovery and management for SQL databases."""

from __future__ import annotations
from typing import List, Tuple, Optional
import pyarrow as pa
import logging

from .connection import SqlConnectionManager
from .types import SqlTypeConverter
from ..config import SqlInputConfig

logger = logging.getLogger(__name__)


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
        """
        connection = self._get_connection()
        cursor = connection.cursor()
        tables = []

        try:
            # Get tables - this works for most ODBC drivers
            for row in cursor.tables():
                schema_name = row.table_schem or 'default'
                table_name = row.table_name
                table_type = row.table_type

                # Include both tables and views
                if table_type in ('TABLE', 'VIEW'):
                    tables.append((schema_name, table_name))

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
                tables = [(row[0], row[1]) for row in cursor.fetchall()]
            except Exception:
                logger.warning("Could not retrieve table list using fallback method")
        finally:
            cursor.close()

        return tables

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
            ValueError: If table specification format is invalid
        """
        available_tables = self.get_table_list()
        specified_tables = []

        for spec in table_specifications:
            schema_name, table_name = self._parse_table_specification(spec)

            # Validate that the table exists
            if (schema_name, table_name) in available_tables:
                specified_tables.append((schema_name, table_name))
            else:
                # Try with default schema if not found
                default_matches = [(s, t) for s, t in available_tables if t == table_name]
                if default_matches:
                    specified_tables.append(default_matches[0])
                    logger.info(f"Using {default_matches[0]} for specification '{spec}'")
                else:
                    logger.warning(f"Table not found: {spec}")

        return specified_tables

    def _parse_table_specification(self, spec: str) -> Tuple[str, str]:
        """Parse a table specification into schema and table name.

        Args:
            spec: Table specification string

        Returns:
            Tuple of (schema_name, table_name)
        """
        if '.' in spec:
            parts = spec.split('.', 1)
            schema_name = parts[0].strip()
            table_name = parts[1].strip()
        else:
            schema_name = 'default'  # Will be resolved to actual default schema
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
        """
        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            # Get column information
            quoted_table = self._quote_identifier(table_name)
            quoted_schema = self._quote_identifier(schema_name)

            # Use ODBC standard columns() method when possible
            columns_info = []
            try:
                for row in cursor.columns(table=table_name, schema=schema_name):
                    columns_info.append({
                        'column_name': row.column_name,
                        'data_type': row.type_name,
                        'column_size': getattr(row, 'column_size', None),
                        'decimal_digits': getattr(row, 'decimal_digits', None),
                        'nullable': getattr(row, 'nullable', True)
                    })
            except Exception:
                # Fallback: Query the table directly to infer types
                try:
                    if schema_name and schema_name != 'default':
                        full_table_name = f"{quoted_schema}.{quoted_table}"
                    else:
                        full_table_name = quoted_table

                    cursor.execute(f"SELECT * FROM {full_table_name} LIMIT 1")

                    # Get column descriptions from cursor
                    for i, desc in enumerate(cursor.description):
                        columns_info.append({
                            'column_name': desc[0],
                            'data_type': self.type_converter.odbc_type_to_string(desc[1]),
                            'column_size': desc[2] if len(desc) > 2 else None,
                            'decimal_digits': desc[5] if len(desc) > 5 else None,
                            'nullable': True
                        })
                except Exception as e:
                    raise RuntimeError(f"Could not determine schema for {schema_name}.{table_name}: {e}")

            # Convert to PyArrow schema
            fields = []
            for col_info in columns_info:
                pa_type = self.type_converter.sql_type_to_pyarrow(
                    col_info['data_type'],
                    col_info.get('column_size'),
                    col_info.get('decimal_digits')
                )

                field = pa.field(
                    col_info['column_name'],
                    pa_type,
                    nullable=col_info.get('nullable', True)
                )
                fields.append(field)

            return pa.schema(fields)

        finally:
            cursor.close()

    def _quote_identifier(self, identifier: str) -> str:
        """Quote database identifier if needed.

        Args:
            identifier: Database identifier (table/column name)

        Returns:
            Quoted identifier
        """
        if not self.config.use_quoted_identifiers:
            return identifier

        # Use double quotes as standard SQL identifier quotes
        return f'"{identifier}"'

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
