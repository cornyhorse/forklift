"""SQL database input handler for reading data from databases via ODBC."""

from __future__ import annotations
import re
from typing import List, Optional, Iterator, Tuple
import pyarrow as pa
import logging

from .config import SqlInputConfig
from ..schema.sql_schema_importer import SqlSchemaImporter

logger = logging.getLogger(__name__)


class SqlInputHandler:
    """Handles SQL database input with ODBC connections and streaming support.

    This class provides functionality for reading data from SQL databases using
    ODBC connections. It supports various database engines (SQLite, PostgreSQL,
    MySQL, Oracle, SQL Server, etc.) through appropriate ODBC drivers.

    Args:
        config: SqlInputConfig instance with database connection configuration

    Attributes:
        config: The configuration object for this input handler
        connection: Active database connection (established when needed)
        schema_importer: Optional SQL schema importer for validation
    """

    def __init__(self, config: SqlInputConfig):
        """Initialize the SQL input handler.

        Args:
            config: Configuration object containing SQL connection parameters
        """
        self.config = config
        self.connection = None
        self.schema_importer: Optional[SqlSchemaImporter] = None

    def set_schema_importer(self, schema_importer: SqlSchemaImporter) -> None:
        """Set the schema importer for validation and type mapping.

        Args:
            schema_importer: SQL schema importer instance
        """
        self.schema_importer = schema_importer

    def connect(self) -> None:
        """Establish database connection using pyodbc.

        Raises:
            ImportError: If pyodbc is not installed
            ConnectionError: If connection fails
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL database connectivity. "
                "Install it with: pip install pyodbc"
            )

        try:
            # Set connection timeout
            pyodbc.pooling = False

            # Build connection string with additional parameters
            conn_str = self.config.connection_string
            if self.config.connection_params:
                params = ";".join(f"{k}={v}" for k, v in self.config.connection_params.items())
                conn_str = f"{conn_str};{params}"

            self.connection = pyodbc.connect(
                conn_str,
                timeout=self.config.connection_timeout
            )

            # Set query timeout
            self.connection.timeout = self.config.query_timeout

            logger.info("Successfully connected to database")

        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
            finally:
                self.connection = None

    def get_table_list(self) -> List[Tuple[str, str]]:
        """Get list of available tables and views.

        Returns:
            List of tuples (schema_name, table_name)

        Raises:
            ConnectionError: If not connected to database
        """
        if not self.connection:
            raise ConnectionError("Not connected to database")

        cursor = self.connection.cursor()
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

    def filter_tables(self, include_patterns: List[str]) -> List[Tuple[str, str]]:
        """Filter tables based on include patterns.

        Args:
            include_patterns: List of glob-like patterns (e.g., ['*.*', 'schema.table'])

        Returns:
            List of matching (schema_name, table_name) tuples
        """
        all_tables = self.get_table_list()

        if not include_patterns or include_patterns == ['*.*']:
            return all_tables

        matched_tables = []

        for pattern in include_patterns:
            if '.' in pattern:
                schema_pattern, table_pattern = pattern.split('.', 1)
            else:
                schema_pattern = '*'
                table_pattern = pattern

            # Convert glob patterns to regex
            schema_regex = self._glob_to_regex(schema_pattern)
            table_regex = self._glob_to_regex(table_pattern)

            for schema_name, table_name in all_tables:
                if (re.match(schema_regex, schema_name, re.IGNORECASE) and
                    re.match(table_regex, table_name, re.IGNORECASE)):
                    if (schema_name, table_name) not in matched_tables:
                        matched_tables.append((schema_name, table_name))

        return matched_tables

    def _glob_to_regex(self, pattern: str) -> str:
        """Convert glob pattern to regex pattern.

        Args:
            pattern: Glob pattern with * and ? wildcards

        Returns:
            Regex pattern string
        """
        # Escape special regex characters except * and ?
        pattern = re.escape(pattern)
        # Convert glob wildcards to regex
        pattern = pattern.replace(r'\*', '.*').replace(r'\?', '.')
        return f'^{pattern}$'

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
        if not self.connection:
            raise ConnectionError("Not connected to database")

        cursor = self.connection.cursor()

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
                            'data_type': self._odbc_type_to_string(desc[1]),
                            'column_size': desc[2] if len(desc) > 2 else None,
                            'decimal_digits': desc[5] if len(desc) > 5 else None,
                            'nullable': True
                        })
                except Exception as e:
                    raise RuntimeError(f"Could not determine schema for {schema_name}.{table_name}: {e}")

            # Convert to PyArrow schema
            fields = []
            for col_info in columns_info:
                pa_type = self._sql_type_to_pyarrow(
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

    def _odbc_type_to_string(self, odbc_type: int) -> str:
        """Convert ODBC type constant to string representation.

        Args:
            odbc_type: ODBC type constant

        Returns:
            String representation of the type
        """
        # Import pyodbc to access type constants
        try:
            import pyodbc

            type_map = {
                pyodbc.SQL_CHAR: 'CHAR',
                pyodbc.SQL_VARCHAR: 'VARCHAR',
                pyodbc.SQL_LONGVARCHAR: 'TEXT',
                pyodbc.SQL_WCHAR: 'NCHAR',
                pyodbc.SQL_WVARCHAR: 'NVARCHAR',
                pyodbc.SQL_WLONGVARCHAR: 'NTEXT',
                pyodbc.SQL_DECIMAL: 'DECIMAL',
                pyodbc.SQL_NUMERIC: 'NUMERIC',
                pyodbc.SQL_SMALLINT: 'SMALLINT',
                pyodbc.SQL_INTEGER: 'INTEGER',
                pyodbc.SQL_REAL: 'REAL',
                pyodbc.SQL_FLOAT: 'FLOAT',
                pyodbc.SQL_DOUBLE: 'DOUBLE',
                pyodbc.SQL_BIT: 'BIT',
                pyodbc.SQL_TINYINT: 'TINYINT',
                pyodbc.SQL_BIGINT: 'BIGINT',
                pyodbc.SQL_BINARY: 'BINARY',
                pyodbc.SQL_VARBINARY: 'VARBINARY',
                pyodbc.SQL_LONGVARBINARY: 'BLOB',
                pyodbc.SQL_TYPE_DATE: 'DATE',
                pyodbc.SQL_TYPE_TIME: 'TIME',
                pyodbc.SQL_TYPE_TIMESTAMP: 'TIMESTAMP',
            }

            return type_map.get(odbc_type, 'VARCHAR')

        except ImportError:
            return 'VARCHAR'

    def _sql_type_to_pyarrow(self, sql_type: str, size: Optional[int] = None,
                           decimal_digits: Optional[int] = None) -> pa.DataType:
        """Convert SQL data type to PyArrow data type.

        Args:
            sql_type: SQL type name
            size: Column size
            decimal_digits: Number of decimal digits

        Returns:
            PyArrow data type
        """
        sql_type = sql_type.upper()

        # Use schema importer mapping if available
        if self.schema_importer and hasattr(self.schema_importer, 'parquet_type_mapping'):
            # This would need to be enhanced to map SQL types to Parquet types
            pass

        # Map common SQL types to PyArrow types
        if sql_type in ('INT', 'INTEGER', 'INT4'):
            return pa.int32()
        elif sql_type in ('BIGINT', 'INT8'):
            return pa.int64()
        elif sql_type in ('SMALLINT', 'INT2'):
            return pa.int16()
        elif sql_type in ('TINYINT', 'INT1'):
            return pa.int8()
        elif sql_type in ('FLOAT', 'REAL'):
            return pa.float32()
        elif sql_type in ('DOUBLE', 'DOUBLE PRECISION', 'FLOAT8'):
            return pa.float64()
        elif sql_type in ('DECIMAL', 'NUMERIC'):
            if size and decimal_digits is not None:
                return pa.decimal128(size, decimal_digits)
            return pa.float64()
        elif sql_type in ('BOOLEAN', 'BOOL', 'BIT'):
            return pa.bool_()
        elif sql_type in ('DATE',):
            return pa.date32()
        elif sql_type in ('TIME',):
            return pa.time64('us')
        elif sql_type in ('TIMESTAMP', 'DATETIME', 'TIMESTAMP WITHOUT TIME ZONE'):
            return pa.timestamp('us')
        elif sql_type in ('TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'):
            return pa.timestamp('us', tz='UTC')
        elif sql_type in ('BINARY', 'VARBINARY', 'BLOB', 'BYTEA'):
            return pa.binary()
        else:
            # Default to string for text types and unknown types
            return pa.string()

    def read_table_data(self, schema_name: str, table_name: str) -> Iterator[pa.RecordBatch]:
        """Read data from a table in batches.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Yields:
            PyArrow RecordBatch objects

        Raises:
            ConnectionError: If not connected to database
        """
        if not self.connection:
            raise ConnectionError("Not connected to database")

        # Get table schema first
        table_schema = self.get_table_schema(schema_name, table_name)

        cursor = self.connection.cursor()

        try:
            # Build query
            quoted_table = self._quote_identifier(table_name)
            quoted_schema = self._quote_identifier(schema_name)

            if schema_name and schema_name != 'default':
                full_table_name = f"{quoted_schema}.{quoted_table}"
            else:
                full_table_name = quoted_table

            query = f"SELECT * FROM {full_table_name}"

            # Set fetch size if specified
            if self.config.fetch_size:
                cursor.arraysize = self.config.fetch_size

            logger.info(f"Executing query: {query}")
            cursor.execute(query)

            # Process data in batches
            while True:
                rows = cursor.fetchmany(self.config.batch_size)
                if not rows:
                    break

                # Convert rows to PyArrow batch
                batch = self._rows_to_recordbatch(rows, table_schema)
                yield batch

        finally:
            cursor.close()

    def _rows_to_recordbatch(self, rows: List[Tuple], schema: pa.Schema) -> pa.RecordBatch:
        """Convert database rows to PyArrow RecordBatch.

        Args:
            rows: List of row tuples from database
            schema: PyArrow schema for the data

        Returns:
            PyArrow RecordBatch
        """
        if not rows:
            return pa.record_batch([], schema)

        # Transpose rows to columns
        columns = list(zip(*rows))

        # Convert each column according to schema
        arrays = []
        for i, (column_data, field) in enumerate(zip(columns, schema)):
            array = self._convert_column_data(column_data, field.type)
            arrays.append(array)

        return pa.record_batch(arrays, schema)

    def _convert_column_data(self, column_data: Tuple, pa_type: pa.DataType) -> pa.Array:
        """Convert column data to PyArrow array with proper type.

        Args:
            column_data: Tuple of column values
            pa_type: Target PyArrow data type

        Returns:
            PyArrow array
        """
        # Handle null values
        processed_data = []
        for value in column_data:
            if value is None:
                processed_data.append(None)
            elif self.config.null_values and str(value) in self.config.null_values:
                processed_data.append(None)
            else:
                processed_data.append(value)

        try:
            return pa.array(processed_data, type=pa_type)
        except Exception as e:
            logger.warning(f"Could not convert column to {pa_type}, using string: {e}")
            # Fallback to string type
            return pa.array([str(v) if v is not None else None for v in processed_data])

    def get_include_patterns(self) -> List[str]:
        """Get include patterns from configuration or schema.

        Returns:
            List of include patterns
        """
        if self.config.include_patterns:
            return self.config.include_patterns

        if self.schema_importer:
            return self.schema_importer.all_include_patterns

        return ['*.*']  # Default to all tables

    def get_tables_to_process(self) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema or config.

        Returns:
            List of tuples (schema_name, table_name, output_name)
        """
        if self.schema_importer:
            # Use explicit table list from schema
            return self.schema_importer.get_table_list()
        elif self.config.include_patterns:
            # Fallback: try to parse simple patterns for backward compatibility
            tables = []
            for pattern in self.config.include_patterns:
                if '.' in pattern and pattern != '*.*':
                    schema_name, table_name = pattern.split('.', 1)
                    if schema_name != '*' and table_name != '*':
                        tables.append((schema_name, table_name, None))
                elif pattern != '*.*':
                    tables.append(('default', pattern, None))
            return tables
        else:
            # No specific tables configured - discover all tables
            return [(schema, table, None) for schema, table in self.get_table_list()]

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
