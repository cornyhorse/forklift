"""Main SQL input handler that orchestrates all SQL input components."""

from __future__ import annotations
from typing import List, Optional, Iterator, Tuple
import pyarrow as pa
import logging

from .connection import SqlConnectionManager
from .schema import SqlSchemaManager
from .reader import SqlDataReader
from ..config import SqlInputConfig
from ...schema.sql_schema_importer import SqlSchemaImporter

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
        connection_manager: Manages database connections
        schema_manager: Handles schema discovery and management
        data_reader: Reads and converts data from the database
        schema_importer: Optional SQL schema importer for validation
    """

    def __init__(self, config: SqlInputConfig):
        """Initialize the SQL input handler.

        Args:
            config: Configuration object containing SQL connection parameters
        """
        self.config = config
        self.connection_manager = SqlConnectionManager(config)
        self.schema_manager = SqlSchemaManager(config, self.connection_manager)
        self.data_reader = SqlDataReader(config, self.connection_manager, self.schema_manager)
        self.schema_importer: Optional[SqlSchemaImporter] = None

    # Backward compatibility properties
    @property
    def connection(self):
        """Backward compatibility property for direct connection access."""
        return self.connection_manager.connection

    @connection.setter
    def connection(self, value):
        """Backward compatibility setter for direct connection access."""
        self.connection_manager.connection = value
        # Also ensure components are aware of the connection change for test mocking
        self.schema_manager._connection_override = value
        self.data_reader._connection_override = value

    @connection.deleter
    def connection(self):
        """Backward compatibility deleter for direct connection access."""
        self.connection_manager.connection = None

    # Backward compatibility methods - delegate to appropriate modules
    def _parse_table_specification(self, spec: str) -> Tuple[str, str]:
        """Parse a table specification into schema and table name.

        Backward compatibility method that delegates to schema manager.
        """
        return self.schema_manager._parse_table_specification(spec)

    def _quote_identifier(self, identifier: str) -> str:
        """Quote database identifier if needed.

        Backward compatibility method that delegates to schema manager.
        """
        return self.schema_manager._quote_identifier(identifier)

    def _odbc_type_to_string(self, odbc_type: int) -> str:
        """Convert ODBC type constant to string representation.

        Backward compatibility method that delegates to type converter.
        """
        return self.data_reader.type_converter.odbc_type_to_string(odbc_type)

    def _sql_type_to_pyarrow(self, sql_type: str, size: Optional[int] = None,
                           decimal_digits: Optional[int] = None) -> pa.DataType:
        """Convert SQL data type to PyArrow data type.

        Backward compatibility method that delegates to type converter.
        """
        return self.data_reader.type_converter.sql_type_to_pyarrow(sql_type, size, decimal_digits)

    def _convert_column_data(self, column_data: tuple, pa_type: pa.DataType) -> pa.Array:
        """Convert column data to PyArrow array with proper type.

        Backward compatibility method that delegates to type converter.
        """
        return self.data_reader.type_converter.convert_column_data(
            column_data, pa_type, self.config.null_values
        )

    def _rows_to_recordbatch(self, rows: List[Tuple], schema: pa.Schema) -> pa.RecordBatch:
        """Convert database rows to PyArrow RecordBatch.

        Backward compatibility method that delegates to data reader.
        """
        return self.data_reader._rows_to_recordbatch(rows, schema)

    def set_schema_importer(self, schema_importer: SqlSchemaImporter) -> None:
        """Set the schema importer for validation and type mapping.

        Args:
            schema_importer: SQL schema importer instance
        """
        self.schema_importer = schema_importer
        self.schema_manager.set_schema_importer(schema_importer)

    def connect(self) -> None:
        """Establish database connection using pyodbc.

        Raises:
            ImportError: If pyodbc is not installed
            ConnectionError: If connection fails
        """
        self.connection_manager.connect()

    def disconnect(self) -> None:
        """Close database connection."""
        self.connection_manager.disconnect()

    def get_table_list(self) -> List[Tuple[str, str]]:
        """Get list of available tables and views.

        Returns:
            List of tuples (schema_name, table_name)

        Raises:
            ConnectionError: If not connected to database
        """
        return self.schema_manager.get_table_list()

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
        # Call handler's method instead of schema manager directly for test compatibility
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
        return self.schema_manager.get_table_schema(schema_name, table_name)

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
        return self.data_reader.read_table_data(schema_name, table_name)

    def get_tables_to_process(self) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of tables to process from schema or config.

        Returns:
            List of tuples (schema_name, table_name, output_name)
        """
        return self.schema_manager.get_tables_to_process(self.schema_importer)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
