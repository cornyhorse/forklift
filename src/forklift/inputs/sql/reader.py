"""SQL data reading and batch processing."""

from __future__ import annotations
from typing import Iterator, List, Tuple
import pyarrow as pa
import logging

from .connection import SqlConnectionManager
from .schema import SqlSchemaManager
from .types import SqlTypeConverter
from ..config import SqlInputConfig

logger = logging.getLogger(__name__)


class SqlDataReader:
    """Handles reading data from SQL databases and converting to PyArrow format."""

    def __init__(self, config: SqlInputConfig, connection_manager: SqlConnectionManager,
                 schema_manager: SqlSchemaManager):
        """Initialize the data reader.

        Args:
            config: SQL input configuration
            connection_manager: Database connection manager
            schema_manager: Schema management instance
        """
        self.config = config
        self.connection_manager = connection_manager
        self.schema_manager = schema_manager
        self.type_converter = SqlTypeConverter()
        self._connection_override = None  # For test mocking support

    def _get_connection(self):
        """Get the database connection, with override support for testing."""
        if self._connection_override is not None:
            return self._connection_override
        return self.connection_manager.get_connection()

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
        connection = self._get_connection()

        # Get table schema first
        table_schema = self.schema_manager.get_table_schema(schema_name, table_name)

        cursor = connection.cursor()

        try:
            # Build query
            quoted_table = self.schema_manager._quote_identifier(table_name)
            quoted_schema = self.schema_manager._quote_identifier(schema_name)

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
            # Create empty arrays for each field in the schema
            empty_arrays = []
            for field in schema:
                empty_arrays.append(pa.array([], type=field.type))
            return pa.record_batch(empty_arrays, schema)

        # Transpose rows to columns
        columns = list(zip(*rows))

        # Convert each column according to schema
        arrays = []
        for i, (column_data, field) in enumerate(zip(columns, schema)):
            array = self.type_converter.convert_column_data(
                column_data,
                field.type,
                self.config.null_values
            )
            arrays.append(array)

        return pa.record_batch(arrays, schema)
