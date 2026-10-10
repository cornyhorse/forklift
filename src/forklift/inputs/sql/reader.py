"""SQL data reading and batch processing."""

from __future__ import annotations

import json
import logging
import time
from typing import Iterator, List, Optional, Tuple

import pyarrow as pa

from ..config import SqlInputConfig
from .connection import READ_RETRIES, SqlConnectionManager
from .errors import ColumnPrivilegeError, describe_database_error, is_privilege_error, listed
from .schema import SqlSchemaManager, _is_default_schema, table_label
from .types import SqlTypeConverter

logger = logging.getLogger(__name__)


class SqlDataReader:
    """Handles reading data from SQL databases and converting to PyArrow format."""

    def __init__(
        self,
        config: SqlInputConfig,
        connection_manager: SqlConnectionManager,
        schema_manager: SqlSchemaManager,
    ):
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

    def read_table_data(
        self, schema_name: str, table_name: str, columns: Optional[List[str]] = None
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table in batches.

        Args:
            schema_name: Database schema name
            table_name: Table name
            columns: The columns to read, in this order (``x-sql`` ``select.columns``); every
                column (``SELECT *``) when None

        Yields:
            PyArrow RecordBatch objects

        Raises:
            ConnectionError: If not connected to database
            ValueError: If the table, or a declared column, is not in the database catalog
            ColumnPrivilegeError: If the database refuses the query for a missing privilege
                (the message names the columns the login may read and how to declare them)
        """
        connection = self._get_connection()

        # Get table schema first. This also validates the requested names against the
        # database catalog: unknown tables and columns raise before any SQL is built.
        table_schema = self.schema_manager.get_table_schema(
            schema_name, table_name, columns=columns
        )
        resolved_schema, resolved_table = self.schema_manager.resolve_table(
            schema_name, table_name
        )

        cursor = connection.cursor()

        try:
            # Build query from catalog-verified, always-quoted identifiers
            full_table_name = self.schema_manager.qualified_table_name(
                resolved_schema, resolved_table
            )
            selected = (
                ", ".join(self.schema_manager._quote_identifier(f.name) for f in table_schema)
                if columns is not None
                else "*"
            )
            query = f"SELECT {selected} FROM {full_table_name}"

            # Set fetch size if specified
            if self.config.fetch_size:
                cursor.arraysize = self.config.fetch_size

            logger.info(f"Executing query: {query}")
            try:
                self._execute(cursor, query)
            except Exception as error:
                self._explain_refusal(
                    error, schema_name, table_name, full_table_name, table_schema, columns
                )
                raise

            # Process data in batches
            while True:
                with self.connection_manager.statement_deadline(cursor):
                    rows = cursor.fetchmany(self.config.batch_size)
                if not rows:
                    break

                # Convert rows to PyArrow batch
                batch = self._rows_to_recordbatch(rows, table_schema)
                yield batch

        finally:
            cursor.close()

    def _execute(self, cursor, query: str) -> None:
        """Run the table's query in a new read transaction (see ``begin_read``).

        On Oracle a table created or altered seconds before the read-only transaction began
        cannot be read in it (ORA-01466); the query is tried again in a new one, up to
        ``READ_RETRIES`` times one second apart.
        """
        retries = 0
        while True:
            self.connection_manager.begin_read()
            try:
                with self.connection_manager.statement_deadline(cursor):
                    cursor.execute(query)
                return
            except Exception as error:
                if retries == READ_RETRIES or not self.connection_manager.read_can_be_retried(
                    error
                ):
                    raise
            retries += 1
            logger.info(
                "The table changed just before the read-only transaction began; reading it "
                "in a new one (retry %s of %s)",
                retries,
                READ_RETRIES,
            )
            time.sleep(1)

    def _explain_refusal(
        self,
        error: Exception,
        schema_name: str,
        table_name: str,
        full_table_name: str,
        table_schema: pa.Schema,
        declared: Optional[List[str]],
    ) -> None:
        """Turn a refused query into an error that says which columns the login may read.

        When the database refused the query for a missing privilege, each column is tried on
        its own (``SELECT <column> ... WHERE 1=0`` reads no rows) to learn which ones the login
        may read, and :class:`ColumnPrivilegeError` names them and the ``select.columns``
        declaration that imports them. Any other error is left to the caller to raise.

        Raises:
            ColumnPrivilegeError: If ``error`` is a privilege error
        """
        if not is_privilege_error(error, self.connection_manager.dbms):
            return
        readable = []
        for field in table_schema:
            probe = self._probe_column(field.name, full_table_name)
            if probe is None:
                readable.append(field.name)
            elif not is_privilege_error(probe, self.connection_manager.dbms):
                return  # not a matter of column privileges after all
        label = table_label(schema_name, table_name)
        reason = describe_database_error(error)
        if declared is not None:
            refused = [field.name for field in table_schema if field.name not in readable]
            if not refused:
                return  # each column may be read: not a matter of column privileges
            message = (
                f"The database refused to read the columns of '{label}' declared in "
                f"select.columns ({reason}): the connecting user may not read "
                f"{listed(refused)}"
                + (f" (it may read {listed(readable)})" if readable else "")
                + ". Remove those from select.columns, or grant the user SELECT on them"
            )
        elif readable:
            select = {"name": table_name, "columns": readable}
            if not _is_default_schema(schema_name):
                select = {"schema": schema_name, **select}
            message = (
                f"The database refused to read every column of '{label}' ({reason}): the "
                f"connecting user may read only the columns {listed(readable)}. To import "
                "those, declare them in the table's x-sql entry: "
                f'"select": {json.dumps(select, ensure_ascii=False)}'
            )
        else:
            message = (
                f"The database refused to read '{label}' ({reason}): the connecting user has "
                "no privilege to read any of its columns. Grant it SELECT on the table, or on "
                "the columns to import and declare those in the table's x-sql select.columns"
            )
        raise ColumnPrivilegeError(message) from error

    def _probe_column(self, column: str, full_table_name: str) -> Optional[Exception]:
        """Run a query that reads ``column`` but no rows; returns its error, or None if allowed."""
        cursor = self._get_connection().cursor()
        try:
            with self.connection_manager.statement_deadline(cursor):
                cursor.execute(
                    f"SELECT {self.schema_manager._quote_identifier(column)} "
                    f"FROM {full_table_name} WHERE 1=0"
                )
            return None
        except Exception as error:
            return error
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
                column_data, field.type, self.config.null_values
            )
            arrays.append(array)

        return pa.record_batch(arrays, schema)
