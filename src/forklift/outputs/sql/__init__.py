"""Write validated Parquet (or Arrow data) to database tables, all or nothing.

``write_table`` loads a Parquet file, ``pyarrow.Table`` or ``RecordBatchReader`` into a table in
PostgreSQL, MySQL/MariaDB, SQL Server or Oracle over ODBC (pyodbc), in mode ``create``,
``append``, ``replace`` or ``upsert``. By default the rows go to a staging table named after the
job and are published in one transaction; a failure or cancellation leaves the table as it was.
See ``forklift.outputs.sql.readme.md`` for the type mapping, what each database guarantees and
the privileges each mode needs.

pyodbc is imported only when a table is written, so this package imports without it.
"""

from .errors import TableWriteCancelled, TableWriteError
from .writer import MODES, STAGING, TableWriteResult, staging_table_name, write_table

__all__ = [
    "MODES",
    "STAGING",
    "TableWriteCancelled",
    "TableWriteError",
    "TableWriteResult",
    "staging_table_name",
    "write_table",
]
