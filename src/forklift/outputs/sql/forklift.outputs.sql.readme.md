# Forklift SQL Outputs Package

`forklift.outputs.sql` writes validated Parquet (or any Arrow data) to a table in **PostgreSQL**,
**MySQL / MariaDB**, **SQL Server** or **Oracle** over ODBC (pyodbc). A load is **all or
nothing**: a failure, a cancellation or a killed process leaves the table as it was, and a retry
of the same job cleans up what an interrupted attempt left. This is how `sql_table` job outputs
reach a database ([ADR 0007](../../../../docs/design/adr/0007-database-sources-and-targets.md)):
the import writes `data.parquet` and `bad_rows.parquet` as always, then `write_table` loads
`data.parquet`, so rejected rows never reach the table.

```python
from forklift.outputs.sql import TableWriteError, write_table

result = write_table(
    "out/data.parquet",             # a Parquet file, a pyarrow.Table or a RecordBatchReader
    "Driver={PostgreSQL Unicode};Server=db;Port=5432;Database=sales;Uid=loader;Pwd=...",
    "orders",
    schema_name="staging",          # the login's default schema (database on MySQL) if None
    mode="upsert",                  # "create" | "append" (default) | "replace" | "upsert"
    key_columns=["order_id"],       # required for upsert; the primary key of a created table
    job_id="7f3a...",               # names the staging table, so a retry cleans up after itself
    progress=print,                 # {"rows_written": n} after each batch
    cancel=lambda: False,           # True stops the write before anything is published
)
result.rows_written, result.table, result.created, result.warnings
```

pyodbc is imported only when a table is written (`pip install forklift-etl[sql]`), so the
package imports without it. The ODBC driver decides the database: forklift reads the server's
`SQL_DBMS_NAME` (`PostgreSQL`, `MySQL`, `MariaDB`, `Microsoft SQL Server`, `Oracle`) and refuses
any other.

## Modes

| Mode | Table missing | Table exists |
|---|---|---|
| `create` | created, rows inserted | refused (`TableWriteError`) |
| `append` | created, rows inserted | rows added |
| `replace` | created, rows inserted | its rows become the source's: every row is deleted and the source's inserted, in one transaction. The table itself (its columns, grants, indexes, triggers, dependent views) stays. |
| `upsert` | created with `key_columns` as primary key | rows whose key is in the table are updated, the others inserted |

A created table gets the column types below, `NOT NULL` for columns the Arrow schema declares
non-nullable, and a primary key on `key_columns` when they are given. Writing to an existing
table never changes its definition.

**Upsert keys.** With `staging="table"` the keys must be unique and non-null in the source:
forklift counts duplicate and empty keys in the staging table and refuses the load (nothing is
published) when there are any. With `staging="none"` rows are applied in order, so the last row
of a key wins. PostgreSQL and MySQL upsert without staging with their own statement (`INSERT ...
ON CONFLICT`, `INSERT ... ON DUPLICATE KEY UPDATE`), which needs a primary key or unique index on
exactly the key columns (forklift checks the catalog first); SQL Server and Oracle use `MERGE`,
which does not.

## Writing to an existing table

Source columns are matched to the table's by name: an exact match first, otherwise the one
column whose name differs only in case. Before anything is written forklift reports, all in one
error:

- source columns the table does not have;
- source columns that cannot be written to their column: a kind of value the column does not
  take (text into an integer column, for example; see below) or a column the database computes
  (generated, computed, identity `ALWAYS`, `rowversion`);
- table columns that need a value in every row (`NOT NULL` without a default, not an identity)
  but are not in the source.

Which source values a column takes:

| Column | Takes |
|---|---|
| boolean (`boolean`, `bit`) | booleans; MySQL's `TINYINT(1)`/`BIT` and Oracle's `NUMBER(p, 0)` take integers too |
| integer | integers (and booleans on MySQL and Oracle, where booleans are numbers) |
| decimal, float | integers, decimals, floats |
| text | strings |
| binary | binary |
| date | dates |
| timestamp, timestamp with time zone (Oracle `DATE` too) | dates, timestamps with or without time zone |
| time (Oracle `INTERVAL DAY TO SECOND`) | times |
| anything else (`uuid`, `json`, enums, `xml`, ...) | strings, converted by the database (with a warning) |

Warnings (in `result.warnings`, never with values) say when time zone-aware timestamps go to a
column without a time zone (written in UTC), naive timestamps to a column with one (taken as
UTC), timestamps to an Oracle `DATE` (whole seconds), or text to a type forklift does not check.

## Type mapping (tables forklift creates)

| Arrow | PostgreSQL | MySQL / MariaDB | SQL Server | Oracle |
|---|---|---|---|---|
| `bool` | `BOOLEAN` | `BOOLEAN` (`TINYINT(1)`) | `BIT` | `NUMBER(1)` |
| `int8` / `int16` | `SMALLINT` | `TINYINT` / `SMALLINT` | `SMALLINT` | `NUMBER(3)` / `NUMBER(5)` |
| `int32` | `INTEGER` | `INT` | `INT` | `NUMBER(10)` |
| `int64` | `BIGINT` | `BIGINT` | `BIGINT` | `NUMBER(19)` |
| `uint8` | `SMALLINT` | `TINYINT UNSIGNED` | `TINYINT` | `NUMBER(3)` |
| `uint16` | `INTEGER` | `SMALLINT UNSIGNED` | `INT` | `NUMBER(5)` |
| `uint32` | `BIGINT` | `INT UNSIGNED` | `BIGINT` | `NUMBER(10)` |
| `uint64` | `NUMERIC(20)` | `BIGINT UNSIGNED` | `DECIMAL(20, 0)` | `NUMBER(20)` |
| `float16` / `float32` | `REAL` | `FLOAT` | `REAL` | `BINARY_FLOAT` |
| `float64` | `DOUBLE PRECISION` | `DOUBLE` | `FLOAT` | `BINARY_DOUBLE` |
| `decimal(p, s)` | `NUMERIC(p, s)`, p ≤ 1000 | `DECIMAL(p, s)`, p ≤ 65, s ≤ 30 | `DECIMAL(p, s)`, p ≤ 38 | `NUMBER(p, s)`, p ≤ 38 |
| `string` (`large_string`, `string_view`, dictionary) | `TEXT` | `LONGTEXT` | `NVARCHAR(MAX)` | `VARCHAR2(4000 CHAR)` |
| `string`, key column | `TEXT` | `VARCHAR(255)` | `NVARCHAR(255)` | `VARCHAR2(255 CHAR)` |
| `binary` (`large_binary`, `binary_view`) | `BYTEA` | `LONGBLOB` | `VARBINARY(MAX)` | `BLOB` |
| `binary`, key column | `BYTEA` | `VARBINARY(255)` | `VARBINARY(255)` | `RAW(255)` |
| `fixed_size_binary(n)` | `BYTEA` | `BINARY(n)`, n ≤ 255 | `BINARY(n)`, n ≤ 8000 | `RAW(n)`, n ≤ 2000 |
| `date32` / `date64` | `DATE` | `DATE` | `DATE` | `DATE` |
| `timestamp` | `TIMESTAMP(6)` | `DATETIME(6)` | `DATETIME2(6)` | `TIMESTAMP(6)` |
| `timestamp` with time zone | `TIMESTAMP(6) WITH TIME ZONE` | `DATETIME(6)`, in UTC | `DATETIMEOFFSET(6)` | `TIMESTAMP(6) WITH TIME ZONE` |
| `time32` / `time64` | `TIME(6)` | `TIME(6)` | `TIME(6)` | `INTERVAL DAY(0) TO SECOND(6)` |

Other Arrow types (lists, structs, maps, durations, `null`) are refused before connecting, all
of them named in one error. A decimal that does not fit the database's limits is refused too.

Notes:

- **Precision.** Timestamps and times are written with microseconds (Python's resolution);
  finer values are truncated, with a warning naming the column. Time zone-aware timestamps are
  written as the same instant: forklift's sessions use UTC (PostgreSQL `SET TIME ZONE 'UTC'`,
  MySQL `SET time_zone = '+00:00'`, Oracle `ALTER SESSION SET TIME_ZONE = '+00:00'`; SQL
  Server stores `+00:00` offsets).
- **MySQL** has no time zone-aware type, so such columns become `DATETIME(6)` holding UTC (with
  a warning). `TIMESTAMP` is not used because it ends in 2038. Strings are `LONGTEXT` because
  `VARCHAR` columns share a 65,535-byte row limit; key columns are `VARCHAR(255)` because
  `TEXT` cannot be indexed without a prefix. Sessions add `STRICT_ALL_TABLES` to `sql_mode`, so
  a value that does not fit its column is an error, never silently cut.
- **SQL Server** strings are Unicode (`NVARCHAR`); `NVARCHAR(MAX)` cannot be a key, hence
  `NVARCHAR(255)` for key columns.
- **Oracle** has no time-of-day type: times become the time since midnight as `INTERVAL DAY(0)
  TO SECOND(6)`. Its ODBC driver cannot fetch `INTERVAL` columns (select
  `EXTRACT(HOUR FROM c)` and so on, or convert them in a view, to read them back through ODBC).
  `VARCHAR2(4000 CHAR)` holds at most 4,000 *bytes* (fewer characters for non-ASCII text);
  longer text needs a `CLOB` column: create the table yourself and use `append`, `replace` or
  `upsert` (forklift then stages text as `CLOB` too). Oracle stores empty strings as `NULL`.
  Plain lower-case names (`orders`, `order_id`) are created in upper case, Oracle's convention
  for unquoted names, so `SELECT order_id FROM orders` works; any other name is created exactly
  as given.

## All or nothing: staging and what each database guarantees

**`staging="table"` (default).** The rows go to a staging table in the target's schema, named
`forklift_stg_<job id>_<hash of job id and table>` (50 characters; upper case on Oracle). Each
batch is committed there, so a long load holds no locks on the target and keeps no huge
transaction open. When every row is staged (and keys are checked), one transaction publishes
them, and the staging table is dropped:

| | Existing table | Missing table |
|---|---|---|
| PostgreSQL, SQL Server | one transaction: `INSERT ... SELECT` from the staging table (`append`); `DELETE` then `INSERT ... SELECT` (`replace`); `UPDATE ... FROM` + `INSERT ... WHERE NOT EXISTS` (PostgreSQL) or `MERGE ... WITH (HOLDLOCK)` (SQL Server) (`upsert`) | one transaction: `CREATE TABLE` + `INSERT ... SELECT` (DDL is transactional on both) |
| MySQL / MariaDB (InnoDB) | the same DML in one transaction (`UPDATE ... JOIN` + `INSERT ... WHERE NOT EXISTS` for upsert) | the staging table gets its primary key and is renamed to the table (`RENAME TABLE`, atomic) |
| Oracle | the same DML in one transaction (`MERGE` for upsert) | the staging table gets its primary key and is renamed (`ALTER TABLE ... RENAME TO`, atomic) |

Readers of the table see either the old rows or all the new ones, never part of a load (on SQL
Server under `READ COMMITTED` without row versioning, readers wait for the publishing
transaction instead). A failure or cancellation before the publishing commit rolls back and
drops the staging table; if even that drop fails (the connection is gone), the next write with
the same `job_id` drops the leftover first. A process killed mid-load leaves its staging table
(the batches committed so far) and nothing else; the retry drops it. If the staging table cannot
be dropped *after* a successful publish, that is only a warning in `result.warnings` (the next
write with the same job id drops it).

**`staging="none"`** writes straight into the table inside one transaction, for logins that may
not create tables: `DELETE` first for `replace`, then the inserts (or upserts), then one commit.
A failure, a cancellation or a lost connection rolls the whole transaction back. While it runs it
holds the locks of all its rows (concurrent readers see the old rows on PostgreSQL, MySQL and
Oracle; they wait on SQL Server without row versioning).

What MySQL and Oracle cannot do: their DDL is not transactional (`CREATE`, `RENAME`, `DROP`
commit at once). So:

- the staging table's creation and every staged batch are committed (that is what a retry
  cleans up);
- with `staging="none"` and a table that does not exist yet, `CREATE TABLE` commits before the
  rows are inserted: the empty table is visible during the load, and forklift drops it if the
  load fails or is cancelled (a killed process leaves it empty);
- publishing into a missing table is a rename, which is atomic, but publishing never creates a
  table and fills it in one transaction.

DML is transactional on all four (MySQL needs a transactional engine, InnoDB being the default).

**Retries after success.** A retry of a job whose publish already committed (the worker died
before reporting) writes again: harmless for `replace` and `upsert`, which are idempotent, but
`append` adds the rows a second time.

## Privileges

Privileges the login needs on each database (the error names the one that was refused):

| | PostgreSQL | MySQL / MariaDB | SQL Server | Oracle |
|---|---|---|---|---|
| staging table (`staging="table"`, and a table forklift creates) | `USAGE` and `CREATE` on the schema (it owns, so may drop, what it creates) | `CREATE`, `DROP`, `INSERT`, `SELECT` on the database (`ALTER` too to rename a staging table into a new table) | `CREATE TABLE` in the database, and `ALTER`, `INSERT`, `SELECT` on the schema | its own schema: `CREATE TABLE` and a tablespace quota; another schema: `CREATE ANY TABLE`, `DROP ANY TABLE`, `INSERT ANY TABLE`, `SELECT ANY TABLE` (`ALTER ANY TABLE` to rename) |
| `append` | `INSERT` | `INSERT` | `INSERT` | `INSERT` |
| `replace` | `INSERT`, `DELETE` | `INSERT`, `DELETE` | `INSERT`, `DELETE` | `INSERT`, `DELETE` |
| `upsert` | `SELECT`, `INSERT`, `UPDATE` | `SELECT`, `INSERT`, `UPDATE` | `SELECT`, `INSERT`, `UPDATE` | `SELECT`, `INSERT`, `UPDATE` |

The mode's privileges are on the target table. Reading the catalog needs nothing more: any
privilege on the table makes it visible in the catalog views forklift reads. `staging="none"`
needs only the mode's privileges, plus those of the first row when the table does not exist yet
(to create it). A login without `CREATE` that writes with the default staging gets an error that
says so and suggests `staging="none"`.

## Errors

`TableWriteError` (and its subclass `TableWriteCancelled`) says which table, mode and step
failed, the SQLSTATE and what it means, the driver's numeric code and what it means on that
database, and, when the database refused for lack of a privilege, the privilege the login needs:

```
Writing table sales.orders (mode 'append', staging 'table') on PostgreSQL failed: the database
refused to create the staging table sales.forklift_stg_7f3a_...: SQLSTATE 42501 (insufficient
privilege). The login needs CREATE on schema sales. Grant it, or pass staging="none" to write
straight into the table inside one transaction (no staging table needed).
```

A failed batch names its rows (`rows 10001 to 20000 of the source`), never their values. The
message never holds cell values, the driver's message text (drivers quote values: SQL Server's
truncation error quotes the cut value, Oracle's cast errors the bound one) or the connection
string (connection errors show it redacted, `Pwd=***`). The original pyodbc error is not chained
(`raise ... from None`), so a logged traceback cannot leak it either. Attributes: `table`,
`mode`, `action`, `sqlstate`, `native_code`, `privilege`, `retryable` (deadlocks, lost
connections, timeouts) and `error_code` (`PERMISSION_DENIED`, `CANCELLED`, `SPEC_INVALID` for a
name the database cannot take or a schema that does not exist, `TARGET_WRITE_FAILED` otherwise).
Invalid arguments (`mode`, `staging`, `batch_size`, `key_columns`) raise `ValueError` before
connecting; a source of the wrong type raises `TypeError`.

## Names

Table, schema and column names are validated (non-empty, no control characters, no leading or
trailing spaces, within the database's limit: 63 bytes on PostgreSQL, 64 characters on MySQL,
128 characters on SQL Server, 128 bytes on Oracle, and no `"` on Oracle) and always quoted, with
the quote character doubled (`"`, `` ` ``, `[...]`). Two source columns whose names differ only
in case are refused. An existing table and its schema are found in the catalog by exact name,
otherwise by the one name that differs only in case (two such names are refused as ambiguous).
Values are always bound as parameters.

## Performance and driver quirks

Measured on the CI services (PostgreSQL 16, MySQL 8.4, SQL Server 2022, Oracle 23ai Free with
psqlODBC 16, MariaDB Connector/ODBC 3.1, Microsoft ODBC Driver 18, Oracle Instant Client 23 ODBC
on one shared 4-CPU machine), 100,000 rows of 8 mixed columns:

| | rows/s, `staging="table"` | rows/s, `staging="none"` | How rows are sent |
|---|---|---|---|
| PostgreSQL | ~59,000 | ~66,000 | multi-row `INSERT ... VALUES` (up to 500 rows, 30,000 parameters a statement) |
| MySQL | ~46,000 | ~45,000 | multi-row `INSERT ... VALUES` |
| SQL Server | ~34,000 | ~39,000 | `fast_executemany` (ODBC parameter arrays), one row a statement |
| Oracle | ~28,000 | ~28,000 | `INSERT ... SELECT ... FROM (SELECT ... FROM dual UNION ALL ...)`, 100 rows a statement |

What was measured and why (single-row `executemany` without `fast_executemany` managed
2,000 to 5,000 rows/s everywhere):

- **psqlODBC**: `fast_executemany` reached ~8,000 rows/s (the driver sends parameter arrays one
  row at a time) and failed on decimals whose scale differs from the first row's ("Converting
  decimal loses precision"). Multi-row statements are 5 to 8 times faster.
- **MariaDB Connector/ODBC**: `fast_executemany` raised `MemoryError` on `LONGTEXT` columns
  when the first row is NULL (the driver describes the parameter as 4 GB) and was no faster.
  The driver drops the fractional seconds of timestamp and time parameters, so they are sent as
  text, and it corrupts UTF-16 text parameters longer than a few hundred characters (characters
  go missing), so forklift sends text as UTF-8 (`setencoding`).
- **Microsoft ODBC Driver 18**: `fast_executemany` is correct (NULL first rows, `MAX` types,
  decimals of any scale, long values) and the fastest; multi-row statements were 5 times
  slower.
- **Oracle ODBC**: `fast_executemany` corrupted text (later rows got zero bytes) and dropped
  fractional seconds, so it is not used. Integers from -2**31 on are bound as 64-bit
  parameters, which the driver refuses: they are sent as decimals. Timestamps lose their
  fractional seconds as parameters, and any parameter meeting an `INTERVAL` fails, so both
  are sent as text and converted on the server. Every value is cast so that the `UNION ALL`
  rows agree on their types. Inside `SELECT ... FROM dual` the driver binds text and binary as
  `VARCHAR2`/`RAW` (at most 32,767 bytes), so rows with a longer `CLOB` or `BLOB` value are
  inserted one at a time with `VALUES` (slower; not possible together with a time column, and
  not for `upsert` with `staging="none"`, which forklift then refuses with a clear error).
  Text needs `NLS_LANG` with a UTF-8 character set (otherwise non-ASCII characters in `CLOB`s
  are replaced); forklift sets `NLS_LANG=.AL32UTF8` when the variable is not set.

`batch_size` (default 10,000) is the number of rows read from the source and committed to the
staging table at a time; progress and cancellation are checked between batches.

## Package layout

- `writer.py`: `write_table`, `TableWriteResult`, `staging_table_name`; the steps of a load
- `dialects.py`: per-database identifiers, types, catalog queries, error codes and SQL
- `columns.py`: Arrow types to kinds, and Arrow values to the Python objects pyodbc binds
- `errors.py`: `TableWriteError`, `TableWriteCancelled`, SQLSTATE meanings

Tests: `tests/unit-tests/test_sql_targets_*.py` (a fake pyodbc, every step and failure on all
four dialects) and `tests/integration-tests/services/test_sql_targets.py` (real servers,
restricted logins, the guarantees above).
