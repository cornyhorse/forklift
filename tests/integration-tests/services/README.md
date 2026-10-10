# Service-backed integration tests

These tests run forklift against real services instead of mocks:

| Service | Image (override) | Host port | Used for |
|---|---|---|---|
| RustFS | `rustfs/rustfs` | 19000 | S3 inputs and outputs, scoped credentials (STS session policies) |
| PostgreSQL | `postgres:16` | 15432 | SQL imports, roles, grants, row-level security, read-only sessions |
| MySQL | `mysql:8.4` | 13306 | SQL imports, users, grants, read-only sessions |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` (`FORKLIFT_TEST_MSSQL_IMAGE`) | 11433 | SQL imports, logins and database users, column grants, type mapping |
| Oracle Database Free | `gvenzl/oracle-free:23-slim-faststart` (`FORKLIFT_TEST_ORACLE_IMAGE`) | 11521 | SQL imports, users as schemas, read-only transactions, type mapping |

RustFS is an S3-compatible object store (Apache-2.0). It replaces MinIO as the store used for
testing; any S3-compatible service works with forklift. The SQL Server and Oracle images are
large (about 2.3 and 6.5 GB); point `FORKLIFT_TEST_MSSQL_IMAGE` / `FORKLIFT_TEST_ORACLE_IMAGE` at
a copy in your own registry to pull them from there.

## Running them

```bash
# ODBC drivers (Debian/Ubuntu); SQL Server's and Oracle's: see "ODBC drivers" below
sudo apt-get install unixodbc odbc-postgresql odbc-mariadb
./scripts/test-services.sh up                                 # start, wait until healthy
FORKLIFT_TEST_SERVICES=1 python -m pytest tests/integration-tests/services --no-cov
./scripts/test-services.sh down                               # stop, delete their data
```

`./scripts/test-services.sh test` does the first two steps in one go, and `up mssql oracle` (or
any list of services) starts only those. Without `FORKLIFT_TEST_SERVICES=1` the tests are
skipped, so the normal test run needs no services. With it, a service that cannot be reached, or
a database whose ODBC driver is missing, fails the tests instead of skipping them, so a CI job
cannot pass by testing nothing. Oracle takes about a minute to become healthy on first start.

Every test creates its own bucket, database logins and namespace and removes them afterwards,
so the tests can run against long-lived services and in any order. The namespace is a schema on
PostgreSQL and SQL Server (in the database `forklift_test`, which the fixtures create on SQL
Server), a database on MySQL and a user on Oracle (each Oracle user is a schema).

## ODBC drivers

| Database | Driver | Debian/Ubuntu |
|---|---|---|
| PostgreSQL | `PostgreSQL Unicode` (psqlODBC) | `apt-get install odbc-postgresql` |
| MySQL | `MariaDB Unicode` (or MySQL Connector/ODBC) | `apt-get install odbc-mariadb` |
| SQL Server | `ODBC Driver 18 for SQL Server` | `msodbcsql18` from packages.microsoft.com (`ACCEPT_EULA=Y`) |
| Oracle | `Oracle 23 ODBC driver` (Instant Client) | Instant Client Basic and ODBC zips from oracle.com, then `odbc_update_ini.sh` |

The fixtures pick the first installed driver whose name matches; `FORKLIFT_TEST_<DB>_DRIVER`
names one explicitly. CI installs all four on `ubuntu-latest` (the services job in
`.github/workflows/test.yml`).

## What they cover

- **`test_sql_privileges.py`** (each test on all four databases unless marked otherwise):
  a login with `SELECT` on one table imports it; a table it may not read fails alone, with a
  reason that names the missing privilege and never quotes the table's data; `select.columns`
  reads the declared columns in their order, matched as the database matches names (Oracle's
  `ID` for `id`), and an unknown one fails the table with the columns there are; column-level
  grants (PostgreSQL, MySQL, SQL Server): the declared columns are imported, and without a
  declaration the error names the columns the login may read and the declaration to add;
  `read_only=True` sessions refuse writes even from a login that may write; query timeouts;
  wrong passwords; passwords never in errors, logs or `metadata.json`; PostgreSQL row-level
  security, schema `USAGE` and a view whose function writes; Oracle's read-only transaction
  renewed before each table.
- **`test_sql_types.py`**: SQL Server (`money`, `datetime2`, `datetimeoffset`,
  `uniqueidentifier`, `rowversion`, ...) and Oracle (`NUMBER` with and without precision,
  `DATE`, time-zone timestamps, `CLOB`, `RAW`, `BOOLEAN`, ...) types as they arrive in Parquet.
- **`test_sql_targets.py`** (each test on all four databases): `forklift.outputs.sql.write_table`
  creates every mapped Arrow type as the documented column type and reads it back unchanged;
  `create`, `append`, `replace` and `upsert` with and without a staging table; a value that does
  not fit, a cancellation and a killed process leave the table unchanged, and a retry with the
  same `job_id` drops the staging table a killed attempt left; logins without `CREATE` (the
  error suggests `staging="none"`), `INSERT`, `DELETE` or `UPDATE` are refused with the
  privilege named; wrong passwords never echoed. `sql_target_helpers.py` adds a login that may
  stage but not delete or update, and reads values back portably.
- **`test_s3_object_store.py`**: CSV, Excel and SQL exports to and from the store; the
  `x-validation` threshold keeping `bad_rows.parquet`; read-only credentials; writers confined
  to the prefix they were granted; refused reads reported as refused, not missing; no objects or
  unfinished multipart uploads left behind by a refused write; wrong secrets never echoed.

`tests/integration-tests/test_local_file_permissions.py` covers the same ground for local files
(unreadable inputs, read-only output directories) and runs in the normal test suite as any
non-root user.

### What a database cannot do

These are tested as what they are, not skipped:

- **Oracle grants `SELECT` on whole tables only** (only `INSERT`, `UPDATE` and `REFERENCES` can be
  granted per column): `GRANT SELECT (id) ON t` fails with ORA-00969. A view of the columns a
  login may read is Oracle's way, and the test imports one.
- **Oracle makes only transactions read-only.** forklift starts a read-only transaction when it
  connects and again before each table. A function running in an autonomous transaction
  (`PRAGMA AUTONOMOUS_TRANSACTION`) writes even from a read-only transaction; a test shows it.
  A table created in the seconds before a read-only transaction began cannot be read in it
  (ORA-01466): forklift retries in a new one, so tests on just-created tables take a moment
  longer on Oracle.
- **SQL Server has no read-only session** a login could set, and its ODBC driver ignores the
  read-only access mode: forklift logs a warning, and the test shows that the login's own
  privileges are all that protects the data. SQL Server functions cannot write, so a view cannot
  hide a write the way a PostgreSQL one can.
- **Oracle's ODBC driver has no query timeout**: forklift cancels the statement itself after
  `query_timeout` (the cancel takes effect when the server next checks, ORA-01013).

## Helpers for other tests

`service_helpers.Database` (the `database`, `postgres`, `mysql`, `mssql`, `oracle` and
`column_grant_database` fixtures yield one, with an empty namespace) offers the same calls on
every database:

| Call | What it does |
|---|---|
| `create_user()` | A login that may connect and nothing else (PostgreSQL: plus `USAGE` on the namespace, unless `schema_usage=False`) |
| `grant(login, table, "SELECT", "INSERT", "UPDATE", "DELETE", columns=())` | Table privileges; `grant_select(login, table, columns=())` and `grant_write(login, table)` are shorthands |
| `owner_login()` | A login that may create tables in the namespace, and write to and drop the tables it creates. MySQL (no table owners), SQL Server (the schema owner owns its objects) and Oracle (the namespace's own user) also let it read, write and drop the admin's tables there |
| `login_connection_string(login)` | The ODBC connection string to give forklift |
| `admin(*statements)`, `query(sql, *params)` | Run SQL as the admin |
| `table(name)` | `namespace.name` for the tests' own (unquoted) SQL |
| `table_names()`, `table_exists(name)`, `column_names(name)` | The namespace's tables and a table's columns, as the catalog spells them; names are matched exactly, else ignoring case |
| `qualified(name)`, `quote(identifier)` | Quoted names as the catalog spells them |
| `row_count(name)`, `rows(name, order_by=None)` | What a table holds |
| `create_slow_view(name)` | A view whose query runs for seconds (timeout tests) |
| `is_read_only_refusal(error)`, `read_only_refusal`, `supports_column_grants` | What the database can do |

## Credentials forklift needs

- **Database**: `SELECT` on the tables in the schema file, or on the columns its
  `select.columns` lists (and `USAGE` on their schema in PostgreSQL). Nothing else: forklift only
  reads, and makes its sessions read-only on PostgreSQL, MySQL, MariaDB and SQLite and its
  transactions read-only on Oracle (SQL Server: connect as a login that may only `SELECT`).
- **Database target** (`write_table`): see the privileges table in
  [forklift.outputs.sql.readme.md](../../../src/forklift/outputs/sql/forklift.outputs.sql.readme.md).
- **S3 input**: `s3:GetObject` on the input and schema objects. No `s3:ListBucket`.
- **S3 output**: `s3:PutObject`, `s3:AbortMultipartUpload` (to clean up a failed upload) and
  `s3:DeleteObject` (outputs of an earlier run in the same destination are removed first) on the
  output prefix.

## Settings

The defaults match `compose.yaml`. Override them to use other servers:

| Variable | Default |
|---|---|
| `FORKLIFT_TEST_S3_ENDPOINT` | `http://127.0.0.1:19000` |
| `FORKLIFT_TEST_S3_ACCESS_KEY` / `_SECRET_KEY` | `forklift-test` / `forklift-test-secret` |
| `FORKLIFT_TEST_PG_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | `127.0.0.1` / `15432` / `forklift_admin` / `forklift-admin-secret` / `forklift_test` |
| `FORKLIFT_TEST_MYSQL_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | `127.0.0.1` / `13306` / `root` / `forklift-admin-secret` / `forklift_test` |
| `FORKLIFT_TEST_MSSQL_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | `127.0.0.1` / `11433` / `sa` / `Forklift-Admin-Secret-1` / `forklift_test` |
| `FORKLIFT_TEST_ORACLE_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | `127.0.0.1` / `11521` / `system` / `forklift-admin-secret` / `FREEPDB1` (the service name) |
| `FORKLIFT_TEST_PG_DRIVER`, `_MYSQL_DRIVER`, `_MSSQL_DRIVER`, `_ORACLE_DRIVER` | the first installed matching ODBC driver |
| `FORKLIFT_TEST_MSSQL_IMAGE`, `FORKLIFT_TEST_ORACLE_IMAGE` (compose only) | the images above |

The admin logins need to create schemas, databases or users and logins, and grant privileges;
on SQL Server also to create the database `forklift_test` and end sessions (`KILL`), on Oracle
to end sessions (`ALTER SYSTEM KILL SESSION`) so a test's users can be dropped.
