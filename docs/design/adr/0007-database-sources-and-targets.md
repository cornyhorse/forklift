# ADR 0007: Relational databases are sources and targets now; warehouses next

- **Status**: Proposed
- **Date**: 2026-10-10
- **Context document**: [platform design](../platform.md), §7.3 and §13
- **Changes**: [ADR 0005](0005-storage-and-destinations.md), which put database tables after
  everything else (milestone M6)

## Context

ADR 0005 made database tables a destination only once Parquet output was dependable. Parquet
output now has full line and branch coverage and runs against a real S3-compatible store
(RustFS) and local volumes in CI. Users want every relational database to be both a source and a
target, and later the cloud warehouses (Snowflake, Databricks, BigQuery).

The engine already reads PostgreSQL and MySQL tables over ODBC. Testing those reads against real
servers with restricted logins found that PostgreSQL connections had never worked and that
`read_only` was not enforced; tests against real servers are therefore part of the decision, not
an afterthought.

## Decision

- **Sources and targets** over ODBC (pyodbc) for PostgreSQL, MySQL/MariaDB, SQL Server and
  Oracle. Each is tested against a real server in CI (one Compose file for local runs and CI),
  as logins with only the privileges a test grants.
- **A load always goes through validated Parquet.** The import writes `data.parquet` and
  `bad_rows.parquet` as before; `forklift.outputs.sql.write_table` then loads `data.parquet` into
  the table. Rejected rows never reach the table, and the Parquet files stay as job artifacts.
- **All or nothing.** By default rows go to a staging table named after the job, and one
  transaction publishes them (insert-select, merge or a swap, whatever is atomic on that
  database); a retry first drops the staging table an earlier attempt left. Logins that may not
  create tables can load directly inside one transaction (`staging="none"`).
- **Modes**: `create`, `append`, `replace`, `upsert` (on key columns).
- **In the service**, databases are `sql` connections with encrypted credentials. Jobs that read
  from or write to them run on the `sql` lane; its workers receive the connection string with
  the lease, hold it in memory and in the job's scratch spec only, and their egress allows the
  configured database hosts.
- **Warehouses come next**, each through its own connector rather than ODBC, because bulk loading
  is what matters there: Snowflake stages and `COPY INTO`, Databricks `COPY INTO` from object
  storage, BigQuery load jobs from Parquet. None of them can run in a container, so their tests
  run in CI only when an account's credentials are configured as repository secrets.

## Consequences

- The SQL lane is part of the service MVP instead of a later milestone; its residual risk (SQL
  credentials reach SQL-lane workers for the duration of a job) is already listed in design §8.
- Four database servers in CI make the services job slower (SQL Server and Oracle images are
  large); they run in their own job, in parallel with the unit tests.
- What "atomic" means differs per database (MySQL and Oracle commit DDL implicitly); the
  guarantees are documented per database next to `write_table`, in "All or nothing" in
  [forklift.outputs.sql.readme.md](../../../src/forklift/outputs/sql/forklift.outputs.sql.readme.md).
