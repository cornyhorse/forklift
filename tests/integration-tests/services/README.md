# Service-backed integration tests

These tests run forklift against real services instead of mocks:

| Service | Image | Host port | Used for |
|---|---|---|---|
| RustFS | `rustfs/rustfs` | 19000 | S3 inputs and outputs, scoped credentials (STS session policies) |
| PostgreSQL | `postgres:16` | 15432 | SQL imports, roles, grants, row-level security, read-only sessions |
| MySQL | `mysql:8.4` | 13306 | SQL imports, users, grants, read-only sessions |

RustFS is an S3-compatible object store (Apache-2.0). It replaces MinIO as the store used for
testing; any S3-compatible service works with forklift.

## Running them

```bash
sudo apt-get install unixodbc odbc-postgresql odbc-mariadb   # ODBC drivers (Debian/Ubuntu)
./scripts/test-services.sh up                                 # start, wait until healthy
FORKLIFT_TEST_SERVICES=1 python -m pytest tests/integration-tests/services --no-cov
./scripts/test-services.sh down                               # stop, delete their data
```

`./scripts/test-services.sh test` does the first two steps in one go. Without
`FORKLIFT_TEST_SERVICES=1` the tests are skipped, so the normal test run needs no services.
With it, a service that cannot be reached fails the tests instead of skipping them, so a CI job
cannot pass by testing nothing.

Every test creates its own bucket, database logins and schema (PostgreSQL) or database (MySQL)
and removes them afterwards, so the tests can run against long-lived services and in any order.

## What they cover

- **`test_sql_privileges.py`** (each test on PostgreSQL and on MySQL unless marked otherwise):
  a login with `SELECT` on one table imports it; a table it may not read fails alone, with a
  reason that names the missing privilege and never quotes the table's data; column-level
  grants; `read_only=True` sessions refuse writes even from a login that may write (and a
  PostgreSQL view whose function writes); query timeouts; wrong passwords; passwords never in
  errors, logs or `metadata.json`; PostgreSQL row-level security and schema `USAGE`.
- **`test_s3_object_store.py`**: CSV, Excel and SQL exports to and from the store; the
  `x-validation` threshold keeping `bad_rows.parquet`; read-only credentials; writers confined
  to the prefix they were granted; refused reads reported as refused, not missing; no objects or
  unfinished multipart uploads left behind by a refused write; wrong secrets never echoed.

`tests/integration-tests/test_local_file_permissions.py` covers the same ground for local files
(unreadable inputs, read-only output directories) and runs in the normal test suite as any
non-root user.

## Credentials forklift needs

- **Database**: `SELECT` on the tables in the schema file (and `USAGE` on their schema in
  PostgreSQL). Nothing else: forklift only reads, and opens read-only sessions on PostgreSQL,
  MySQL, MariaDB and SQLite.
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
| `FORKLIFT_TEST_PG_DRIVER`, `FORKLIFT_TEST_MYSQL_DRIVER` | the first installed matching ODBC driver |

The admin logins need to create schemas or databases and logins, and grant privileges.
