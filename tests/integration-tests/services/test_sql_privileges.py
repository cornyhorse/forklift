"""SQL imports against real PostgreSQL and MySQL servers, as users with limited privileges.

Each test in ``TestLeastPrivilegeLogins``, ``TestReadOnlySessions`` and ``TestAuthentication``
runs on both servers. A login gets exactly the grants a test needs, so the tests show what
forklift does with what a database lets a user see and do:

* A login that may read one table imports it; a table it may not read fails on its own with a
  reason that names the missing privilege but never quotes the table's data.
* ``read_only=True`` (the default) is enforced by the database session itself, so even a login
  that could write cannot write through forklift.
* Wrong credentials fail before anything is written, and passwords never reach errors, logs or
  ``metadata.json``.
"""

from __future__ import annotations

import json
import logging

import pyarrow.parquet as pq
import pytest
from service_helpers import Database, Postgres, sql_schema_file

from forklift import import_sql
from forklift.engine.exceptions import ProcessingError
from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql.connection import SqlConnectionManager

pytestmark = pytest.mark.services

ORDERS = [(1, "9.50", "ana@example.com"), (2, "12.00", "bo@example.com"), (3, "3.25", None)]
SECRET_TOKEN = "tok-do-not-leak-7f3a"


def _seed(database: Database) -> None:
    database.admin(
        f"CREATE TABLE {database.table('orders')} "
        "(id INTEGER PRIMARY KEY, amount DECIMAL(10, 2), email VARCHAR(100))",
        f"CREATE TABLE {database.table('secrets')} (id INTEGER, token VARCHAR(100))",
        f"INSERT INTO {database.table('orders')} VALUES "
        + ", ".join(
            f"({i}, {amount}, {'NULL' if email is None else repr(email)})"
            for i, amount, email in ORDERS
        ),
        f"INSERT INTO {database.table('secrets')} VALUES (1, '{SECRET_TOKEN}')",
    )


@pytest.fixture
def seeded(database: Database) -> Database:
    _seed(database)
    return database


def _import(database, login, tmp_path, tables, **kwargs):
    return import_sql(
        database.login_connection_string(login),
        tmp_path / "out",
        sql_schema_file(tmp_path, database.namespace, tables),
        **kwargs,
    )


def _rows(path):
    return [
        (row["id"], f"{row['amount']:.2f}", row["email"])
        for row in pq.read_table(path).to_pylist()
    ]


class TestLeastPrivilegeLogins:
    def test_login_with_select_on_one_table_imports_that_table(self, seeded, tmp_path):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        results = _import(seeded, login, tmp_path, ["orders"])

        assert results.total_rows == 3
        assert results.errors == []
        assert _rows(tmp_path / "out" / "orders.parquet") == [
            (i, amount, email) for i, amount, email in ORDERS
        ]

    def test_table_the_login_cannot_read_fails_alone_with_the_reason(self, seeded, tmp_path):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        results = _import(seeded, login, tmp_path, ["orders", "secrets"], continue_on_error=True)

        out = tmp_path / "out"
        assert (out / "orders.parquet").exists()
        assert not (out / "secrets.parquet").exists()
        (error,) = results.errors
        assert error.startswith(f"{seeded.namespace}.secrets: ")
        # PostgreSQL lists the table and refuses the SELECT (SQLSTATE 42501); MySQL hides
        # tables a user has no privileges on, so the catalog lookup fails instead. Both say why.
        assert "privilege" in error
        metadata = json.loads((out / "metadata.json").read_text())
        (failed,) = metadata["failed_tables"]
        assert failed["table"] == "secrets"
        assert "privilege" in failed["reason"]
        assert SECRET_TOKEN not in (out / "metadata.json").read_text()

    def test_without_continue_on_error_the_import_raises_after_the_readable_tables(
        self, seeded, tmp_path
    ):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        with pytest.raises(ProcessingError, match="1 of 2 tables failed") as raised:
            _import(seeded, login, tmp_path, ["orders", "secrets"])

        assert "privilege" in str(raised.value)
        assert SECRET_TOKEN not in str(raised.value)
        assert raised.value.results.total_rows == 3
        assert (tmp_path / "out" / "orders.parquet").exists()

    def test_column_level_grant_is_reported_as_a_privilege_error(self, seeded, tmp_path):
        # forklift selects every column, so a login that may read only some of them is refused
        login = seeded.create_user()
        seeded.grant_select(login, "orders", columns=["id", "amount"])

        results = _import(seeded, login, tmp_path, ["orders"], continue_on_error=True)

        (error,) = results.errors
        assert "SQLSTATE 42" in error  # 42501 (PostgreSQL) or 42000 (MySQL): access rule
        assert "ana@example.com" not in error
        assert not (tmp_path / "out" / "orders.parquet").exists()


class TestReadOnlySessions:
    def test_read_only_session_refuses_writes_from_a_login_that_could_write(self, seeded):
        login = seeded.create_user()
        seeded.grant_write(login, "orders")
        manager = SqlConnectionManager(
            SqlInputConfig(connection_string=seeded.login_connection_string(login))
        )
        assert manager.config.read_only  # the default

        with manager:
            cursor = manager.get_connection().cursor()
            with pytest.raises(Exception) as raised:
                cursor.execute(f"DELETE FROM {seeded.table('orders')}")
                manager.get_connection().commit()

        assert raised.value.args[0] == "25006"  # read-only SQL transaction
        assert seeded.admin(f"SELECT COUNT(*) FROM {seeded.table('orders')}") == [(3,)]

    def test_the_same_login_can_write_when_read_only_is_turned_off(self, seeded):
        login = seeded.create_user()
        seeded.grant_write(login, "orders")
        manager = SqlConnectionManager(
            SqlInputConfig(
                connection_string=seeded.login_connection_string(login), read_only=False
            )
        )

        with manager:
            connection = manager.get_connection()
            connection.cursor().execute(f"DELETE FROM {seeded.table('orders')} WHERE id = 3")
            connection.commit()

        assert seeded.admin(f"SELECT COUNT(*) FROM {seeded.table('orders')}") == [(2,)]

    def test_query_timeout_cancels_a_slow_table(self, database, tmp_path):
        if database.kind == "postgres":
            slow = "SELECT 1 AS x FROM pg_sleep(5)"
        else:
            slow = "SELECT SLEEP(5) AS x"
        database.admin(f"CREATE VIEW {database.table('slow')} AS {slow}")
        login = database.create_user()
        database.grant_select(login, "slow")

        results = _import(
            database, login, tmp_path, ["slow"], query_timeout=1, continue_on_error=True
        )

        (error,) = results.errors
        assert error.startswith(f"{database.namespace}.slow: ")
        assert not (tmp_path / "out" / "slow.parquet").exists()


class TestAuthentication:
    def test_wrong_password_fails_before_anything_is_written(self, seeded, tmp_path, caplog):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")
        wrong = type(login)(login.user, "not-the-password-" + login.password)
        caplog.set_level(logging.DEBUG)

        with pytest.raises(ConnectionError) as raised:
            _import(seeded, wrong, tmp_path, ["orders"])

        assert wrong.password not in str(raised.value)
        assert wrong.password not in caplog.text
        out = tmp_path / "out"
        assert not out.exists() or not any(out.glob("*.parquet"))

    def test_password_is_not_written_to_metadata_or_logs(self, seeded, tmp_path, caplog):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")
        caplog.set_level(logging.DEBUG)

        _import(seeded, login, tmp_path, ["orders"])

        assert login.password not in (tmp_path / "out" / "metadata.json").read_text()
        assert login.password not in caplog.text

    def test_login_without_any_grant_cannot_import(self, seeded, tmp_path):
        login = seeded.create_user()  # PostgreSQL: schema USAGE only; MySQL: no grants at all

        with pytest.raises((ConnectionError, ProcessingError)) as raised:
            _import(seeded, login, tmp_path, ["orders"])

        assert login.password not in str(raised.value)
        assert not (tmp_path / "out" / "orders.parquet").exists()


class TestPostgresOnly:
    def test_row_level_security_limits_the_import_to_visible_rows(self, postgres, tmp_path):
        _seed(postgres)
        login = postgres.create_user()
        postgres.grant_select(login, "orders")
        postgres.admin(
            f"ALTER TABLE {postgres.table('orders')} ENABLE ROW LEVEL SECURITY",
            f"CREATE POLICY own_rows ON {postgres.table('orders')} FOR SELECT "
            f"TO {login.user} USING (email LIKE '%@example.com')",
        )

        results = _import(postgres, login, tmp_path, ["orders"])

        assert results.total_rows == 2  # the row without an e-mail is not visible
        assert [r[0] for r in _rows(tmp_path / "out" / "orders.parquet")] == [1, 2]

    def test_schema_without_usage_is_a_privilege_error(self, postgres: Postgres, tmp_path):
        _seed(postgres)
        login = postgres.create_user(schema_usage=False)
        postgres.grant_select(login, "orders")  # useless without USAGE on the schema

        results = _import(postgres, login, tmp_path, ["orders"], continue_on_error=True)

        (error,) = results.errors
        assert "privilege" in error
        assert not (tmp_path / "out" / "orders.parquet").exists()

    def test_read_only_session_blocks_writes_hidden_in_a_view(self, postgres, tmp_path):
        # A view can call a function that writes. Reading it through forklift must not write.
        _seed(postgres)
        audit, orders = postgres.table("audit"), postgres.table("orders")
        postgres.admin(
            f"CREATE TABLE {audit} (seen_at TIMESTAMP)",
            f"CREATE FUNCTION {postgres.namespace}.touch() RETURNS INTEGER LANGUAGE sql AS "
            f"'INSERT INTO {audit} VALUES (now()) RETURNING 1'",
            f"CREATE VIEW {postgres.table('orders_view')} AS "
            f"SELECT id, {postgres.namespace}.touch() AS touched FROM {orders}",
        )
        login = postgres.create_user()
        postgres.grant_select(login, "orders_view")
        postgres.grant_write(login, "audit")

        results = _import(postgres, login, tmp_path, ["orders_view"], continue_on_error=True)

        (error,) = results.errors
        assert "25006" in error  # cannot execute INSERT in a read-only transaction
        assert postgres.admin(f"SELECT COUNT(*) FROM {audit}") == [(0,)]
