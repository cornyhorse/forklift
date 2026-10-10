"""SQL imports against real PostgreSQL, MySQL, SQL Server and Oracle servers, as limited logins.

The tests that take the ``database`` fixture run on all four servers. A login gets exactly the
grants a test needs, so the tests show what forklift does with what a database lets a user see
and do:

* A login that may read one table imports it; a table it may not read fails on its own with a
  reason that names the missing privilege but never quotes the table's data.
* A login that may read only some columns imports the columns the schema declares in
  ``select.columns``; without a declaration the error names the columns to declare.
* ``read_only=True`` (the default) is enforced by the database itself where it can be, so even
  a login that could write cannot write through forklift.
* Wrong credentials fail before anything is written, and passwords never reach errors, logs or
  ``metadata.json``.

What a database cannot do is tested as such rather than skipped: Oracle grants SELECT on whole
tables only (its column-grant test shows the grant refused, and a view limiting the columns
instead), SQL Server has no read-only session forklift could set (the read-only test shows the
warning, and that the login's own privileges are what protects the data), and an Oracle function
running in an autonomous transaction writes even from a read-only transaction.
"""

from __future__ import annotations

import json
import logging

import pyarrow.parquet as pq
import pytest
from service_helpers import Database, Oracle, Postgres, sql_schema_file

from forklift import import_sql
from forklift.engine.exceptions import ProcessingError
from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.sql.connection import SqlConnectionManager
from forklift.inputs.sql.errors import database_error_codes

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


@pytest.fixture
def column_seeded(column_grant_database: Database) -> Database:
    _seed(column_grant_database)
    return column_grant_database


def _import(database, login, tmp_path, tables, columns=None, **kwargs):
    return import_sql(
        database.login_connection_string(login),
        tmp_path / "out",
        sql_schema_file(tmp_path, database.namespace, tables, columns=columns),
        **kwargs,
    )


def _records(path):
    # Oracle stores unquoted names in upper case, and forklift keeps the catalog's spelling
    return [
        {key.lower(): value for key, value in row.items()}
        for row in pq.read_table(path).to_pylist()
    ]


def _rows(path):
    return [(row["id"], f"{row['amount']:.2f}", row["email"]) for row in _records(path)]


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
        # PostgreSQL lists the table and refuses the SELECT (SQLSTATE 42501); MySQL, SQL Server
        # and Oracle hide tables a user has no privileges on, so the catalog lookup fails
        # instead. Both say why.
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


class TestDeclaredColumns:
    def test_declared_columns_are_read_in_the_declared_order(self, seeded, tmp_path):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        results = _import(seeded, login, tmp_path, ["orders"], columns={"orders": ["email", "id"]})

        assert results.total_rows == 3
        table = pq.read_table(tmp_path / "out" / "orders.parquet")
        assert [name.lower() for name in table.column_names] == ["email", "id"]
        assert [
            (row["email"], row["id"]) for row in _records(tmp_path / "out" / "orders.parquet")
        ] == [(email, i) for i, _, email in ORDERS]

    def test_names_are_matched_like_the_database_matches_them(self, seeded, tmp_path):
        # Oracle stores ID, PostgreSQL id; either spelling finds the column on every database
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        _import(seeded, login, tmp_path, ["orders"], columns={"orders": ["ID", "Amount"]})

        names = pq.read_table(tmp_path / "out" / "orders.parquet").column_names
        assert [name.lower() for name in names] == ["id", "amount"]
        assert names == seeded.column_names("orders")[:2]  # the catalog's own spelling

    def test_unknown_declared_column_fails_the_table_with_the_columns_there_are(
        self, seeded, tmp_path
    ):
        login = seeded.create_user()
        seeded.grant_select(login, "orders")

        results = _import(
            seeded,
            login,
            tmp_path,
            ["orders"],
            columns={"orders": ["id", "e_mail"]},
            continue_on_error=True,
        )

        (error,) = results.errors
        assert "ColumnLookupError" in error
        assert "Column 'e_mail' declared in select.columns" in error
        assert "email" in error.lower()  # the catalog's columns are listed
        assert not (tmp_path / "out" / "orders.parquet").exists()


class TestColumnGrants:
    def test_login_with_column_grants_imports_the_declared_columns(self, column_seeded, tmp_path):
        login = column_seeded.create_user()
        column_seeded.grant_select(login, "orders", columns=["id", "amount"])

        results = _import(
            column_seeded, login, tmp_path, ["orders"], columns={"orders": ["id", "amount"]}
        )

        assert results.errors == []
        rows = _records(tmp_path / "out" / "orders.parquet")
        assert [(row["id"], f"{row['amount']:.2f}") for row in rows] == [
            (i, amount) for i, amount, _ in ORDERS
        ]

    def test_without_a_declaration_the_error_names_the_declaration_that_fixes_it(
        self, column_seeded, tmp_path
    ):
        login = column_seeded.create_user()
        column_seeded.grant_select(login, "orders", columns=["id", "amount"])

        results = _import(column_seeded, login, tmp_path, ["orders"], continue_on_error=True)

        (error,) = results.errors
        assert "ColumnPrivilegeError" in error
        assert "may read only the columns id, amount" in error
        assert (
            f'"select": {{"schema": "{column_seeded.namespace}", "name": "orders", '
            '"columns": ["id", "amount"]}' in error
        )
        assert "ana@example.com" not in error
        assert not (tmp_path / "out" / "orders.parquet").exists()

    def test_declared_column_the_login_may_not_read_is_named(self, column_seeded, tmp_path):
        login = column_seeded.create_user()
        column_seeded.grant_select(login, "orders", columns=["id", "amount"])

        results = _import(
            column_seeded,
            login,
            tmp_path,
            ["orders"],
            columns={"orders": ["id", "email"]},
            continue_on_error=True,
        )

        (error,) = results.errors
        if column_seeded.kind == "mysql":
            # MySQL's catalog hides the columns a user may not read
            assert "Column 'email' declared in select.columns" in error
            assert "no privileges on it" in error
        else:
            assert "may not read email (it may read id)" in error
        assert "ana@example.com" not in error

    def test_oracle_grants_select_on_whole_tables_so_a_view_limits_the_columns(
        self, oracle: Oracle, tmp_path
    ):
        import pyodbc

        _seed(oracle)
        login = oracle.create_user()
        with pytest.raises(pyodbc.Error) as refused:
            oracle.grant_select(login, "orders", columns=["id", "amount"])
        assert database_error_codes(refused.value)[1] == 969  # ORA-00969: missing ON keyword

        # Oracle's way: a view of the columns the login may read
        oracle.admin(
            f"CREATE VIEW {oracle.table('orders_public')} AS "
            f"SELECT id, amount FROM {oracle.table('orders')}"
        )
        oracle.grant_select(login, "orders_public")

        results = _import(oracle, login, tmp_path, ["orders_public"])

        assert results.total_rows == 3
        assert pq.read_table(tmp_path / "out" / "orders_public.parquet").column_names == [
            "ID",
            "AMOUNT",
        ]


class TestReadOnlySessions:
    def test_read_only_session_refuses_writes_where_the_database_can(self, seeded, caplog):
        login = seeded.create_user()
        seeded.grant_write(login, "orders")
        manager = SqlConnectionManager(
            SqlInputConfig(connection_string=seeded.login_connection_string(login))
        )
        assert manager.config.read_only  # the default
        caplog.set_level(logging.WARNING)

        with manager:
            cursor = manager.get_connection().cursor()
            if seeded.read_only_refusal is None:
                # SQL Server has no read-only session: forklift says so, and only the login's
                # privileges stand between it and the data
                assert "cannot make a microsoft sql server session read-only" in caplog.text
                cursor.execute(f"DELETE FROM {seeded.table('orders')} WHERE id = 3")
                manager.get_connection().commit()
                assert seeded.row_count("orders") == 2
                return
            with pytest.raises(Exception) as raised:
                cursor.execute(f"DELETE FROM {seeded.table('orders')}")
                manager.get_connection().commit()

        # 25006 "read-only SQL transaction" (PostgreSQL, MySQL); ORA-01456 (Oracle)
        assert seeded.is_read_only_refusal(raised.value)
        assert seeded.row_count("orders") == 3

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

        assert seeded.row_count("orders") == 2

    def test_query_timeout_cancels_a_slow_table(self, database, tmp_path):
        database.create_slow_view("slow")
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
        login = seeded.create_user()  # PostgreSQL: schema USAGE only; elsewhere no grants at all

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
        assert postgres.row_count("audit") == 0


class TestOracleOnly:
    def test_each_table_is_read_in_a_new_read_only_transaction(self, oracle: Oracle):
        # SET TRANSACTION READ ONLY ends with the transaction. After a commit, reading a table
        # starts a new read-only transaction, so the session still cannot write.
        import pyodbc

        _seed(oracle)
        login = oracle.create_user()
        oracle.grant_write(login, "orders")
        handler = SqlInputHandler(
            SqlInputConfig(connection_string=oracle.login_connection_string(login))
        )

        with handler:
            connection = handler.connection_manager.get_connection()
            connection.commit()  # ends the read-only transaction begun when connecting
            rows = sum(b.num_rows for b in handler.read_table_data(oracle.namespace, "orders"))
            with pytest.raises(pyodbc.Error) as raised:
                connection.cursor().execute(f"DELETE FROM {oracle.table('orders')}")

        assert rows == 3
        assert oracle.is_read_only_refusal(raised.value)  # ORA-01456
        assert oracle.row_count("orders") == 3

    def test_autonomous_transaction_in_a_view_writes_despite_read_only(
        self, oracle: Oracle, tmp_path
    ):
        # Oracle's read-only transaction covers the reading transaction only. A function that
        # runs in its own (autonomous) transaction, with its owner's privileges, still writes:
        # only who may create such views protects against them.
        _seed(oracle)
        log, orders = oracle.table("audit_log"), oracle.table("orders")
        oracle.admin(
            f"CREATE TABLE {log} (seen_at TIMESTAMP)",
            f"CREATE FUNCTION {oracle.namespace}.touch RETURN NUMBER AS "
            "PRAGMA AUTONOMOUS_TRANSACTION; BEGIN "
            f"INSERT INTO {log} VALUES (SYSTIMESTAMP); COMMIT; RETURN 1; END;",
            f"CREATE VIEW {oracle.table('orders_view')} AS "
            f"SELECT id, {oracle.namespace}.touch() AS touched FROM {orders}",
        )
        login = oracle.create_user()
        oracle.grant_select(login, "orders_view")

        results = _import(oracle, login, tmp_path, ["orders_view"])

        assert results.total_rows == 3
        assert oracle.row_count("audit_log") == 3
