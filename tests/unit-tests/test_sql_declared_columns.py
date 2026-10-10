"""Reading only the columns a schema declares, and explaining a refused read.

``x-sql`` table entries may list the columns to read in ``select.columns``, so a login with
column-level grants can import what it may read. Without a declaration every column is read,
and when the database refuses for a missing privilege the error says which columns the login
may read and how to declare them. These tests use a fake ODBC database; the service tests in
tests/integration-tests/services run the same cases against real servers.
"""

from __future__ import annotations

import json
import sys
import types
from types import SimpleNamespace

import pyarrow as pa
import pytest

from forklift.engine.exceptions import COLUMN_MISSING, PERMISSION_DENIED
from forklift.engine.importers.sql_importer import _failure_reason
from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.sql.errors import (
    ColumnLookupError,
    ColumnPrivilegeError,
    TableLookupError,
    database_error_codes,
    describe_database_error,
    is_privilege_error,
)
from forklift.inputs.sql.schema import select_declared_columns
from forklift.schema.sql_schema_importer import SqlSchemaImporter, sql_column_problem

SCHEMA_ID = "https://github.com/cornyhorse/forklift/schema-standards/test.json"


def undecodable(message):
    """What pyodbc raises when the driver's message is not valid UTF-16 (here a lone surrogate)."""
    try:
        (message.encode("utf-16-le") + b"\x00\xd8A\x00").decode("utf-16-le")
    except UnicodeDecodeError as cause:
        error = SystemError("<class 'pyodbc.Error'> returned a result with an exception set")
        error.__cause__ = cause
        return error


class DriverError(Exception):
    """Shaped like pyodbc.Error: args are (SQLSTATE, message)."""


def denied(code=1, state="42501"):
    return DriverError(
        state, f"[{state}] permission denied: 'ana@example.com' ({code}) (SQLExecDirectW)"
    )


class FakeCursor:
    def __init__(self, database):
        self.database = database
        self.arraysize = 1
        self._rows = []

    def tables(self, table=None):
        return [
            SimpleNamespace(table_schem=schema, table_name=name, table_type="TABLE")
            for schema, name in self.database.tables
            if table in (None, name)
        ]

    def columns(self, table=None, schema=None):
        return [
            SimpleNamespace(
                table_schem=schema,
                table_name=table,
                column_name=name,
                type_name=type_name,
                column_size=None,
                decimal_digits=None,
                nullable=True,
                data_type=None,
            )
            for name, type_name in self.database.columns
        ]

    def execute(self, statement, *parameters):
        self.database.executed.append((statement, parameters))
        error = self.database.refuse(statement)
        if error is not None:
            raise error
        if statement.startswith("SELECT owner"):  # the Oracle dictionary: tables
            self._rows = [row for row in self.database.tables]
        elif statement.startswith("SELECT column_name"):  # the Oracle dictionary: columns
            self._rows = list(self.database.oracle_columns)
        else:
            self._rows = list(self.database.rows)

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size):
        rows, self._rows = self._rows[:size], self._rows[size:]
        return rows

    def close(self):
        pass


class FakeDatabase:
    """One table ``sales.orders`` (id, amount, email); ``refuse`` decides which queries fail."""

    def __init__(self, refuse=lambda statement: None):
        self.tables = [("sales", "orders")]
        self.columns = [("id", "INTEGER"), ("amount", "DOUBLE"), ("email", "VARCHAR")]
        self.oracle_columns = []
        self.rows = [(1, 9.5, "ana@example.com")]
        self.refuse = refuse
        self.executed = []

    def cursor(self):
        return FakeCursor(self)

    def getinfo(self, code):
        return '"'

    def rollback(self):
        self.executed.append(("<rollback>", ()))

    def queries(self):
        return [statement for statement, _ in self.executed if statement.startswith("SELECT")]


@pytest.fixture(autouse=True)
def fake_pyodbc(monkeypatch):
    module = types.ModuleType("pyodbc")
    module.SQL_IDENTIFIER_QUOTE_CHAR = 29
    monkeypatch.setitem(sys.modules, "pyodbc", module)
    return module


def _handler(database, dbms="postgresql"):
    handler = SqlInputHandler(SqlInputConfig(connection_string="DSN=x", batch_size=10))
    handler.connection = database
    handler.connection_manager.dbms = dbms
    return handler


def _refuse_columns(*refused, error=None):
    """Refuse ``SELECT *`` and any query that reads one of ``refused``."""

    def refuse(statement):
        if statement.startswith("SELECT * ") or any(f'"{c}"' in statement for c in refused):
            return error or denied()
        return None

    return refuse


# --------------------------------------------------------------------------- the schema file


def _schema(select):
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "t",
        "type": "object",
        "x-sql": {"tables": [{"select": select, "outputName": "orders"}]},
    }


class TestSchemaDeclaration:
    def test_declared_columns_are_returned_in_their_order(self):
        importer = SqlSchemaImporter(
            _schema({"schema": "sales", "name": "orders", "columns": ["email", "id"]})
        )

        assert importer.get_selected_columns("sales", "orders", "orders") == ["email", "id"]

    def test_an_entry_without_columns_reads_every_column(self):
        importer = SqlSchemaImporter(_schema({"schema": "sales", "name": "orders"}))

        assert importer.get_selected_columns("sales", "orders", "orders") is None

    def test_entries_are_told_apart_by_their_output_name(self):
        schema = _schema({"name": "orders", "columns": ["id"]})
        schema["x-sql"]["tables"].append(
            {"select": {"name": "orders", "columns": ["email"]}, "outputName": "emails"}
        )
        importer = SqlSchemaImporter(schema)

        assert importer.get_selected_columns("default", "orders", "orders") == ["id"]
        assert importer.get_selected_columns("default", "orders", "emails") == ["email"]
        assert importer.get_selected_columns("default", "orders", "other") is None

    def test_malformed_entries_are_skipped_when_looking_up(self):
        importer = SqlSchemaImporter(
            {"x-sql": {"tables": ["nonsense", {"select": "orders"}]}}, validate=False
        )

        assert importer.get_selected_columns("default", "orders") is None

    @pytest.mark.parametrize(
        "columns, problem",
        [
            ([], "select.columns must be a non-empty array of column names"),
            ("id", "select.columns must be a non-empty array of column names"),
            (["id", 3], "select.columns[1] must be a string"),
            (["id", ""], "select.columns[1] must not be empty"),
            (["id", "x" * 129], "select.columns[1] is longer than 128 characters"),
            ([" id"], "select.columns[0] must not start or end with whitespace"),
            (["a\x00b"], "select.columns[0] contains control or non-printing characters"),
            (["a--b"], "select.columns[0] contains an SQL comment marker"),
            (['a"b'], "select.columns[0] contains a quote, semicolon or backslash"),
            (["id", "amount", "id"], "select.columns[2] repeats the column 'id'"),
        ],
    )
    def test_invalid_declarations_are_rejected_when_the_schema_loads(self, columns, problem):
        with pytest.raises(Exception, match="Schema validation failed") as raised:
            SqlSchemaImporter(_schema({"name": "orders", "columns": columns}))

        assert f"Table 0 {problem}" in str(raised.value)

    @pytest.mark.parametrize("name", ["Unit Price (USD)", "growth_%", "Größe", "a/b"])
    def test_column_names_may_hold_characters_table_names_may_not(self, name):
        assert sql_column_problem(name) is None


# --------------------------------------------------------------------------- the catalog


def _info(*names):
    return [{"column_name": name, "data_type": "INTEGER"} for name in names]


class TestDeclaredColumnsAgainstTheCatalog:
    def test_exact_names_win_then_names_that_differ_in_case(self):
        selected = select_declared_columns(_info("ID", "amount", "Amount"), ["amount", "id"], "t")

        assert [info["column_name"] for info in selected] == ["amount", "ID"]

    def test_unknown_column_lists_the_catalog_columns(self):
        with pytest.raises(ColumnLookupError) as raised:
            select_declared_columns(_info("id", "email"), ["e_mail"], "sales.orders")

        assert str(raised.value) == (
            "Column 'e_mail' declared in select.columns of table 'sales.orders' was not found "
            "in the database catalog, or the connecting user has no privileges on it; the "
            "catalog lists the columns id, email"
        )
        assert raised.value.error_code == COLUMN_MISSING

    def test_a_long_column_list_is_cut_short(self):
        names = [f"c{i}" for i in range(60)]

        with pytest.raises(ColumnLookupError, match=r"c49 and 10 more$"):
            select_declared_columns(_info(*names), ["nope"], "t")

    def test_a_name_matching_two_spellings_is_ambiguous(self):
        with pytest.raises(ColumnLookupError, match=r"several columns that differ only in case"):
            select_declared_columns(_info("Amount", "AMOUNT"), ["amount"], "t")

    def test_an_empty_declaration_is_rejected(self):
        with pytest.raises(ColumnLookupError, match="declares no columns; leave it out"):
            select_declared_columns(_info("id"), [], "t")

    def test_two_declared_names_for_one_column_are_rejected(self):
        with pytest.raises(ColumnLookupError, match="declares the column 'ID' twice"):
            select_declared_columns(_info("ID"), ["id", "Id"], "t")


class TestReadingDeclaredColumns:
    def test_only_the_declared_columns_are_selected_in_their_order(self):
        database = FakeDatabase()
        database.rows = [(9.5, 1)]
        handler = _handler(database)

        batches = list(handler.read_table_data("sales", "orders", columns=["amount", "ID"]))

        assert database.queries()[-1] == 'SELECT "amount", "id" FROM "sales"."orders"'
        assert batches[0].schema.names == ["amount", "id"]
        assert batches[0].to_pylist() == [{"amount": 9.5, "id": 1}]

    def test_without_a_declaration_every_column_is_read(self):
        database = FakeDatabase()
        handler = _handler(database)

        (batch,) = list(handler.read_table_data("sales", "orders"))

        assert database.queries()[-1] == 'SELECT * FROM "sales"."orders"'
        assert batch.schema.names == ["id", "amount", "email"]

    def test_schema_of_declared_columns(self):
        handler = _handler(FakeDatabase())

        schema = handler.get_table_schema("sales", "orders", columns=["email"])

        assert schema == pa.schema([pa.field("email", pa.string())])

    def test_unknown_declared_column_names_the_requested_table(self):
        handler = _handler(FakeDatabase())

        with pytest.raises(ColumnLookupError, match="of table 'orders'"):
            list(handler.read_table_data("default", "orders", columns=["nope"]))


# --------------------------------------------------------------------------- refusals


class TestRefusedReads:
    def test_refused_select_star_names_the_columns_to_declare(self):
        database = FakeDatabase(_refuse_columns("email"))
        handler = _handler(database)

        with pytest.raises(ColumnPrivilegeError) as raised:
            list(handler.read_table_data("sales", "orders"))

        message = str(raised.value)
        select = {"schema": "sales", "name": "orders", "columns": ["id", "amount"]}
        assert message == (
            "The database refused to read every column of 'sales.orders' (SQLSTATE 42501, "
            "insufficient privilege, driver error 1): the connecting user may read only the "
            "columns id, amount. To import those, declare them in the table's x-sql entry: "
            f'"select": {json.dumps(select)}'
        )
        assert "ana@example.com" not in message
        assert raised.value.error_code == PERMISSION_DENIED
        assert isinstance(raised.value.__cause__, DriverError)
        # each column was tried on its own, without reading rows
        assert 'SELECT "email" FROM "sales"."orders" WHERE 1=0' in database.queries()

    def test_table_without_a_schema_gets_a_declaration_without_one(self):
        database = FakeDatabase(_refuse_columns("email"))
        database.tables = [("default", "orders")]

        with pytest.raises(ColumnPrivilegeError) as raised:
            list(_handler(database).read_table_data("default", "orders"))

        assert '"select": {"name": "orders", "columns": ["id", "amount"]}' in str(raised.value)
        assert "every column of 'orders'" in str(raised.value)

    def test_login_that_may_read_no_column_is_told_to_get_a_grant(self):
        handler = _handler(FakeDatabase(_refuse_columns("id", "amount", "email")))

        with pytest.raises(ColumnPrivilegeError) as raised:
            list(handler.read_table_data("sales", "orders"))

        assert "no privilege to read any of its columns. Grant it SELECT on the table" in str(
            raised.value
        )

    def test_declared_columns_the_login_may_not_read_are_named(self):
        handler = _handler(FakeDatabase(_refuse_columns("email")))

        with pytest.raises(ColumnPrivilegeError) as raised:
            list(handler.read_table_data("sales", "orders", columns=["id", "email"]))

        assert str(raised.value) == (
            "The database refused to read the columns of 'sales.orders' declared in "
            "select.columns (SQLSTATE 42501, insufficient privilege, driver error 1): the "
            "connecting user may not read email (it may read id). Remove those from "
            "select.columns, or grant the user SELECT on them"
        )

    def test_declared_columns_none_of_which_may_be_read(self):
        handler = _handler(FakeDatabase(_refuse_columns("email", "id")))

        with pytest.raises(ColumnPrivilegeError, match=r"may not read id, email\. Remove"):
            list(handler.read_table_data("sales", "orders", columns=["id", "email"]))

    def test_declared_columns_refused_together_but_readable_alone_keep_the_database_error(
        self,
    ):
        def refuse(statement):
            return denied() if '"id", "amount"' in statement else None

        handler = _handler(FakeDatabase(refuse))

        with pytest.raises(DriverError):
            list(handler.read_table_data("sales", "orders", columns=["id", "amount"]))

    def test_other_errors_are_raised_as_they_are(self):
        timeout = DriverError("HYT00", "[HYT00] timeout expired (0) (SQLExecDirectW)")
        database = FakeDatabase(_refuse_columns(error=timeout))

        with pytest.raises(DriverError) as raised:
            list(_handler(database).read_table_data("sales", "orders"))

        assert raised.value is timeout
        assert not any("WHERE 1=0" in query for query in database.queries())

    def test_a_probe_failing_for_another_reason_keeps_the_database_error(self):
        def refuse(statement):
            if statement.startswith("SELECT * "):
                return denied()
            if '"amount"' in statement:
                return DriverError("HY000", "[HY000] connection lost (0) (SQLExecDirectW)")
            return None

        with pytest.raises(DriverError, match="42501"):
            list(_handler(FakeDatabase(refuse)).read_table_data("sales", "orders"))

    def test_the_failure_reason_of_a_table_is_the_explanation(self):
        error = ColumnPrivilegeError("The database refused to read every column of 't'")

        assert _failure_reason(error) == str(error)
        assert _failure_reason(TableLookupError("Table 't' was not found")) == (
            "Table 't' was not found"
        )


class TestPrivilegeErrors:
    @pytest.mark.parametrize(
        "dbms, error",
        [
            ("postgresql", denied(1, "42501")),
            ("anything", denied(1, "42501")),  # the standard SQLSTATE counts everywhere
            ("mysql", denied(1142, "42000")),
            ("mariadb", denied(1143, "42000")),
            ("microsoft sql server", denied(230, "42000")),
            ("microsoft sql server", denied(229, "42000")),
            ("oracle", denied(1031, "HY000")),
            ("oracle", denied(41900, "HY000")),
            ("oracle", undecodable("[Oracle][ODBC][Ora]ORA-01031: insufficient privileges")),
        ],
    )
    def test_missing_privileges_are_recognised(self, dbms, error):
        assert is_privilege_error(error, dbms)

    @pytest.mark.parametrize(
        "dbms, error",
        [
            ("mysql", denied(1064, "42000")),  # a syntax error
            ("sqlite", denied(1142, "42000")),  # a code that means nothing there
            ("", denied(230, "42000")),
            ("postgresql", ValueError("no SQLSTATE")),
            ("oracle", SystemError("not a driver message")),
        ],
    )
    def test_other_errors_are_not(self, dbms, error):
        assert not is_privilege_error(error, dbms)

    def test_codes_are_read_from_pyodbc_errors(self):
        assert database_error_codes(denied(230, "42000")) == ("42000", 230)
        assert database_error_codes(DriverError("42000", "no code")) == ("42000", None)
        assert database_error_codes(DriverError("not a state", "x (1) (SQLExecDirectW)")) == (
            None,
            None,
        )
        assert describe_database_error(RuntimeError("x")) == ""
        assert describe_database_error(undecodable("[Oracle][ODBC][Ora]ORA-00942: x")) == (
            "no SQLSTATE, driver error 942"
        )


# --------------------------------------------------------------------------- Oracle


class TestOracleCatalog:
    def test_tables_and_columns_come_from_the_data_dictionary(self):
        database = FakeDatabase()
        database.tables = [("SALES", "ORDERS"), ("SALES", "orders")]
        database.oracle_columns = [
            ("ID", "NUMBER", None, 0, "N"),  # INTEGER is NUMBER(*,0)
            ("PLAIN", "NUMBER", None, None, "Y"),
            ("PRICE", "NUMBER", 10.0, 2.0, "Y"),
            ("SEEN", "DATE", None, None, "Y"),
            ("AT", "TIMESTAMP(6) WITH TIME ZONE", None, 6.0, "Y"),
            ("RAW_DATA", "RAW", None, None, "Y"),
        ]
        handler = _handler(database, dbms="oracle")

        schema = handler.get_table_schema("sales", "ORDERS")

        assert schema == pa.schema(
            [
                pa.field("ID", pa.decimal128(38, 0), nullable=False),
                pa.field("PLAIN", pa.float64()),
                pa.field("PRICE", pa.decimal128(10, 2)),
                pa.field("SEEN", pa.timestamp("us")),  # Oracle's DATE holds a time of day
                pa.field("AT", pa.timestamp("us", tz="UTC")),
                pa.field("RAW_DATA", pa.binary()),
            ]
        )
        statement, parameters = database.executed[0]
        assert "UPPER(table_name) = UPPER(?)" in statement and parameters == ("ORDERS", "ORDERS")
        assert database.executed[1][1] == ("SALES", "ORDERS")

    def test_every_spelling_is_found_so_ambiguity_is_reported(self):
        database = FakeDatabase()
        database.tables = [("SALES", "ORDERS"), ("SALES", "orders")]

        with pytest.raises(TableLookupError) as raised:
            _handler(database, dbms="oracle").get_table_schema("sales", "Orders")

        assert str(raised.value) == (
            "Table 'sales.Orders' is ambiguous: found as SALES.ORDERS, SALES.orders, names that "
            "differ only in case; give the exact spelling"
        )

    def test_the_exact_spelling_of_the_table_wins_whatever_the_schema_s_case(self):
        database = FakeDatabase()
        database.tables = [("SALES", "ORDERS"), ("SALES", "orders")]
        database.oracle_columns = [("ID", "NUMBER", 10.0, 0.0, "Y")]

        _handler(database, dbms="oracle").get_table_schema("sales", "orders")

        assert database.executed[1][1] == ("SALES", "orders")


class TestOracleReadOnlyRetry:
    @pytest.fixture
    def no_sleep(self, monkeypatch):
        pauses = []
        monkeypatch.setattr("forklift.inputs.sql.reader.time.sleep", pauses.append)
        return pauses

    def _oracle(self, failures):
        changed = DriverError(
            "HY000", "[HY000] ORA-01466: unable to read data (1466) (SQLExecDirectW)"
        )
        remaining = [failures]

        def refuse(statement):
            if statement.startswith('SELECT "ID"') and remaining[0]:
                remaining[0] -= 1
                return changed
            return None

        database = FakeDatabase(refuse)
        database.tables = [("SALES", "ORDERS")]
        database.oracle_columns = [("ID", "NUMBER", 10.0, 0.0, "Y")]
        database.rows = [(1,)]
        handler = _handler(database, dbms="oracle")
        handler.connection_manager._transaction_read_only = "SET TRANSACTION READ ONLY"
        return handler, database

    def test_a_table_changed_just_before_the_transaction_is_read_in_a_new_one(self, no_sleep):
        handler, database = self._oracle(failures=2)

        (batch,) = list(handler.read_table_data("sales", "orders", columns=["id"]))

        assert batch.num_rows == 1
        assert no_sleep == [1, 1]
        statements = [statement for statement, _ in database.executed]
        assert statements.count("SET TRANSACTION READ ONLY") == 3  # one per attempt
        assert statements.count("<rollback>") == 3

    def test_retries_end(self, no_sleep):
        handler, _ = self._oracle(failures=100)

        with pytest.raises(DriverError, match="ORA-01466"):
            list(handler.read_table_data("sales", "orders", columns=["id"]))

        assert len(no_sleep) == 5

    def test_without_a_read_only_transaction_nothing_is_retried(self, no_sleep):
        handler, _ = self._oracle(failures=1)
        handler.connection_manager._transaction_read_only = None

        with pytest.raises(DriverError, match="ORA-01466"):
            list(handler.read_table_data("sales", "orders", columns=["id"]))

        assert no_sleep == []
