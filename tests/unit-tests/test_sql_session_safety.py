"""Read-only sessions and query timeouts set on the database session itself.

ODBC drivers commonly ignore pyodbc's ``readonly`` flag (psqlODBC and MariaDB Connector/ODBC
both do), and psqlODBC rejects the attribute behind ``Connection.timeout``. So after connecting,
forklift sends session statements for the databases it knows and falls back to the driver
attributes (with a warning) elsewhere. These tests use a fake pyodbc; the service tests in
tests/integration-tests/services check the same behaviour against real PostgreSQL and MySQL.
"""

from __future__ import annotations

import logging
import sys
import types

import pytest

from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql.connection import SqlConnectionManager


class FakeError(Exception):
    pass


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, statement):
        self.connection.executed.append(statement)
        if any(statement.startswith(prefix) for prefix in self.connection.failing):
            raise FakeError(
                "HY000", "[HY000] driver text quoting 'secret' (1193) (SQLExecDirectW)"
            )

    def close(self):
        pass


class FakeConnection:
    def __init__(self, dbms, failing=(), timeout_supported=True):
        self.dbms = dbms
        self.failing = failing
        self.timeout_supported = timeout_supported
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._timeout = None

    def getinfo(self, code):
        assert code == 17
        if isinstance(self.dbms, Exception):
            raise self.dbms
        return self.dbms

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        if not self.timeout_supported:
            raise FakeError("HY000", "Couldn't set unsupported connect attribute 113")
        self._timeout = value


@pytest.fixture
def pyodbc(monkeypatch):
    module = types.ModuleType("pyodbc")
    module.Error = FakeError
    module.SQL_DBMS_NAME = 17
    module.pooling = True
    module.connection = None

    def connect(connection_string, **kwargs):
        module.connect_kwargs = kwargs
        return module.connection

    module.connect = connect
    monkeypatch.setitem(sys.modules, "pyodbc", module)
    return module


def _connect(pyodbc, connection, **config):
    pyodbc.connection = connection
    manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x", **config))
    manager.connect()
    return manager


class TestKnownDatabases:
    @pytest.mark.parametrize(
        "dbms, read_only, timeout",
        [
            (
                "PostgreSQL",
                "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
                "SET statement_timeout = 12000",
            ),
            (
                "MySQL",
                "SET SESSION TRANSACTION READ ONLY",
                "SET SESSION max_execution_time = 12000",
            ),
            (
                "MariaDB",
                "SET SESSION TRANSACTION READ ONLY",
                "SET SESSION max_statement_time = 12",
            ),
        ],
    )
    def test_session_is_made_read_only_with_a_statement_timeout(
        self, pyodbc, dbms, read_only, timeout
    ):
        connection = FakeConnection(dbms)

        _connect(pyodbc, connection, query_timeout=12)

        assert connection.executed == [read_only, timeout]
        assert connection.commits == 2  # the settings apply to the transactions that follow
        assert connection.timeout is None  # the driver attribute is not needed
        assert pyodbc.connect_kwargs == {"timeout": 30, "readonly": True}

    def test_sqlite_is_read_only_through_query_only_and_uses_the_driver_timeout(self, pyodbc):
        connection = FakeConnection("SQLite")

        _connect(pyodbc, connection, query_timeout=5)

        assert connection.executed == ["PRAGMA query_only = ON"]
        assert connection.timeout == 5

    def test_read_only_false_sends_no_read_only_statement(self, pyodbc, caplog):
        connection = FakeConnection("PostgreSQL")

        _connect(pyodbc, connection, read_only=False, query_timeout=3)

        assert connection.executed == ["SET statement_timeout = 3000"]
        assert "readonly" not in pyodbc.connect_kwargs
        assert "read_only" not in caplog.text

    def test_zero_query_timeout_sets_no_timeout(self, pyodbc):
        connection = FakeConnection("MySQL")

        _connect(pyodbc, connection, query_timeout=0)

        assert connection.executed == ["SET SESSION TRANSACTION READ ONLY"]
        assert connection.timeout is None


class TestOtherDatabases:
    def test_unknown_database_warns_that_read_only_rests_on_the_driver(self, pyodbc, caplog):
        connection = FakeConnection("Microsoft SQL Server")
        caplog.set_level(logging.WARNING)

        _connect(pyodbc, connection, query_timeout=7)

        assert connection.executed == []
        assert connection.timeout == 7
        assert "cannot make a microsoft sql server session read-only" in caplog.text
        assert "login that may only SELECT" in caplog.text

    @pytest.mark.parametrize("dbms", [FakeError("HYC00", "optional feature"), None, 42])
    def test_unknown_dbms_name_is_treated_as_an_unknown_database(self, pyodbc, caplog, dbms):
        connection = FakeConnection(dbms)
        caplog.set_level(logging.WARNING)

        manager = _connect(pyodbc, connection, query_timeout=7)

        assert manager.dbms_name() == ""
        assert connection.executed == []
        assert "cannot make a database session read-only" in caplog.text

    def test_driver_without_query_timeouts_is_logged_not_fatal(self, pyodbc, caplog):
        connection = FakeConnection("Microsoft SQL Server", timeout_supported=False)
        caplog.set_level(logging.WARNING)

        manager = _connect(pyodbc, connection, read_only=False, query_timeout=9)

        assert manager.is_connected()
        assert "does not support query timeouts; query_timeout=9 is not applied" in caplog.text


class TestFailures:
    def test_failed_read_only_statement_closes_the_connection(self, pyodbc):
        connection = FakeConnection("MySQL", failing=("SET SESSION TRANSACTION",))
        pyodbc.connection = connection
        manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x"))

        with pytest.raises(ConnectionError) as raised:
            manager.connect()

        message = str(raised.value)
        assert "Could not make the database session read-only" in message
        assert "SQLSTATE HY000" in message and "read_only=False" in message
        assert "secret" not in message  # the driver's message text is not repeated
        assert connection.closed and not manager.is_connected()

    def test_failed_timeout_statement_falls_back_to_the_driver_timeout(self, pyodbc):
        # A MariaDB server reported as MySQL has no max_execution_time
        connection = FakeConnection("MySQL", failing=("SET SESSION max_execution_time",))

        manager = _connect(pyodbc, connection, query_timeout=4)

        assert manager.is_connected()
        assert connection.rollbacks == 1
        assert connection.timeout == 4
