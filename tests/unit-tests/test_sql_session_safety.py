"""Read-only sessions and query timeouts set on the database session itself.

ODBC drivers commonly ignore pyodbc's ``readonly`` flag (psqlODBC and MariaDB Connector/ODBC
both do), and psqlODBC rejects the attribute behind ``Connection.timeout``. So after connecting,
forklift sends session statements for the databases it knows and falls back to the driver
attributes (with a warning) elsewhere. Oracle can only make a transaction read-only, and its
driver has no query timeout. These tests use a fake pyodbc; the service tests in
tests/integration-tests/services check the same behaviour against real PostgreSQL, MySQL, SQL
Server and Oracle.
"""

from __future__ import annotations

import datetime
import logging
import struct
import sys
import threading
import types

import pytest

from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql.connection import (
    SqlConnectionManager,
    _datetimeoffset,
    _oracle_boolean,
)


class FakeError(Exception):
    pass


def undecodable(message):
    """What pyodbc raises when the driver's message is not valid UTF-16 (here a lone surrogate)."""
    try:
        (message.encode("utf-16-le") + b"\x00\xd8A\x00").decode("utf-16-le")
    except UnicodeDecodeError as cause:
        error = SystemError("<class 'pyodbc.Error'> returned a result with an exception set")
        error.__cause__ = cause
        return error


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, statement):
        self.connection.executed.append(statement)
        self.connection.events.append(statement)
        if any(statement.startswith(prefix) for prefix in self.connection.failing):
            raise self.connection.failure or FakeError(
                "HY000", "[HY000] driver text quoting 'secret' (1193) (SQLExecDirectW)"
            )

    def close(self):
        pass


class FakeConnection:
    def __init__(self, dbms, failing=(), timeout_supported=True, failure=None, timeout_error=None):
        self.dbms = dbms
        self.failing = failing
        self.failure = failure  # what the failing statements raise (default: a FakeError)
        self.timeout_supported = timeout_supported
        self.timeout_error = timeout_error
        self.executed = []
        self.events = []  # statements, commits and rollbacks in order
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._timeout = None
        self.converters = {}

    def add_output_converter(self, code, converter):
        self.converters[code] = converter

    def set_attr(self, attribute, value):
        self.events.append(("set_attr", attribute, value))

    def getinfo(self, code):
        assert code == 17
        if isinstance(self.dbms, Exception):
            raise self.dbms
        return self.dbms

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.events.append("commit")
        self.commits += 1

    def rollback(self):
        self.events.append("rollback")
        self.rollbacks += 1

    def close(self):
        self.closed = True

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        if not self.timeout_supported:
            raise self.timeout_error or FakeError(
                "HY000", "Couldn't set unsupported connect attribute 113"
            )
        self._timeout = value


@pytest.fixture
def pyodbc(monkeypatch):
    module = types.ModuleType("pyodbc")
    module.Error = FakeError
    module.SQL_DBMS_NAME = 17
    module.SQL_ATTR_ACCESS_MODE = 101
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


class TestSqlServer:
    def test_read_only_rests_on_the_driver_and_is_warned_about(self, pyodbc, caplog):
        # SQL Server has no read-only session or transaction a login could set
        connection = FakeConnection("Microsoft SQL Server")
        caplog.set_level(logging.WARNING)

        _connect(pyodbc, connection, query_timeout=7)

        assert connection.executed == []
        assert connection.timeout == 7
        assert "cannot make a microsoft sql server session read-only" in caplog.text
        assert "login that may only SELECT" in caplog.text

    def test_datetimeoffset_values_are_read_with_a_converter(self, pyodbc):
        # pyodbc cannot read SQL_SS_TIMESTAMPOFFSET (-155) on its own
        connection = FakeConnection("Microsoft SQL Server")

        _connect(pyodbc, connection)

        assert connection.converters == {-155: _datetimeoffset}


class TestOracle:
    def test_session_is_set_to_utc_and_a_read_only_transaction_is_started(self, pyodbc):
        connection = FakeConnection("Oracle")

        manager = _connect(pyodbc, connection, query_timeout=0)

        assert connection.events == [
            "ALTER SESSION SET TIME_ZONE = 'UTC'",
            "commit",
            # the driver's own read-only mode is switched off: it collides with forklift's
            ("set_attr", 101, 0),
            "SET TRANSACTION READ ONLY",  # not committed: committing would end it
        ]
        assert manager.dbms == "oracle"
        assert connection.converters == {-7: _oracle_boolean}

    def test_each_read_starts_a_new_read_only_transaction(self, pyodbc):
        connection = FakeConnection("Oracle")
        manager = _connect(pyodbc, connection, query_timeout=0)
        connection.events.clear()

        manager.begin_read()
        manager.begin_read()

        assert connection.events == ["rollback", "SET TRANSACTION READ ONLY"] * 2

    def test_begin_read_after_disconnecting_does_nothing(self, pyodbc):
        connection = FakeConnection("Oracle")
        manager = _connect(pyodbc, connection, query_timeout=0)
        manager.disconnect()
        connection.events.clear()

        manager.begin_read()

        assert connection.events == []

    def test_without_read_only_no_transaction_is_started(self, pyodbc):
        connection = FakeConnection("Oracle")
        manager = _connect(pyodbc, connection, read_only=False, query_timeout=0)

        manager.begin_read()

        assert "SET TRANSACTION READ ONLY" not in connection.executed
        assert connection.rollbacks == 0

    def test_failed_read_only_transaction_fails_the_connection(self, pyodbc):
        connection = FakeConnection("Oracle", failing=("SET TRANSACTION",))
        pyodbc.connection = connection
        manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x"))

        with pytest.raises(ConnectionError, match="Could not make the database session"):
            manager.connect()

        assert connection.closed

    def test_driver_without_query_timeouts_gets_forklift_s_own_deadline(self, pyodbc, caplog):
        # Oracle's ODBC driver rejects the query timeout attribute (HYC00)
        connection = FakeConnection("Oracle", timeout_supported=False)
        caplog.set_level(logging.INFO)

        manager = _connect(pyodbc, connection, query_timeout=9)

        assert manager.cancel_after == 9
        assert "forklift cancels a statement that runs longer than query_timeout=9" in (
            caplog.text
        )


class TestUndecodableDriverMessages:
    """pyodbc raises SystemError when it cannot decode the driver's message (Oracle's driver sends
    undecodable bytes now and then); the session is set up as it is after any driver error."""

    def test_a_rejected_timeout_attribute_still_gets_forklift_s_own_deadline(self, pyodbc):
        connection = FakeConnection(
            "Oracle",
            timeout_supported=False,
            timeout_error=undecodable("[Oracle][ODBC]Optional feature not implemented."),
        )

        manager = _connect(pyodbc, connection, query_timeout=9)

        assert manager.cancel_after == 9 and manager.is_connected()

    def test_a_driver_that_cannot_say_which_database_it_is(self, pyodbc):
        manager = _connect(pyodbc, FakeConnection(undecodable("no such information")))

        assert manager.dbms_name() == ""

    def test_a_refused_read_only_transaction_closes_the_connection(self, pyodbc):
        connection = FakeConnection(
            "Oracle",
            failing=("SET TRANSACTION READ ONLY",),
            failure=undecodable("[Oracle][ODBC][Ora]ORA-01031: insufficient privileges"),
        )
        pyodbc.connection = connection
        manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x"))

        with pytest.raises(ConnectionError) as raised:
            manager.connect()

        assert "(SystemError, no SQLSTATE, driver error 1031)" in str(raised.value)
        assert connection.closed and not manager.is_connected()

    def test_a_refused_statement_without_any_code(self, pyodbc):
        connection = FakeConnection(
            "MySQL", failing=("SET SESSION TRANSACTION",), failure=FakeError()
        )
        pyodbc.connection = connection
        manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x"))

        with pytest.raises(ConnectionError, match=r"\(FakeError, no SQLSTATE reported\)"):
            manager.connect()


class TestStatementDeadline:
    class BlockingCursor:
        """A statement that runs until it is cancelled."""

        def __init__(self):
            self.cancelled = threading.Event()

        def cancel(self):
            self.cancelled.set()

        def execute(self):
            assert self.cancelled.wait(timeout=30), "the statement was never cancelled"
            raise FakeError("HYT00", "ORA-01013: user requested cancel (1013) (SQLExecDirectW)")

    def test_a_statement_still_running_at_the_deadline_is_cancelled(self, pyodbc):
        manager = _connect(pyodbc, FakeConnection("Oracle", timeout_supported=False))
        manager.cancel_after = 0.01
        cursor = self.BlockingCursor()

        with pytest.raises(FakeError, match="HYT00"):
            with manager.statement_deadline(cursor):
                cursor.execute()

        assert cursor.cancelled.is_set()

    def test_a_statement_that_finishes_in_time_is_left_alone(self, pyodbc):
        manager = _connect(pyodbc, FakeConnection("Oracle", timeout_supported=False))
        manager.cancel_after = 30
        cursor = self.BlockingCursor()

        with manager.statement_deadline(cursor):
            pass

        assert not cursor.cancelled.is_set()

    def test_without_a_client_side_timeout_nothing_is_cancelled(self, pyodbc):
        manager = _connect(pyodbc, FakeConnection("PostgreSQL"), query_timeout=5)
        cursor = self.BlockingCursor()

        with manager.statement_deadline(cursor):
            pass

        assert manager.cancel_after is None
        assert not cursor.cancelled.is_set()


class TestConverters:
    def test_datetimeoffset_becomes_an_aware_utc_datetime(self):
        raw = struct.pack("<6hI2h", 2024, 1, 2, 3, 4, 5, 123456789, 2, 30)

        value = _datetimeoffset(raw)

        assert value == datetime.datetime(
            2024, 1, 2, 0, 34, 5, 123456, tzinfo=datetime.timezone.utc
        )

    def test_negative_offsets_move_the_time_forward(self):
        raw = struct.pack("<6hI2h", 2024, 1, 2, 23, 0, 0, 0, -5, 0)

        assert _datetimeoffset(raw) == datetime.datetime(
            2024, 1, 3, 4, 0, tzinfo=datetime.timezone.utc
        )

    @pytest.mark.parametrize(
        "raw, expected", [(b"1", True), (b"0", False), (b"\x01", True), (b"\x00", False)]
    )
    def test_oracle_boolean_characters_become_bools(self, raw, expected):
        assert _oracle_boolean(raw) is expected

    def test_null_values_stay_null(self):
        assert _datetimeoffset(None) is None
        assert _oracle_boolean(None) is None


class TestOtherDatabases:
    @pytest.mark.parametrize("dbms", [FakeError("HYC00", "optional feature"), None, 42])
    def test_unknown_dbms_name_is_treated_as_an_unknown_database(self, pyodbc, caplog, dbms):
        connection = FakeConnection(dbms)
        caplog.set_level(logging.WARNING)

        manager = _connect(pyodbc, connection, query_timeout=7)

        assert manager.dbms_name() == ""
        assert connection.executed == []
        assert "cannot make a database session read-only" in caplog.text

    def test_begin_read_does_nothing_where_the_session_is_read_only(self, pyodbc):
        connection = FakeConnection("PostgreSQL")
        manager = _connect(pyodbc, connection, query_timeout=0)
        connection.events.clear()

        manager.begin_read()

        assert connection.events == []


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
