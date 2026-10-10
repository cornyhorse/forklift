"""Database connection management for SQL inputs."""

from __future__ import annotations

import datetime
import logging
import struct
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

from ..config import SqlInputConfig
from .errors import database_error_codes

logger = logging.getLogger(__name__)

# Session statements that make the database itself enforce ``read_only`` and ``query_timeout``,
# by the server's SQL_DBMS_NAME (lower case, matched as a prefix). ODBC drivers commonly ignore
# pyodbc's ``readonly`` flag (SQL_ATTR_ACCESS_MODE) - psqlODBC and MariaDB Connector/ODBC both
# do - and psqlODBC rejects the connection timeout attribute behind ``Connection.timeout``.
_READ_ONLY_STATEMENTS = {
    "postgresql": "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    "mysql": "SET SESSION TRANSACTION READ ONLY",
    "mariadb": "SET SESSION TRANSACTION READ ONLY",
    "sqlite": "PRAGMA query_only = ON",
}
_TIMEOUT_STATEMENTS = {
    "postgresql": "SET statement_timeout = {milliseconds}",
    "mysql": "SET SESSION max_execution_time = {milliseconds}",
    "mariadb": "SET SESSION max_statement_time = {seconds}",
}
# Oracle can make only a transaction read-only: SET TRANSACTION READ ONLY must be the first
# statement of a transaction and lasts until it ends. forklift starts one when it connects and
# again before each table it reads (see SqlConnectionManager.begin_read).
_TRANSACTION_READ_ONLY_STATEMENTS = {
    "oracle": "SET TRANSACTION READ ONLY",
}
# ORA-01466 "unable to read data - table definition has changed": a read-only transaction cannot
# read a table created or altered in the last few seconds (Oracle maps its snapshot to a time
# with a few seconds' resolution). A read-only transaction started a moment later can.
_TABLE_CHANGED_SINCE_SNAPSHOT = 1466
#: How many times, one second apart, forklift starts a new read-only transaction for such a table
READ_RETRIES = 5
# Statements run on every connection (whatever read_only says) so values come back the way
# forklift maps them. Oracle returns TIMESTAMP WITH (LOCAL) TIME ZONE values in the session time
# zone; in UTC they match the timestamp[us, UTC] type they are read as.
_SETUP_STATEMENTS = {
    "oracle": ("ALTER SESSION SET TIME_ZONE = 'UTC'",),
}

# SQL_ATTR_ACCESS_MODE value: the driver itself does nothing to keep the session read-only
_SQL_MODE_READ_WRITE = 0

# ODBC type codes that pyodbc cannot read, or reads wrongly, on some databases
_SQL_BIT = -7
_SQL_SS_TIMESTAMPOFFSET = -155  # SQL Server datetimeoffset: pyodbc says "not yet supported"


def _datetimeoffset(raw: Optional[bytes]) -> Optional[datetime.datetime]:
    """A SQL Server ``datetimeoffset`` (SQL_SS_TIMESTAMPOFFSET_STRUCT) as an aware UTC datetime."""
    if raw is None:
        return None
    year, month, day, hour, minute, second, nanoseconds, offset_hours, offset_minutes = (
        struct.unpack("<6hI2h", raw)
    )
    offset = datetime.timezone(datetime.timedelta(hours=offset_hours, minutes=offset_minutes))
    local = datetime.datetime(
        year, month, day, hour, minute, second, nanoseconds // 1000, tzinfo=offset
    )
    return local.astimezone(datetime.timezone.utc)


def _oracle_boolean(raw: Optional[bytes]) -> Optional[bool]:
    """An Oracle ``BOOLEAN`` as a bool.

    The driver sends the characters ``1`` and ``0``, which pyodbc's own bit conversion reads as
    False both times.
    """
    if raw is None:
        return None
    return raw not in (b"0", b"\x00")


# Output converters registered on the connection, by the server's SQL_DBMS_NAME prefix
_OUTPUT_CONVERTERS = {
    "microsoft sql server": {_SQL_SS_TIMESTAMPOFFSET: _datetimeoffset},
    "oracle": {_SQL_BIT: _oracle_boolean},
}


def _session_statement(statements: dict, dbms: str):
    for name, statement in statements.items():
        if dbms.startswith(name):
            return statement
    return None


def _format_connection_params(params) -> str:
    """Render extra connection parameters as ``key=value;`` pairs without injection.

    Values containing ``;``, ``{``, ``}``, ``=`` or surrounding whitespace are wrapped in
    braces with ``}`` doubled (the ODBC escaping rule), so a value can never add or
    override other connection attributes. Keys that could do the same are rejected.

    Raises:
        ValueError: If a key is empty or contains ``;``, ``=``, ``{`` or ``}``
    """
    parts = []
    for key, value in params.items():
        key = str(key)
        if not key.strip() or any(ch in key for ch in ";={}"):
            raise ValueError("Invalid connection parameter name")
        text = str(value)
        if text == "" or text != text.strip() or any(ch in text for ch in ";={}"):
            text = "{" + text.replace("}", "}}") + "}"
        parts.append(f"{key}={text}")
    return ";".join(parts)


class SqlConnectionManager:
    """Manages database connections using pyodbc.

    This class handles establishing, maintaining, and closing database connections
    with proper error handling and timeout configuration.
    """

    def __init__(self, config: SqlInputConfig):
        """Initialize the connection manager.

        Args:
            config: Configuration object containing SQL connection parameters
        """
        self.config = config
        self.connection = None
        #: The server's SQL_DBMS_NAME in lower case, known once connected (``""`` until then)
        self.dbms = ""
        #: Seconds after which forklift cancels a statement itself (drivers without timeouts)
        self.cancel_after: Optional[float] = None
        self._transaction_read_only: Optional[str] = None

    def connect(self) -> None:
        """Establish database connection using pyodbc.

        Raises:
            ImportError: If pyodbc is not installed
            ConnectionError: If connection fails
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL database connectivity. "
                "Install it with: pip install pyodbc"
            )

        # Build connection string with additional parameters (a bad parameter is a
        # configuration error, not a connection failure)
        conn_str = self.config.connection_string
        if self.config.connection_params:
            params = _format_connection_params(self.config.connection_params)
            conn_str = f"{conn_str};{params}"

        try:
            pyodbc.pooling = False

            connect_kwargs = {"timeout": self.config.connection_timeout}
            if self.config.read_only:
                # Also ask the driver (SQL_ATTR_ACCESS_MODE); many ignore it, so the session
                # is made read-only below as well
                connect_kwargs["readonly"] = True
            self.connection = pyodbc.connect(conn_str, **connect_kwargs)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

        try:
            self._configure_session(pyodbc)
        except BaseException:
            self.disconnect()
            raise

        logger.info("Successfully connected to database")

    def dbms_name(self) -> str:
        """The server's SQL_DBMS_NAME in lower case (``""`` when the driver does not say)."""
        import pyodbc

        try:
            name = self.get_connection().getinfo(pyodbc.SQL_DBMS_NAME)
        except pyodbc.Error:
            return ""
        return name.strip().lower() if isinstance(name, str) else ""

    def _configure_session(self, pyodbc) -> None:
        """Make the session read-only and apply the query timeout where the database can.

        PostgreSQL, MySQL, MariaDB and SQLite get session statements, so the database itself
        refuses writes and cancels slow statements. Oracle can only make a transaction
        read-only: one is started now and again before each table (:meth:`begin_read`).
        Elsewhere (SQL Server, for one) ``read_only`` rests on the driver honouring the access
        mode (logged as a warning: connect as a login that may only SELECT). The timeout falls
        back to pyodbc's ``Connection.timeout`` and, where the driver has none (Oracle's), to
        forklift cancelling the statement itself (:meth:`statement_deadline`).

        Raises:
            ConnectionError: If the read-only statement fails; the import does not continue on
                a session that could write
        """
        dbms = self.dbms = self.dbms_name()
        self.cancel_after = None
        self._transaction_read_only = None

        for code, converter in (_session_statement(_OUTPUT_CONVERTERS, dbms) or {}).items():
            self.connection.add_output_converter(code, converter)
        for statement in _session_statement(_SETUP_STATEMENTS, dbms) or ():
            self._run_session_statement(statement)

        if self.config.read_only:
            self._make_read_only(pyodbc, dbms)

        timeout = self.config.query_timeout
        if timeout:
            statement = _session_statement(_TIMEOUT_STATEMENTS, dbms)
            if statement:
                try:
                    self._run_session_statement(
                        statement.format(milliseconds=int(timeout * 1000), seconds=timeout)
                    )
                    return
                except pyodbc.Error:
                    # e.g. a MariaDB server reported as MySQL (no max_execution_time)
                    self.connection.rollback()
            try:
                self.connection.timeout = timeout
            except pyodbc.Error:
                self.cancel_after = timeout
                logger.info(
                    "The ODBC driver does not support query timeouts; forklift cancels a "
                    "statement that runs longer than query_timeout=%s seconds itself",
                    timeout,
                )

    def _make_read_only(self, pyodbc, dbms: str) -> None:
        session = _session_statement(_READ_ONLY_STATEMENTS, dbms)
        transaction = _session_statement(_TRANSACTION_READ_ONLY_STATEMENTS, dbms)
        if not (session or transaction):
            logger.warning(
                "read_only: forklift cannot make a %s session read-only; it relies on the "
                "ODBC driver honouring the read-only access mode, which many ignore. "
                "Connect as a login that may only SELECT.",
                dbms or "database",
            )
            return
        try:
            if session:
                self._run_session_statement(session)
            else:
                # Oracle's driver honours the read-only access mode asked for at connect by
                # sending SET TRANSACTION READ ONLY before prepared statements and catalog
                # calls, which fails (ORA-01453) inside the transaction forklift makes
                # read-only itself; it would not cover plain statements anyway.
                self.connection.set_attr(pyodbc.SQL_ATTR_ACCESS_MODE, _SQL_MODE_READ_WRITE)
                self._transaction_read_only = transaction
                self._execute(transaction)  # not committed: the transaction is the read-only part
        except pyodbc.Error as e:
            state = e.args[0] if e.args else "unknown"
            raise ConnectionError(
                f"Could not make the database session read-only ({type(e).__name__}, "
                f"SQLSTATE {state}); pass read_only=False to connect without it"
            ) from None

    def begin_read(self) -> None:
        """Start the transaction the next query runs in, read-only where only a transaction can be.

        On Oracle the read-only transaction started when connecting ends at a commit or a
        rollback, so before each table forklift ends the current transaction (nothing was
        written: it is read-only) and starts a new read-only one. Elsewhere the whole session
        is read-only (or cannot be made so) and this does nothing.
        """
        if self._transaction_read_only and self.connection is not None:
            self.connection.rollback()
            self._execute(self._transaction_read_only)

    def read_can_be_retried(self, error: BaseException) -> bool:
        """True when a query failed only because its read-only transaction began too early.

        Oracle refuses (ORA-01466) to read a table created or altered just before the
        read-only transaction began; :meth:`begin_read` a moment later starts one that can.
        """
        return bool(self._transaction_read_only) and (
            database_error_codes(error)[1] == _TABLE_CHANGED_SINCE_SNAPSHOT
        )

    @contextmanager
    def statement_deadline(self, cursor) -> Iterator[None]:
        """Cancel the statement ``cursor`` runs in the block once ``query_timeout`` has passed.

        Only drivers without query timeouts need it (``cancel_after`` is set); for the others,
        and without a timeout, it does nothing. The cancelled call raises the driver's error
        (Oracle: ORA-01013, SQLSTATE HYT00).
        """
        if not self.cancel_after:
            yield
            return
        timer = threading.Timer(self.cancel_after, cursor.cancel)
        timer.daemon = True
        timer.start()
        try:
            yield
        finally:
            timer.cancel()

    def _execute(self, statement: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(statement)
        finally:
            cursor.close()

    def _run_session_statement(self, statement: str) -> None:
        self._execute(statement)
        self.connection.commit()

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
            finally:
                self.connection = None

    def is_connected(self) -> bool:
        """Check if connection is active.

        Returns:
            True if connected, False otherwise
        """
        return self.connection is not None

    def get_connection(self):
        """Get the active connection.

        Returns:
            Active database connection

        Raises:
            ConnectionError: If not connected to database
        """
        if not self.connection:
            raise ConnectionError("Not connected to database")
        return self.connection

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
