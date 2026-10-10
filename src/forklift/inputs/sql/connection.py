"""Database connection management for SQL inputs."""

from __future__ import annotations

import logging

from ..config import SqlInputConfig

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
        refuses writes and cancels slow statements. Elsewhere ``read_only`` rests on the driver
        honouring the access mode (logged as a warning: connect as a login that may only
        SELECT), and the timeout on pyodbc's ``Connection.timeout``.

        Raises:
            ConnectionError: If the read-only session statement fails; the import does not
                continue on a session that could write
        """
        dbms = self.dbms_name()

        if self.config.read_only:
            statement = _session_statement(_READ_ONLY_STATEMENTS, dbms)
            if statement:
                try:
                    self._run_session_statement(statement)
                except pyodbc.Error as e:
                    state = e.args[0] if e.args else "unknown"
                    raise ConnectionError(
                        f"Could not make the database session read-only ({type(e).__name__}, "
                        f"SQLSTATE {state}); pass read_only=False to connect without it"
                    ) from None
            else:
                logger.warning(
                    "read_only: forklift cannot make a %s session read-only; it relies on the "
                    "ODBC driver honouring the read-only access mode, which many ignore. "
                    "Connect as a login that may only SELECT.",
                    dbms or "database",
                )

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
                logger.warning(
                    "The ODBC driver does not support query timeouts; query_timeout=%s is "
                    "not applied",
                    timeout,
                )

    def _run_session_statement(self, statement: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(statement)
        finally:
            cursor.close()
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
