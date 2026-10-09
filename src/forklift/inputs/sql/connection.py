"""Database connection management for SQL inputs."""

from __future__ import annotations

import logging

from ..config import SqlInputConfig

logger = logging.getLogger(__name__)


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
            # Set connection timeout
            pyodbc.pooling = False

            connect_kwargs = {"timeout": self.config.connection_timeout}
            if self.config.read_only:
                # Ask the driver for a read-only session (SQL_ATTR_ACCESS_MODE); drivers that
                # cannot honour it ignore or reject it - set read_only=False for those.
                connect_kwargs["readonly"] = True
            self.connection = pyodbc.connect(conn_str, **connect_kwargs)

            # Set query timeout
            self.connection.timeout = self.config.query_timeout

            logger.info("Successfully connected to database")

        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

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
