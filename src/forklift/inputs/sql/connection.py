"""Database connection management for SQL inputs."""

from __future__ import annotations
import logging

from ..config import SqlInputConfig

logger = logging.getLogger(__name__)


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

        try:
            # Set connection timeout
            pyodbc.pooling = False

            # Build connection string with additional parameters
            conn_str = self.config.connection_string
            if self.config.connection_params:
                params = ";".join(f"{k}={v}" for k, v in self.config.connection_params.items())
                conn_str = f"{conn_str};{params}"

            self.connection = pyodbc.connect(
                conn_str,
                timeout=self.config.connection_timeout
            )

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
