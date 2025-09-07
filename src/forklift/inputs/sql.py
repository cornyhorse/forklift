"""SQL database input handler for reading data from databases via ODBC.

This module provides backward compatibility by importing from the new modular structure.
For new code, consider importing directly from the sql package submodules.
"""

import logging

# Import the main handler for backward compatibility
from .sql.handler import SqlInputHandler

# Also make the individual components available for advanced usage
from .sql.connection import SqlConnectionManager
from .sql.schema import SqlSchemaManager
from .sql.reader import SqlDataReader
from .sql.types import SqlTypeConverter

# Expose logger for backward compatibility with tests
logger = logging.getLogger(__name__)

__all__ = [
    'SqlInputHandler',
    'SqlConnectionManager',
    'SqlSchemaManager',
    'SqlDataReader',
    'SqlTypeConverter',
    'logger'
]
