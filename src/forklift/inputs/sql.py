"""SQL database input handler for reading data from databases via ODBC.

This module provides backward compatibility by importing from the new modular structure.
For new code, consider importing directly from the sql package submodules.
"""

import logging  # pragma: no cover

# Import the main handler for backward compatibility
from .sql.handler import SqlInputHandler  # pragma: no cover

# Also make the individual components available for advanced usage
from .sql.connection import SqlConnectionManager  # pragma: no cover
from .sql.schema import SqlSchemaManager  # pragma: no cover
from .sql.reader import SqlDataReader  # pragma: no cover
from .sql.types import SqlTypeConverter  # pragma: no cover

# Expose logger for backward compatibility with tests
logger = logging.getLogger(__name__)  # pragma: no cover

__all__ = [  # pragma: no cover
    'SqlInputHandler',
    'SqlConnectionManager',
    'SqlSchemaManager',
    'SqlDataReader',
    'SqlTypeConverter',
]
