"""SQL input package for database connectivity and data reading."""

import logging

from .handler import SqlInputHandler
from .connection import SqlConnectionManager
from .schema import SqlSchemaManager
from .reader import SqlDataReader
from .types import SqlTypeConverter

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
