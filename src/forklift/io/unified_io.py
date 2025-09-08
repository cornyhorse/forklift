"""Unified I/O handler for local files and S3 objects.

This module provides a unified interface for reading from and writing to both
local filesystem and S3, integrating with ForkliftCore's streaming architecture.

The original file has been refactored into a package for better maintainability.
All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .unified_io.core import UnifiedIOHandler
from .unified_io.csv_operations import UnifiedCSVWriter
from .unified_io.parquet_operations import S3ParquetWriter
from .unified_io.utils import get_s3_client
from .unified_io import create_parquet_writer

# Import required dependencies for backward compatibility
from .s3_streaming import S3Path
import pyarrow.parquet as pq
from pathlib import Path

__all__ = [
    'UnifiedIOHandler',
    'UnifiedCSVWriter',
    'S3ParquetWriter',
    'create_parquet_writer',
    'get_s3_client',
    'S3Path',
    'pq',
    'Path'
]
