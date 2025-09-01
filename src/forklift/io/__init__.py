"""I/O utilities for ForkliftCore.

This module provides unified I/O capabilities for both local filesystem
and S3, enabling seamless streaming between different storage systems.
"""

from .s3_streaming import S3StreamingClient, S3Path, is_s3_path, get_s3_client
from .unified_io import UnifiedIOHandler, UnifiedCSVWriter, S3ParquetWriter, create_parquet_writer

__all__ = [
    'S3StreamingClient',
    'S3Path',
    'is_s3_path',
    'get_s3_client',
    'UnifiedIOHandler',
    'UnifiedCSVWriter',
    'S3ParquetWriter',
    'create_parquet_writer'
]
