"""Unified I/O package for processing local and S3 files.

This module provides comprehensive I/O capabilities for both local filesystem
and S3 operations. The original file has been refactored into a package
for better maintainability. All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .core import UnifiedIOHandler
from .csv_operations import UnifiedCSVWriter, CSVOperations
from .parquet_operations import S3ParquetWriter, ParquetOperations
from .file_operations import FileOperations
from .io_operations import IOOperations
from .utils import get_s3_client

# Import required dependencies for backward compatibility
from ..s3_streaming import S3Path
import pyarrow.parquet as pq
from pathlib import Path

# Import the factory function for parquet writers
def create_parquet_writer(path, schema, s3_client=None, compression='snappy', **kwargs):
    """Create appropriate parquet writer for local or S3 output.

    This function maintains backward compatibility with the original module.
    """
    from ..s3_streaming import is_s3_path
    import pyarrow.parquet as pq

    if is_s3_path(path):
        # Use the module-level S3ParquetWriter for proper test mocking
        return S3ParquetWriter(path, schema, s3_client=s3_client,
                              compression=compression, **kwargs)
    else:
        # Ensure parent directory exists for local files
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        return pq.ParquetWriter(path, schema, compression=compression, **kwargs)

__all__ = [
    'UnifiedIOHandler',
    'UnifiedCSVWriter',
    'S3ParquetWriter',
    'CSVOperations',
    'ParquetOperations',
    'FileOperations',
    'IOOperations',
    'create_parquet_writer',
    'get_s3_client',
    'S3Path',
    'pq',
    'Path'
]
