"""S3 streaming utilities for reading and writing data to/from S3.

This module provides S3 streaming capabilities using boto3, supporting both
input streaming (reading from S3) and output streaming (writing to S3) with
chunked uploads for large files.

This module has been refactored into a package for better maintainability.
All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .s3_streaming.path import S3Path
from .s3_streaming.client import S3StreamingClient
from .s3_streaming.writer import S3StreamingWriter
from .s3_streaming.utils import is_s3_path, get_s3_client

__all__ = [
    'S3Path',
    'S3StreamingClient',
    'S3StreamingWriter',
    'is_s3_path',
    'get_s3_client'
]
