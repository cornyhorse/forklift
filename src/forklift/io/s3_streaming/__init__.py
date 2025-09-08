"""S3 streaming utilities for reading and writing data to/from S3.

This package provides S3 streaming capabilities using boto3, supporting both
input streaming (reading from S3) and output streaming (writing to S3) with
chunked uploads for large files.
"""

from .path import S3Path
from .client import S3StreamingClient
from .writer import S3StreamingWriter
from .utils import is_s3_path, get_s3_client

__all__ = [
    'S3Path',
    'S3StreamingClient',
    'S3StreamingWriter',
    'is_s3_path',
    'get_s3_client'
]
