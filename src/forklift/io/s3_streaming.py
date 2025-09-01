"""S3 streaming utilities for reading and writing data to/from S3.

This module provides S3 streaming capabilities using boto3, supporting both
input streaming (reading from S3) and output streaming (writing to S3) with
chunked uploads for large files.
"""

from __future__ import annotations
import io
import csv
from typing import Iterator, Optional, Dict, Any, Union, BinaryIO, TextIO
from pathlib import Path
from urllib.parse import urlparse
import tempfile
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3Path:
    """Utility class for parsing and working with S3 paths."""

    def __init__(self, s3_uri: str):
        """Initialize S3Path from S3 URI.

        Args:
            s3_uri: S3 URI in format s3://bucket/key

        Raises:
            ValueError: If URI is not a valid S3 path
        """
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")

        parsed = urlparse(s3_uri)
        self.bucket = parsed.netloc
        self.key = parsed.path.lstrip('/')
        self.uri = s3_uri

        if not self.bucket:
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Bucket name is required")

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"S3Path('{self.uri}')"

    @property
    def parent(self) -> 'S3Path':
        """Get parent S3 path (directory)."""
        if '/' not in self.key:
            return S3Path(f's3://{self.bucket}/')
        parent_key = '/'.join(self.key.split('/')[:-1])
        return S3Path(f's3://{self.bucket}/{parent_key}')

    @property
    def name(self) -> str:
        """Get file name (last component of key)."""
        if '/' not in self.key:
            return self.key
        return self.key.split('/')[-1]

    def join(self, *parts: str) -> 'S3Path':
        """Join additional path components."""
        key_parts = [self.key] + list(parts)
        new_key = '/'.join(part.strip('/') for part in key_parts if part.strip('/'))
        return S3Path(f's3://{self.bucket}/{new_key}')


class S3StreamingClient:
    """Client for streaming data to/from S3 using boto3."""

    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 region_name: Optional[str] = None,
                 **kwargs):
        """Initialize S3 streaming client.

        Args:
            aws_access_key_id: AWS access key ID (optional, uses boto3 default credential chain)
            aws_secret_access_key: AWS secret access key (optional)
            aws_session_token: AWS session token (optional, for temporary credentials)
            region_name: AWS region name (optional, uses boto3 default)
            **kwargs: Additional boto3 client parameters
        """
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )
        self._s3_client = self._session.client('s3', **kwargs)

    def exists(self, s3_path: Union[str, S3Path]) -> bool:
        """Check if S3 object exists.

        Args:
            s3_path: S3 path to check

        Returns:
            True if object exists, False otherwise
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        try:
            self._s3_client.head_object(Bucket=s3_path.bucket, Key=s3_path.key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def get_size(self, s3_path: Union[str, S3Path]) -> int:
        """Get size of S3 object in bytes.

        Args:
            s3_path: S3 path to check

        Returns:
            Size in bytes

        Raises:
            ClientError: If object doesn't exist
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        response = self._s3_client.head_object(Bucket=s3_path.bucket, Key=s3_path.key)
        return response['ContentLength']

    def open_for_read(self, s3_path: Union[str, S3Path],
                      encoding: str = 'utf-8',
                      chunk_size: int = 8192) -> TextIO:
        """Open S3 object for streaming read.

        Args:
            s3_path: S3 path to read from
            encoding: Text encoding for the file
            chunk_size: Size of chunks to read at a time

        Returns:
            Text stream for reading

        Raises:
            ClientError: If object doesn't exist or access is denied
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        response = self._s3_client.get_object(Bucket=s3_path.bucket, Key=s3_path.key)

        # Wrap the StreamingBody in a TextIOWrapper for text operations
        binary_stream = response['Body']
        return io.TextIOWrapper(binary_stream, encoding=encoding)

    def open_for_write(self, s3_path: Union[str, S3Path],
                       encoding: str = 'utf-8') -> 'S3StreamingWriter':
        """Open S3 object for streaming write using multipart upload.

        Args:
            s3_path: S3 path to write to
            encoding: Text encoding for the file

        Returns:
            S3StreamingWriter for writing data
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        return S3StreamingWriter(self._s3_client, s3_path, encoding=encoding)

    def list_objects(self, s3_prefix: Union[str, S3Path],
                     max_keys: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """List objects with given prefix.

        Args:
            s3_prefix: S3 path prefix to list
            max_keys: Maximum number of keys to return

        Yields:
            Dictionary with object metadata (Key, Size, LastModified, etc.)
        """
        if isinstance(s3_prefix, str):
            s3_prefix = S3Path(s3_prefix)

        paginator = self._s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=s3_prefix.bucket,
            Prefix=s3_prefix.key,
            MaxKeys=max_keys or 1000
        )

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    yield obj


class S3StreamingWriter:
    """Streaming writer for S3 using multipart upload."""

    def __init__(self, s3_client, s3_path: S3Path, encoding: str = 'utf-8',
                 part_size: int = 100 * 1024 * 1024):  # 100MB default
        """Initialize S3 streaming writer.

        Args:
            s3_client: boto3 S3 client
            s3_path: S3 path to write to
            encoding: Text encoding
            part_size: Size of each multipart upload part (minimum 5MB for S3)
        """
        self._s3_client = s3_client
        self._s3_path = s3_path
        self._encoding = encoding
        self._part_size = max(part_size, 5 * 1024 * 1024)  # Minimum 5MB

        # Initialize multipart upload
        self._upload_id = self._s3_client.create_multipart_upload(
            Bucket=s3_path.bucket,
            Key=s3_path.key
        )['UploadId']

        self._parts = []
        self._part_number = 1
        self._buffer = io.BytesIO()
        self._closed = False

    def write(self, data: str) -> int:
        """Write text data to S3 stream.

        Args:
            data: Text data to write

        Returns:
            Number of characters written
        """
        if self._closed:
            raise ValueError("Cannot write to closed stream")

        # Convert to bytes and write to buffer
        data_bytes = data.encode(self._encoding)
        bytes_written = self._buffer.write(data_bytes)

        # Upload part if buffer is large enough
        if self._buffer.tell() >= self._part_size:
            self._upload_part()

        return len(data)

    def _upload_part(self):
        """Upload current buffer as a part."""
        if self._buffer.tell() == 0:
            return

        # Get buffer contents
        self._buffer.seek(0)
        part_data = self._buffer.read()

        # Upload part
        response = self._s3_client.upload_part(
            Bucket=self._s3_path.bucket,
            Key=self._s3_path.key,
            PartNumber=self._part_number,
            UploadId=self._upload_id,
            Body=part_data
        )

        # Track part
        self._parts.append({
            'ETag': response['ETag'],
            'PartNumber': self._part_number
        })

        self._part_number += 1
        self._buffer = io.BytesIO()  # Reset buffer

    def close(self):
        """Close the stream and complete multipart upload."""
        if self._closed:
            return

        try:
            # Upload any remaining data
            self._upload_part()

            # Complete multipart upload
            self._s3_client.complete_multipart_upload(
                Bucket=self._s3_path.bucket,
                Key=self._s3_path.key,
                UploadId=self._upload_id,
                MultipartUpload={'Parts': self._parts}
            )
        except Exception:
            # Abort upload on failure
            try:
                self._s3_client.abort_multipart_upload(
                    Bucket=self._s3_path.bucket,
                    Key=self._s3_path.key,
                    UploadId=self._upload_id
                )
            except Exception:
                pass  # Best effort cleanup
            raise
        finally:
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def is_s3_path(path: Union[str, Path]) -> bool:
    """Check if a path is an S3 URI.

    Args:
        path: Path to check

    Returns:
        True if path is S3 URI, False otherwise
    """
    if isinstance(path, Path):
        path = str(path)
    return isinstance(path, str) and path.startswith('s3://')


def get_s3_client(**kwargs) -> S3StreamingClient:
    """Get S3 streaming client with default configuration.

    Args:
        **kwargs: Additional configuration for S3StreamingClient

    Returns:
        Configured S3StreamingClient instance
    """
    return S3StreamingClient(**kwargs)
