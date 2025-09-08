"""Main S3 streaming client for reading and writing data to/from S3."""

from __future__ import annotations
import io
from typing import Iterator, Optional, Dict, Any, Union, BinaryIO, TextIO

import boto3
from botocore.exceptions import ClientError

from .path import S3Path
from .writer import S3StreamingWriter


class S3StreamingClient:
    """Client for streaming data to/from S3 using boto3."""

    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 region_name: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 **kwargs):
        """Initialize S3 streaming client.

        Args:
            aws_access_key_id: AWS access key ID (optional, uses boto3 default credential chain)
            aws_secret_access_key: AWS secret access key (optional)
            aws_session_token: AWS session token (optional, for temporary credentials)
            region_name: AWS region name (optional, uses boto3 default)
            endpoint_url: Custom S3 endpoint URL (optional, for S3-compatible services like Hetzner)
            **kwargs: Additional boto3 client parameters
        """
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )

        # Add endpoint_url to kwargs if provided
        if endpoint_url:
            kwargs['endpoint_url'] = endpoint_url

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
                      chunk_size: int = 8192, mode: str = 'r') -> Union[TextIO, BinaryIO]:
        """Open S3 object for streaming read.

        Args:
            s3_path: S3 path to read from
            encoding: Text encoding for the file (ignored for binary mode)
            chunk_size: Size of chunks to read at a time
            mode: Read mode - 'r' for text, 'rb' for binary

        Returns:
            Text stream for reading in text mode, binary stream for binary mode

        Raises:
            ClientError: If object doesn't exist or access is denied
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        response = self._s3_client.get_object(Bucket=s3_path.bucket, Key=s3_path.key)
        binary_stream = response['Body']

        # Return binary stream for binary mode, text wrapper for text mode
        if 'b' in mode:
            return binary_stream
        else:
            return io.TextIOWrapper(binary_stream, encoding=encoding)

    def open_for_write(self, s3_path: Union[str, S3Path],
                       encoding: str = 'utf-8', mode: str = 'w') -> S3StreamingWriter:
        """Open S3 object for streaming write using multipart upload.

        Args:
            s3_path: S3 path to write to
            encoding: Text encoding for the file
            mode: Write mode - 'w' for text, 'wb' for binary

        Returns:
            S3StreamingWriter for writing data
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        return S3StreamingWriter(self._s3_client, s3_path, encoding=encoding, mode=mode)

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
