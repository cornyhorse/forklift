"""Basic I/O operations for unified file handling."""

from __future__ import annotations
from typing import Union, TextIO
from pathlib import Path

from ..s3_streaming import S3StreamingClient, S3StreamingWriter, is_s3_path


class IOOperations:
    """Handles basic I/O operations for both local and S3 paths."""

    def __init__(self, s3_client: S3StreamingClient):
        """Initialize I/O operations handler.

        Args:
            s3_client: S3 streaming client for S3 operations
        """
        self.s3_client = s3_client

    def open_for_read(self, path: Union[str, Path],
                      encoding: str = 'utf-8',
                      **kwargs) -> TextIO:
        """Open file/object for reading.

        Args:
            path: Local file path or S3 URI
            encoding: Text encoding
            **kwargs: Additional arguments for file opening

        Returns:
            Text stream for reading
        """
        if is_s3_path(path):
            return self.s3_client.open_for_read(path, encoding=encoding)
        else:
            return open(path, 'r', encoding=encoding, **kwargs)

    def open_for_write(self, path: Union[str, Path],
                       encoding: str = 'utf-8',
                       **kwargs) -> Union[TextIO, S3StreamingWriter]:
        """Open file/object for writing.

        Args:
            path: Local file path or S3 URI
            encoding: Text encoding
            **kwargs: Additional arguments for file opening

        Returns:
            Text stream for writing
        """
        if is_s3_path(path):
            return self.s3_client.open_for_write(path, encoding=encoding)
        else:
            # Ensure parent directory exists for local files only
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            return open(path, 'w', encoding=encoding, **kwargs)
