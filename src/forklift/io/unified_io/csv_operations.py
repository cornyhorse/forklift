"""CSV operations for unified I/O handling."""

from __future__ import annotations
import csv
from typing import Iterator, Union, List
from pathlib import Path

from ..s3_streaming import S3StreamingClient
from .io_operations import IOOperations


class UnifiedCSVWriter:
    """Context manager for CSV writing to local files or S3."""

    def __init__(self, io_handler, path: Union[str, Path],
                 delimiter: str = ',', quotechar: str = '"', encoding: str = 'utf-8',
                 **kwargs):
        """Initialize CSV writer.

        Args:
            io_handler: UnifiedIOHandler instance (for backward compatibility)
            path: Output path (local or S3)
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            encoding: Text encoding
            **kwargs: Additional CSV writer arguments
        """
        self.io_handler = io_handler  # Keep for backward compatibility
        self.io_operations = getattr(io_handler, 'io_operations', io_handler)
        self.path = path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.kwargs = kwargs
        self._file = None
        self._writer = None

    def __enter__(self):
        """Enter context and return CSV writer."""
        # Use the io_handler's open_for_write method directly for test compatibility
        if hasattr(self.io_handler, 'open_for_write'):
            self._file = self.io_handler.open_for_write(
                self.path, encoding=self.encoding
            )
        else:
            # Fallback to io_operations for cases where io_handler doesn't have the method
            self._file = self.io_operations.open_for_write(
                self.path, encoding=self.encoding
            )

        self._writer = csv.writer(
            self._file,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            **self.kwargs
        )
        return self._writer

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and close file."""
        if self._file:
            # Check if the file object has a close method and call it
            if hasattr(self._file, 'close'):
                self._file.close()


class CSVOperations:
    """Handles CSV operations for both local and S3 paths."""

    def __init__(self, s3_client: S3StreamingClient):
        """Initialize CSV operations handler.

        Args:
            s3_client: S3 streaming client for S3 operations
        """
        self.s3_client = s3_client
        self.io_operations = IOOperations(s3_client)

    def csv_reader(self, path: Union[str, Path],
                   delimiter: str = ',',
                   quotechar: str = '"',
                   encoding: str = 'utf-8',
                   **kwargs) -> Iterator[List[str]]:
        """Create CSV reader for file/object.

        Args:
            path: Local file path or S3 URI
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            encoding: Text encoding
            **kwargs: Additional CSV reader arguments

        Yields:
            List of field values for each row
        """
        # Use the unified handler's open_for_read method to maintain compatibility
        with self.io_operations.open_for_read(path, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar, **kwargs)
            for row in reader:
                yield row

    def csv_writer(self, path: Union[str, Path],
                   delimiter: str = ',',
                   quotechar: str = '"',
                   encoding: str = 'utf-8',
                   **kwargs) -> UnifiedCSVWriter:
        """Create CSV writer for file/object.

        Args:
            path: Local file path or S3 URI
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            encoding: Text encoding
            **kwargs: Additional CSV writer arguments

        Returns:
            CSV writer context manager
        """
        return UnifiedCSVWriter(self.io_operations, path, delimiter=delimiter,
                              quotechar=quotechar, encoding=encoding, **kwargs)
