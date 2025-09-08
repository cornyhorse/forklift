"""Core unified I/O handler class."""

from __future__ import annotations
from typing import Optional, Union, TextIO, Iterator, List
from pathlib import Path
import csv

from ..s3_streaming import S3StreamingClient, S3StreamingWriter
from .file_operations import FileOperations
from .io_operations import IOOperations
from .csv_operations import CSVOperations, UnifiedCSVWriter
from .parquet_operations import ParquetOperations


class UnifiedIOHandler:
    """Unified I/O handler for local files and S3 objects."""

    def __init__(self, s3_client: Optional[S3StreamingClient] = None):
        """Initialize unified I/O handler.

        Args:
            s3_client: Optional S3 client. If None, will create default client when needed.
        """
        self._s3_client = s3_client
        self._file_ops = None
        self._io_ops = None
        self._csv_ops = None
        self._parquet_ops = None

    @property
    def s3_client(self) -> S3StreamingClient:
        """Get S3 client, creating one if needed."""
        if self._s3_client is None:
            from ..s3_streaming import get_s3_client
            self._s3_client = get_s3_client()
        return self._s3_client

    @s3_client.setter
    def s3_client(self, value: S3StreamingClient) -> None:
        """Set S3 client."""
        self._s3_client = value
        # Reset operation handlers to use new client
        self._file_ops = None
        self._io_ops = None
        self._csv_ops = None
        self._parquet_ops = None

    @s3_client.deleter
    def s3_client(self) -> None:
        """Delete S3 client reference."""
        self._s3_client = None
        # Reset operation handlers
        self._file_ops = None
        self._io_ops = None
        self._csv_ops = None
        self._parquet_ops = None

    @property
    def file_operations(self) -> FileOperations:
        """Get file operations handler."""
        if self._file_ops is None:
            self._file_ops = FileOperations(self.s3_client)
        return self._file_ops

    @property
    def io_operations(self) -> IOOperations:
        """Get I/O operations handler."""
        if self._io_ops is None:
            self._io_ops = IOOperations(self.s3_client)
        return self._io_ops

    @property
    def csv_operations(self) -> CSVOperations:
        """Get CSV operations handler."""
        if self._csv_ops is None:
            self._csv_ops = CSVOperations(self.s3_client)
        return self._csv_ops

    @property
    def parquet_operations(self) -> ParquetOperations:
        """Get Parquet operations handler."""
        if self._parquet_ops is None:
            self._parquet_ops = ParquetOperations(self.s3_client)
        return self._parquet_ops

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists (local file or S3 object)."""
        return self.file_operations.exists(path)

    def get_size(self, path: Union[str, Path]) -> int:
        """Get size of file/object in bytes."""
        return self.file_operations.get_size(path)

    def open_for_read(self, path: Union[str, Path],
                      encoding: str = 'utf-8',
                      **kwargs) -> TextIO:
        """Open file/object for reading."""
        return self.io_operations.open_for_read(path, encoding=encoding, **kwargs)

    def open_for_write(self, path: Union[str, Path],
                       encoding: str = 'utf-8',
                       **kwargs) -> Union[TextIO, S3StreamingWriter]:
        """Open file/object for writing."""
        return self.io_operations.open_for_write(path, encoding=encoding, **kwargs)

    def csv_reader(self, path: Union[str, Path],
                   delimiter: str = ',',
                   quotechar: str = '"',
                   encoding: str = 'utf-8',
                   **kwargs) -> Iterator[List[str]]:
        """Create CSV reader for file/object."""
        # Use self.open_for_read directly to maintain test compatibility
        with self.open_for_read(path, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar, **kwargs)
            for row in reader:
                yield row

    def csv_writer(self, path: Union[str, Path],
                   delimiter: str = ',',
                   quotechar: str = '"',
                   encoding: str = 'utf-8',
                   **kwargs) -> UnifiedCSVWriter:
        """Create CSV writer for file/object."""
        return UnifiedCSVWriter(self, path, delimiter=delimiter,
                              quotechar=quotechar, encoding=encoding, **kwargs)

    def copy_file(self, src_path: Union[str, Path],
                  dest_path: Union[str, Path],
                  chunk_size: int = 8192) -> None:
        """Copy file between local/S3 locations."""
        from ..s3_streaming import S3Path, is_s3_path

        src_is_s3 = is_s3_path(src_path)
        dest_is_s3 = is_s3_path(dest_path)

        if src_is_s3 and dest_is_s3:
            # S3 to S3 - use S3 copy via file operations
            return self.file_operations.copy_file(src_path, dest_path, chunk_size)
        else:
            # Stream copy for other combinations - use handler methods for test compatibility
            with self.open_for_read(src_path, encoding='utf-8') as src_f:
                with self.open_for_write(dest_path, encoding='utf-8') as dest_f:
                    while True:
                        chunk = src_f.read(chunk_size)
                        if not chunk:
                            break
                        dest_f.write(chunk)
