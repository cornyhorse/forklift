"""Unified I/O handler for local files and S3 objects.

This module provides a unified interface for reading from and writing to both
local filesystem and S3, integrating with ForkliftCore's streaming architecture.
"""

from __future__ import annotations
import csv
import tempfile
from typing import Iterator, Optional, Union, TextIO, BinaryIO, Tuple, List
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from .s3_streaming import S3StreamingClient, S3Path, is_s3_path, S3StreamingWriter


class UnifiedIOHandler:
    """Unified I/O handler for local files and S3 objects."""

    def __init__(self, s3_client: Optional[S3StreamingClient] = None):
        """Initialize unified I/O handler.

        Args:
            s3_client: Optional S3 client. If None, will create default client when needed.
        """
        self._s3_client = s3_client

    @property
    def s3_client(self) -> S3StreamingClient:
        """Get S3 client, creating one if needed."""
        if self._s3_client is None:
            from .s3_streaming import get_s3_client
            self._s3_client = get_s3_client()
        return self._s3_client

    @s3_client.setter
    def s3_client(self, value: S3StreamingClient) -> None:
        """Set S3 client."""
        self._s3_client = value

    @s3_client.deleter
    def s3_client(self) -> None:
        """Delete S3 client reference."""
        self._s3_client = None

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists (local file or S3 object).

        Args:
            path: Local file path or S3 URI

        Returns:
            True if path exists, False otherwise
        """
        if is_s3_path(path):
            return self.s3_client.exists(path)
        else:
            return Path(path).exists()

    def get_size(self, path: Union[str, Path]) -> int:
        """Get size of file/object in bytes.

        Args:
            path: Local file path or S3 URI

        Returns:
            Size in bytes
        """
        if is_s3_path(path):
            return self.s3_client.get_size(path)
        else:
            return Path(path).stat().st_size

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
        with self.open_for_read(path, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar, **kwargs)
            for row in reader:
                yield row

    def csv_writer(self, path: Union[str, Path],
                   delimiter: str = ',',
                   quotechar: str = '"',
                   encoding: str = 'utf-8',
                   **kwargs) -> 'UnifiedCSVWriter':
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
        return UnifiedCSVWriter(self, path, delimiter=delimiter,
                              quotechar=quotechar, encoding=encoding, **kwargs)

    def copy_file(self, src_path: Union[str, Path],
                  dest_path: Union[str, Path],
                  chunk_size: int = 8192) -> None:
        """Copy file between local/S3 locations.

        Supports:
        - Local to local
        - Local to S3
        - S3 to local
        - S3 to S3

        Args:
            src_path: Source path (local or S3)
            dest_path: Destination path (local or S3)
            chunk_size: Size of chunks to copy
        """
        src_is_s3 = is_s3_path(src_path)
        dest_is_s3 = is_s3_path(dest_path)

        if src_is_s3 and dest_is_s3:
            # S3 to S3 - use S3 copy
            src_s3_path = S3Path(str(src_path))
            dest_s3_path = S3Path(str(dest_path))

            copy_source = {
                'Bucket': src_s3_path.bucket,
                'Key': src_s3_path.key
            }
            self.s3_client._s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_s3_path.bucket,
                Key=dest_s3_path.key
            )
        else:
            # Stream copy for other combinations
            with self.open_for_read(src_path, encoding='utf-8') as src_f:
                with self.open_for_write(dest_path, encoding='utf-8') as dest_f:
                    while True:
                        chunk = src_f.read(chunk_size)
                        if not chunk:
                            break
                        dest_f.write(chunk)


class UnifiedCSVWriter:
    """Context manager for CSV writing to local files or S3."""

    def __init__(self, io_handler: UnifiedIOHandler, path: Union[str, Path],
                 delimiter: str = ',', quotechar: str = '"', encoding: str = 'utf-8',
                 **kwargs):
        """Initialize CSV writer.

        Args:
            io_handler: UnifiedIOHandler instance
            path: Output path (local or S3)
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            encoding: Text encoding
            **kwargs: Additional CSV writer arguments
        """
        self.io_handler = io_handler
        self.path = path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.kwargs = kwargs
        self._file = None
        self._writer = None

    def __enter__(self) -> csv.writer:
        """Enter context and return CSV writer."""
        self._file = self.io_handler.open_for_write(
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
            self._file.close()


class S3ParquetWriter:
    """Parquet writer that can output to S3 using streaming."""

    def __init__(self, s3_path: Union[str, S3Path], schema: pa.Schema,
                 s3_client: Optional[S3StreamingClient] = None,
                 compression: str = 'snappy',
                 **parquet_kwargs):
        """Initialize S3 Parquet writer.

        Args:
            s3_path: S3 path for output
            schema: PyArrow schema for the data
            s3_client: Optional S3 client
            compression: Compression algorithm
            **parquet_kwargs: Additional parquet writer arguments
        """
        if isinstance(s3_path, str):
            s3_path = S3Path(s3_path)

        self.s3_path = s3_path
        self.schema = schema
        self.compression = compression
        self.parquet_kwargs = parquet_kwargs

        if s3_client is None:
            from .s3_streaming import get_s3_client
            s3_client = get_s3_client()
        self.s3_client = s3_client

        # Use a temporary file for local parquet writing, then upload
        self._temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self._temp_path = Path(self._temp_file.name)
        self._temp_file.close()

        # Initialize parquet writer
        self._writer = pq.ParquetWriter(
            self._temp_path,
            schema,
            compression=compression,
            **parquet_kwargs
        )

    def write_table(self, table: pa.Table) -> None:
        """Write PyArrow table to parquet.

        Args:
            table: PyArrow table to write
        """
        self._writer.write_table(table)

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Write PyArrow record batch to parquet.

        Args:
            batch: PyArrow record batch to write
        """
        table = pa.Table.from_batches([batch])
        self.write_table(table)

    def close(self) -> None:
        """Close writer and upload to S3."""
        # Close parquet writer
        self._writer.close()

        try:
            # Upload to S3
            with open(self._temp_path, 'rb') as f:
                self.s3_client._s3_client.upload_fileobj(
                    f,
                    self.s3_path.bucket,
                    self.s3_path.key
                )
        finally:
            # Clean up temp file
            try:
                self._temp_path.unlink()
            except Exception:
                pass  # Best effort cleanup

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def create_parquet_writer(path: Union[str, Path], schema: pa.Schema,
                         s3_client: Optional[S3StreamingClient] = None,
                         compression: str = 'snappy',
                         **kwargs) -> Union[pq.ParquetWriter, S3ParquetWriter]:
    """Create appropriate parquet writer for local or S3 output.

    Args:
        path: Output path (local or S3)
        schema: PyArrow schema
        s3_client: Optional S3 client for S3 paths
        compression: Compression algorithm
        **kwargs: Additional parquet writer arguments

    Returns:
        ParquetWriter instance appropriate for the path type
    """
    if is_s3_path(path):
        return S3ParquetWriter(path, schema, s3_client=s3_client,
                              compression=compression, **kwargs)
    else:
        # Ensure parent directory exists for local files
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        return pq.ParquetWriter(path, schema, compression=compression, **kwargs)


def get_s3_client(**kwargs) -> S3StreamingClient:
    """Get S3 streaming client with default configuration.

    This function is used by tests for mocking purposes.

    Args:
        **kwargs: Additional configuration for S3StreamingClient

    Returns:
        Configured S3StreamingClient instance
    """
    from .s3_streaming import get_s3_client as _get_s3_client
    return _get_s3_client(**kwargs)
