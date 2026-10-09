"""Unified I/O handler for local files and S3 objects.

This module provides a unified interface for reading from and writing to both
local filesystem and S3, integrating with ForkliftCore's streaming architecture.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from pathlib import Path
from typing import BinaryIO, Iterator, List, Optional, TextIO, Union

import pyarrow as pa
import pyarrow.parquet as pq

from .s3_streaming import (
    S3Path,
    S3StreamingClient,
    S3StreamingWriter,
    is_s3_path,
    normalize_s3_uri,
)

_COPY_BUFFER_SIZE = 1024 * 1024


def _is_utf8(encoding: Optional[str]) -> bool:
    return bool(encoding) and encoding.lower().replace("_", "-") in ("utf-8", "utf8")


def _spool_to_seekable(stream) -> BinaryIO:
    """Copy a non-seekable binary stream into a temporary, seekable file.

    Parquet and Excel readers need random access, which an S3 response body cannot offer. The
    temporary file is removed automatically when it is closed.
    """
    spooled = tempfile.TemporaryFile(mode="w+b")
    try:
        shutil.copyfileobj(stream, spooled, _COPY_BUFFER_SIZE)
        spooled.seek(0)
    except BaseException:
        spooled.close()
        raise
    finally:
        try:
            stream.close()
        except Exception:
            pass
    return spooled


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
            return self.s3_client.exists(normalize_s3_uri(path))
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
            return self.s3_client.get_size(normalize_s3_uri(path))
        else:
            return Path(path).stat().st_size

    def open_for_read(
        self, path: Union[str, Path], encoding: str = "utf-8", mode: str = "r", **kwargs
    ) -> Union[TextIO, BinaryIO]:
        """Open file/object for reading.

        Text mode (the default) opens with ``newline=""`` so line breaks inside quoted CSV
        fields are preserved exactly. Binary mode - ``mode="rb"`` or ``encoding="binary"`` -
        returns a binary file object; for S3 the object is downloaded into a temporary
        seekable file (Parquet/Excel readers need ``seek``) unless ``seekable=False`` is
        passed to get the raw forward-only stream.

        Args:
            path: Local file path or S3 URI
            encoding: Text encoding (ignored in binary mode)
            mode: ``"r"`` for text or ``"rb"`` for binary
            **kwargs: Additional arguments for file opening

        Returns:
            Text or binary stream for reading
        """
        binary = "b" in mode or encoding == "binary"
        seekable = kwargs.pop("seekable", True)

        if is_s3_path(path):
            uri = normalize_s3_uri(path)
            if binary:
                stream = self.s3_client.open_for_read(uri, mode="rb")
                return _spool_to_seekable(stream) if seekable else stream
            return self.s3_client.open_for_read(
                uri, encoding=encoding, newline=kwargs.get("newline", "")
            )

        if binary:
            for text_only in ("encoding", "errors", "newline"):
                kwargs.pop(text_only, None)
            return open(path, "rb", **kwargs)
        kwargs.setdefault("newline", "")
        return open(path, "r", encoding=encoding, **kwargs)

    def open_for_write(
        self, path: Union[str, Path], encoding: str = "utf-8", mode: str = "w", **kwargs
    ) -> Union[TextIO, BinaryIO, S3StreamingWriter]:
        """Open file/object for writing.

        Args:
            path: Local file path or S3 URI
            encoding: Text encoding
            mode: ``"w"`` for text (default) or ``"wb"`` for binary
            **kwargs: Additional arguments for file opening

        Returns:
            Stream for writing
        """
        binary = "b" in mode
        if is_s3_path(path):
            uri = normalize_s3_uri(path)
            if binary:
                return self.s3_client.open_for_write(uri, encoding=encoding, mode=mode)
            return self.s3_client.open_for_write(uri, encoding=encoding)
        else:
            # Ensure parent directory exists for local files only
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            if binary:
                return open(path, mode, **kwargs)
            return open(path, "w", encoding=encoding, **kwargs)

    def csv_reader(
        self,
        path: Union[str, Path],
        delimiter: str = ",",
        quotechar: str = '"',
        encoding: str = "utf-8",
        **kwargs,
    ) -> Iterator[List[str]]:
        """Create CSV reader for file/object.

        A UTF-8 byte order mark at the start of the file is dropped (otherwise it would end
        up in the first header cell, or break quoting of the first field).

        Args:
            path: Local file path or S3 URI
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            encoding: Text encoding
            **kwargs: Additional CSV reader arguments

        Yields:
            List of field values for each row
        """
        read_encoding = "utf-8-sig" if _is_utf8(encoding) else encoding
        with self.open_for_read(path, encoding=read_encoding) as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar, **kwargs)
            for row in reader:
                yield row

    def csv_writer(
        self,
        path: Union[str, Path],
        delimiter: str = ",",
        quotechar: str = '"',
        encoding: str = "utf-8",
        **kwargs,
    ) -> "UnifiedCSVWriter":
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
        return UnifiedCSVWriter(
            self, path, delimiter=delimiter, quotechar=quotechar, encoding=encoding, **kwargs
        )

    def copy_file(
        self, src_path: Union[str, Path], dest_path: Union[str, Path], chunk_size: int = 8192
    ) -> None:
        """Copy file between local/S3 locations, byte for byte.

        Supports:
        - Local to local
        - Local to S3
        - S3 to local
        - S3 to S3 (server-side managed copy, so objects over 5 GB work)

        The copy is binary: it never re-encodes text or rewrites line endings. A failed copy
        does not leave a truncated destination behind (local copies go through a temporary
        file, S3 uploads are aborted).

        Args:
            src_path: Source path (local or S3)
            dest_path: Destination path (local or S3)
            chunk_size: Size of chunks to copy

        Raises:
            shutil.SameFileError: If source and destination are the same file/object
        """
        src_is_s3 = is_s3_path(src_path)
        dest_is_s3 = is_s3_path(dest_path)

        if src_is_s3 and dest_is_s3:
            # S3 to S3 - use the managed server-side copy (multipart above 5 GB)
            src_s3_path = S3Path(normalize_s3_uri(src_path))
            dest_s3_path = S3Path(normalize_s3_uri(dest_path))
            if (src_s3_path.bucket, src_s3_path.key) == (dest_s3_path.bucket, dest_s3_path.key):
                raise shutil.SameFileError("Source and destination are the same S3 object")

            copy_source = {"Bucket": src_s3_path.bucket, "Key": src_s3_path.key}
            self.s3_client._s3_client.copy(copy_source, dest_s3_path.bucket, dest_s3_path.key)
            return

        if not src_is_s3 and not dest_is_s3:
            src_local, dest_local = Path(src_path), Path(dest_path)
            if src_local.resolve() == dest_local.resolve() or (
                dest_local.exists() and os.path.samefile(src_local, dest_local)
            ):
                raise shutil.SameFileError("Source and destination are the same file")

        with self.open_for_read(src_path, mode="rb", seekable=False) as src_f:
            if dest_is_s3:
                # The S3 writer aborts the upload if the copy raises
                with self.open_for_write(dest_path, mode="wb") as dest_f:
                    self._copy_stream(src_f, dest_f, chunk_size)
            else:
                self._copy_to_local(src_f, Path(dest_path), chunk_size)

    @staticmethod
    def _copy_stream(src_f, dest_f, chunk_size: int) -> None:
        while True:
            chunk = src_f.read(chunk_size)
            if not chunk:
                break
            dest_f.write(chunk)

    def _copy_to_local(self, src_f, dest: Path, chunk_size: int) -> None:
        """Write a stream to ``dest`` through a temporary file in the same directory."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_name = tempfile.mkstemp(dir=str(dest.parent), prefix=f".{dest.name}.")
        try:
            with os.fdopen(tmp_fd, "wb") as tmp_f:
                self._copy_stream(src_f, tmp_f, chunk_size)
            os.replace(tmp_name, dest)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise


class UnifiedCSVWriter:
    """Context manager for CSV writing to local files or S3."""

    def __init__(
        self,
        io_handler: UnifiedIOHandler,
        path: Union[str, Path],
        delimiter: str = ",",
        quotechar: str = '"',
        encoding: str = "utf-8",
        **kwargs,
    ):
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
        self._file = self.io_handler.open_for_write(self.path, encoding=self.encoding)
        self._writer = csv.writer(
            self._file, delimiter=self.delimiter, quotechar=self.quotechar, **self.kwargs
        )
        return self._writer

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and close file (an S3 upload is aborted when the body failed)."""
        if self._file:
            if exc_type is not None and hasattr(self._file, "abort"):
                self._file.abort()
            else:
                self._file.close()


class S3ParquetWriter:
    """Parquet writer that can output to S3 using streaming."""

    def __init__(
        self,
        s3_path: Union[str, S3Path],
        schema: pa.Schema,
        s3_client: Optional[S3StreamingClient] = None,
        compression: str = "snappy",
        **parquet_kwargs,
    ):
        """Initialize S3 Parquet writer.

        Args:
            s3_path: S3 path for output
            schema: PyArrow schema for the data
            s3_client: Optional S3 client
            compression: Compression algorithm
            **parquet_kwargs: Additional parquet writer arguments
        """
        if isinstance(s3_path, (str, os.PathLike)):
            s3_path = S3Path(s3_path)

        self.s3_path = s3_path
        self.schema = schema
        self.compression = compression
        self.parquet_kwargs = parquet_kwargs
        self._closed = False
        self._writer = None
        self._temp_path = None

        if s3_client is None:
            from .s3_streaming import get_s3_client

            s3_client = get_s3_client()
        self.s3_client = s3_client

        # Use a temporary file for local parquet writing, then upload
        self._temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        self._temp_path = Path(self._temp_file.name)
        self._temp_file.close()

        # Initialize parquet writer; never leak the temp file if that fails
        try:
            self._writer = pq.ParquetWriter(
                self._temp_path, schema, compression=compression, **parquet_kwargs
            )
        except BaseException:
            self._remove_temp_file()
            raise

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

    def _remove_temp_file(self) -> None:
        try:
            if self._temp_path is not None:
                self._temp_path.unlink()
        except Exception:
            pass  # Best effort cleanup

    def close(self) -> None:
        """Close writer and upload to S3 (no-op once closed or aborted)."""
        if self._closed:
            return
        self._closed = True

        try:
            # Close parquet writer
            self._writer.close()

            # Upload to S3
            with open(self._temp_path, "rb") as f:
                self.s3_client._s3_client.upload_fileobj(f, self.s3_path.bucket, self.s3_path.key)
        finally:
            # Clean up temp file, whether or not the upload succeeded
            self._remove_temp_file()

    def abort(self) -> None:
        """Discard the partial output without uploading anything.

        Safe to call repeatedly and after ``close()``.
        """
        if self._closed:
            return
        self._closed = True
        try:
            self._writer.close()
        except Exception:
            pass
        self._remove_temp_file()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # A failed body must not publish a partial Parquet file
            self.abort()
        else:
            self.close()


def create_parquet_writer(
    path: Union[str, Path],
    schema: pa.Schema,
    s3_client: Optional[S3StreamingClient] = None,
    compression: str = "snappy",
    **kwargs,
) -> Union[pq.ParquetWriter, S3ParquetWriter]:
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
        return S3ParquetWriter(
            normalize_s3_uri(path), schema, s3_client=s3_client, compression=compression, **kwargs
        )
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
