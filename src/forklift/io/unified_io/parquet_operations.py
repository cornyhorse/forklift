"""Parquet operations for unified I/O handling."""

from __future__ import annotations
import tempfile
from typing import Optional, Union, TYPE_CHECKING
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from ..s3_streaming import S3StreamingClient, S3Path, is_s3_path

if TYPE_CHECKING:
    # For type hints only, avoid circular imports
    pass


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
            from ..s3_streaming import get_s3_client
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
        table = pa.Table.from_batches([batch], schema=batch.schema)
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


class ParquetOperations:
    """Handles Parquet operations for both local and S3 paths."""

    def __init__(self, s3_client: S3StreamingClient):
        """Initialize Parquet operations handler.

        Args:
            s3_client: S3 streaming client for S3 operations
        """
        self.s3_client = s3_client

    def create_parquet_writer(self, path: Union[str, Path], schema: pa.Schema,
                             compression: str = 'snappy',
                             **kwargs) -> Union[pq.ParquetWriter, S3ParquetWriter]:
        """Create appropriate parquet writer for local or S3 output.

        Args:
            path: Output path (local or S3)
            schema: PyArrow schema
            compression: Compression algorithm
            **kwargs: Additional parquet writer arguments

        Returns:
            ParquetWriter instance appropriate for the path type
        """
        if is_s3_path(path):
            return S3ParquetWriter(path, schema, s3_client=self.s3_client,
                                  compression=compression, **kwargs)
        else:
            # Ensure parent directory exists for local files
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            return pq.ParquetWriter(path, schema, compression=compression, **kwargs)
