"""Parquet output handler for writing processed data to Parquet files."""

from __future__ import annotations
from pathlib import Path
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq

from .config import OutputConfig


class ParquetOutputHandler:
    """Handles writing data to Parquet files.

    This class manages the creation and writing of Parquet files with configurable
    compression and row group settings. It maintains multiple writers for different
    output streams (e.g., valid data vs. invalid data).

    Args:
        config: OutputConfig instance with writing configuration

    Attributes:
        config: The configuration object for this output handler
        writers: Dictionary mapping file paths to ParquetWriter instances
    """

    def __init__(self, config: OutputConfig):
        """Initialize the Parquet output handler.

        Args:
            config: Configuration object containing output parameters
        """
        self.config = config
        self.writers: Dict[str, pq.ParquetWriter] = {}

    def create_writer(self, file_path: Path, schema: pa.Schema) -> pq.ParquetWriter:
        """Create a new Parquet writer for the specified file.

        Args:
            file_path: Path where the Parquet file will be written
            schema: PyArrow schema defining the structure of the data

        Returns:
            ParquetWriter instance configured with the output settings
        """
        writer = pq.ParquetWriter(
            file_path,
            schema,
            compression=self.config.compression,
        )
        self.writers[str(file_path)] = writer
        return writer

    def write_batch(self, writer: pq.ParquetWriter, batch: pa.RecordBatch):
        """Write a batch to the parquet file.

        Converts the RecordBatch to a Table and writes it using the provided writer.
        Only writes non-empty batches to avoid creating empty row groups.
        Uses the configured row_group_size to control row group sizing.

        Args:
            writer: ParquetWriter instance for the target file
            batch: PyArrow RecordBatch containing data to write
        """
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table, row_group_size=self.config.row_group_size)

    def close_all_writers(self):
        """Close all open Parquet writers and clear the writers dictionary.

        This method should be called at the end of processing to ensure
        all files are properly finalized and closed.
        """
        for writer in self.writers.values():
            writer.close()
        self.writers.clear()
