"""Output handlers for writing processed data.

This module provides output handler classes for writing processed data to various
formats including Parquet files, manifest generation for data catalogs, and
metadata file creation for processing statistics.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


@dataclass
class OutputConfig:
    """Configuration for output writing operations.

    Args:
        compression: Compression algorithm for output files (default: snappy)
        create_manifest: Whether to generate manifest files (default: True)
        create_metadata: Whether to generate metadata files (default: True)
        row_group_size: Number of rows per row group in parquet (default: 50000)
    """
    compression: str = "snappy"
    create_manifest: bool = True
    create_metadata: bool = True
    row_group_size: int = 50000


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
            row_group_size=self.config.row_group_size,
        )
        self.writers[str(file_path)] = writer
        return writer

    def write_batch(self, writer: pq.ParquetWriter, batch: pa.RecordBatch):
        """Write a batch to the parquet file.

        Converts the RecordBatch to a Table and writes it using the provided writer.
        Only writes non-empty batches to avoid creating empty row groups.

        Args:
            writer: ParquetWriter instance for the target file
            batch: PyArrow RecordBatch containing data to write
        """
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def close_all_writers(self):
        """Close all open Parquet writers and clear the writers dictionary.

        This method should be called at the end of processing to ensure
        all files are properly finalized and closed.
        """
        for writer in self.writers.values():
            writer.close()
        self.writers.clear()


class ManifestGenerator:
    """Generates manifest files for output datasets.

    This class creates manifest files compatible with data catalog systems
    like Databricks and Apache Iceberg, providing metadata about output files
    including file sizes and record counts.
    """

    @staticmethod
    def create_manifest(output_dir: Path, files: List[str]) -> str:
        """Create a manifest file listing output files.

        Generates a JSON manifest file containing metadata about all output files
        in a format compatible with modern data catalog systems.

        Args:
            output_dir: Directory where the manifest file will be created
            files: List of file paths to include in the manifest

        Returns:
            Path to the created manifest file as a string

        Note:
            The manifest includes file paths, sizes, record counts, and timestamps
            in a standardized format for data catalog integration.
        """
        manifest_path = output_dir / "manifest.json"

        manifest = {
            "format_version": "1.0",
            "files": [
                {
                    "file_path": str(Path(f).name),
                    "file_size": Path(f).stat().st_size if Path(f).exists() else 0,
                    "record_count": ManifestGenerator._get_parquet_row_count(f),
                }
                for f in files
            ],
            "created_at": pd.Timestamp.now().isoformat(),
            "total_files": len(files),
            "total_size": sum(Path(f).stat().st_size if Path(f).exists() else 0 for f in files),
        }

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        return str(manifest_path)

    @staticmethod
    def _get_parquet_row_count(file_path: str) -> int:
        """Get row count from a Parquet file.

        Reads the Parquet file metadata to extract the total number of rows
        without loading the actual data into memory.

        Args:
            file_path: Path to the Parquet file to analyze

        Returns:
            Number of rows in the Parquet file (0 if file cannot be read)
        """
        try:
            parquet_file = pq.ParquetFile(file_path)
            return parquet_file.metadata.num_rows
        except Exception:
            return 0


class MetadataGenerator:
    """Generates metadata files with processing statistics.

    This class creates comprehensive metadata files containing processing
    statistics, configuration details, and column-level analytics for
    data quality and lineage tracking.
    """

    @staticmethod
    def create_metadata(output_dir: Path, processing_stats: Dict[str, Any]) -> str:
        """Create metadata file with processing statistics.

        Generates a comprehensive metadata file containing processing summary,
        input configuration, column statistics, and execution details.

        Args:
            output_dir: Directory where the metadata file will be created
            processing_stats: Dictionary containing processing statistics and configuration

        Returns:
            Path to the created metadata file as a string

        Note:
            The metadata includes processing summary, column statistics,
            and configuration details for data lineage and quality tracking.
        """
        metadata_path = output_dir / "metadata.json"

        # Add column-level statistics if data files exist
        column_stats = {}
        if processing_stats.get("output_files"):
            column_stats = MetadataGenerator._generate_column_stats(
                processing_stats["output_files"][0]  # Main data file
            )

        metadata = {
            "processing_summary": processing_stats.get("processing_summary", {}),
            "input_config": processing_stats.get("input_config", {}),
            "output_files": processing_stats.get("output_files", []),
            "column_statistics": column_stats,
            "created_at": pd.Timestamp.now().isoformat(),
        }

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        return str(metadata_path)

    @staticmethod
    def _generate_column_stats(data_file: str) -> Dict[str, Any]:
        """Generate column-level statistics from parquet file.

        Analyzes the data in the Parquet file to generate comprehensive
        column-level statistics including data types, null counts, and
        type-specific metrics like min/max values and unique counts.

        Args:
            data_file: Path to the Parquet file to analyze

        Returns:
            Dictionary containing column statistics or error information

        Note:
            Statistics include data types, null counts, min/max values for
            numeric columns, and unique counts for string columns.
        """
        try:
            # Read a sample of the data for statistics
            table = pq.read_table(data_file)
            df = table.to_pandas()

            stats = {}
            for column in df.columns:
                col_stats = {
                    "dtype": str(df[column].dtype),
                    "null_count": int(df[column].isnull().sum()),
                    "non_null_count": int(df[column].count()),
                }

                # Add type-specific statistics
                if df[column].dtype in ['int64', 'float64']:
                    col_stats.update({
                        "min": float(df[column].min()) if pd.notna(df[column].min()) else None,
                        "max": float(df[column].max()) if pd.notna(df[column].max()) else None,
                        "mean": float(df[column].mean()) if pd.notna(df[column].mean()) else None,
                    })
                elif df[column].dtype == 'object':
                    col_stats.update({
                        "unique_count": int(df[column].nunique()),
                        "most_common": df[column].value_counts().head().to_dict() if len(df[column]) > 0 else {},
                    })

                stats[column] = col_stats

            return stats

        except Exception as e:
            return {"error": f"Could not generate column statistics: {str(e)}"}


class IcebergOutputHandler:
    """Handles Iceberg table output (placeholder for future implementation).

    This class will provide functionality for writing data to Apache Iceberg
    tables with support for schema evolution, time travel, and ACID transactions.

    Args:
        config: Dictionary containing Iceberg-specific configuration

    Attributes:
        config: The configuration dictionary for this output handler
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Iceberg output handler.

        Args:
            config: Configuration dictionary containing Iceberg parameters
        """
        self.config = config

    def write_table(self, table: pa.Table):
        """Write table to Iceberg format.

        Args:
            table: PyArrow Table containing data to write

        Raises:
            NotImplementedError: This functionality is not yet implemented
        """
        raise NotImplementedError("Iceberg output not yet implemented")


class DeltaOutputHandler:
    """Handles Delta Lake output (placeholder for future implementation).

    This class will provide functionality for writing data to Delta Lake
    tables with support for ACID transactions, schema enforcement, and
    time travel capabilities.

    Args:
        config: Dictionary containing Delta Lake configuration

    Attributes:
        config: The configuration dictionary for this output handler
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Delta Lake output handler.

        Args:
            config: Configuration dictionary containing Delta Lake parameters
        """
        self.config = config

    def write_table(self, table: pa.Table):
        """Write table to Delta Lake format.

        Args:
            table: PyArrow Table containing data to write

        Raises:
            NotImplementedError: This functionality is not yet implemented
        """
        raise NotImplementedError("Delta Lake output not yet implemented")
