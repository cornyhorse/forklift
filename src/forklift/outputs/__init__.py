"""Output handlers for writing processed data."""

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
    """Configuration for output writing."""
    compression: str = "snappy"
    create_manifest: bool = True
    create_metadata: bool = True
    row_group_size: int = 50000


class ParquetOutputHandler:
    """Handles writing data to Parquet files."""

    def __init__(self, config: OutputConfig):
        self.config = config
        self.writers: Dict[str, pq.ParquetWriter] = {}

    def create_writer(self, file_path: Path, schema: pa.Schema) -> pq.ParquetWriter:
        """Create a new Parquet writer."""
        writer = pq.ParquetWriter(
            file_path,
            schema,
            compression=self.config.compression,
            row_group_size=self.config.row_group_size,
        )
        self.writers[str(file_path)] = writer
        return writer

    def write_batch(self, writer: pq.ParquetWriter, batch: pa.RecordBatch):
        """Write a batch to the parquet file."""
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def close_all_writers(self):
        """Close all open writers."""
        for writer in self.writers.values():
            writer.close()
        self.writers.clear()


class ManifestGenerator:
    """Generates manifest files for output datasets."""

    @staticmethod
    def create_manifest(output_dir: Path, files: List[str]) -> str:
        """Create a manifest file listing output files."""
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
        """Get row count from parquet file."""
        try:
            parquet_file = pq.ParquetFile(file_path)
            return parquet_file.metadata.num_rows
        except Exception:
            return 0


class MetadataGenerator:
    """Generates metadata files with processing statistics."""

    @staticmethod
    def create_metadata(output_dir: Path, processing_stats: Dict[str, Any]) -> str:
        """Create metadata file with processing statistics."""
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
        """Generate column-level statistics from parquet file."""
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
    """Handles Iceberg table output (placeholder for future implementation)."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def write_table(self, table: pa.Table):
        """Write table to Iceberg format."""
        raise NotImplementedError("Iceberg output not yet implemented")


class DeltaOutputHandler:
    """Handles Delta Lake output (placeholder for future implementation)."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def write_table(self, table: pa.Table):
        """Write table to Delta Lake format."""
        raise NotImplementedError("Delta Lake output not yet implemented")
