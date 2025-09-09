"""Manifest generator for creating data catalog-compatible manifest files."""

from __future__ import annotations
import json
from pathlib import Path
from typing import List
from datetime import datetime

import pyarrow.parquet as pq


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
            "created_at": datetime.now().isoformat(),
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
