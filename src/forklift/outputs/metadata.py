"""Metadata generator for creating processing statistics and configuration details."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


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
            for file_path in processing_stats["output_files"]:
                if Path(file_path).exists() and file_path.endswith(".parquet"):
                    try:
                        # Only the footer is read: no column data is loaded
                        parquet_file = pq.ParquetFile(file_path)
                        schema = parquet_file.schema_arrow
                        column_stats[str(Path(file_path).name)] = {
                            "num_columns": len(schema),
                            "num_rows": parquet_file.metadata.num_rows,
                            "column_names": schema.names,
                            "column_types": [str(field.type) for field in schema],
                        }
                    except (OSError, pa.ArrowException) as exc:
                        # Unreadable output file: skip its column stats, but say so
                        logger.warning(
                            "Skipping column statistics for %s: %s", file_path, type(exc).__name__
                        )

        metadata = {
            "processing_summary": processing_stats.get("processing_summary", {}),
            "input_config": processing_stats.get("input_config", {}),
            "output_files": processing_stats.get("output_files", []),
            "column_statistics": column_stats,
            "created_at": datetime.now().isoformat(),
            "metadata_version": "1.0",
        }

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        return str(metadata_path)
