"""Configuration classes for output operations."""

from __future__ import annotations
from dataclasses import dataclass


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
