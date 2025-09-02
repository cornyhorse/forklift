"""Output handlers for writing processed data.

This module provides a clean interface to various output handler classes for writing
processed data to different formats including Parquet files, manifest generation for
data catalogs, and metadata file creation for processing statistics.

The module is organized with separate files for each responsibility:
- config.py: Configuration classes
- parquet.py: Parquet file output handling
- manifest.py: Data catalog manifest generation
- metadata.py: Processing statistics and metadata
"""

# Import configuration
from .config import OutputConfig

# Import core output handlers
from .parquet import ParquetOutputHandler
from .manifest import ManifestGenerator
from .metadata import MetadataGenerator

# Define public API
__all__ = [
    # Configuration
    "OutputConfig",

    # Core handlers
    "ParquetOutputHandler",
    "ManifestGenerator",
    "MetadataGenerator",
]
