"""Metadata collection module for Forklift.

This module provides functionality to collect metadata about both input data
(for schema generation) and output data (for processing pipeline analysis).
"""

from .output_metadata_collector import OutputMetadataCollector

__all__ = ['OutputMetadataCollector']
