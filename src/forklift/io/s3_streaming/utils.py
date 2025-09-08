"""Utility functions for S3 streaming operations."""

from __future__ import annotations
from pathlib import Path
from typing import Union

from .client import S3StreamingClient


def is_s3_path(path: Union[str, Path]) -> bool:
    """Check if a path is an S3 URI.

    Args:
        path: Path to check

    Returns:
        True if path is S3 URI, False otherwise
    """
    if isinstance(path, Path):
        path = str(path)
    return isinstance(path, str) and path.startswith('s3://')


def get_s3_client(**kwargs) -> S3StreamingClient:
    """Get S3 streaming client with default configuration.

    Args:
        **kwargs: Additional configuration for S3StreamingClient

    Returns:
        Configured S3StreamingClient instance
    """
    return S3StreamingClient(**kwargs)
