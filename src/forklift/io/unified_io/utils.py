"""Utility functions for unified I/O operations."""

from __future__ import annotations

from ..s3_streaming import S3StreamingClient


def get_s3_client(**kwargs) -> S3StreamingClient:
    """Get S3 streaming client with default configuration.

    This function is used by tests for mocking purposes.

    Args:
        **kwargs: Additional configuration for S3StreamingClient

    Returns:
        Configured S3StreamingClient instance
    """
    from ..s3_streaming import get_s3_client as _get_s3_client
    return _get_s3_client(**kwargs)
