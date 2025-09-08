"""S3 path utilities for parsing and working with S3 paths."""

from __future__ import annotations
from urllib.parse import urlparse


class S3Path:
    """Utility class for parsing and working with S3 paths."""

    def __init__(self, s3_uri: str):
        """Initialize S3Path from S3 URI.

        Args:
            s3_uri: S3 URI in format s3://bucket/key

        Raises:
            ValueError: If URI is not a valid S3 path
        """
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")

        parsed = urlparse(s3_uri)
        self.bucket = parsed.netloc
        self.key = parsed.path.lstrip('/')
        self.uri = s3_uri

        if not self.bucket:
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Bucket name is required")

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"S3Path('{self.uri}')"

    @property
    def parent(self) -> 'S3Path':
        """Get parent S3 path (directory)."""
        if '/' not in self.key:
            return S3Path(f's3://{self.bucket}/')
        parent_key = '/'.join(self.key.split('/')[:-1])
        return S3Path(f's3://{self.bucket}/{parent_key}')

    @property
    def name(self) -> str:
        """Get file name (last component of key)."""
        if '/' not in self.key:
            return self.key
        return self.key.split('/')[-1]

    def join(self, *parts: str) -> 'S3Path':
        """Join additional path components."""
        key_parts = [self.key] + list(parts)
        new_key = '/'.join(part.strip('/') for part in key_parts if part.strip('/'))
        return S3Path(f's3://{self.bucket}/{new_key}')
