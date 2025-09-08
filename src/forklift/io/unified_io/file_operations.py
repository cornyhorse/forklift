"""File operations for unified I/O handling."""

from __future__ import annotations
from typing import Union
from pathlib import Path

from ..s3_streaming import S3StreamingClient, is_s3_path


class FileOperations:
    """Handles basic file operations for both local and S3 paths."""

    def __init__(self, s3_client: S3StreamingClient):
        """Initialize file operations handler.

        Args:
            s3_client: S3 streaming client for S3 operations
        """
        self.s3_client = s3_client

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists (local file or S3 object).

        Args:
            path: Local file path or S3 URI

        Returns:
            True if path exists, False otherwise
        """
        if is_s3_path(path):
            return self.s3_client.exists(path)
        else:
            return Path(path).exists()

    def get_size(self, path: Union[str, Path]) -> int:
        """Get size of file/object in bytes.

        Args:
            path: Local file path or S3 URI

        Returns:
            Size in bytes
        """
        if is_s3_path(path):
            return self.s3_client.get_size(path)
        else:
            return Path(path).stat().st_size

    def copy_file(self, src_path: Union[str, Path],
                  dest_path: Union[str, Path],
                  chunk_size: int = 8192) -> None:
        """Copy file between S3 locations using S3 native copy.

        This method only handles S3-to-S3 copies. Other combinations
        are handled by the UnifiedIOHandler directly.

        Args:
            src_path: Source S3 path
            dest_path: Destination S3 path
            chunk_size: Unused for S3-to-S3 copies
        """
        from ..s3_streaming import S3Path

        # Only handle S3 to S3 copies
        if not (is_s3_path(src_path) and is_s3_path(dest_path)):
            raise ValueError("FileOperations.copy_file only handles S3-to-S3 copies")

        # S3 to S3 - use S3 copy
        src_s3_path = S3Path(str(src_path))
        dest_s3_path = S3Path(str(dest_path))

        copy_source = {
            'Bucket': src_s3_path.bucket,
            'Key': src_s3_path.key
        }
        self.s3_client._s3_client.copy_object(
            CopySource=copy_source,
            Bucket=dest_s3_path.bucket,
            Key=dest_s3_path.key
        )
