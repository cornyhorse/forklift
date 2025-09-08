"""S3 streaming writer for multipart uploads."""

from __future__ import annotations
import io

from .path import S3Path


class S3StreamingWriter:
    """Streaming writer for S3 using multipart upload."""

    def __init__(self, s3_client, s3_path: S3Path, encoding: str = 'utf-8',
                 part_size: int = 100 * 1024 * 1024, mode: str = 'w'):  # 100MB default
        """Initialize S3 streaming writer.

        Args:
            s3_client: boto3 S3 client
            s3_path: S3 path to write to
            encoding: Text encoding (ignored for binary mode)
            part_size: Size of each multipart upload part (minimum 5MB for S3)
            mode: Write mode - 'w' for text, 'wb' for binary
        """
        self._s3_client = s3_client
        self._s3_path = s3_path
        self._encoding = encoding
        self._part_size = max(part_size, 5 * 1024 * 1024)  # Minimum 5MB
        self._mode = mode
        self._is_binary = 'b' in mode

        # Initialize multipart upload
        self._upload_id = self._s3_client.create_multipart_upload(
            Bucket=s3_path.bucket,
            Key=s3_path.key
        )['UploadId']

        self._parts = []
        self._part_number = 1
        self._buffer = io.BytesIO()
        self._closed = False
        self._position = 0  # Track current position for tell()

    @property
    def closed(self):
        """Return whether the file is closed."""
        return self._closed

    @property
    def mode(self):
        """Return the file mode."""
        return self._mode

    def tell(self):
        """Return current position in the stream."""
        return self._position

    def flush(self):
        """Flush write buffers (no-op for S3 streaming)."""
        pass

    def seekable(self):
        """Return whether object supports random access (always False for S3 streaming)."""
        return False

    def writable(self):
        """Return whether object was opened for writing."""
        return True

    def readable(self):
        """Return whether object was opened for reading."""
        return False

    def write(self, data) -> int:
        """Write data to S3 stream.

        Args:
            data: Text data (str) for text mode, binary data (bytes) for binary mode

        Returns:
            Number of characters/bytes written
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        # Handle both text and binary data
        if isinstance(data, str):
            if self._is_binary:
                raise ValueError("Cannot write string data in binary mode")
            data_bytes = data.encode(self._encoding)
            return_count = len(data)
        elif isinstance(data, bytes):
            data_bytes = data
            return_count = len(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}. Expected str or bytes.")

        # Write to buffer
        bytes_written = self._buffer.write(data_bytes)
        self._position += return_count

        # Upload part if buffer is large enough
        if self._buffer.tell() >= self._part_size:
            self._upload_part()

        return return_count

    def _upload_part(self):
        """Upload current buffer as a part."""
        if self._buffer.tell() == 0:
            return

        # Get buffer contents
        self._buffer.seek(0)
        part_data = self._buffer.read()

        # Upload part
        response = self._s3_client.upload_part(
            Bucket=self._s3_path.bucket,
            Key=self._s3_path.key,
            PartNumber=self._part_number,
            UploadId=self._upload_id,
            Body=part_data
        )

        # Track part
        self._parts.append({
            'ETag': response['ETag'],
            'PartNumber': self._part_number
        })

        self._part_number += 1
        self._buffer = io.BytesIO()  # Reset buffer

    def close(self):
        """Close the stream and complete multipart upload."""
        if self._closed:
            return

        try:
            # Upload any remaining data
            if self._buffer.tell() > 0:
                self._upload_part()

            # Handle different upload scenarios
            if not self._parts:
                # No parts uploaded - use simple put_object instead
                # This happens with small files that don't reach the part size threshold
                self._abort_upload()  # Clean up the multipart upload

                # Get all data and upload as single object
                self._buffer.seek(0)
                data = self._buffer.read()
                if data:  # Only upload if there's actually data
                    self._s3_client.put_object(
                        Bucket=self._s3_path.bucket,
                        Key=self._s3_path.key,
                        Body=data
                    )
            else:
                # Complete multipart upload with valid parts
                self._s3_client.complete_multipart_upload(
                    Bucket=self._s3_path.bucket,
                    Key=self._s3_path.key,
                    UploadId=self._upload_id,
                    MultipartUpload={'Parts': self._parts}
                )
        except Exception:
            # Abort upload on failure
            self._abort_upload()
            raise
        finally:
            self._closed = True

    def _abort_upload(self):
        """Abort the multipart upload (cleanup method)."""
        try:
            self._s3_client.abort_multipart_upload(
                Bucket=self._s3_path.bucket,
                Key=self._s3_path.key,
                UploadId=self._upload_id
            )
        except Exception:
            pass  # Best effort cleanup

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
