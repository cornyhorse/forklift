"""UnifiedIOHandler / S3ParquetWriter: client property and cleanup when streams fail.

S3 is a MagicMock S3StreamingClient; nothing touches the network.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from forklift.io import unified_io
from forklift.io.s3_streaming import S3StreamingClient
from forklift.io.unified_io import S3ParquetWriter, UnifiedIOHandler


class _FailingRaw(io.RawIOBase):
    """A response body that delivers ``data`` once, then fails like a dropped connection."""

    def __init__(self, data: bytes):
        self._data = data

    def readable(self):
        return True

    def readinto(self, buffer):
        if not self._data:
            raise ConnectionResetError("connection reset by peer")
        size = len(self._data)
        buffer[:size], self._data = self._data, b""
        return size


class _UncloseableStream(io.BytesIO):
    """A body whose first ``close()`` fails (later ones, e.g. on garbage collection, work)."""

    close_attempts = 0

    def close(self):
        self.close_attempts += 1
        if self.close_attempts == 1:
            raise OSError("close failed")
        super().close()


def _handler_reading(stream) -> UnifiedIOHandler:
    client = MagicMock(spec=S3StreamingClient)
    client.open_for_read.return_value = stream
    return UnifiedIOHandler(s3_client=client)


class TestS3ClientProperty:
    def test_assigned_client_is_used(self):
        handler = UnifiedIOHandler()
        client = MagicMock(spec=S3StreamingClient)
        handler.s3_client = client
        assert handler.s3_client is client

    def test_deleting_the_client_makes_the_next_access_create_a_new_one(self):
        handler = UnifiedIOHandler(s3_client=MagicMock(spec=S3StreamingClient))
        del handler.s3_client
        with patch("forklift.io.s3_streaming.get_s3_client") as get_s3_client:
            assert handler.s3_client is get_s3_client.return_value
        get_s3_client.assert_called_once_with()


class TestSeekableS3Reads:
    def test_failed_download_closes_the_temporary_file_and_the_stream(self, monkeypatch):
        spooled = []
        real_temporary_file = unified_io.tempfile.TemporaryFile

        def recording_temporary_file(*args, **kwargs):
            spooled.append(real_temporary_file(*args, **kwargs))
            return spooled[-1]

        monkeypatch.setattr(unified_io.tempfile, "TemporaryFile", recording_temporary_file)
        stream = io.BufferedReader(_FailingRaw(b"PAR1 partial"))

        with pytest.raises(ConnectionResetError, match="connection reset"):
            _handler_reading(stream).open_for_read("s3://bkt/data.parquet", mode="rb")

        assert len(spooled) == 1 and spooled[0].closed
        assert stream.closed

    def test_error_closing_the_response_body_is_ignored(self):
        body = _UncloseableStream(b"complete body")
        handler = _handler_reading(body)
        with handler.open_for_read("s3://bkt/data.bin", mode="rb") as f:
            assert body.close_attempts == 1
            assert f.seekable()
            assert f.read() == b"complete body"


class TestCopyToLocal:
    def test_failed_copy_leaves_no_destination_or_temporary_file(self, tmp_path):
        handler = _handler_reading(io.BufferedReader(_FailingRaw(b"first chunk")))
        dest = tmp_path / "out" / "copy.csv"

        with pytest.raises(ConnectionResetError):
            handler.copy_file("s3://bkt/src.csv", dest, chunk_size=4)

        assert list(dest.parent.iterdir()) == []

    def test_copy_error_is_reported_even_if_the_temporary_file_cannot_be_removed(
        self, tmp_path, monkeypatch
    ):
        handler = _handler_reading(io.BufferedReader(_FailingRaw(b"first chunk")))
        removed = []

        def failing_unlink(path):
            removed.append(path)
            raise PermissionError("locked")

        monkeypatch.setattr(unified_io.os, "unlink", failing_unlink)

        with pytest.raises(ConnectionResetError):
            handler.copy_file("s3://bkt/src.csv", tmp_path / "copy.csv")

        assert len(removed) == 1
        assert not (tmp_path / "copy.csv").exists()


class TestS3ParquetWriterAbort:
    def test_abort_removes_the_temp_file_even_if_closing_the_writer_fails(self):
        client = MagicMock(spec=S3StreamingClient)
        client._s3_client = MagicMock()
        schema = pa.schema([("a", pa.int64())])
        with patch.object(unified_io.pq, "ParquetWriter") as parquet_writer:
            parquet_writer.return_value.close.side_effect = OSError("disk full")
            writer = S3ParquetWriter("s3://bkt/out.parquet", schema, s3_client=client)
            temp_path = writer._temp_path
            assert temp_path.exists()

            writer.abort()  # does not raise

        assert not temp_path.exists()
        client._s3_client.upload_fileobj.assert_not_called()
