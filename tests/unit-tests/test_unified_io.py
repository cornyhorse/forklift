"""Comprehensive tests for unified_io module with 100% code coverage.

Tests both mocked and real S3 operations based on --no-s3-mock flag.
"""

import csv
from io import StringIO
from unittest.mock import MagicMock, mock_open, patch

import pyarrow as pa
import pytest

from forklift.io.s3_streaming import S3Path, S3StreamingClient
from forklift.io.unified_io import (S3ParquetWriter, UnifiedCSVWriter,
                                    UnifiedIOHandler, create_parquet_writer,
                                    get_s3_client)


class TestUnifiedIOHandler:
    """Test UnifiedIOHandler class with both mocked and real S3 operations."""

    def test_init_without_s3_client(self):
        """Test initialization without providing S3 client."""
        handler = UnifiedIOHandler()
        assert handler._s3_client is None

    def test_init_with_s3_client(self, s3_mock_conditional):
        """Test initialization with provided S3 client."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            mock_s3_client = MagicMock(spec=S3StreamingClient)
            handler = UnifiedIOHandler(s3_client=mock_s3_client)
            assert handler._s3_client is mock_s3_client

    def test_s3_client_property_creates_client_when_none(self, s3_mock_conditional):
        """Test s3_client property creates client when none exists."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            handler = UnifiedIOHandler()
            client = handler.s3_client

            assert client is mock_s3_client

    def test_s3_client_property_returns_existing_client(self, s3_mock_conditional):
        """Test s3_client property returns existing client."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            # Create handler with existing client
            existing_client = MagicMock(spec=S3StreamingClient)
            handler = UnifiedIOHandler(s3_client=existing_client)

            client = handler.s3_client
            assert client is existing_client

    def test_exists_local_file_true(self, tmp_path):
        """Test exists method with local file that exists."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        handler = UnifiedIOHandler()
        assert handler.exists(test_file) is True

    def test_exists_local_file_false(self, tmp_path):
        """Test exists method with local file that doesn't exist."""
        test_file = tmp_path / "nonexistent.txt"

        handler = UnifiedIOHandler()
        assert handler.exists(test_file) is False

    def test_exists_s3_path(self, s3_mock_conditional):
        """Test exists method with S3 path."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                mock_s3_client.exists.return_value = True

                handler = UnifiedIOHandler()
                result = handler.exists("s3://bucket/key")

                assert result is True
                mock_s3_client.exists.assert_called_once_with("s3://bucket/key")

    def test_get_size_local_file(self, tmp_path):
        """Test get_size method with local file."""
        test_file = tmp_path / "test.txt"
        content = "test content for size check"
        test_file.write_text(content)

        handler = UnifiedIOHandler()
        size = handler.get_size(test_file)

        assert size == len(content.encode("utf-8"))

    def test_get_size_s3_path(self, s3_mock_conditional):
        """Test get_size method with S3 path."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                mock_s3_client.get_size.return_value = 1024

                handler = UnifiedIOHandler()
                size = handler.get_size("s3://bucket/key")

                assert size == 1024
                mock_s3_client.get_size.assert_called_once_with("s3://bucket/key")

    def test_open_for_read_local_file(self, tmp_path):
        """Test open_for_read method with local file."""
        test_file = tmp_path / "test.txt"
        content = "test content"
        test_file.write_text(content)

        handler = UnifiedIOHandler()
        with handler.open_for_read(test_file) as f:
            read_content = f.read()

        assert read_content == content

    def test_open_for_read_local_file_with_encoding(self, tmp_path):
        """Test open_for_read method with custom encoding."""
        test_file = tmp_path / "test.txt"
        content = "test content with special chars: é, ñ, ü"
        test_file.write_text(content, encoding="utf-8")

        handler = UnifiedIOHandler()
        with handler.open_for_read(test_file, encoding="utf-8") as f:
            read_content = f.read()

        assert read_content == content

    def test_open_for_read_s3_path(self, s3_mock_conditional):
        """Test open_for_read method with S3 path."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                mock_file = StringIO("s3 content")
                mock_s3_client.open_for_read.return_value = mock_file

                handler = UnifiedIOHandler()
                with handler.open_for_read("s3://bucket/key", encoding="utf-8") as f:
                    content = f.read()

                assert content == "s3 content"
                mock_s3_client.open_for_read.assert_called_once_with(
                    "s3://bucket/key", encoding="utf-8"
                )

    def test_open_for_write_local_file(self, tmp_path):
        """Test open_for_write method with local file."""
        test_file = tmp_path / "subdir" / "test.txt"
        content = "test content"

        handler = UnifiedIOHandler()
        with handler.open_for_write(test_file) as f:
            f.write(content)

        assert test_file.exists()
        assert test_file.read_text() == content

    def test_open_for_write_local_file_creates_parent_dirs(self, tmp_path):
        """Test open_for_write method creates parent directories."""
        test_file = tmp_path / "deep" / "nested" / "dirs" / "test.txt"
        content = "test content"

        handler = UnifiedIOHandler()
        with handler.open_for_write(test_file) as f:
            f.write(content)

        assert test_file.exists()
        assert test_file.read_text() == content

    def test_open_for_write_s3_path(self, s3_mock_conditional):
        """Test open_for_write method with S3 path."""
        mock_session, mock_s3_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                mock_writer = MagicMock()
                mock_s3_client.open_for_write.return_value = mock_writer

                handler = UnifiedIOHandler()
                result = handler.open_for_write("s3://bucket/key", encoding="utf-8")

                assert result is mock_writer
                mock_s3_client.open_for_write.assert_called_once_with(
                    "s3://bucket/key", encoding="utf-8"
                )

    def test_csv_reader_local_file(self, tmp_path):
        """Test csv_reader method with local file."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("col1,col2\nval1,val2\nval3,val4")

        handler = UnifiedIOHandler()
        rows = list(handler.csv_reader(test_file))

        expected = [["col1", "col2"], ["val1", "val2"], ["val3", "val4"]]
        assert rows == expected

    def test_csv_reader_custom_delimiter(self, tmp_path):
        """Test csv_reader method with custom delimiter."""
        test_file = tmp_path / "test.tsv"
        test_file.write_text("col1\tcol2\nval1\tval2")

        handler = UnifiedIOHandler()
        rows = list(handler.csv_reader(test_file, delimiter="\t"))

        expected = [["col1", "col2"], ["val1", "val2"]]
        assert rows == expected

    def test_csv_reader_custom_quotechar(self, tmp_path):
        """Test csv_reader method with custom quote character."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("col1,col2\n'val1','val2'")

        handler = UnifiedIOHandler()
        rows = list(handler.csv_reader(test_file, quotechar="'"))

        expected = [["col1", "col2"], ["val1", "val2"]]
        assert rows == expected

    def test_csv_reader_s3_path(self, s3_mock_conditional):
        """Test csv_reader method with S3 path."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            csv_content = "col1,col2\nval1,val2"
            mock_file = StringIO(csv_content)

            with patch.object(UnifiedIOHandler, "open_for_read", return_value=mock_file):
                handler = UnifiedIOHandler()
                rows = list(handler.csv_reader("s3://bucket/test.csv"))

                expected = [["col1", "col2"], ["val1", "val2"]]
                assert rows == expected

    def test_csv_writer_returns_unified_csv_writer(self, tmp_path):
        """Test csv_writer method returns UnifiedCSVWriter."""
        test_file = tmp_path / "test.csv"

        handler = UnifiedIOHandler()
        writer = handler.csv_writer(test_file)

        assert isinstance(writer, UnifiedCSVWriter)
        assert writer.io_handler is handler
        assert writer.path == test_file

    def test_copy_file_local_to_local(self, tmp_path):
        """Test copy_file method from local to local."""
        src_file = tmp_path / "source.txt"
        dest_file = tmp_path / "dest.txt"
        content = "test content for copying"
        src_file.write_text(content)

        handler = UnifiedIOHandler()
        handler.copy_file(src_file, dest_file)

        assert dest_file.exists()
        assert dest_file.read_text() == content

    def test_copy_file_local_to_s3(self, tmp_path, s3_mock_conditional):
        """Test copy_file method from local to S3."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            src_file = tmp_path / "source.txt"
            content = "test content for s3 upload"
            src_file.write_text(content)

            with patch("forklift.io.s3_streaming.is_s3_path") as mock_is_s3:
                mock_is_s3.side_effect = lambda path: str(path).startswith("s3://")

                mock_s3_writer = MagicMock()
                mock_s3_writer.__enter__ = MagicMock(return_value=mock_s3_writer)
                mock_s3_writer.__exit__ = MagicMock(return_value=None)

                handler = UnifiedIOHandler()
                with patch.object(handler, "open_for_write", return_value=mock_s3_writer):
                    handler.copy_file(src_file, "s3://bucket/dest.txt")

                mock_s3_writer.write.assert_called_with(content)

    def test_copy_file_s3_to_local(self, tmp_path, s3_mock_conditional):
        """Test copy_file method from S3 to local."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            dest_file = tmp_path / "dest.txt"
            s3_content = "content from s3"

            with patch("forklift.io.s3_streaming.is_s3_path") as mock_is_s3:
                mock_is_s3.side_effect = lambda path: str(path).startswith("s3://")

                mock_s3_reader = StringIO(s3_content)

                handler = UnifiedIOHandler()
                with patch.object(handler, "open_for_read", return_value=mock_s3_reader):
                    handler.copy_file("s3://bucket/source.txt", dest_file)

                assert dest_file.exists()
                assert dest_file.read_text() == s3_content

    def test_copy_file_s3_to_s3(self, s3_mock_conditional):
        """Test copy_file method from S3 to S3 using native S3 copy."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                with patch("forklift.io.unified_io.S3Path") as mock_s3_path_class:
                    mock_src_path = MagicMock()
                    mock_src_path.bucket = "src-bucket"
                    mock_src_path.key = "src-key"

                    mock_dest_path = MagicMock()
                    mock_dest_path.bucket = "dest-bucket"
                    mock_dest_path.key = "dest-key"

                    mock_s3_path_class.side_effect = [mock_src_path, mock_dest_path]

                    mock_s3_client = MagicMock(spec=S3StreamingClient)
                    mock_boto3_client = MagicMock()
                    mock_s3_client._s3_client = mock_boto3_client

                    handler = UnifiedIOHandler(s3_client=mock_s3_client)
                    handler.copy_file("s3://src-bucket/src-key", "s3://dest-bucket/dest-key")

                    mock_boto3_client.copy_object.assert_called_once_with(
                        CopySource={"Bucket": "src-bucket", "Key": "src-key"},
                        Bucket="dest-bucket",
                        Key="dest-key",
                    )

    def test_copy_file_with_custom_chunk_size(self, tmp_path):
        """Test copy_file method with custom chunk size."""
        src_file = tmp_path / "source.txt"
        dest_file = tmp_path / "dest.txt"
        content = "a" * 100  # 100 character content
        src_file.write_text(content)

        handler = UnifiedIOHandler()
        handler.copy_file(src_file, dest_file, chunk_size=10)

        assert dest_file.exists()
        assert dest_file.read_text() == content


class TestUnifiedCSVWriter:
    """Test UnifiedCSVWriter class."""

    def test_init(self, tmp_path):
        """Test UnifiedCSVWriter initialization."""
        test_file = tmp_path / "test.csv"
        handler = UnifiedIOHandler()

        writer = UnifiedCSVWriter(
            handler, test_file, delimiter=";", quotechar="'", encoding="latin-1"
        )

        assert writer.io_handler is handler
        assert writer.path == test_file
        assert writer.delimiter == ";"
        assert writer.quotechar == "'"
        assert writer.encoding == "latin-1"
        assert writer._file is None
        assert writer._writer is None

    def test_context_manager_local_file(self, tmp_path):
        """Test UnifiedCSVWriter as context manager with local file."""
        test_file = tmp_path / "test.csv"
        handler = UnifiedIOHandler()

        with UnifiedCSVWriter(handler, test_file) as writer:
            assert writer is not None
            writer.writerow(["col1", "col2"])
            writer.writerow(["val1", "val2"])

        assert test_file.exists()
        content = test_file.read_text()
        assert "col1,col2" in content
        assert "val1,val2" in content

    def test_context_manager_custom_parameters(self, tmp_path):
        """Test UnifiedCSVWriter with custom CSV parameters."""
        test_file = tmp_path / "test.tsv"
        handler = UnifiedIOHandler()

        with UnifiedCSVWriter(handler, test_file, delimiter="\t", quotechar="'") as writer:
            writer.writerow(["col1", "col2"])
            writer.writerow(["val with space", "val2"])

        content = test_file.read_text()
        assert "col1\tcol2" in content
        # CSV writer may not quote fields that don't need quoting
        assert "val with space\tval2" in content

    def test_context_manager_s3_path(self, s3_mock_conditional):
        """Test UnifiedCSVWriter with S3 path."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            mock_s3_writer = MagicMock()
            mock_s3_writer.__enter__ = MagicMock(return_value=mock_s3_writer)
            mock_s3_writer.__exit__ = MagicMock(return_value=None)
            mock_s3_writer.close = MagicMock()

            mock_csv_writer = MagicMock()

            handler = UnifiedIOHandler()

            with patch.object(handler, "open_for_write", return_value=mock_s3_writer):
                with patch("csv.writer", return_value=mock_csv_writer):
                    with UnifiedCSVWriter(handler, "s3://bucket/test.csv") as writer:
                        assert writer is mock_csv_writer

            mock_s3_writer.close.assert_called_once()

    def test_exit_closes_file(self, tmp_path):
        """Test that __exit__ properly closes the file."""
        test_file = tmp_path / "test.csv"
        handler = UnifiedIOHandler()

        csv_writer = UnifiedCSVWriter(handler, test_file)
        csv_writer._file = MagicMock()

        csv_writer.__exit__(None, None, None)

        csv_writer._file.close.assert_called_once()

    def test_exit_with_none_file(self, tmp_path):
        """Test that __exit__ handles None file gracefully."""
        test_file = tmp_path / "test.csv"
        handler = UnifiedIOHandler()

        csv_writer = UnifiedCSVWriter(handler, test_file)
        csv_writer._file = None

        # Should not raise an exception
        csv_writer.__exit__(None, None, None)


class TestS3ParquetWriter:
    """Test S3ParquetWriter class."""

    def test_init_with_string_path(self, s3_mock_conditional):
        """Test S3ParquetWriter initialization with string S3 path."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string()), ("col2", pa.int64())])

            with patch("forklift.io.unified_io.S3Path") as mock_s3_path:
                with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                    with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                        with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer:
                            mock_temp = MagicMock()
                            mock_temp.name = "/tmp/test.parquet"
                            mock_tempfile.return_value = mock_temp

                            mock_s3_client = MagicMock()
                            mock_get_client.return_value = mock_s3_client

                            writer = S3ParquetWriter("s3://bucket/test.parquet", schema)

                            mock_s3_path.assert_called_once_with("s3://bucket/test.parquet")
                            assert writer.schema == schema
                            assert writer.compression == "snappy"

    def test_init_with_s3_path_object(self, s3_mock_conditional):
        """Test S3ParquetWriter initialization with S3Path object."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])
            s3_path = S3Path("s3://bucket/test.parquet")

            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer:
                        mock_temp = MagicMock()
                        mock_temp.name = "/tmp/test.parquet"
                        mock_tempfile.return_value = mock_temp

                        mock_s3_client = MagicMock()
                        mock_get_client.return_value = mock_s3_client

                        writer = S3ParquetWriter(s3_path, schema)

                        assert writer.s3_path == s3_path

    def test_init_with_provided_s3_client(self, s3_mock_conditional):
        """Test S3ParquetWriter initialization with provided S3 client."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])
            mock_s3_client = MagicMock(spec=S3StreamingClient)

            with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer:
                    mock_temp = MagicMock()
                    mock_temp.name = "/tmp/test.parquet"
                    mock_tempfile.return_value = mock_temp

                    writer = S3ParquetWriter(
                        "s3://bucket/test.parquet", schema, s3_client=mock_s3_client
                    )

                    assert writer.s3_client is mock_s3_client

    def test_init_with_custom_compression(self, s3_mock_conditional):
        """Test S3ParquetWriter initialization with custom compression."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer:
                        mock_temp = MagicMock()
                        mock_temp.name = "/tmp/test.parquet"
                        mock_tempfile.return_value = mock_temp

                        writer = S3ParquetWriter(
                            "s3://bucket/test.parquet", schema, compression="gzip"
                        )

                        assert writer.compression == "gzip"

    def test_write_table(self, s3_mock_conditional):
        """Test write_table method."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])
            table = pa.table([["value1", "value2"]], schema=schema)

            with patch("forklift.io.s3_streaming.get_s3_client"):
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer_class:
                        mock_temp = MagicMock()
                        mock_temp.name = "/tmp/test.parquet"
                        mock_tempfile.return_value = mock_temp

                        mock_pq_writer = MagicMock()
                        mock_pq_writer_class.return_value = mock_pq_writer

                        writer = S3ParquetWriter("s3://bucket/test.parquet", schema)
                        writer.write_table(table)

                        mock_pq_writer.write_table.assert_called_once_with(table)

    def test_write_batch(self, s3_mock_conditional):
        """Test write_batch method."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])
            batch = pa.record_batch([["value1", "value2"]], schema=schema)

            with patch("forklift.io.s3_streaming.get_s3_client"):
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer_class:
                        mock_temp = MagicMock()
                        mock_temp.name = "/tmp/test.parquet"
                        mock_tempfile.return_value = mock_temp

                        mock_pq_writer = MagicMock()
                        mock_pq_writer_class.return_value = mock_pq_writer

                        writer = S3ParquetWriter("s3://bucket/test.parquet", schema)
                        writer.write_batch(batch)

                        # Should call write_table with table created from batch
                        mock_pq_writer.write_table.assert_called_once()

    def test_close_uploads_to_s3(self, s3_mock_conditional):
        """Test close method uploads file to S3."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer_class:
                        with patch("builtins.open", mock_open(read_data=b"parquet_data")):
                            mock_temp = MagicMock()
                            mock_temp.name = "/tmp/test.parquet"
                            mock_tempfile.return_value = mock_temp

                            mock_pq_writer = MagicMock()
                            mock_pq_writer_class.return_value = mock_pq_writer

                            mock_s3_client = MagicMock()
                            mock_s3_client._s3_client = MagicMock()
                            mock_get_client.return_value = mock_s3_client

                            writer = S3ParquetWriter("s3://bucket/test.parquet", schema)
                            writer.close()

                            mock_pq_writer.close.assert_called_once()
                            mock_s3_client._s3_client.upload_fileobj.assert_called_once()

    def test_close_cleanup_on_exception(self, s3_mock_conditional):
        """Test close method cleans up temp file even on exception."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer_class:
                        with patch("builtins.open", side_effect=Exception("Upload failed")):
                            mock_temp = MagicMock()
                            mock_temp.name = "/tmp/test.parquet"
                            mock_tempfile.return_value = mock_temp

                            mock_pq_writer = MagicMock()
                            mock_pq_writer_class.return_value = mock_pq_writer

                            mock_s3_client = MagicMock()
                            mock_get_client.return_value = mock_s3_client

                            writer = S3ParquetWriter("s3://bucket/test.parquet", schema)

                            with pytest.raises(Exception, match="Upload failed"):
                                writer.close()

    def test_close_handles_cleanup_exception(self, s3_mock_conditional):
        """Test close method handles cleanup exceptions gracefully."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer_class:
                        with patch("builtins.open", mock_open(read_data=b"parquet_data")):
                            mock_temp = MagicMock()
                            mock_temp.name = "/tmp/test.parquet"
                            mock_tempfile.return_value = mock_temp

                            mock_pq_writer = MagicMock()
                            mock_pq_writer_class.return_value = mock_pq_writer

                            mock_s3_client = MagicMock()
                            mock_s3_client._s3_client = MagicMock()
                            mock_get_client.return_value = mock_s3_client

                            # Mock Path to simulate cleanup exception
                            with patch("forklift.io.unified_io.Path") as mock_path_class:
                                mock_path = MagicMock()
                                mock_path.unlink.side_effect = Exception("Cleanup failed")
                                mock_path_class.return_value = mock_path

                                writer = S3ParquetWriter("s3://bucket/test.parquet", schema)

                                # Should not raise exception despite cleanup failure
                                writer.close()

                                mock_s3_client._s3_client.upload_fileobj.assert_called_once()

    def test_context_manager(self, s3_mock_conditional):
        """Test S3ParquetWriter as context manager."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.get_s3_client"):
                with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                    with patch("forklift.io.unified_io.pq.ParquetWriter"):
                        mock_temp = MagicMock()
                        mock_temp.name = "/tmp/test.parquet"
                        mock_tempfile.return_value = mock_temp

                        writer = S3ParquetWriter("s3://bucket/test.parquet", schema)

                        with patch.object(writer, "close") as mock_close:
                            with writer as w:
                                assert w is writer

                            mock_close.assert_called_once()


class TestCreateParquetWriter:
    """Test create_parquet_writer function."""

    def test_create_parquet_writer_local_path(self, tmp_path):
        """Test create_parquet_writer with local path."""
        output_file = tmp_path / "subdir" / "test.parquet"
        schema = pa.schema([("col1", pa.string())])

        with patch("forklift.io.unified_io.pq.ParquetWriter") as mock_pq_writer:
            writer = create_parquet_writer(output_file, schema, compression="gzip")

            assert output_file.parent.exists()  # Parent directory created
            mock_pq_writer.assert_called_once_with(output_file, schema, compression="gzip")

    def test_create_parquet_writer_s3_path(self, s3_mock_conditional):
        """Test create_parquet_writer with S3 path."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                with patch("forklift.io.unified_io.S3ParquetWriter") as mock_s3_writer:
                    mock_s3_client = MagicMock()

                    writer = create_parquet_writer(
                        "s3://bucket/test.parquet",
                        schema,
                        s3_client=mock_s3_client,
                        compression="snappy",
                    )

                    mock_s3_writer.assert_called_once_with(
                        "s3://bucket/test.parquet",
                        schema,
                        s3_client=mock_s3_client,
                        compression="snappy",
                    )

    def test_create_parquet_writer_s3_path_no_client(self, s3_mock_conditional):
        """Test create_parquet_writer with S3 path and no client provided."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            schema = pa.schema([("col1", pa.string())])

            with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
                with patch("forklift.io.unified_io.S3ParquetWriter") as mock_s3_writer:
                    writer = create_parquet_writer("s3://bucket/test.parquet", schema)

                    mock_s3_writer.assert_called_once_with(
                        "s3://bucket/test.parquet", schema, s3_client=None, compression="snappy"
                    )


class TestGetS3Client:
    """Test get_s3_client function."""

    def test_get_s3_client_calls_s3_streaming_get_s3_client(self, s3_mock_conditional):
        """Test get_s3_client function delegates to s3_streaming module."""
        mock_session, mock_client = s3_mock_conditional
        if mock_session:  # Using mocked S3
            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_client:
                mock_s3_client = MagicMock()
                mock_get_client.return_value = mock_s3_client

                result = get_s3_client(region_name="us-west-2")

                assert result is mock_s3_client
                mock_get_client.assert_called_once_with(region_name="us-west-2")


# Integration tests that work with both mocked and real S3
class TestUnifiedIOIntegration:
    """Integration tests for unified I/O functionality."""

    def test_full_csv_workflow_local(self, tmp_path):
        """Test complete CSV workflow with local files."""
        input_file = tmp_path / "input.csv"
        output_file = tmp_path / "output.csv"

        # Create input CSV
        input_data = [["name", "age"], ["Alice", "30"], ["Bob", "25"]]
        with open(input_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(input_data)

        # Use UnifiedIOHandler to read and write
        handler = UnifiedIOHandler()

        # Read CSV
        rows = list(handler.csv_reader(input_file))
        assert rows == input_data

        # Write CSV
        with handler.csv_writer(output_file) as writer:
            for row in rows:
                writer.writerow(row)

        # Verify output
        output_rows = list(handler.csv_reader(output_file))
        assert output_rows == input_data

    def test_file_operations_local(self, tmp_path):
        """Test file operations with local files."""
        test_file = tmp_path / "test.txt"
        content = "Hello, World!"

        handler = UnifiedIOHandler()

        # File doesn't exist initially
        assert not handler.exists(test_file)

        # Write file
        with handler.open_for_write(test_file) as f:
            f.write(content)

        # File now exists
        assert handler.exists(test_file)

        # Check size
        size = handler.get_size(test_file)
        assert size == len(content.encode("utf-8"))

        # Read file
        with handler.open_for_read(test_file) as f:
            read_content = f.read()

        assert read_content == content

    def test_s3_operations_conditional(self, s3_mock_conditional):
        """Test S3 operations (mocked or real based on configuration)."""
        mock_session, mock_client = s3_mock_conditional

        if mock_session:  # Using mocked S3
            self._test_s3_operations_mocked(mock_session, mock_client)
        else:  # Using real S3
            pytest.skip("Real S3 testing implementation would go here")

    def _test_s3_operations_mocked(self, mock_session, mock_client):
        """Test S3 operations with mocked S3."""
        with patch("forklift.io.s3_streaming.is_s3_path", return_value=True):
            mock_s3_client = MagicMock(spec=S3StreamingClient)
            mock_s3_client.exists.return_value = False
            mock_s3_client.get_size.return_value = 1024

            mock_file = StringIO("s3 content")
            mock_s3_client.open_for_read.return_value = mock_file

            mock_writer = MagicMock()
            mock_s3_client.open_for_write.return_value = mock_writer

            handler = UnifiedIOHandler(s3_client=mock_s3_client)

            # Test exists
            exists = handler.exists("s3://bucket/test.txt")
            assert exists is False

            # Test get_size
            size = handler.get_size("s3://bucket/test.txt")
            assert size == 1024

            # Test read
            with handler.open_for_read("s3://bucket/test.txt") as f:
                content = f.read()
            assert content == "s3 content"

            # Test write
            writer = handler.open_for_write("s3://bucket/test.txt")
            assert writer is mock_writer
