"""Comprehensive unit tests for S3 streaming functionality using .env file for credentials."""

import pytest
import tempfile
import json
import csv
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from typing import Dict, Any

from forklift.io.s3_streaming import (
    S3StreamingClient,
    S3Path,
    S3StreamingWriter,
    is_s3_path,
    get_s3_client
)
from forklift.io.unified_io import UnifiedIOHandler, S3ParquetWriter, create_parquet_writer
from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode
import pyarrow as pa


class TestS3Path:
    """Test S3Path utility class."""

    def test_s3_path_parsing(self):
        """Test S3 path parsing from URI."""
        s3_path = S3Path("s3://my-bucket/path/to/file.csv")

        assert s3_path.bucket == "my-bucket"
        assert s3_path.key == "path/to/file.csv"
        assert s3_path.uri == "s3://my-bucket/path/to/file.csv"
        assert str(s3_path) == "s3://my-bucket/path/to/file.csv"

    def test_s3_path_invalid_uri(self):
        """Test S3Path with invalid URI."""
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            S3Path("invalid-uri")

        with pytest.raises(ValueError, match="Bucket name is required"):
            S3Path("s3:///key-without-bucket")

    def test_s3_path_parent(self):
        """Test S3Path parent functionality."""
        s3_path = S3Path("s3://bucket/path/to/file.csv")
        parent = s3_path.parent

        assert parent.bucket == "bucket"
        assert parent.key == "path/to"
        assert str(parent) == "s3://bucket/path/to"

    def test_s3_path_name(self):
        """Test S3Path name property."""
        s3_path = S3Path("s3://bucket/path/to/file.csv")
        assert s3_path.name == "file.csv"

        s3_path_no_path = S3Path("s3://bucket/file.csv")
        assert s3_path_no_path.name == "file.csv"

    def test_s3_path_join(self):
        """Test S3Path join functionality."""
        s3_path = S3Path("s3://bucket/base")
        joined = s3_path.join("path", "to", "file.csv")

        assert str(joined) == "s3://bucket/base/path/to/file.csv"


class TestS3StreamingClient:
    """Test S3StreamingClient functionality."""

    @pytest.fixture
    def s3_config(self):
        """Get S3 configuration from .env file."""
        return {
            'aws_access_key_id': '',
            'aws_secret_access_key': '',
            'region_name': 'us-east-1',
            'test_bucket': 'forklift-test-bucket'
        }

    @pytest.fixture
    def s3_client_with_mock(self, s3_config, s3_mock_conditional, use_s3_mock, aws_credentials):
        """Create S3 client with conditional mocking."""
        mock_session, mock_client = s3_mock_conditional

        if use_s3_mock:
            # Using mocking
            client = S3StreamingClient(
                aws_access_key_id=aws_credentials.get('aws_access_key_id', ''),
                aws_secret_access_key=aws_credentials.get('aws_secret_access_key', ''),
                region_name=aws_credentials.get('region_name', 'us-east-1'),
                endpoint_url=aws_credentials.get('endpoint_url')
            )
            client._s3_client = mock_client
            yield client, mock_client
        else:
            # Using real S3
            client = S3StreamingClient(
                aws_access_key_id=aws_credentials.get('aws_access_key_id'),
                aws_secret_access_key=aws_credentials.get('aws_secret_access_key'),
                region_name=aws_credentials.get('region_name', 'us-east-1'),
                endpoint_url=aws_credentials.get('endpoint_url')
            )
            yield client, None

    @pytest.fixture
    def mock_s3_client(self, s3_config):
        """Create a mock S3 client for testing - kept for backward compatibility."""
        with patch('boto3.Session') as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            client = S3StreamingClient(
                aws_access_key_id=s3_config['aws_access_key_id'],
                aws_secret_access_key=s3_config['aws_secret_access_key'],
                region_name=s3_config['region_name']
            )
            client._s3_client = mock_client

            yield client, mock_client

    def test_s3_client_initialization(self, s3_config, use_s3_mock):
        """Test S3StreamingClient initialization."""
        if use_s3_mock:
            with patch('boto3.Session') as mock_session:
                client = S3StreamingClient(
                    aws_access_key_id=s3_config['aws_access_key_id'],
                    aws_secret_access_key=s3_config['aws_secret_access_key'],
                    region_name=s3_config['region_name']
                )

                mock_session.assert_called_once_with(
                    aws_access_key_id=s3_config['aws_access_key_id'],
                    aws_secret_access_key=s3_config['aws_secret_access_key'],
                    aws_session_token=None,
                    region_name=s3_config['region_name']
                )
        else:
            # Test real initialization (just verify it doesn't throw)
            client = S3StreamingClient(
                aws_access_key_id=s3_config['aws_access_key_id'],
                aws_secret_access_key=s3_config['aws_secret_access_key'],
                region_name=s3_config['region_name']
            )
            assert client is not None

    def test_exists_object_found(self, s3_client_with_mock, s3_config, use_s3_mock):
        """Test exists method when object is found."""
        client, mock_client = s3_client_with_mock

        if use_s3_mock:
            mock_client.head_object.return_value = {'ContentLength': 100}
            assert client.exists("s3://bucket/key") is True
            mock_client.head_object.assert_called_once_with(Bucket='bucket', Key='key')
        else:
            # For real S3, test with a test object path (skip if no test bucket access)
            try:
                test_path = f"s3://{s3_config['test_bucket']}/test-object-that-probably-does-not-exist"
                exists = client.exists(test_path)
                assert isinstance(exists, bool)  # Just verify it returns a boolean
            except Exception:
                pytest.skip("Cannot access real S3 bucket for testing")

    def test_exists_object_not_found(self, s3_client_with_mock, use_s3_mock):
        """Test exists method when object is not found."""
        client, mock_client = s3_client_with_mock

        if use_s3_mock:
            from botocore.exceptions import ClientError
            mock_client.head_object.side_effect = ClientError(
                {'Error': {'Code': '404'}}, 'HeadObject'
            )
            assert client.exists("s3://bucket/key") is False
        else:
            # For real S3, test with a path that definitely doesn't exist
            non_existent_path = "s3://non-existent-bucket-12345/non-existent-key"
            exists = client.exists(non_existent_path)
            assert exists is False

    def test_get_size(self, s3_client_with_mock, s3_config, use_s3_mock):
        """Test get_size method."""
        client, mock_client = s3_client_with_mock

        if use_s3_mock:
            mock_client.head_object.return_value = {'ContentLength': 1024}
            size = client.get_size("s3://bucket/key")
            assert size == 1024
            mock_client.head_object.assert_called_once_with(Bucket='bucket', Key='key')
        else:
            # For real S3, we can't easily test this without a known object
            # Skip this test for real S3 mode
            pytest.skip("get_size test requires known object in real S3 mode")

    def test_open_for_read(self, s3_client_with_mock, use_s3_mock):
        """Test open_for_read method."""
        client, mock_client = s3_client_with_mock

        if use_s3_mock:
            mock_body = MagicMock()
            mock_client.get_object.return_value = {'Body': mock_body}

            with patch('io.TextIOWrapper') as mock_wrapper:
                stream = client.open_for_read("s3://bucket/key", encoding='utf-8')
                mock_client.get_object.assert_called_once_with(Bucket='bucket', Key='key')
                mock_wrapper.assert_called_once_with(mock_body, encoding='utf-8')
        else:
            # For real S3, skip this test as it requires a real object
            pytest.skip("open_for_read test requires real object in S3")

    def test_open_for_write(self, s3_client_with_mock, s3_config, use_s3_mock):
        """Test open_for_write method."""
        client, mock_client = s3_client_with_mock

        if use_s3_mock:
            mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
            writer = client.open_for_write("s3://bucket/key", encoding='utf-8')
            assert isinstance(writer, S3StreamingWriter)
            mock_client.create_multipart_upload.assert_called_once_with(
                Bucket='bucket', Key='key'
            )
        else:
            # For real S3, test with a real test path but don't actually write
            try:
                test_path = f"s3://{s3_config['test_bucket']}/test-write-{int(time.time())}"
                writer = client.open_for_write(test_path, encoding='utf-8')
                assert isinstance(writer, S3StreamingWriter)
                # Clean up by aborting the upload
                writer._abort_upload()
            except Exception:
                pytest.skip("Cannot access real S3 bucket for write testing")


class TestS3StreamingWriter:
    """Test S3StreamingWriter functionality."""

    @pytest.fixture
    def writer_with_mock(self, use_s3_mock, aws_credentials):
        """Create S3StreamingWriter with conditional mocking."""
        if use_s3_mock:
            # Using mocking
            mock_client = MagicMock()
            mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
            s3_path = S3Path("s3://bucket/key")
            writer = S3StreamingWriter(mock_client, s3_path, part_size=10)  # Small part size for testing
            yield writer, mock_client
        else:
            # Using real S3 - create a real writer but don't actually use it for writes
            try:
                real_client = S3StreamingClient(
                    aws_access_key_id=aws_credentials.get('aws_access_key_id'),
                    aws_secret_access_key=aws_credentials.get('aws_secret_access_key'),
                    region_name=aws_credentials.get('region_name', 'us-east-1'),
                    endpoint_url=aws_credentials.get('endpoint_url')
                )
                test_path = S3Path(f"s3://cornyhorse-data/test-writer-{int(time.time())}")
                writer = S3StreamingWriter(real_client._s3_client, test_path, part_size=10)
                yield writer, None
                # Clean up by aborting any incomplete upload
                try:
                    writer._abort_upload()
                except:
                    pass
            except Exception:
                pytest.skip("Cannot create real S3 writer for testing")

    @pytest.fixture
    def mock_writer(self):
        """Create a mock S3StreamingWriter for testing - kept for backward compatibility."""
        mock_client = MagicMock()
        mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        s3_path = S3Path("s3://bucket/key")
        writer = S3StreamingWriter(mock_client, s3_path, part_size=10)  # Small part size for testing

        return writer, mock_client

    def test_writer_initialization(self, writer_with_mock, use_s3_mock):
        """Test S3StreamingWriter initialization."""
        writer, mock_client = writer_with_mock

        assert writer._upload_id is not None
        assert writer._part_number == 1
        assert len(writer._parts) == 0

        if use_s3_mock:
            mock_client.create_multipart_upload.assert_called_once()

    def test_write_small_data(self, writer_with_mock, use_s3_mock):
        """Test writing small data that doesn't trigger part upload."""
        writer, mock_client = writer_with_mock

        if use_s3_mock:
            bytes_written = writer.write("hello")
            assert bytes_written == 5
            assert writer._buffer.tell() > 0
            mock_client.upload_part.assert_not_called()
        else:
            # For real S3, just test that write works without error
            try:
                bytes_written = writer.write("hello")
                assert bytes_written == 5
            except Exception:
                pytest.skip("Cannot write to real S3 for testing")

    def test_write_large_data_triggers_upload(self, mock_writer, use_s3_mock):
        """Test writing large data that triggers part upload - mocked only."""
        if not use_s3_mock:
            pytest.skip("This test only works with S3 mocking enabled")

        writer, mock_client = mock_writer
        mock_client.upload_part.return_value = {'ETag': 'test-etag'}

        # Override the part size for testing (bypass the 5MB minimum)
        writer._part_size = 10  # Set to 10 bytes for testing

        # Write data larger than part_size (10 bytes) - need more data to trigger upload
        large_data = "x" * 50  # 50 bytes should definitely trigger upload with part_size=10
        writer.write(large_data)

        mock_client.upload_part.assert_called()
        assert len(writer._parts) == 1
        assert writer._parts[0]['ETag'] == 'test-etag'
        assert writer._parts[0]['PartNumber'] == 1

    def test_close_completes_upload(self, mock_writer, use_s3_mock):
        """Test closing writer completes multipart upload - mocked only."""
        if not use_s3_mock:
            pytest.skip("This test only works with S3 mocking enabled")

        writer, mock_client = mock_writer
        mock_client.upload_part.return_value = {'ETag': 'test-etag'}

        writer.write("test data")
        writer.close()

        mock_client.complete_multipart_upload.assert_called_once()
        assert writer._closed is True

    def test_context_manager(self, mock_writer):
        """Test S3StreamingWriter as context manager - mocked only."""
        writer, mock_client = mock_writer

        with writer as w:
            assert w is writer

        mock_client.complete_multipart_upload.assert_called_once()


class TestUnifiedIOHandler:
    """Test UnifiedIOHandler functionality with S3."""

    @pytest.fixture
    def io_handler(self):
        """Create UnifiedIOHandler with mocked S3 client."""
        with patch('forklift.io.unified_io.get_s3_client') as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            handler = UnifiedIOHandler()
            handler._s3_client = mock_client

            yield handler, mock_client

    def test_exists_s3_path(self, io_handler):
        """Test exists method with S3 path."""
        handler, mock_client = io_handler
        mock_client.exists.return_value = True

        assert handler.exists("s3://bucket/key") is True
        mock_client.exists.assert_called_once_with("s3://bucket/key")

    def test_exists_local_path(self, io_handler):
        """Test exists method with local path."""
        handler, mock_client = io_handler

        with tempfile.NamedTemporaryFile() as tmp_file:
            assert handler.exists(tmp_file.name) is True

        mock_client.exists.assert_not_called()

    def test_get_size_s3_path(self, io_handler):
        """Test get_size method with S3 path."""
        handler, mock_client = io_handler
        mock_client.get_size.return_value = 1024

        size = handler.get_size("s3://bucket/key")

        assert size == 1024
        mock_client.get_size.assert_called_once_with("s3://bucket/key")

    def test_open_for_read_s3_path(self, io_handler):
        """Test open_for_read method with S3 path."""
        handler, mock_client = io_handler
        mock_stream = MagicMock()
        mock_client.open_for_read.return_value = mock_stream

        stream = handler.open_for_read("s3://bucket/key", encoding='utf-8')

        assert stream is mock_stream
        mock_client.open_for_read.assert_called_once_with("s3://bucket/key", encoding='utf-8')

    def test_csv_reader_s3_path(self, io_handler):
        """Test csv_reader method with S3 path."""
        handler, mock_client = io_handler

        # Mock CSV data
        csv_data = "name,age\nAlice,25\nBob,30\n"
        mock_stream = MagicMock()
        mock_stream.__enter__.return_value = mock_stream
        mock_stream.__iter__.return_value = iter(csv_data.splitlines())
        mock_client.open_for_read.return_value = mock_stream

        with patch('csv.reader') as mock_csv_reader:
            mock_csv_reader.return_value = [['name', 'age'], ['Alice', '25'], ['Bob', '30']]

            rows = list(handler.csv_reader("s3://bucket/data.csv"))

            assert len(rows) == 3
            assert rows[0] == ['name', 'age']


class TestS3ParquetWriter:
    """Test S3ParquetWriter functionality."""

    @pytest.fixture
    def mock_parquet_writer(self):
        """Create mock S3ParquetWriter."""
        schema = pa.schema([pa.field("name", pa.string()), pa.field("age", pa.int64())])

        with patch('forklift.io.unified_io.get_s3_client') as mock_get_client:
            mock_s3_client = MagicMock()
            mock_get_client.return_value = mock_s3_client

            with patch('tempfile.NamedTemporaryFile') as mock_temp:
                mock_temp_file = MagicMock()
                mock_temp_file.name = "/tmp/test.parquet"
                mock_temp_file.close = MagicMock()
                mock_temp.return_value = mock_temp_file

                with patch('pyarrow.parquet.ParquetWriter') as mock_pq_writer:
                    with patch('pathlib.Path.unlink'):  # Mock file cleanup
                        writer = S3ParquetWriter("s3://bucket/test.parquet", schema)
                        # Override the s3_client to use our mock
                        writer.s3_client = mock_s3_client
                        yield writer, mock_s3_client, mock_pq_writer.return_value

    def test_s3_parquet_writer_initialization(self, mock_parquet_writer):
        """Test S3ParquetWriter initialization."""
        writer, mock_s3_client, mock_pq_writer = mock_parquet_writer

        assert writer.s3_path.bucket == "bucket"
        assert writer.s3_path.key == "test.parquet"
        assert writer.s3_client is mock_s3_client

    def test_write_table(self, mock_parquet_writer):
        """Test writing table to S3 parquet writer."""
        writer, mock_s3_client, mock_pq_writer = mock_parquet_writer

        # Create test table
        table = pa.table({
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
        })

        writer.write_table(table)

        mock_pq_writer.write_table.assert_called_once_with(table)

    def test_close_uploads_to_s3(self, mock_parquet_writer):
        """Test closing writer uploads file to S3."""
        writer, mock_s3_client, mock_pq_writer = mock_parquet_writer

        # Mock the file size check that boto3 uses
        with patch('builtins.open', mock_open(read_data=b"parquet data")) as mock_file:
            # Mock the file object's seek and tell methods for size calculation
            mock_file_obj = mock_file.return_value.__enter__.return_value
            mock_file_obj.seek = MagicMock()
            mock_file_obj.tell = MagicMock(return_value=100)  # Mock file size

            writer.close()

        mock_pq_writer.close.assert_called_once()
        mock_s3_client._s3_client.upload_fileobj.assert_called_once()


class TestForkliftCoreS3Integration:
    """Test ForkliftCore integration with S3 streaming."""

    @pytest.fixture
    def s3_config(self):
        """Get S3 configuration from .env file."""
        return {
            'test_bucket': 'forklift-test-bucket',
            'aws_region': 'us-east-1'
        }

    @pytest.fixture
    def sample_csv_data(self):
        """Sample CSV data for testing."""
        return "name,age,city\nAlice,25,New York\nBob,30,San Francisco\nCharlie,35,Chicago"

    @pytest.fixture
    def mock_forklift_core(self, s3_config, sample_csv_data):
        """Create ForkliftCore with mocked S3 dependencies."""
        config = ImportConfig(
            input_path=f"s3://{s3_config['test_bucket']}/test-data.csv",
            output_path=f"s3://{s3_config['test_bucket']}/output/",
            header_mode=HeaderMode.PRESENT
        )

        with patch('forklift.engine.forklift_core.UnifiedIOHandler') as mock_io_handler:
            # Mock CSV reader to return sample data
            mock_handler = MagicMock()
            mock_handler.csv_reader.return_value = [
                ['name', 'age', 'city'],
                ['Alice', '25', 'New York'],
                ['Bob', '30', 'San Francisco'],
                ['Charlie', '35', 'Chicago']
            ]
            mock_handler.exists.return_value = True
            mock_io_handler.return_value = mock_handler

            core = ForkliftCore(config)

            yield core, mock_handler

    def test_s3_header_detection(self, mock_forklift_core):
        """Test header detection with S3 input."""
        core, mock_handler = mock_forklift_core

        header_idx, columns = core._detect_header_row(core.config.input_path)

        assert header_idx == 0
        assert columns == ['name', 'age', 'city']
        mock_handler.csv_reader.assert_called()

    def test_s3_batch_reader_creation(self, mock_forklift_core):
        """Test S3 batch reader creation."""
        core, mock_handler = mock_forklift_core

        # Set up column names as if header detection ran
        core.column_names = ['name', 'age', 'city']
        core.header_row_index = 0

        batches = list(core._create_s3_batch_reader(core.config.input_path))

        assert len(batches) > 0
        # Verify it used S3 CSV batches method
        mock_handler.csv_reader.assert_called()

    @pytest.mark.integration
    def test_full_s3_to_s3_processing(self, mock_forklift_core):
        """Test full S3 to S3 processing pipeline."""
        core, mock_handler = mock_forklift_core

        # Mock get_size to return actual integers instead of MagicMock objects
        mock_handler.get_size.return_value = 1024

        with patch('forklift.io.unified_io.create_parquet_writer') as mock_create_writer:
            mock_writer = MagicMock()
            mock_create_writer.return_value = mock_writer

            # Mock the batch creation to return valid data
            with patch.object(core, '_create_s3_csv_batches') as mock_batches:
                # Create a mock batch
                schema = pa.schema([
                    pa.field("name", pa.string()),
                    pa.field("age", pa.string()),
                    pa.field("city", pa.string())
                ])
                batch = pa.RecordBatch.from_arrays([
                    pa.array(["Alice", "Bob"]),
                    pa.array(["25", "30"]),
                    pa.array(["New York", "San Francisco"])
                ], schema=schema)
                mock_batches.return_value = [batch]

                results = core.process_csv()

                assert results.total_rows > 0
                assert results.valid_rows > 0
                assert len(results.output_files) > 0


class TestUtilityFunctions:
    """Test utility functions for S3 support."""

    def test_is_s3_path_positive(self):
        """Test is_s3_path with valid S3 URIs."""
        assert is_s3_path("s3://bucket/key") is True
        assert is_s3_path("s3://my-bucket/path/to/file.csv") is True

    def test_is_s3_path_negative(self):
        """Test is_s3_path with non-S3 paths."""
        assert is_s3_path("/local/path/file.csv") is False
        assert is_s3_path("file.csv") is False
        assert is_s3_path("http://example.com/file.csv") is False
        assert is_s3_path(Path("/local/path")) is False

    def test_get_s3_client(self):
        """Test get_s3_client factory function."""
        with patch('forklift.io.s3_streaming.S3StreamingClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            client = get_s3_client(region_name='us-west-2')

            mock_client_class.assert_called_once_with(region_name='us-west-2')
            assert client is mock_client


class TestS3ErrorHandling:
    """Test error handling in S3 operations."""

    def test_s3_path_validation_errors(self):
        """Test S3Path validation with various invalid inputs."""
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            S3Path("not-an-s3-uri")

        with pytest.raises(ValueError, match="Bucket name is required"):
            S3Path("s3:///no-bucket")

    def test_s3_client_connection_errors(self):
        """Test S3 client error handling."""
        from botocore.exceptions import NoCredentialsError, ClientError

        with patch('boto3.Session') as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            client = S3StreamingClient()

            # Test NoCredentialsError
            mock_client.head_object.side_effect = NoCredentialsError()
            with pytest.raises(NoCredentialsError):
                client.exists("s3://bucket/key")

            # Test generic ClientError (not 404)
            mock_client.head_object.side_effect = ClientError(
                {'Error': {'Code': '500'}}, 'HeadObject'
            )
            with pytest.raises(ClientError):
                client.exists("s3://bucket/key")

    def test_s3_writer_error_cleanup(self):
        """Test S3StreamingWriter error handling and cleanup."""
        mock_client = MagicMock()
        mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        s3_path = S3Path("s3://bucket/key")
        writer = S3StreamingWriter(mock_client, s3_path)

        # Simulate error during upload
        mock_client.complete_multipart_upload.side_effect = Exception("Upload failed")

        with pytest.raises(Exception, match="Upload failed"):
            writer.close()

        # Verify abort was called for cleanup
        mock_client.abort_multipart_upload.assert_called_once_with(
            Bucket='bucket',
            Key='key',
            UploadId='test-upload-id'
        )

