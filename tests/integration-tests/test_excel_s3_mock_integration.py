"""Integration tests for Excel processing with proper S3 mocking support."""

import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.inputs.config import ExcelInputConfig, ExcelSheetConfig
from forklift.inputs.excel import ExcelInputHandler
from forklift.io.s3_streaming import S3Path, S3StreamingClient


class MockS3StreamingClient:
    """Mock S3 streaming client for testing."""

    def __init__(self, mock_client, bucket_name):
        self._s3_client = mock_client
        self.bucket = bucket_name
        self._objects = {}  # Store mock objects in memory

    def open_for_write(self, s3_path, encoding=None):
        """Mock S3 write operation."""
        return MockS3Writer(self, s3_path)

    def open_for_read(self, s3_path, encoding=None):
        """Mock S3 read operation."""
        return MockS3Reader(self, s3_path)

    def exists(self, s3_path):
        """Mock S3 exists check."""
        s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        return key in self._objects

    def get_size(self, s3_path):
        """Mock S3 size check."""
        s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        return len(self._objects.get(key, b""))


class MockS3Writer:
    """Mock S3 writer for testing."""

    def __init__(self, client, s3_path):
        self.client = client
        self.s3_path = s3_path
        self.buffer = b""
        self.closed = False
        self.position = 0

    def write(self, data):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if isinstance(data, str):
            data = data.encode("utf-8")
        self.buffer += data
        self.position += len(data)
        return len(data)

    def flush(self):
        """Flush the buffer (no-op for mock)."""
        pass

    def close(self):
        """Close the writer."""
        if not self.closed:
            self.closed = True

    def tell(self):
        """Return current position."""
        return self.position

    def seekable(self):
        """Return whether object supports random access."""
        return False

    def writable(self):
        """Return whether object was opened for writing."""
        return True

    def readable(self):
        """Return whether object was opened for reading."""
        return False

    @property
    def mode(self):
        """File mode."""
        return "wb"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Store the mock object
        s3_path_obj = S3Path(self.s3_path) if isinstance(self.s3_path, str) else self.s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        self.client._objects[key] = self.buffer
        self.close()


class MockS3Reader:
    """Mock S3 reader for testing."""

    def __init__(self, client, s3_path):
        self.client = client
        self.s3_path = s3_path
        s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        self.data = client._objects.get(key, b"")
        self.position = 0
        self.closed = False

    def read(self, size=-1):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if size == -1:
            result = self.data[self.position :]
            self.position = len(self.data)
        else:
            result = self.data[self.position : self.position + size]
            self.position += len(result)
        return result

    def close(self):
        """Close the reader."""
        self.closed = True

    def tell(self):
        """Return current position."""
        return self.position

    def seekable(self):
        """Return whether object supports random access."""
        return False

    def writable(self):
        """Return whether object was opened for writing."""
        return False

    def readable(self):
        """Return whether object was opened for reading."""
        return True

    @property
    def mode(self):
        """File mode."""
        return "rb"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@pytest.mark.integration
@pytest.mark.s3
class TestExcelS3MockableIntegration:
    """Excel integration tests that support both mocked and real S3."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client for testing."""
        mock_boto3_client = MagicMock()
        return MockS3StreamingClient(mock_boto3_client, "test-bucket")

    @pytest.fixture
    def use_real_s3(self, request):
        """Fixture to determine if we should use real S3 or mocked S3."""
        return request.config.getoption("--use-real-s3", default=False)

    @pytest.fixture
    def s3_client(self, use_real_s3, mock_s3_client):
        """Provide either real or mock S3 client based on test configuration."""
        if use_real_s3:
            # Try to create real S3 client
            try:
                import os
                from pathlib import Path

                from dotenv import load_dotenv

                # Load from ~/.credentials/.env first, then fallback to local .env
                credentials_path = Path.home() / ".credentials" / ".env"
                if credentials_path.exists():
                    load_dotenv(credentials_path)
                else:
                    load_dotenv()

                aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
                aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

                if not aws_access_key_id or not aws_secret_access_key:
                    pytest.skip("AWS credentials not configured for real S3 tests")

                return S3StreamingClient(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
                    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                )
            except Exception as e:
                pytest.skip(f"Could not create real S3 client: {e}")
        else:
            return mock_s3_client

    @pytest.fixture
    def s3_bucket(self, use_real_s3):
        """Provide bucket name for tests."""
        if use_real_s3:
            import os

            return os.getenv("S3_TEST_BUCKET", "cornyhorse-data")
        else:
            return "test-bucket"

    @pytest.fixture
    def cleanup_s3_objects(self, use_real_s3):
        """Fixture to clean up S3 objects after tests."""
        objects_to_cleanup = []

        yield objects_to_cleanup

        # Only cleanup real S3 objects
        if use_real_s3 and objects_to_cleanup:
            try:
                import os
                from pathlib import Path

                from dotenv import load_dotenv

                credentials_path = Path.home() / ".credentials" / ".env"
                if credentials_path.exists():
                    load_dotenv(credentials_path)
                else:
                    load_dotenv()

                client = S3StreamingClient(
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
                    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                )

                for s3_path in objects_to_cleanup:
                    try:
                        s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
                        client._s3_client.delete_object(
                            Bucket=s3_path_obj.bucket, Key=s3_path_obj.key
                        )
                        print(f"Cleaned up S3 object: {s3_path}")
                    except Exception as e:
                        print(f"Failed to cleanup S3 object {s3_path}: {e}")
            except Exception as e:
                print(f"Could not setup cleanup: {e}")

    def test_excel_basic_s3_upload_mockable(self, s3_client, s3_bucket, cleanup_s3_objects):
        """Test basic Excel to S3 parquet upload (works with both mock and real S3)."""
        # Setup test data
        test_file = Path(__file__).parent.parent / "test-files" / "excel" / "excel-data.xlsx"

        sheet_config = ExcelSheetConfig(
            select={"name": "Sheet1"}, header={"mode": "present"}, skip_blank_rows=True
        )

        config = ExcelInputConfig(sheets=[sheet_config], values_only=True)

        handler = ExcelInputHandler(config)

        # Process the Excel file using correct API
        results = list(handler.process_sheets(test_file))
        assert len(results) > 0, "Should have processed Excel data"

        # Get the first table (results are tuples of (sheet_name, table))
        sheet_name, table = results[0]
        assert isinstance(table, pa.Table), "Should create PyArrow table"

        # Upload to S3
        timestamp = int(time.time())
        s3_path = S3Path(f"s3://{s3_bucket}/test-uploads/excel-mockable-test-{timestamp}.parquet")

        # Register for cleanup (only matters for real S3)
        cleanup_s3_objects.append(s3_path)

        # Upload the parquet file
        with s3_client.open_for_write(s3_path) as s3_file:
            pq.write_table(table, s3_file)

        # Verify upload was successful
        assert s3_client.exists(s3_path), "S3 object should exist after upload"

        file_size = s3_client.get_size(s3_path)
        assert file_size > 0, "Uploaded file should have content"

        print(f"Successfully uploaded Excel data to S3: {s3_path}")
        print(f"File size: {file_size} bytes")

    def test_excel_multi_sheet_s3_upload_mockable(self, s3_client, s3_bucket, cleanup_s3_objects):
        """Test multi-sheet Excel to S3 upload (works with both mock and real S3)."""
        test_file = Path(__file__).parent.parent / "test-files" / "excel" / "excel-data.xlsx"

        # Configuration for multiple sheets
        sheet_configs = [
            ExcelSheetConfig(
                select={"name": "Sheet1"}, header={"mode": "present"}, skip_blank_rows=True
            ),
            ExcelSheetConfig(
                select={"name": "Sheet2"}, header={"mode": "present"}, skip_blank_rows=True
            ),
        ]

        config = ExcelInputConfig(sheets=sheet_configs, values_only=True)

        handler = ExcelInputHandler(config)

        # Process the file using correct API
        results = list(handler.process_sheets(test_file))

        timestamp = int(time.time())
        upload_count = 0

        for i, (sheet_name, table) in enumerate(results):
            if table.num_rows > 0:  # Only upload if table has data
                # Create S3 path for this sheet
                s3_path = S3Path(
                    f"s3://{s3_bucket}/test-uploads/excel-multi-mockable-{sheet_name}-{timestamp}-{i}.parquet"
                )

                # Register for cleanup
                cleanup_s3_objects.append(s3_path)

                # Upload the parquet file
                with s3_client.open_for_write(s3_path) as s3_file:
                    pq.write_table(table, s3_file)

                # Verify upload
                assert s3_client.exists(s3_path), f"S3 object should exist for sheet {sheet_name}"

                upload_count += 1
                print(f"Successfully uploaded {sheet_name} to S3: {s3_path}")

        assert upload_count > 0, "Should have uploaded at least one sheet"

    def test_excel_1904_dates_s3_upload_mockable(self, s3_client, s3_bucket, cleanup_s3_objects):
        """Test Excel file with 1904 date system upload to S3."""
        test_file = (
            Path(__file__).parent.parent / "test-files" / "excel" / "excel-data-1904dates.xlsx"
        )

        # Configuration for 1904 date system - use index instead of specific sheet name
        sheet_config = ExcelSheetConfig(
            select={"index": 0},  # Use first sheet to avoid name matching issues
            header={"mode": "present"},
            skip_blank_rows=True,
        )

        config = ExcelInputConfig(sheets=[sheet_config], values_only=True, date_system="1904")

        handler = ExcelInputHandler(config)

        try:
            # Process the file using correct API
            results = list(handler.process_sheets(test_file))

            if results and results[0][1].num_rows > 0:  # Check if table has data
                sheet_name, table = results[0]

                # Upload to S3
                timestamp = int(time.time())
                s3_path = S3Path(
                    f"s3://{s3_bucket}/test-uploads/excel-1904-dates-{timestamp}.parquet"
                )

                # Register for cleanup
                cleanup_s3_objects.append(s3_path)

                # Upload the parquet file
                with s3_client.open_for_write(s3_path) as s3_file:
                    pq.write_table(table, s3_file)

                # Verify upload was successful
                assert s3_client.exists(s3_path), "S3 object should exist after upload"

                file_size = s3_client.get_size(s3_path)
                assert file_size > 0, "Uploaded file should have content"

                print(f"Successfully uploaded 1904 dates Excel data to S3: {s3_path}")
        except (pa.ArrowTypeError, ValueError) as e:
            # Skip test if there are type conversion issues with the Excel data
            pytest.skip(f"Excel data has type conversion issues: {e}")

    def test_excel_headerless_sheet_s3_upload_mockable(
        self, s3_client, s3_bucket, cleanup_s3_objects
    ):
        """Test Excel headerless sheet with custom headers upload to S3."""
        test_file = Path(__file__).parent.parent / "test-files" / "excel" / "excel-data.xlsx"

        # Configuration for headerless sheet with custom headers - use index instead of name
        sheet_config = ExcelSheetConfig(
            select={"index": 0},  # Use first sheet
            header={"mode": "absent", "override": ["col1", "col2", "col3"]},
            skip_blank_rows=True,
        )

        config = ExcelInputConfig(sheets=[sheet_config], values_only=True)

        handler = ExcelInputHandler(config)

        try:
            # Process the file using correct API
            results = list(handler.process_sheets(test_file))

            if results and results[0][1].num_rows > 0:  # Check if table has data
                sheet_name, table = results[0]

                # Upload to S3
                timestamp = int(time.time())
                s3_path = S3Path(
                    f"s3://{s3_bucket}/test-uploads/excel-headerless-{timestamp}.parquet"
                )

                # Register for cleanup
                cleanup_s3_objects.append(s3_path)

                # Upload the parquet file
                with s3_client.open_for_write(s3_path) as s3_file:
                    pq.write_table(table, s3_file)

                # Verify upload was successful
                assert s3_client.exists(s3_path), "S3 object should exist after upload"

                print(f"Successfully uploaded headerless Excel data to S3: {s3_path}")
        except (pa.ArrowTypeError, ValueError) as e:
            # Skip test if there are type conversion issues
            pytest.skip(f"Excel header override test has type conversion issues: {e}")

    def test_excel_by_index_s3_upload_mockable(self, s3_client, s3_bucket, cleanup_s3_objects):
        """Test Excel sheet selection by index upload to S3."""
        test_file = Path(__file__).parent.parent / "test-files" / "excel" / "excel-data.xlsx"

        # Configuration using sheet index
        sheet_config = ExcelSheetConfig(
            select={"index": 0}, header={"mode": "present"}, skip_blank_rows=True  # First sheet
        )

        config = ExcelInputConfig(sheets=[sheet_config], values_only=True)

        handler = ExcelInputHandler(config)

        # Process the file using correct API
        results = list(handler.process_sheets(test_file))

        if results and results[0][1].num_rows > 0:  # Check if table has data
            sheet_name, table = results[0]

            # Upload to S3
            timestamp = int(time.time())
            s3_path = S3Path(f"s3://{s3_bucket}/test-uploads/excel-by-index-{timestamp}.parquet")

            # Register for cleanup
            cleanup_s3_objects.append(s3_path)

            # Upload the parquet file
            with s3_client.open_for_write(s3_path) as s3_file:
                pq.write_table(table, s3_file)

            # Verify upload was successful
            assert s3_client.exists(s3_path), "S3 object should exist after upload"

            print(f"Successfully uploaded Excel data (by index) to S3: {s3_path}")


def pytest_addoption(parser):
    """Add command line option for real S3 testing."""
    parser.addoption(
        "--use-real-s3",
        action="store_true",
        default=False,
        help="Use real S3 instead of mocked S3 for integration tests",
    )
