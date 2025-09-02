"""Unified mockable integration tests for CSV and FWF files with S3 uploads."""

import pytest
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import MagicMock
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from forklift.inputs.fwf_utils import create_fwf_config_from_schema
from forklift.io.s3_streaming import S3StreamingClient, S3Path
from forklift.io.unified_io import UnifiedIOHandler
from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


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
        return len(self._objects.get(key, b''))

    def list_objects(self, bucket, prefix=""):
        """Mock S3 list objects."""
        keys = []
        prefix_key = f"{bucket}/{prefix}"
        for key in self._objects.keys():
            if key.startswith(prefix_key):
                keys.append(key.replace(f"{bucket}/", ""))
        return keys


class MockS3Writer:
    """Mock S3 writer for testing."""

    def __init__(self, client, s3_path):
        self.client = client
        self.s3_path = s3_path
        self.buffer = b''

    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        self.buffer += data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Store the mock object
        s3_path_obj = S3Path(self.s3_path) if isinstance(self.s3_path, str) else self.s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        self.client._objects[key] = self.buffer


class MockS3Reader:
    """Mock S3 reader for testing."""

    def __init__(self, client, s3_path):
        self.client = client
        self.s3_path = s3_path
        s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
        key = f"{s3_path_obj.bucket}/{s3_path_obj.key}"
        self.data = client._objects.get(key, b'')
        self.position = 0

    def read(self, size=-1):
        if size == -1:
            result = self.data[self.position:]
            self.position = len(self.data)
        else:
            result = self.data[self.position:self.position + size]
            self.position += len(result)
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.mark.integration
class TestMockableFileUploadIntegration:
    """Unified integration tests for CSV and FWF files with proper S3 mocking support."""

    @pytest.fixture
    def s3_client(self, s3_mock_conditional, aws_credentials, s3_test_bucket):
        """Create S3 client (real or mocked) for integration tests."""
        mock_session, mock_client = s3_mock_conditional

        if mock_session is not None:
            # Using mocked S3
            return MockS3StreamingClient(mock_client, s3_test_bucket)
        else:
            # Using real S3
            return S3StreamingClient(
                aws_access_key_id=aws_credentials['aws_access_key_id'],
                aws_secret_access_key=aws_credentials['aws_secret_access_key'],
                region_name=aws_credentials['region_name'],
                endpoint_url=aws_credentials['endpoint_url']
            )

    @pytest.fixture
    def cleanup_s3_objects(self, s3_client, s3_test_bucket, s3_mock_conditional):
        """Fixture to clean up S3 objects after tests."""
        objects_to_cleanup = []

        yield objects_to_cleanup

        # Cleanup after test (only for real S3)
        mock_session, mock_client = s3_mock_conditional
        if mock_session is None and objects_to_cleanup:  # Real S3
            for s3_path in objects_to_cleanup:
                try:
                    s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
                    s3_client._s3_client.delete_object(
                        Bucket=s3_path_obj.bucket,
                        Key=s3_path_obj.key
                    )
                except Exception:
                    pass  # Best effort cleanup

    @pytest.fixture(scope="class")
    def test_files_dir(self):
        """Get the test-files directory path."""
        return Path(__file__).parent.parent / "test-files"

    def test_csv_to_parquet_s3_upload_mockable(self, s3_client, s3_test_bucket, cleanup_s3_objects, s3_mock_conditional, test_files_dir):
        """Test CSV processing and parquet upload to S3 (supports mocking)."""
        # Create test CSV data
        csv_data = [
            "id,name,amount,status",
            "1,John Doe,1234.56,active",
            "2,Jane Smith,2345.67,active",
            "3,Bob Johnson,3456.78,inactive"
        ]

        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
            csv_path = Path(tmp_file.name)
            for line in csv_data:
                tmp_file.write(line + '\n')

        try:
            # Create temporary output parquet file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                parquet_path = Path(tmp_parquet.name)

            # Process CSV to parquet using ForkliftCore
            config = ImportConfig(
                input_path=str(csv_path),
                output_path=str(parquet_path.parent),
                header_mode=HeaderMode.PRESENT,
                delimiter=",",
                encoding="utf-8"
            )


            try:
                # Process CSV to parquet
                core = ForkliftCore(config)
                results = core.process_csv()

                # Read the generated parquet file to get the table
                parquet_files = results.output_files
                data_file = None
                if parquet_files:
                    # Use the first output file (data.parquet)
                    data_file = next((f for f in parquet_files if "data.parquet" in f), parquet_files[0])
                    table = pq.read_table(data_file)
                else:
                    # If no output files, read the expected output path
                    table = pq.read_table(parquet_path)

                # Verify table has data
                assert table.num_rows == 3, "Should have 3 data records"
                assert table.num_columns >= 4, "Should have at least 4 columns"

                # Use the actual output file for upload if available
                upload_file = data_file if data_file else parquet_path

                # Upload to S3
                test_key = f"forklift/integration-test/csv-mockable-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_test_bucket}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                # Upload parquet file to S3
                with open(upload_file, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path) as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload succeeded
                assert s3_client.exists(s3_path), "CSV parquet file should exist in S3"

                # Additional verification for real S3 only
                mock_session, mock_client = s3_mock_conditional
                if mock_session is None:  # Real S3 only
                    file_size = s3_client.get_size(s3_path)
                    assert file_size > 0, "Uploaded parquet file should have content"

            finally:
                # Clean up any generated files
                if 'parquet_files' in locals() and parquet_files:
                    for f in parquet_files:
                        try:
                            Path(f).unlink()
                        except:
                            pass
                parquet_path.unlink()

        finally:
            csv_path.unlink()

    def test_fwf_to_parquet_s3_upload_mockable(self, s3_client, s3_test_bucket, cleanup_s3_objects, s3_mock_conditional):
        """Test FWF processing and parquet upload to S3 (supports mocking)."""
        # Define field specification for test data
        fields = [
            FwfFieldSpec("id", 1, 5, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 6, 20, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 26, 10, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("status", 36, 8, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(fields=fields, skip_blank_lines=True)

        # Create test FWF data
        test_data = [
            "00001John Doe          0000012500ACTIVE  ",
            "00002Jane Smith        0000025000ACTIVE  ",
            "00003Bob Johnson       0000015000INACTIVE"
        ]

        # Create temporary FWF file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + '\n')

        try:
            handler = FwfInputHandler(config)

            # Create Arrow table from FWF file
            table = handler.create_arrow_table(fwf_path)

            # Verify table has data
            assert table.num_rows == 3, "Should have 3 records"
            assert table.num_columns > 0, "Should have columns"

            # Create temporary parquet file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                parquet_path = Path(tmp_parquet.name)

            try:
                # Write to parquet file
                pa.parquet.write_table(table, parquet_path)

                # Upload to S3
                test_key = f"forklift/integration-test/fwf-mockable-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_test_bucket}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                # Upload parquet file to S3
                with open(parquet_path, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path) as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload succeeded
                assert s3_client.exists(s3_path), "FWF parquet file should exist in S3"

                # Additional verification for real S3 only
                mock_session, mock_client = s3_mock_conditional
                if mock_session is None:  # Real S3 only
                    file_size = s3_client.get_size(s3_path)
                    assert file_size > 0, "Uploaded parquet file should have content"

            finally:
                parquet_path.unlink()

        finally:
            fwf_path.unlink()

    def test_multi_schema_fwf_to_parquet_s3_upload_mockable(self, s3_client, s3_test_bucket, cleanup_s3_objects, s3_mock_conditional):
        """Test multi-schema FWF processing and parquet upload to S3 (supports mocking)."""
        # Create multi-schema configuration
        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")

        conditional_schemas = [
            FwfConditionalSchema("H", "Header Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("batch_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("batch_date", 10, 8, parquet_type="string"),
                FwfFieldSpec("record_count", 18, 5, align="right", pad="0", parquet_type="int32")
            ]),
            FwfConditionalSchema("D", "Detail Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("transaction_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("amount", 10, 10, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("description", 20, 15, align="left", parquet_type="string")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
            skip_blank_lines=True
        )

        # Create test multi-schema data
        test_data = [
            "H0000123420250101000003",  # Header
            "D000001230000125000Payment 1      ",  # Detail
            "D000002340000250000Payment 2      ",  # Detail
            "D000003450000187500Payment 3      "   # Detail
        ]

        # Create temporary FWF file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + '\n')

        try:
            handler = FwfInputHandler(config)

            # Create Arrow table from multi-schema FWF file
            table = handler.create_arrow_table(fwf_path)

            # Verify table structure
            assert table.num_rows == 4, "Should have 4 records"

            df = table.to_pandas()
            record_types = df['record_type'].unique()
            assert 'H' in record_types, "Should have header records"
            assert 'D' in record_types, "Should have detail records"

            # Create temporary parquet file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                parquet_path = Path(tmp_parquet.name)

            try:
                # Write to parquet file
                pa.parquet.write_table(table, parquet_path)

                # Upload to S3
                test_key = f"forklift/integration-test/multi-fwf-mockable-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_test_bucket}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                # Upload parquet file to S3
                with open(parquet_path, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path) as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload succeeded
                assert s3_client.exists(s3_path), "Multi-schema parquet file should exist in S3"

                # Additional verification for real S3 only
                mock_session, mock_client = s3_mock_conditional
                if mock_session is None:  # Real S3 only
                    file_size = s3_client.get_size(s3_path)
                    assert file_size > 0, "Uploaded parquet file should have content"

            finally:
                parquet_path.unlink()

        finally:
            fwf_path.unlink()

    def test_batch_file_processing_s3_upload_mockable(self, s3_client, s3_test_bucket, cleanup_s3_objects, s3_mock_conditional):
        """Test batch processing of multiple file types and upload to S3 (supports mocking)."""
        # Create multiple test files
        test_files = []

        # CSV file
        csv_data = ["id,name,amount", "1,Alice,100.50", "2,Bob,200.75"]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
            csv_path = Path(tmp_file.name)
            for line in csv_data:
                tmp_file.write(line + '\n')
            test_files.append(('csv', csv_path))

        # FWF file
        fwf_fields = [
            FwfFieldSpec("id", 1, 3, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("data", 4, 10, align="left", parquet_type="string")
        ]
        fwf_config = FwfInputConfig(fields=fwf_fields, skip_blank_lines=True)
        fwf_data = ["001Data1    ", "002Data2    ", "003Data3    "]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in fwf_data:
                tmp_file.write(line + '\n')
            test_files.append(('fwf', fwf_path))

        try:
            processed_files = []

            for file_type, file_path in test_files:
                # Process each file type
                if file_type == 'csv':
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                        parquet_path = Path(tmp_parquet.name)

                    config = ImportConfig(
                        input_path=str(file_path),
                        output_path=str(parquet_path.parent),
                        header_mode=HeaderMode.PRESENT,
                        delimiter=",",
                        encoding="utf-8"
                    )

                    core = ForkliftCore(config)
                    results = core.process_csv()

                    # Get the generated parquet file
                    if results.output_files:
                        upload_file = next((f for f in results.output_files if "data.parquet" in f), results.output_files[0])
                    else:
                        upload_file = parquet_path

                elif file_type == 'fwf':
                    handler = FwfInputHandler(fwf_config)
                    table = handler.create_arrow_table(file_path)

                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                        parquet_path = Path(tmp_parquet.name)
                    pa.parquet.write_table(table, parquet_path)
                    upload_file = parquet_path

                # Upload to S3
                test_key = f"forklift/integration-test/batch-{file_type}-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_test_bucket}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                with open(upload_file, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path) as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload
                assert s3_client.exists(s3_path), f"{file_type.upper()} parquet file should exist in S3"
                processed_files.append(s3_path)

                # Clean up the upload file
                try:
                    Path(upload_file).unlink()
                except:
                    pass

            # Verify all files were processed
            assert len(processed_files) == 2, "Should have processed both CSV and FWF files"

            # Additional verification for real S3 only
            mock_session, mock_client = s3_mock_conditional
            if mock_session is None:  # Real S3 only
                for s3_path in processed_files:
                    file_size = s3_client.get_size(s3_path)
                    assert file_size > 0, f"Uploaded file {s3_path} should have content"

        finally:
            # Cleanup local test files
            for _, file_path in test_files:
                file_path.unlink()
