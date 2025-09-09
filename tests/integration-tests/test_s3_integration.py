"""Integration tests for S3 streaming with real S3 operations using .env file configuration."""

import pytest
import tempfile
import json
import csv
from pathlib import Path
from typing import Dict, Any
import time

from forklift.io.s3_streaming import S3StreamingClient, S3Path
from forklift.io.unified_io import UnifiedIOHandler
from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode
import pyarrow as pa


@pytest.mark.integration
class TestS3StreamingIntegration:
    """Integration tests that use real S3 operations."""

    @pytest.fixture(scope="class")
    def s3_config(self):
        """Get S3 configuration from .env file."""
        config = {
            'aws_access_key_id': None,
            'aws_secret_access_key': None,
            'region_name': 'us-east-1',
            'test_bucket': 'cornyhorse-data',
            'endpoint_url': None
        }

        # Load from environment variables or .env file
        from dotenv import load_dotenv
        import os
        from pathlib import Path

        # Load from ~/.credentials/.env first, then fallback to local .env
        credentials_path = Path.home() / '.credentials' / '.env'
        if credentials_path.exists():
            load_dotenv(credentials_path)
        else:
            load_dotenv()  # fallback to local .env

        config['aws_access_key_id'] = os.getenv('AWS_ACCESS_KEY_ID')
        config['aws_secret_access_key'] = os.getenv('AWS_SECRET_ACCESS_KEY')
        config['region_name'] = os.getenv('AWS_DEFAULT_REGION', 'eu-north-1')
        config['test_bucket'] = os.getenv('S3_TEST_BUCKET', 'cornyhorse-data')
        config['endpoint_url'] = os.getenv('AWS_ENDPOINT_URL')

        # Skip if no credentials are configured
        if not config['aws_access_key_id'] or not config['aws_secret_access_key']:
            pytest.skip("AWS credentials not configured")

        return config

    @pytest.fixture(scope="class")
    def s3_client(self, s3_config):
        """Create real S3 client for integration tests."""
        return S3StreamingClient(
            aws_access_key_id=s3_config['aws_access_key_id'],
            aws_secret_access_key=s3_config['aws_secret_access_key'],
            region_name=s3_config['region_name'],
            endpoint_url=s3_config['endpoint_url']
        )

    @pytest.fixture
    def test_data(self):
        """Sample test data for S3 operations."""
        return {
            'csv_content': "name,age,city,salary\nAlice,25,New York,75000\nBob,30,San Francisco,85000\nCharlie,35,Chicago,70000\nDiana,28,Boston,80000",
            'schema_content': {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "city": {"type": "string"},
                    "salary": {"type": "number"}
                },
                "required": ["name", "age"]
            }
        }

    @pytest.fixture
    def cleanup_s3_objects(self, s3_config):
        """Fixture to clean up S3 objects before tests (to ensure clean state) but preserve after tests for investigation."""
        objects_to_cleanup = []

        # Create client for cleanup operations
        client = S3StreamingClient(
            aws_access_key_id=s3_config['aws_access_key_id'],
            aws_secret_access_key=s3_config['aws_secret_access_key'],
            region_name=s3_config['region_name'],
            endpoint_url=s3_config['endpoint_url']
        )

        yield objects_to_cleanup

        # Note: We intentionally do NOT clean up after tests to allow investigation
        # Files are left in place for debugging purposes
        print(f"\nTest completed. Files left in S3 for investigation:")
        for s3_path in objects_to_cleanup:
            print(f"  {s3_path}")

    @pytest.fixture(autouse=True)
    def cleanup_before_test(self, s3_config):
        """Clean up any existing test files before running tests to ensure clean state."""
        client = S3StreamingClient(
            aws_access_key_id=s3_config['aws_access_key_id'],
            aws_secret_access_key=s3_config['aws_secret_access_key'],
            region_name=s3_config['region_name'],
            endpoint_url=s3_config['endpoint_url']
        )

        # Define test prefixes to clean up
        test_prefixes = [
            "forklift/integration-test/",
            "forklift/performance-test/",
            "forklift/test-files-upload/",
            "forklift/pipeline-test/"
        ]

        # Clean up any existing test files
        for prefix in test_prefixes:
            try:
                response = client._s3_client.list_objects_v2(
                    Bucket=s3_config['test_bucket'],
                    Prefix=prefix
                )

                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    if objects_to_delete:
                        client._s3_client.delete_objects(
                            Bucket=s3_config['test_bucket'],
                            Delete={'Objects': objects_to_delete}
                        )
                        print(f"Cleaned up {len(objects_to_delete)} existing objects with prefix: {prefix}")
            except Exception as e:
                print(f"Warning: Could not clean up prefix {prefix}: {e}")

    def test_s3_upload_download_roundtrip(self, s3_client, s3_config, test_data, cleanup_s3_objects):
        """Test uploading and downloading data to/from S3."""
        test_key = f"forklift/integration-test/roundtrip-test.csv"
        s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
        cleanup_s3_objects.append(s3_path)

        # Upload test data
        with s3_client.open_for_write(s3_path, encoding='utf-8') as writer:
            writer.write(test_data['csv_content'])

        # Verify object exists
        assert s3_client.exists(s3_path)

        # Download and verify content
        with s3_client.open_for_read(s3_path, encoding='utf-8') as reader:
            downloaded_content = reader.read()

        assert downloaded_content.strip() == test_data['csv_content']

    def test_large_file_multipart_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test multipart upload with large file."""
        test_key = f"forklift/integration-test/large-multipart-test.csv"
        s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
        cleanup_s3_objects.append(s3_path)

        # Create large CSV content (> 5MB to trigger multipart)
        header = "id,name,description,data\n"
        large_content = header

        # Generate enough data to exceed part size
        for i in range(50000):  # Should create ~5MB+ file
            large_content += f"{i},name_{i},description for item {i},{'x' * 100}\n"

        # Upload large content
        with s3_client.open_for_write(s3_path, encoding='utf-8') as writer:
            writer.write(large_content)

        # Verify upload succeeded
        assert s3_client.exists(s3_path)

        # Verify size
        size = s3_client.get_size(s3_path)
        assert size > 5 * 1024 * 1024  # Should be > 5MB

    def test_unified_io_handler_s3_operations(self, s3_config, test_data, cleanup_s3_objects):
        """Test UnifiedIOHandler with real S3 operations."""
        io_handler = UnifiedIOHandler()

        test_key = f"forklift/integration-test/unified-handler-test.csv"
        s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
        cleanup_s3_objects.append(s3_path)

        # Write using unified handler
        with io_handler.open_for_write(s3_path, encoding='utf-8') as writer:
            writer.write(test_data['csv_content'])

        # Verify exists
        assert io_handler.exists(s3_path)

        # Get size
        size = io_handler.get_size(s3_path)
        assert size > 0

        # Read CSV using unified handler
        rows = list(io_handler.csv_reader(s3_path, encoding='utf-8'))

        assert len(rows) == 5  # Header + 4 data rows
        assert rows[0] == ['name', 'age', 'city', 'salary']
        assert rows[1] == ['Alice', '25', 'New York', '75000']

    def test_forklift_core_s3_to_s3_processing(self, s3_config, test_data, cleanup_s3_objects):
        """Test ForkliftCore processing S3 input to S3 output."""
        # Use consistent paths instead of timestamps
        input_key = f"forklift/integration-test/core-processing-input.csv"
        schema_key = f"forklift/integration-test/core-processing-schema.json"
        output_prefix = f"forklift/integration-test/core-processing-output/"

        input_s3_path = f"s3://{s3_config['test_bucket']}/{input_key}"
        schema_s3_path = f"s3://{s3_config['test_bucket']}/{schema_key}"
        output_s3_path = f"s3://{s3_config['test_bucket']}/{output_prefix}"

        cleanup_s3_objects.extend([input_s3_path, schema_s3_path])

        # Upload test data and schema to S3

        with UnifiedIOHandler().open_for_write(input_s3_path, encoding='utf-8') as writer:
            writer.write(test_data['csv_content'])

        with UnifiedIOHandler().open_for_write(schema_s3_path, encoding='utf-8') as writer:
            json.dump(test_data['schema_content'], writer, indent=2)

        # Configure ForkliftCore for S3 to S3 processing
        config = ImportConfig(
            input_path=input_s3_path,
            output_path=output_s3_path,
            schema_file=schema_s3_path,
            header_mode=HeaderMode.PRESENT,
            batch_size=2  # Small batch size for testing
        )

        # Process the data
        core = ForkliftCore(config)
        results = core.process_csv()

        # Verify results
        assert results.total_rows == 4  # 4 data rows (excluding header)
        assert results.valid_rows > 0
        assert len(results.output_files) > 0

        # Verify output files exist in S3
        for output_file in results.output_files:
            assert UnifiedIOHandler().exists(output_file)
            cleanup_s3_objects.append(output_file)

        # Verify manifest and metadata files
        if results.manifest_file:
            assert UnifiedIOHandler().exists(results.manifest_file)
            cleanup_s3_objects.append(results.manifest_file)

        if results.metadata_file:
            assert UnifiedIOHandler().exists(results.metadata_file)
            cleanup_s3_objects.append(results.metadata_file)

    def test_mixed_local_s3_processing(self, s3_config, test_data, cleanup_s3_objects):
        """Test processing with local input and S3 output."""
        # Use consistent path instead of timestamp
        output_prefix = f"forklift/integration-test/mixed-output/"
        output_s3_path = f"s3://{s3_config['test_bucket']}/{output_prefix}"

        # Create temporary local input file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write(test_data['csv_content'])
            local_input_path = tmp_file.name

        try:
            # Configure ForkliftCore for local to S3 processing
            config = ImportConfig(
                input_path=local_input_path,
                output_path=output_s3_path,
                header_mode=HeaderMode.PRESENT
            )

            # Process the data
            core = ForkliftCore(config)
            results = core.process_csv()

            # Verify results
            assert results.total_rows == 4
            assert results.valid_rows == 4
            assert len(results.output_files) > 0

            # Verify output files exist in S3
            io_handler = UnifiedIOHandler()
            for output_file in results.output_files:
                assert io_handler.exists(output_file)
                cleanup_s3_objects.append(output_file)

            # Clean up additional files
            if results.manifest_file:
                cleanup_s3_objects.append(results.manifest_file)
            if results.metadata_file:
                cleanup_s3_objects.append(results.metadata_file)

        finally:
            # Clean up local temp file
            Path(local_input_path).unlink(missing_ok=True)

    def test_error_handling_nonexistent_bucket(self, s3_config):
        """Test error handling with non-existent bucket."""
        client = S3StreamingClient(
            aws_access_key_id=s3_config['aws_access_key_id'],
            aws_secret_access_key=s3_config['aws_secret_access_key'],
            region_name=s3_config['region_name'],
            endpoint_url=s3_config['endpoint_url']
        )

        nonexistent_path = "s3://this-bucket-should-not-exist-12345/test.csv"

        # Should return False for exists check
        assert not client.exists(nonexistent_path)

        # Should raise error for actual operations
        from botocore.exceptions import ClientError
        with pytest.raises(ClientError):
            client.get_size(nonexistent_path)

    def test_s3_csv_with_special_characters(self, s3_client, s3_config, cleanup_s3_objects):
        """Test S3 operations with special characters in data."""
        test_key = f"forklift/integration-test/special-chars-{int(time.time())}.csv"
        s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
        cleanup_s3_objects.append(s3_path)

        # CSV with special characters, quotes, commas
        special_content = '''name,description,notes
"Smith, John","Senior Developer, Team Lead","Works on ""critical"" projects"
"José García","Data Scientist","Specializes in ML & AI"
"李小明","Software Engineer","Full-stack developer"'''

        # Upload
        with s3_client.open_for_write(s3_path, encoding='utf-8') as writer:
            writer.write(special_content)

        # Read back using CSV reader
        io_handler = UnifiedIOHandler()
        rows = list(io_handler.csv_reader(s3_path, encoding='utf-8'))

        assert len(rows) == 4  # Header + 3 data rows
        assert rows[1][0] == "Smith, John"  # Comma preserved
        assert rows[1][1] == "Senior Developer, Team Lead"
        assert rows[1][2] == 'Works on "critical" projects'  # Quotes handled
        assert rows[2][0] == "José García"  # Unicode characters
        assert rows[3][0] == "李小明"  # Unicode characters
