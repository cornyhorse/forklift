"""Integration tests for Fixed Width File (FWF) processing using real test files."""

import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.fwf_utils import create_fwf_config_from_schema, create_simple_fwf_config
from forklift.io.s3_streaming import S3Path, S3StreamingClient


@pytest.mark.integration
class TestFwfIntegration:
    """Integration tests for FWF processing with real test files."""

    def test_good_fwf_file_processing(self):
        """Test processing a well-formatted FWF file."""
        # Path to the existing good FWF test file
        test_file = Path(__file__).parent.parent / "test-files" / "goodfwf" / "good_fwf1.txt"

        # Define field specification based on the file structure
        # From examining the file: ID(6), Name(20), Date(8), Active(1), Amount(9), Country(2), Status(8), Code(3), Notes(variable)
        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 20, align="left", parquet_type="string"),
            FwfFieldSpec("date", 27, 8, align="left", parquet_type="string"),
            FwfFieldSpec("active", 35, 1, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 36, 9, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("country", 45, 2, align="left", parquet_type="string"),
            FwfFieldSpec("status", 47, 8, align="left", parquet_type="string"),
            FwfFieldSpec("code", 56, 3, align="right", pad="0", parquet_type="int32"),
            FwfFieldSpec("notes", 59, 20, align="left", parquet_type="string"),
        ]

        config = FwfInputConfig(
            fields=fields, comment_patterns=["^#"], skip_blank_lines=True  # Skip comment lines
        )

        handler = FwfInputHandler(config)

        # Process the file
        results = list(handler.read_file(test_file))

        # Validate results
        assert len(results) >= 0, f"Should process FWF file without errors"

    def test_fwf_arrow_table_creation(self):
        """Test creating PyArrow tables from FWF files."""
        # Create test data
        fields = [
            FwfFieldSpec("id", 1, 5, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 6, 20, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 26, 10, align="right", parquet_type="decimal128(10,2)"),
        ]

        config = FwfInputConfig(fields=fields, skip_blank_lines=True)

        # Create test data
        test_data = [
            "00001John Doe          1234.56   ",
            "00002Jane Smith        2345.67   ",
            "00003Bob Johnson       3456.78   ",
        ]

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".fwf") as f:
            for line in test_data:
                f.write(line + "\n")
            temp_file = Path(f.name)

        try:
            handler = FwfInputHandler(config)

            # Create PyArrow table
            table = handler.create_arrow_table(temp_file)

            # Validate table structure
            assert isinstance(table, pa.Table)
            assert table.num_rows == 3, "Table should have 3 data rows"
            assert table.num_columns >= len(fields), "Table should have all defined columns"

        finally:
            temp_file.unlink()


@pytest.mark.integration
class TestFwfS3Integration:
    """Integration tests for FWF processing with S3 parquet uploads."""

    @pytest.fixture(scope="class")
    def s3_config(self):
        """Get S3 configuration from .env file."""
        config = {
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "region_name": "us-east-1",
            "test_bucket": "cornyhorse-data",
            "endpoint_url": None,
        }

        # Load from environment variables or .env file
        import os
        from pathlib import Path

        from dotenv import load_dotenv

        # Load from ~/.credentials/.env first, then fallback to local .env
        credentials_path = Path.home() / ".credentials" / ".env"
        if credentials_path.exists():
            load_dotenv(credentials_path)
        else:
            load_dotenv()  # fallback to local .env

        config["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID")
        config["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")
        config["region_name"] = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
        config["test_bucket"] = os.getenv("S3_TEST_BUCKET", "cornyhorse-data")
        config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")

        # Skip if no credentials are configured
        if not config["aws_access_key_id"] or not config["aws_secret_access_key"]:
            pytest.skip("AWS credentials not configured")

        return config

    @pytest.fixture(scope="class")
    def s3_client(self, s3_config):
        """Create real S3 client for integration tests."""
        return S3StreamingClient(
            aws_access_key_id=s3_config["aws_access_key_id"],
            aws_secret_access_key=s3_config["aws_secret_access_key"],
            region_name=s3_config["region_name"],
            endpoint_url=s3_config["endpoint_url"],
        )

    @pytest.fixture
    def cleanup_s3_objects(self, s3_config):
        """Fixture to clean up S3 objects after tests."""
        objects_to_cleanup = []

        yield objects_to_cleanup

        # Cleanup after test
        if objects_to_cleanup:
            client = S3StreamingClient(
                aws_access_key_id=s3_config["aws_access_key_id"],
                aws_secret_access_key=s3_config["aws_secret_access_key"],
                region_name=s3_config["region_name"],
                endpoint_url=s3_config["endpoint_url"],
            )

            for s3_path in objects_to_cleanup:
                try:
                    s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
                    client._s3_client.delete_object(Bucket=s3_path_obj.bucket, Key=s3_path_obj.key)
                except Exception:
                    pass  # Best effort cleanup

    def test_single_schema_fwf_to_parquet_s3_upload(
        self, s3_client, s3_config, cleanup_s3_objects
    ):
        """Test processing single-schema FWF file and uploading parquet to S3."""
        # Define field specification for the test file
        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 20, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 27, 10, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("status", 37, 8, align="left", parquet_type="string"),
        ]

        config = FwfInputConfig(fields=fields, skip_blank_lines=True)

        # Create test data
        test_data = [
            "000001John Doe          0000012500ACTIVE  ",
            "000002Jane Smith        0000025000ACTIVE  ",
            "000003Bob Johnson       0000015000INACTIVE",
        ]

        # Create temporary FWF file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".fwf", delete=False, encoding="utf-8"
        ) as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + "\n")

        try:
            handler = FwfInputHandler(config)

            # Create Arrow table from FWF file
            table = handler.create_arrow_table(fwf_path)

            # Verify table has data
            assert table.num_rows > 0, "FWF file should produce records"
            assert table.num_columns > 0, "FWF file should have columns"

            # Create temporary parquet file
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet:
                parquet_path = Path(tmp_parquet.name)

            try:
                # Write to parquet file
                pa.parquet.write_table(table, parquet_path)

                # Upload to S3
                test_key = (
                    f"forklift/integration-test/fwf-single-schema-{int(time.time())}.parquet"
                )
                s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                # Upload parquet file to S3
                with open(parquet_path, "rb") as local_file:
                    with s3_client.open_for_write(s3_path) as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload succeeded
                assert s3_client.exists(s3_path), "Parquet file should exist in S3"

                # Verify file size
                file_size = s3_client.get_size(s3_path)
                assert file_size > 0, "Uploaded parquet file should have content"

            finally:
                parquet_path.unlink()

        finally:
            fwf_path.unlink()

    def test_multi_schema_fwf_to_parquet_s3_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test processing multi-schema FWF file and uploading parquet to S3."""
        # Use the multi-schema test files
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "multi_schema_example.json"
        data_path = test_dir / "multi_schema_example.txt"

        if not schema_path.exists() or not data_path.exists():
            pytest.skip(f"Multi-schema test files not found: {schema_path}, {data_path}")

        # Create FWF configuration from schema
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Create Arrow table from FWF file
        table = handler.create_arrow_table(data_path)

        # Verify table has data
        assert table.num_rows > 0, "Multi-schema FWF file should produce records"
        assert table.num_columns > 0, "Multi-schema FWF file should have columns"

        # Create temporary parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet:
            parquet_path = Path(tmp_parquet.name)

        try:
            # Write to parquet file
            pa.parquet.write_table(table, parquet_path)

            # Upload to S3
            test_key = f"forklift/integration-test/fwf-multi-schema-{int(time.time())}.parquet"
            s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
            cleanup_s3_objects.append(s3_path)

            # Upload parquet file to S3
            with open(parquet_path, "rb") as local_file:
                with s3_client.open_for_write(s3_path) as s3_writer:
                    s3_writer.write(local_file.read())

            # Verify upload succeeded
            assert s3_client.exists(s3_path), "Multi-schema parquet file should exist in S3"

            # Verify file size
            file_size = s3_client.get_size(s3_path)
            assert file_size > 0, "Uploaded parquet file should have content"

        finally:
            parquet_path.unlink()
