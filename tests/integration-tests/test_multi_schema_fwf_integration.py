"""Integration tests for multi-schema FWF processing with S3 uploads."""

import pytest
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import pandas as pd

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from forklift.inputs.fwf_utils import create_fwf_config_from_schema
from forklift.io.s3_streaming import S3StreamingClient, S3Path


@pytest.mark.integration
@pytest.mark.s3
class TestMultiSchemaFwfS3Integration:
    """Integration tests for multi-schema FWF processing with S3 parquet uploads."""

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
    def cleanup_s3_objects(self, s3_config):
        """Fixture to clean up S3 objects after tests."""
        objects_to_cleanup = []

        yield objects_to_cleanup

        # Cleanup after test
        if objects_to_cleanup:
            client = S3StreamingClient(
                aws_access_key_id=s3_config['aws_access_key_id'],
                aws_secret_access_key=s3_config['aws_secret_access_key'],
                region_name=s3_config['region_name'],
                endpoint_url=s3_config['endpoint_url']
            )

            for s3_path in objects_to_cleanup:
                try:
                    s3_path_obj = S3Path(s3_path) if isinstance(s3_path, str) else s3_path
                    client._s3_client.delete_object(
                        Bucket=s3_path_obj.bucket,
                        Key=s3_path_obj.key
                    )
                except Exception:
                    pass  # Best effort cleanup

    def test_banking_multi_schema_fwf_s3_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test banking multi-schema FWF processing and S3 upload."""
        # Use banking multi-schema test files
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "banking_multi_schema.json"
        data_path = test_dir / "banking_multi_schema.txt"

        if not schema_path.exists() or not data_path.exists():
            pytest.skip(f"Banking multi-schema test files not found: {schema_path}, {data_path}")

        # Create FWF configuration from schema
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Process the FWF file
        table = handler.create_arrow_table(data_path)

        # Verify table structure
        assert table.num_rows > 0, "Banking FWF should produce records"
        df = table.to_pandas()

        # Verify different record types exist
        if 'record_type' in df.columns:
            record_types = df['record_type'].unique()
            assert len(record_types) > 1, "Should have multiple record types"

        # Upload to S3
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            pa.parquet.write_table(table, tmp_path)

            test_key = f"forklift/integration-test/banking-multi-schema-{int(time.time())}.parquet"
            s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
            cleanup_s3_objects.append(s3_path)

            with open(tmp_path, 'rb') as local_file:
                with s3_client.open_for_write(s3_path, mode='wb') as s3_writer:
                    s3_writer.write(local_file.read())

            assert s3_client.exists(s3_path), "Banking parquet should exist in S3"
            assert s3_client.get_size(s3_path) > 0, "Banking parquet should have content"

        finally:
            tmp_path.unlink()

    def test_retail_multi_schema_fwf_s3_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test retail multi-schema FWF processing and S3 upload."""
        # Use retail multi-schema test files
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "retail_multi_schema.json"
        data_path = test_dir / "retail_multi_schema.txt"

        if not schema_path.exists() or not data_path.exists():
            pytest.skip(f"Retail multi-schema test files not found: {schema_path}, {data_path}")

        # Create FWF configuration from schema
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Process the FWF file
        table = handler.create_arrow_table(data_path)

        # Verify table structure
        assert table.num_rows > 0, "Retail FWF should produce records"
        df = table.to_pandas()

        # Verify different record types exist
        if 'record_type' in df.columns:
            record_types = df['record_type'].unique()
            assert len(record_types) > 1, "Should have multiple record types"

        # Upload to S3
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            pa.parquet.write_table(table, tmp_path)

            test_key = f"forklift/integration-test/retail-multi-schema-{int(time.time())}.parquet"
            s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
            cleanup_s3_objects.append(s3_path)

            with open(tmp_path, 'rb') as local_file:
                with s3_client.open_for_write(s3_path, mode='wb') as s3_writer:
                    s3_writer.write(local_file.read())

            assert s3_client.exists(s3_path), "Retail parquet should exist in S3"
            assert s3_client.get_size(s3_path) > 0, "Retail parquet should have content"

        finally:
            tmp_path.unlink()

    def test_custom_multi_schema_fwf_s3_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test custom multi-schema FWF creation and S3 upload."""
        # Create a custom multi-schema configuration
        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")

        conditional_schemas = [
            # Customer records
            FwfConditionalSchema("C", "Customer Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("customer_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("customer_name", 10, 30, align="left", parquet_type="string"),
                FwfFieldSpec("registration_date", 40, 8, parquet_type="string"),
                FwfFieldSpec("status", 48, 8, align="left", parquet_type="string")
            ]),
            # Order records
            FwfConditionalSchema("O", "Order Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("order_id", 2, 10, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("customer_id", 12, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("order_date", 20, 8, parquet_type="string"),
                FwfFieldSpec("total_amount", 28, 12, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("currency", 40, 3, parquet_type="string")
            ]),
            # Product records
            FwfConditionalSchema("P", "Product Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("product_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("product_name", 10, 25, align="left", parquet_type="string"),
                FwfFieldSpec("category", 35, 15, align="left", parquet_type="string"),
                FwfFieldSpec("unit_price", 50, 10, align="right", pad="0", parquet_type="int64")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
            skip_blank_lines=True
        )

        # Create test data with multiple record types
        test_data = [
            "C00001234Customer Alpha Corp        20250101ACTIVE  ",
            "O0000567890000123420250115000125000USD",
            "P00000001Widget A                Electronics  000002500",
            "C00002345Customer Beta Ltd          20250102ACTIVE  ",
            "O0000567900000234520250116000087500USD",
            "P00000002Widget B                Electronics  000003750",
            "C00003456Customer Gamma Inc         20250103INACTIVE",
            "O0000568000000345620250117000156200USD",
            "P00000003Service Pack A            Services     000010000"
        ]

        # Create temporary FWF file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + '\n')

        try:
            handler = FwfInputHandler(config)

            # Process the multi-schema FWF file
            table = handler.create_arrow_table(fwf_path)

            # Verify table structure
            assert table.num_rows == len(test_data), f"Should have {len(test_data)} records"

            df = table.to_pandas()
            record_types = df['record_type'].unique()
            expected_types = {'C', 'O', 'P'}
            assert set(record_types) == expected_types, f"Should have record types {expected_types}"

            # Verify each record type has expected fields
            customers = df[df['record_type'] == 'C']
            orders = df[df['record_type'] == 'O']
            products = df[df['record_type'] == 'P']

            assert len(customers) == 3, "Should have 3 customer records"
            assert len(orders) == 3, "Should have 3 order records"
            assert len(products) == 3, "Should have 3 product records"

            # Verify specific field values
            assert 'customer_name' in customers.columns
            assert 'total_amount' in orders.columns
            assert 'product_name' in products.columns

            # Upload to S3
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet_file:
                parquet_path = Path(tmp_parquet_file.name)

            try:
                pa.parquet.write_table(table, parquet_path)

                test_key = f"forklift/integration-test/custom-multi-schema-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                with open(parquet_path, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path, mode='wb') as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload
                assert s3_client.exists(s3_path), "Custom multi-schema parquet should exist in S3"
                assert s3_client.get_size(s3_path) > 0, "Custom multi-schema parquet should have content"

                # Download and verify content
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as download_file:
                    download_path = Path(download_file.name)

                try:
                    with s3_client.open_for_read(s3_path, mode='rb') as s3_reader:
                        with open(download_path, 'wb') as local_writer:
                            local_writer.write(s3_reader.read())

                    # Verify downloaded content
                    downloaded_table = pa.parquet.read_table(download_path)
                    downloaded_df = downloaded_table.to_pandas()

                    assert downloaded_df['record_type'].nunique() == 3, "Should preserve all record types"
                    assert len(downloaded_df) == len(test_data), "Should preserve all records"

                finally:
                    download_path.unlink()

            finally:
                parquet_path.unlink()

        finally:
            fwf_path.unlink()

    def test_hierarchical_multi_schema_fwf_s3_upload(self, s3_client, s3_config, cleanup_s3_objects):
        """Test hierarchical multi-schema FWF with parent-child relationships."""
        # Create hierarchical schema (Invoice -> Line Items -> Taxes)
        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")

        conditional_schemas = [
            # Invoice header
            FwfConditionalSchema("I", "Invoice Header", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("invoice_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("customer_id", 10, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("invoice_date", 18, 8, parquet_type="string"),
                FwfFieldSpec("due_date", 26, 8, parquet_type="string"),
                FwfFieldSpec("currency", 34, 3, parquet_type="string")
            ]),
            # Line items
            FwfConditionalSchema("L", "Line Item", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("invoice_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("line_number", 10, 3, align="right", pad="0", parquet_type="int32"),
                FwfFieldSpec("product_code", 13, 12, align="left", parquet_type="string"),
                FwfFieldSpec("quantity", 25, 5, align="right", pad="0", parquet_type="int32"),
                FwfFieldSpec("unit_price", 30, 10, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("line_total", 40, 10, align="right", pad="0", parquet_type="int64")
            ]),
            # Tax details
            FwfConditionalSchema("T", "Tax Detail", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("invoice_id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("tax_type", 10, 8, align="left", parquet_type="string"),
                FwfFieldSpec("tax_rate", 18, 5, align="right", pad="0", parquet_type="int32"),
                FwfFieldSpec("tax_amount", 23, 10, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("tax_base", 33, 10, align="right", pad="0", parquet_type="int64")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
            skip_blank_lines=True
        )

        # Create hierarchical test data
        test_data = [
            # Invoice 1
            "I000010010000123420250115202502150USD",
            "L00001001001WIDGET_A    000050000100000000500000",
            "L00001001002SERVICE_B   000020000250000000500000",
            "T00001001SALES   100000010000000100000",
            "T00001001VAT     150000015000000100000",
            # Invoice 2
            "I000020020000567820250116202502160EUR",
            "L00002002001GADGET_X    000030000333000000999000",
            "L00002002002WARRANTY    000010001000000001000000",
            "T00002002SALES   080000080000000100000",
            "T00002002VAT     200000200000000100000"
        ]

        # Create temporary FWF file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + '\n')

        try:
            handler = FwfInputHandler(config)

            # Process the hierarchical FWF file
            table = handler.create_arrow_table(fwf_path)

            # Verify table structure
            assert table.num_rows == len(test_data), f"Should have {len(test_data)} records"

            df = table.to_pandas()
            record_types = df['record_type'].unique()
            expected_types = {'I', 'L', 'T'}
            assert set(record_types) == expected_types, f"Should have record types {expected_types}"

            # Verify hierarchical relationships
            invoices = df[df['record_type'] == 'I']
            line_items = df[df['record_type'] == 'L']
            taxes = df[df['record_type'] == 'T']

            assert len(invoices) == 2, "Should have 2 invoice headers"
            assert len(line_items) == 4, "Should have 4 line items"
            assert len(taxes) == 4, "Should have 4 tax records"

            # Verify invoice IDs are consistent across record types
            invoice_ids = set(invoices['invoice_id'])
            line_item_invoice_ids = set(line_items['invoice_id'])
            tax_invoice_ids = set(taxes['invoice_id'])

            assert invoice_ids == line_item_invoice_ids, "Line items should reference valid invoices"
            assert invoice_ids == tax_invoice_ids, "Tax records should reference valid invoices"

            # Upload to S3
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet_file:
                parquet_path = Path(tmp_parquet_file.name)

            try:
                pa.parquet.write_table(table, parquet_path)

                test_key = f"forklift/integration-test/hierarchical-multi-schema-{int(time.time())}.parquet"
                s3_path = f"s3://{s3_config['test_bucket']}/{test_key}"
                cleanup_s3_objects.append(s3_path)

                with open(parquet_path, 'rb') as local_file:
                    with s3_client.open_for_write(s3_path, mode='wb') as s3_writer:
                        s3_writer.write(local_file.read())

                # Verify upload
                assert s3_client.exists(s3_path), "Hierarchical parquet should exist in S3"
                assert s3_client.get_size(s3_path) > 0, "Hierarchical parquet should have content"

                # Download and verify hierarchical relationships are preserved
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as download_file:
                    download_path = Path(download_file.name)

                try:
                    with s3_client.open_for_read(s3_path, mode='rb') as s3_reader:
                        with open(download_path, 'wb') as local_writer:
                            local_writer.write(s3_reader.read())

                    # Verify downloaded hierarchical structure
                    downloaded_table = pa.parquet.read_table(download_path)
                    downloaded_df = downloaded_table.to_pandas()

                    # Check hierarchical integrity
                    d_invoices = downloaded_df[downloaded_df['record_type'] == 'I']
                    d_lines = downloaded_df[downloaded_df['record_type'] == 'L']
                    d_taxes = downloaded_df[downloaded_df['record_type'] == 'T']

                    assert len(d_invoices) == 2, "Should preserve invoice headers"
                    assert len(d_lines) == 4, "Should preserve line items"
                    assert len(d_taxes) == 4, "Should preserve tax records"

                    # Verify relationships
                    d_invoice_ids = set(d_invoices['invoice_id'])
                    d_line_invoice_ids = set(d_lines['invoice_id'])
                    d_tax_invoice_ids = set(d_taxes['invoice_id'])

                    assert d_invoice_ids == d_line_invoice_ids, "Downloaded line items should reference valid invoices"
                    assert d_invoice_ids == d_tax_invoice_ids, "Downloaded tax records should reference valid invoices"

                finally:
                    download_path.unlink()

            finally:
                parquet_path.unlink()

        finally:
            fwf_path.unlink()


@pytest.mark.integration
class TestMultiSchemaFwfLocal:
    """Local integration tests for multi-schema FWF processing (no S3)."""

    def test_example_multi_schema_processing(self):
        """Test the corrected multi-schema example file."""
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "multi_schema_example.json"
        data_path = test_dir / "multi_schema_example.txt"

        if not schema_path.exists() or not data_path.exists():
            pytest.skip(f"Multi-schema example files not found")

        # Create FWF configuration from schema
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Process the file
        records = list(handler.read_file(data_path))

        # Verify we got the expected number of records
        assert len(records) == 9, f"Expected 9 records, got {len(records)}"

        # Separate records by type
        headers = [r for r in records if r['record_type'] == 'H']
        details = [r for r in records if r['record_type'] == 'D']
        trailers = [r for r in records if r['record_type'] == 'T']

        assert len(headers) == 2, "Should have 2 header records"
        assert len(details) == 5, "Should have 5 detail records"
        assert len(trailers) == 2, "Should have 2 trailer records"

        # Verify field values are correctly parsed with the fixed schema
        detail1 = details[0]
        assert detail1['transaction_id'] == 1
        assert detail1['amount_cents'] == 12500  # This should now work with corrected schema
        assert detail1['quantity'] == 10

    def test_large_multi_schema_processing(self):
        """Test processing large multi-schema FWF files."""
        # Create large multi-schema test data
        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")

        conditional_schemas = [
            FwfConditionalSchema("A", "Type A", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("data", 10, 20, align="left", parquet_type="string")
            ]),
            FwfConditionalSchema("B", "Type B", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("id", 2, 8, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("amount", 10, 12, align="right", pad="0", parquet_type="int64")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
            skip_blank_lines=True
        )

        # Generate large test data
        test_data = []
        for i in range(5000):
            if i % 2 == 0:
                # Type A record
                line = f"A{i:08d}{'Data_' + str(i):20}"
            else:
                # Type B record
                line = f"B{i:08d}{(i * 100):012d}"
            test_data.append(line)

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fwf', delete=False, encoding='utf-8') as tmp_file:
            fwf_path = Path(tmp_file.name)
            for line in test_data:
                tmp_file.write(line + '\n')

        try:
            handler = FwfInputHandler(config)

            # Process large file
            import time
            start_time = time.time()

            table = handler.create_arrow_table(fwf_path)

            end_time = time.time()
            processing_time = end_time - start_time

            # Verify results
            assert table.num_rows == 5000, "Should process all 5000 records"

            df = table.to_pandas()
            record_types = df['type'].unique()
            assert set(record_types) == {'A', 'B'}, "Should have both record types"

            type_a_count = len(df[df['type'] == 'A'])
            type_b_count = len(df[df['type'] == 'B'])

            assert type_a_count == 2500, "Should have 2500 Type A records"
            assert type_b_count == 2500, "Should have 2500 Type B records"

            print(f"Processed {table.num_rows} multi-schema records in {processing_time:.2f} seconds")
            assert processing_time < 15.0, f"Processing should be under 15 seconds, took {processing_time:.2f}s"

        finally:
            fwf_path.unlink()
