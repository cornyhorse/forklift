"""Integration tests for Multi-Schema Fixed Width File (FWF) processing."""

import pytest
import tempfile
from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import pandas as pd

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from forklift.inputs.fwf_utils import create_fwf_config_from_schema


@pytest.mark.integration
class TestMultiSchemaFwfIntegration:
    """Integration tests for multi-schema FWF processing with real test files."""

    def test_banking_multi_schema_processing(self):
        """Test processing a banking file with H/D/S/T record types."""
        # Use the banking multi-schema test files
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "banking_multi_schema.json"
        data_path = test_dir / "banking_multi_schema.txt"

        # Load configuration and create handler
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Process the file
        records = list(handler.read_file(data_path))

        # Verify total record count
        assert len(records) == 13, f"Expected 13 records, got {len(records)}"

        # Separate records by type
        by_type = {}
        for record in records:
            rtype = record['record_type']
            if rtype not in by_type:
                by_type[rtype] = []
            by_type[rtype].append(record)

        # Verify record type counts
        assert len(by_type['H']) == 2, f"Expected 2 header records, got {len(by_type['H'])}"
        assert len(by_type['D']) == 8, f"Expected 8 detail records, got {len(by_type['D'])}"
        assert len(by_type['S']) == 2, f"Expected 2 summary records, got {len(by_type['S'])}"
        assert len(by_type['T']) == 1, f"Expected 1 trailer record, got {len(by_type['T'])}"

        # Validate header record structure and data
        header1 = by_type['H'][0]
        expected_header_fields = {'record_type', 'batch_date', 'batch_id', 'batch_name', 'institution_name'}
        actual_header_fields = {k for k in header1.keys() if not k.startswith('__')}
        assert actual_header_fields == expected_header_fields

        assert header1['record_type'] == 'H'
        assert header1['batch_date'] == '20241201'
        assert header1['batch_id'] == 1
        assert header1['batch_name'] == 'DAILY_BATCH'
        assert header1['institution_name'] == 'First National Bank'

        # Validate detail record structure and data
        detail1 = by_type['D'][0]
        expected_detail_fields = {'record_type', 'transaction_id', 'account_number', 'transaction_type',
                                 'amount_cents', 'currency', 'transaction_date', 'transaction_time', 'channel'}
        actual_detail_fields = {k for k in detail1.keys() if not k.startswith('__')}
        assert actual_detail_fields == expected_detail_fields

        assert detail1['record_type'] == 'D'
        assert detail1['transaction_id'] == 1
        assert detail1['account_number'] == '12345'
        assert detail1['transaction_type'] == 'TRANSFER'
        assert detail1['amount_cents'] == 25000
        assert detail1['currency'] == 'USD'
        assert detail1['channel'] == 'ONLINE'

        # Validate summary record structure and data
        summary1 = by_type['S'][0]
        expected_summary_fields = {'record_type', 'summary_date', 'transaction_count',
                                  'total_amount_cents', 'currency', 'summary_notes'}
        actual_summary_fields = {k for k in summary1.keys() if not k.startswith('__')}
        assert actual_summary_fields == expected_summary_fields

        assert summary1['record_type'] == 'S'
        assert summary1['summary_date'] == '20241201'
        assert summary1['transaction_count'] == 3
        assert summary1['total_amount_cents'] == 31500
        assert summary1['currency'] == 'USD'
        assert summary1['summary_notes'] == 'Transaction Summary'

        # Validate trailer record structure and data
        trailer = by_type['T'][0]
        expected_trailer_fields = {'record_type', 'trailer_date', 'total_records',
                                  'grand_total_cents', 'currency', 'process_date', 'status_message'}
        actual_trailer_fields = {k for k in trailer.keys() if not k.startswith('__')}
        assert actual_trailer_fields == expected_trailer_fields

        assert trailer['record_type'] == 'T'
        assert trailer['trailer_date'] == '20241202'
        assert trailer['total_records'] == 7
        assert trailer['grand_total_cents'] == 124000
        assert trailer['currency'] == 'USD'
        assert trailer['status_message'] == 'End of Processing'

    def test_retail_multi_schema_processing(self):
        """Test processing a retail inventory file with H/P/I/A/T record types."""
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "retail_multi_schema.json"
        data_path = test_dir / "retail_multi_schema.txt"

        # Load configuration and create handler
        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Process the file
        records = list(handler.read_file(data_path))

        # Verify total record count
        assert len(records) == 10, f"Expected 10 records, got {len(records)}"

        # Separate records by type
        by_type = {}
        for record in records:
            rtype = record['record_type']
            if rtype not in by_type:
                by_type[rtype] = []
            by_type[rtype].append(record)

        # Verify record type counts
        assert len(by_type['H']) == 1, f"Expected 1 header record, got {len(by_type['H'])}"
        assert len(by_type['P']) == 3, f"Expected 3 product records, got {len(by_type['P'])}"
        assert len(by_type['I']) == 3, f"Expected 3 inventory records, got {len(by_type['I'])}"
        assert len(by_type['A']) == 2, f"Expected 2 adjustment records, got {len(by_type['A'])}"
        assert len(by_type['T']) == 1, f"Expected 1 trailer record, got {len(by_type['T'])}"

        # Validate product record structure and data
        product1 = by_type['P'][0]
        expected_product_fields = {'record_type', 'product_id', 'product_code', 'category',
                                  'unit_price_cents', 'description'}
        actual_product_fields = {k for k in product1.keys() if not k.startswith('__')}
        assert actual_product_fields == expected_product_fields

        assert product1['record_type'] == 'P'
        assert product1['product_id'] == 1
        assert product1['product_code'] == 'WIDGET_A'
        assert product1['category'] == 'Electronics'
        assert product1['unit_price_cents'] == 12500
        assert product1['description'] == 'Regular Item'

        # Validate inventory record structure and data
        inventory1 = by_type['I'][0]
        expected_inventory_fields = {'record_type', 'product_id', 'quantity_on_hand', 'quantity_reserved',
                                    'quantity_available', 'location_code', 'last_updated', 'status'}
        actual_inventory_fields = {k for k in inventory1.keys() if not k.startswith('__')}
        assert actual_inventory_fields == expected_inventory_fields

        assert inventory1['record_type'] == 'I'
        assert inventory1['product_id'] == 1
        assert inventory1['quantity_on_hand'] == 50
        assert inventory1['quantity_reserved'] == 10
        assert inventory1['quantity_available'] == 45
        assert inventory1['location_code'] == 'STK'
        assert inventory1['status'] == 'Initial Stock'

        # Validate adjustment record structure and data
        adjustment1 = by_type['A'][0]
        expected_adjustment_fields = {'record_type', 'product_id', 'adjustment_qty', 'adjustment_value_cents',
                                     'adjustment_type', 'adjustment_date', 'reason_code'}
        actual_adjustment_fields = {k for k in adjustment1.keys() if not k.startswith('__')}
        assert actual_adjustment_fields == expected_adjustment_fields

        assert adjustment1['record_type'] == 'A'
        assert adjustment1['product_id'] == 1
        assert adjustment1['adjustment_qty'] == 1
        assert adjustment1['adjustment_value_cents'] == 45
        assert adjustment1['adjustment_type'] == 'ADJ+'
        assert adjustment1['reason_code'] == 'Damaged Return'

    def test_multi_schema_arrow_schema_generation(self):
        """Test PyArrow schema generation for multi-schema files."""
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "banking_multi_schema.json"

        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Generate PyArrow schema
        arrow_schema = handler.get_arrow_schema()

        # Verify the schema contains all unique fields from all conditional schemas
        field_names = {field.name for field in arrow_schema}

        # Fields that should be present from all schemas
        expected_fields = {
            'record_type',  # Common to all
            'batch_date', 'batch_id', 'batch_name', 'institution_name',  # Header fields
            'transaction_id', 'account_number', 'transaction_type', 'amount_cents', 'currency',
            'transaction_date', 'transaction_time', 'channel',  # Detail fields
            'summary_date', 'transaction_count', 'total_amount_cents', 'summary_notes',  # Summary fields
            'trailer_date', 'total_records', 'grand_total_cents', 'process_date', 'status_message',  # Trailer fields
            '__line_number__', '__source_file__'  # Metadata fields
        }

        assert field_names == expected_fields, f"Schema fields mismatch. Missing: {expected_fields - field_names}, Extra: {field_names - expected_fields}"

        # Verify data types are correct
        field_types = {field.name: field.type for field in arrow_schema}

        # Check some key field types
        assert field_types['record_type'] == pa.string()
        assert field_types['batch_id'] == pa.int64()
        assert field_types['transaction_id'] == pa.int64()
        assert field_types['amount_cents'] == pa.int64()
        assert field_types['__line_number__'] == pa.int64()

    def test_multi_schema_error_handling(self):
        """Test error handling with malformed multi-schema data."""
        # Create a test file with some invalid records
        invalid_data = """H20241201000001DAILY_BATCH      First National Bank             
D00000112345TRANSFER    0000025000USD20241201101030ONLINE    
X00000212346UNKNOWN     0000001500USD20241201103045INVALID   
D00000312347WITHDRAWAL  0000005000USD20241201105020BRANCH    
T20241201000002000030000USD20241201End of Processing          """

        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "banking_multi_schema.json"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(invalid_data)
            temp_data_path = Path(f.name)

        try:
            config = create_fwf_config_from_schema(schema_path)
            handler = FwfInputHandler(config)

            # Process the file - should handle unknown record types gracefully
            records = list(handler.read_file(temp_data_path))

            # Should only process valid record types (H, D, T), skip unknown 'X'
            assert len(records) == 4, f"Expected 4 valid records, got {len(records)}"

            # Verify record types
            record_types = [r['record_type'] for r in records]
            assert 'H' in record_types
            assert 'D' in record_types
            assert 'T' in record_types
            assert 'X' not in record_types  # Invalid record type should be skipped

        finally:
            temp_data_path.unlink()

    def test_multi_schema_field_validation(self):
        """Test field overlap validation across multiple schemas."""
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "retail_multi_schema.json"

        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Verify configuration was loaded correctly
        assert config.conditional_schemas is not None
        assert len(config.conditional_schemas) == 5  # H, P, I, A, T

        # Verify flag column configuration
        assert config.flag_column is not None
        assert config.flag_column.name == 'record_type'
        assert config.flag_column.start == 1
        assert config.flag_column.length == 1

        # Verify each schema has valid field positions (no overlaps within schema)
        for schema in config.conditional_schemas:
            sorted_fields = sorted(schema.fields, key=lambda f: f.start)
            for i in range(len(sorted_fields) - 1):
                current = sorted_fields[i]
                next_field = sorted_fields[i + 1]
                current_end = current.start + current.length - 1
                assert current_end < next_field.start, f"Field overlap in schema {schema.flag_value}: {current.name} and {next_field.name}"

    def test_multi_schema_create_arrow_table(self):
        """Test creating PyArrow table from multi-schema FWF file."""
        test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
        schema_path = test_dir / "banking_multi_schema.json"
        data_path = test_dir / "banking_multi_schema.txt"

        config = create_fwf_config_from_schema(schema_path)
        handler = FwfInputHandler(config)

        # Create PyArrow table
        table = handler.create_arrow_table(data_path)

        # Verify table structure
        assert isinstance(table, pa.Table)
        assert table.num_rows == 13  # Total records in the file

        # Verify columns exist
        column_names = table.column_names
        assert 'record_type' in column_names
        assert 'batch_date' in column_names
        assert 'transaction_id' in column_names
        assert '__line_number__' in column_names
        assert '__source_file__' in column_names

        # Convert to pandas for easier validation
        df = table.to_pandas()

        # Verify record type distribution
        type_counts = df['record_type'].value_counts()
        assert type_counts['H'] == 2
        assert type_counts['D'] == 8
        assert type_counts['S'] == 2
        assert type_counts['T'] == 1

        # Verify data integrity for specific fields
        header_rows = df[df['record_type'] == 'H']
        assert all(header_rows['batch_name'].notna())
        assert all(header_rows['institution_name'].notna())

        detail_rows = df[df['record_type'] == 'D']
        assert all(detail_rows['transaction_id'].notna())
        assert all(detail_rows['amount_cents'].notna())
        assert all(detail_rows['currency'] == 'USD')

    def test_multi_schema_performance_large_file(self):
        """Test performance with a larger multi-schema file."""
        # Generate a larger test file with multiple batches
        large_data_lines = []

        # Generate 10 batches with multiple transactions each
        for batch in range(1, 11):
            batch_date = f"2024120{batch % 10}"
            large_data_lines.append(f"H{batch_date}{batch:06d}BATCH_{batch:03d}    Performance Test Bank           ")

            # Add 10 transactions per batch
            for txn in range(1, 11):
                txn_id = (batch - 1) * 10 + txn
                account = f"{12345 + txn:05d}"
                amount = f"{(txn * 1000):010d}"
                large_data_lines.append(f"D{txn_id:06d}{account}TRANSFER    {amount}USD{batch_date}101030ONLINE    ")

            # Add summary for each batch
            total_amount = f"{(10 * 1000 * (1 + 10) // 2):010d}"  # Sum of 1000, 2000, ..., 10000
            large_data_lines.append(f"S{batch_date}{10:06d}{total_amount}USD        Batch {batch} Summary         ")

        # Add final trailer
        large_data_lines.append("T20241210{:06d}{:010d}USD20241210Performance Test Complete   ".format(
            10 * 11,  # 10 batches * 11 records each (1 header + 10 details) + 10 summaries
            10 * 55000  # Total of all batch amounts
        ))

        large_data = '\n'.join(large_data_lines)

        # Use temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(large_data)
            temp_data_path = Path(f.name)

        try:
            test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
            schema_path = test_dir / "banking_multi_schema.json"

            config = create_fwf_config_from_schema(schema_path)
            handler = FwfInputHandler(config)

            # Process the large file
            import time
            start_time = time.time()
            records = list(handler.read_file(temp_data_path))
            end_time = time.time()

            processing_time = end_time - start_time

            # Verify results
            expected_records = 10 + 100 + 10 + 1  # 10 headers + 100 details + 10 summaries + 1 trailer
            assert len(records) == expected_records, f"Expected {expected_records} records, got {len(records)}"

            # Verify performance (should process reasonably quickly)
            records_per_second = len(records) / processing_time if processing_time > 0 else float('inf')
            assert records_per_second > 1000, f"Performance too slow: {records_per_second:.2f} records/second"

            print(f"Processed {len(records)} records in {processing_time:.3f} seconds ({records_per_second:.2f} records/sec)")

        finally:
            temp_data_path.unlink()


if __name__ == "__main__":
    # Run a simple test to verify functionality
    test_suite = TestMultiSchemaFwfIntegration()
    test_suite.test_banking_multi_schema_processing()
    print("✅ Multi-schema FWF integration tests passed!")
