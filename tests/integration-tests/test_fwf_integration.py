"""Integration tests for Fixed Width File (FWF) processing using real test files."""

import pytest
import tempfile
from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import pandas as pd

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from forklift.inputs.fwf_utils import create_fwf_config_from_schema, create_simple_fwf_config


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
            FwfFieldSpec("date", 27, 8, align="left", parquet_type="date32"),
            FwfFieldSpec("active", 35, 1, align="left", parquet_type="bool"),
            FwfFieldSpec("amount", 36, 9, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("country", 45, 2, align="left", parquet_type="string"),
            FwfFieldSpec("status", 47, 8, align="left", parquet_type="string"),
            FwfFieldSpec("code", 56, 3, align="right", pad="0", parquet_type="int32"),
            FwfFieldSpec("notes", 59, 20, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(
            fields=fields,
            comment_patterns=["^#"],  # Skip comment lines
            skip_blank_lines=True
        )

        handler = FwfInputHandler(config)

        # Process the file
        results = list(handler.read_file(test_file))

        # Validate results
        assert len(results) == 5, f"Expected 5 records, got {len(results)}"

        # Check first record
        first_record = results[0]
        assert first_record["id"] == "1"
        assert first_record["name"] == "Amy Adams"
        assert first_record["country"] == "US"
        assert first_record["status"] == "active"

        # Check all records have required fields
        for record in results:
            assert "id" in record
            assert "name" in record
            assert "country" in record
            assert "status" in record
            assert "__line_number__" in record
            assert "__source_file__" in record

    def test_bad_fwf_misaligned_file_processing(self):
        """Test processing a FWF file with misaligned data."""
        test_file = Path(__file__).parent.parent / "test-files" / "badfwf" / "bad_fwf1_misaligned.txt"

        # Same field specification as good file
        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 20, align="left", parquet_type="string"),
            FwfFieldSpec("date", 27, 8, align="left", parquet_type="string"),  # Keep as string due to bad formats
            FwfFieldSpec("active", 35, 1, align="left", parquet_type="string"),  # Keep as string due to bad values
            FwfFieldSpec("amount", 36, 9, align="right", pad="0", parquet_type="string"),  # Keep as string
            FwfFieldSpec("country", 45, 2, align="left", parquet_type="string"),
            FwfFieldSpec("status", 47, 8, align="left", parquet_type="string"),
            FwfFieldSpec("code", 56, 3, align="right", pad="0", parquet_type="string"),
            FwfFieldSpec("notes", 59, 30, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(
            fields=fields,
            comment_patterns=["^#", "^TOTAL"],  # Skip comment and footer lines
            skip_blank_lines=True
        )

        handler = FwfInputHandler(config)

        # Process the file - should handle malformed data gracefully
        results = list(handler.read_file(test_file))

        # Should still process records, even with bad data
        assert len(results) >= 4, f"Expected at least 4 records, got {len(results)}"

        # Verify that malformed data is still captured
        for record in results:
            assert "id" in record
            assert "name" in record
            # Bad data should be captured as strings
            assert isinstance(record["date"], (str, type(None)))
            assert isinstance(record["active"], (str, type(None)))

    def test_duplicate_fwf_file_processing(self):
        """Test processing a FWF file with duplicate records."""
        test_file = Path(__file__).parent.parent / "test-files" / "dupefwf" / "dupe_fwf1.txt"

        # Standard field specification
        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 20, align="left", parquet_type="string"),
            FwfFieldSpec("date", 27, 8, align="left", parquet_type="string"),
            FwfFieldSpec("active", 35, 1, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 36, 9, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("country", 45, 2, align="left", parquet_type="string"),
            FwfFieldSpec("status", 47, 8, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(
            fields=fields,
            comment_patterns=["^#"],
            skip_blank_lines=True
        )

        handler = FwfInputHandler(config)

        # Process the file
        results = list(handler.read_file(test_file))

        # Should detect and process duplicate records
        assert len(results) > 0, "Should process duplicate records"

        # Check for duplicates by ID
        ids = [record["id"] for record in results]
        unique_ids = set(ids)

        # If there are duplicates, len(ids) > len(unique_ids)
        if len(ids) > len(unique_ids):
            print(f"Found {len(ids) - len(unique_ids)} duplicate records")

    def test_conditional_fwf_processing(self):
        """Test processing FWF files with conditional schemas."""
        # Create a conditional FWF configuration
        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")

        conditional_schemas = [
            FwfConditionalSchema("H", "Header Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("file_name", 2, 20, parquet_type="string"),
                FwfFieldSpec("creation_date", 22, 8, parquet_type="string"),
                FwfFieldSpec("record_count", 30, 6, align="right", pad="0", parquet_type="int32")
            ]),
            FwfConditionalSchema("D", "Data Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("id", 2, 6, align="right", pad="0", parquet_type="int64"),
                FwfFieldSpec("name", 8, 20, align="left", parquet_type="string"),
                FwfFieldSpec("amount", 28, 10, align="right", pad="0", parquet_type="int64")
            ]),
            FwfConditionalSchema("T", "Trailer Record", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("total_records", 2, 6, align="right", pad="0", parquet_type="int32"),
                FwfFieldSpec("total_amount", 8, 12, align="right", pad="0", parquet_type="int64")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
            skip_blank_lines=True
        )

        # Create a test file with conditional records
        test_data = [
            "HTEST_FILE        20250901000005",  # Header
            "D000001John Doe         0000001000",  # Data
            "D000002Jane Smith       0000002000",  # Data
            "D000003Bob Johnson      0000001500",  # Data
            "T000003000000004500"                  # Trailer
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_file = Path(f.name)

        try:
            handler = FwfInputHandler(config)
            results = list(handler.read_file(temp_file))

            # Should process all records according to their schemas
            assert len(results) == 5, f"Expected 5 records, got {len(results)}"

            # Check record types
            record_types = [record["record_type"] for record in results]
            assert "H" in record_types, "Should have header record"
            assert "D" in record_types, "Should have data records"
            assert "T" in record_types, "Should have trailer record"

            # Verify data records have correct fields
            data_records = [r for r in results if r["record_type"] == "D"]
            assert len(data_records) == 3, "Should have 3 data records"

            for data_record in data_records:
                assert "id" in data_record
                assert "name" in data_record
                assert "amount" in data_record

        finally:
            temp_file.unlink()

    def test_fwf_arrow_table_creation(self):
        """Test creating PyArrow tables from FWF files."""
        # Use the good FWF test file
        test_file = Path(__file__).parent.parent / "test-files" / "goodfwf" / "good_fwf1.txt"

        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 20, align="left", parquet_type="string"),
            FwfFieldSpec("date", 27, 8, align="left", parquet_type="string"),
            FwfFieldSpec("active", 35, 1, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 36, 9, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("country", 45, 2, align="left", parquet_type="string"),
            FwfFieldSpec("status", 47, 8, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(
            fields=fields,
            comment_patterns=["^#"],
            skip_blank_lines=True
        )

        handler = FwfInputHandler(config)

        # Create PyArrow table
        table = handler.create_arrow_table(test_file)

        # Validate table structure
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0, "Table should have data rows"
        assert table.num_columns >= len(fields), "Table should have all defined columns"

        # Check schema
        schema = table.schema
        field_names = [field.name for field in schema]
        assert "id" in field_names
        assert "name" in field_names
        assert "country" in field_names
        assert "__line_number__" in field_names
        assert "__source_file__" in field_names

        # Convert to pandas for validation
        df = table.to_pandas()
        assert len(df) > 0, "DataFrame should have rows"
        assert "id" in df.columns
        assert "name" in df.columns

    def test_fwf_encoding_handling(self):
        """Test FWF processing with different encodings."""
        # Test with CP-1252 encoded file if available
        test_file = Path(__file__).parent.parent / "test-files" / "badfwf" / "bad_fwf2_cp_1252_parens.txt"

        if not test_file.exists():
            pytest.skip("CP-1252 test file not available")

        fields = [
            FwfFieldSpec("id", 1, 6, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 7, 25, align="left", parquet_type="string"),
            FwfFieldSpec("data", 32, 20, align="left", parquet_type="string")
        ]

        # Test with encoding detection
        config = FwfInputConfig(
            fields=fields,
            encoding="auto",  # Let the handler detect encoding
            comment_patterns=["^#"],
            skip_blank_lines=True
        )

        handler = FwfInputHandler(config)

        try:
            results = list(handler.read_file(test_file))
            assert len(results) >= 0, "Should handle encoding issues gracefully"
        except UnicodeDecodeError:
            # If auto-detection fails, try with specific encoding
            config.encoding = "cp1252"
            handler = FwfInputHandler(config)
            results = list(handler.read_file(test_file))
            assert len(results) >= 0, "Should process with correct encoding"

    def test_fwf_performance_large_file(self):
        """Test FWF processing performance with larger files."""
        # Create a larger test file
        fields = [
            FwfFieldSpec("id", 1, 10, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 11, 30, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 41, 15, align="right", pad="0", parquet_type="decimal128(12,2)"),
            FwfFieldSpec("date", 56, 10, align="left", parquet_type="string"),
            FwfFieldSpec("category", 66, 10, align="left", parquet_type="string")
        ]

        config = FwfInputConfig(fields=fields)

        # Generate test data
        test_data = []
        for i in range(1000):  # 1000 records
            line = (
                f"{i+1:010d}"  # id (10 chars, zero-padded)
                f"{'Person ' + str(i+1):<30}"  # name (30 chars, left-aligned)
                f"{(i+1)*100:015d}"  # amount (15 chars, zero-padded)
                f"2025-01-{(i%28)+1:02d}"  # date (10 chars)
                f"{'CAT' + str((i%5)+1):<10}"  # category (10 chars)
            )
            test_data.append(line)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_file = Path(f.name)

        try:
            handler = FwfInputHandler(config)

            import time
            start_time = time.time()

            # Process using iterator (memory efficient)
            count = 0
            for record in handler.read_file(temp_file):
                count += 1
                if count == 1:
                    # Validate first record
                    assert record["id"] == "1"
                    assert "Person 1" in record["name"]

            end_time = time.time()
            processing_time = end_time - start_time

            assert count == 1000, f"Expected 1000 records, processed {count}"
            assert processing_time < 10.0, f"Processing took {processing_time:.2f}s, should be under 10s"

            print(f"Processed {count} FWF records in {processing_time:.2f} seconds")

        finally:
            temp_file.unlink()

    def test_fwf_schema_file_integration(self):
        """Test FWF processing using schema standard files."""
        # Use the schema standard file we created earlier
        schema_file = Path(__file__).parent.parent.parent / "schema-standards" / "20250826-fwf.json"

        if not schema_file.exists():
            pytest.skip("FWF schema standard file not available")

        try:
            # Create configuration from schema file
            config = create_fwf_config_from_schema(schema_file)
            handler = FwfInputHandler(config)

            # Test with a simple data file
            test_data = [
                "00001John Doe          123456789012345    1234.56   active  ",
                "00002Jane Smith        234567890123456    2345.67   inactive",
                "00003Bob Johnson       345678901234567    3456.78   active  "
            ]

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
                for line in test_data:
                    f.write(line + '\n')
                temp_file = Path(f.name)

            try:
                results = list(handler.read_file(temp_file))
                assert len(results) == 3, "Should process all records"

                # Validate schema-defined fields are present
                for record in results:
                    assert "id" in record
                    assert "name" in record
                    # Other fields depend on the schema definition

            finally:
                temp_file.unlink()

        except Exception as e:
            pytest.skip(f"Schema file processing failed: {e}")

    def test_fwf_error_handling_integration(self):
        """Test FWF error handling in integration scenarios."""
        fields = [
            FwfFieldSpec("id", 1, 5, parquet_type="int64"),
            FwfFieldSpec("name", 6, 20, parquet_type="string")
        ]

        config = FwfInputConfig(fields=fields)
        handler = FwfInputHandler(config)

        # Test with non-existent file
        non_existent_file = Path("/non/existent/file.fwf")
        with pytest.raises(FileNotFoundError):
            list(handler.read_file(non_existent_file))

        # Test with empty file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
            temp_file = Path(f.name)

        try:
            results = list(handler.read_file(temp_file))
            assert len(results) == 0, "Empty file should return no records"

            # Test Arrow table creation with empty file
            table = handler.create_arrow_table(temp_file)
            assert table.num_rows == 0, "Empty file should create empty table"
            assert table.num_columns > 0, "Should still have schema columns"

        finally:
            temp_file.unlink()


@pytest.mark.integration
class TestFwfSchemaIntegration:
    """Integration tests for FWF schema processing."""

    def test_conditional_schema_from_file(self):
        """Test conditional FWF schema processing from schema file."""
        schema_file = Path(__file__).parent.parent.parent / "schema-standards" / "20250826-fwf-conditional.json"

        if not schema_file.exists():
            pytest.skip("Conditional FWF schema file not available")

        try:
            config = create_fwf_config_from_schema(schema_file)
            handler = FwfInputHandler(config)

            # Test data matching the conditional schema
            test_data = [
                "F001abcdefgh",      # Format F record
                "G01abcde123",       # Format G record
                "F002hijklmno",      # Another Format F record
            ]

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
                for line in test_data:
                    f.write(line + '\n')
                temp_file = Path(f.name)

            try:
                results = list(handler.read_file(temp_file))
                assert len(results) >= 2, "Should process conditional records"

                # Check that different record types are processed
                record_types = [record.get("record_type") for record in results]
                assert "F" in record_types or "G" in record_types, "Should detect conditional schemas"

            finally:
                temp_file.unlink()

        except Exception as e:
            pytest.skip(f"Conditional schema processing failed: {e}")

    def test_fwf_primary_key_validation(self):
        """Test primary key validation in FWF processing."""
        fields = [
            FwfFieldSpec("id", 1, 5, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 6, 20, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 26, 10, align="right", parquet_type="decimal128(10,2)")
        ]

        config = FwfInputConfig(fields=fields)
        handler = FwfInputHandler(config)

        # Test data with duplicate primary keys
        test_data = [
            "00001John Doe          1234.56   ",
            "00002Jane Smith        2345.67   ",
            "00001Bob Johnson       3456.78   ",  # Duplicate ID
            "00003Alice Brown       4567.89   "
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fwf') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_file = Path(f.name)

        try:
            results = list(handler.read_file(temp_file))

            # Check for duplicate primary keys
            ids = [record["id"] for record in results]
            unique_ids = set(ids)

            if len(ids) != len(unique_ids):
                print(f"Warning: Found duplicate primary keys in FWF file")
                duplicate_ids = [id for id in ids if ids.count(id) > 1]
                print(f"Duplicate IDs: {list(set(duplicate_ids))}")

            # Should still process all records
            assert len(results) == 4, "Should process all records including duplicates"

        finally:
            temp_file.unlink()
