"""Test multi-schema FWF functionality."""

from pathlib import Path
from typing import Any, Dict, List

import pytest

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.fwf_utils import create_fwf_config_from_schema


def test_multi_schema_fwf_processing():
    """Test processing of FWF file with multiple record types (H, D, T)."""

    # Path to our test files - go up one level from integration-tests to reach test-files
    test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
    schema_path = test_dir / "multi_schema_example.json"
    data_path = test_dir / "multi_schema_example.txt"

    # Create FWF configuration from schema
    config = create_fwf_config_from_schema(schema_path)
    handler = FwfInputHandler(config)

    # Process the file
    records = list(handler.read_file(data_path))

    # Verify we got the expected number of records
    assert len(records) == 9  # 2 headers + 5 details + 2 trailers

    # Separate records by type
    headers = [r for r in records if r["record_type"] == "H"]
    details = [r for r in records if r["record_type"] == "D"]
    trailers = [r for r in records if r["record_type"] == "T"]

    assert len(headers) == 2
    assert len(details) == 5
    assert len(trailers) == 2

    # Verify header record structure
    header1 = headers[0]
    expected_header_fields = {
        "record_type",
        "batch_date",
        "batch_id",
        "description",
        "__line_number__",
        "__source_file__",
    }
    assert set(header1.keys()) == expected_header_fields
    assert header1["record_type"] == "H"
    assert header1["batch_date"] == "20241201"
    assert header1["batch_id"] == "BATCH001"  # This is a string, not an integer
    assert header1["description"] == "Daily Sales Report"

    # Verify detail record structure
    detail1 = details[0]
    expected_detail_fields = {
        "record_type",
        "transaction_id",
        "product_code",
        "amount_cents",
        "currency",
        "transaction_date",
        "quantity",
        "__line_number__",
        "__source_file__",
    }
    assert set(detail1.keys()) == expected_detail_fields
    assert detail1["record_type"] == "D"
    assert detail1["transaction_id"] == 1  # This should be an integer based on schema
    assert detail1["product_code"] == "PRODUCT_A"
    assert detail1["amount_cents"] == 12500  # This should be an integer
    assert detail1["currency"] == "USD"
    assert detail1["quantity"] == 10  # This should be an integer

    # Verify trailer record structure
    trailer1 = trailers[0]
    expected_trailer_fields = {
        "record_type",
        "record_count",
        "total_amount_cents",
        "currency",
        "summary_date",
        "notes",
        "__line_number__",
        "__source_file__",
    }
    assert set(trailer1.keys()) == expected_trailer_fields
    assert trailer1["record_type"] == "T"
    assert trailer1["record_count"] == 3
    assert trailer1["total_amount_cents"] == 36450
    assert trailer1["currency"] == "USD"
    assert trailer1["notes"] == "Summary record"


def test_multi_schema_arrow_schema_generation():
    """Test PyArrow schema generation for multi-schema FWF."""

    test_dir = Path(__file__).parent.parent / "test-files" / "goodfwf"
    schema_path = test_dir / "multi_schema_example.json"

    config = create_fwf_config_from_schema(schema_path)
    handler = FwfInputHandler(config)

    # Generate PyArrow schema
    arrow_schema = handler.get_arrow_schema()

    # Verify the schema contains all unique fields from all conditional schemas
    field_names = [field.name for field in arrow_schema]

    # Fields that should be present from all schemas
    expected_fields = {
        "record_type",  # Common to all
        "batch_date",
        "batch_id",
        "description",  # Header fields
        "transaction_id",
        "product_code",
        "amount_cents",
        "currency",
        "transaction_date",
        "quantity",  # Detail fields
        "record_count",
        "total_amount_cents",
        "summary_date",
        "notes",  # Trailer fields
        "__line_number__",
        "__source_file__",  # Metadata fields
    }

    assert set(field_names) == expected_fields


if __name__ == "__main__":
    test_multi_schema_fwf_processing()
    test_multi_schema_arrow_schema_generation()
    print("All multi-schema FWF tests passed!")
