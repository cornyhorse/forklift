#!/usr/bin/env python3
"""Test script to verify WriteTimeValidator fix."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import pyarrow as pa
from forklift.processors.write_time_validator import WriteTimeConfig, WriteTimeValidator

def test_exception_handling():
    """Test that exception handling works correctly."""
    print("Testing WriteTimeValidator exception handling...")

    # Create test data
    data = {
        'id': [1, 2, 3],
        'name': ["Alice", "Bob", "Charlie"],
        'email': ["alice@example.com", "bob@example.com", "charlie@example.com"],
        'age': [25, 30, 35]
    }
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('age', pa.int32())
    ])
    valid_data = pa.record_batch(data, schema=schema)

    # Create a validator with bad config that will cause exceptions
    bad_config = WriteTimeConfig(expected_schema="not_a_schema")  # Invalid type
    validator = WriteTimeValidator(bad_config)

    print(f"Config expected_schema type: {type(bad_config.expected_schema)}")

    batch, results = validator.process_batch(valid_data)

    print(f"Number of validation results: {len(results)}")
    print("Results:")
    for i, r in enumerate(results):
        print(f"  {i+1}. Error code: {r.error_code}, Message: {r.error_message}")

    # Check if we have the expected error
    has_write_validation_error = any(r.error_code == "WRITE_VALIDATION_ERROR" for r in results)
    print(f"Has WRITE_VALIDATION_ERROR: {has_write_validation_error}")

    if has_write_validation_error:
        print("✅ Exception handling test PASSED")
        return True
    else:
        print("❌ Exception handling test FAILED")
        return False

if __name__ == "__main__":
    success = test_exception_handling()
    sys.exit(0 if success else 1)
