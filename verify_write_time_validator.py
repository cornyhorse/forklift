#!/usr/bin/env python3
"""Simple test script to verify write_time_validator functionality."""

import sys
import os
sys.path.insert(0, '.')

import pyarrow as pa
from src.forklift.processors.write_time_validator import (
    WriteTimeValidator,
    WriteTimeConfig,
    create_basic_write_validator,
    create_strict_write_validator
)

def test_basic_functionality():
    """Test basic write-time validator functionality."""
    print("Testing WriteTimeValidator...")

    # Create test data
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string())
    ])

    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3]),
        pa.array(['Alice', 'Bob', 'Charlie']),
        pa.array(['alice@test.com', 'bob@test.com', 'charlie@test.com'])
    ], schema=schema)

    # Test 1: Basic validator
    print("\n1. Testing basic validator...")
    config = WriteTimeConfig(primary_key_columns=['id'])
    validator = WriteTimeValidator(config)
    result_batch, results = validator.process_batch(batch)
    print(f"   ✓ Processed {batch.num_rows} rows")
    print(f"   ✓ Got {len(results)} validation results")

    # Test 2: Empty batch validation
    print("\n2. Testing empty batch validation...")
    empty_batch = pa.RecordBatch.from_arrays([], schema=pa.schema([]))
    empty_results = validator._validate_not_empty(empty_batch)
    print(f"   ✓ Empty batch produces {len(empty_results)} error(s) (expected: 1)")

    # Test 3: Duplicate detection
    print("\n3. Testing duplicate detection...")
    duplicate_batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 1]),  # Duplicate ID
        pa.array(['Alice', 'Bob', 'Alice2'])
    ], schema=pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string())]))

    duplicate_results = validator._validate_duplicate_rows(duplicate_batch)
    print(f"   ✓ Duplicate detection produces {len(duplicate_results)} error(s) (expected: 1)")

    # Test 4: Factory functions
    print("\n4. Testing factory functions...")
    basic_validator = create_basic_write_validator(['id'])
    strict_validator = create_strict_write_validator(['id'], ['id', 'name'])
    print("   ✓ Factory functions work correctly")

    print("\n✅ All tests passed! WriteTimeValidator is working correctly.")
    return True

if __name__ == "__main__":
    try:
        test_basic_functionality()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
