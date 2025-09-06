#!/usr/bin/env python3
"""Comprehensive test script to verify all fixes are working."""

import sys
from pathlib import Path
import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import pyarrow as pa
from forklift.utils.date_parser import coerce_datetime
from forklift.processors.write_time_validator import WriteTimeConfig, WriteTimeValidator
from forklift.inputs.fwf_utils import create_fwf_config_from_schema

def test_datetime_strict_format():
    """Test datetime strict format enforcement (test_enforce_mode_strict_format)."""
    print("1. Testing datetime strict format enforcement...")

    try:
        from forklift.utils.transformations import DateTimeTransformConfig, DataTransformer

        config = DateTimeTransformConfig(
            mode="enforce",
            format="YYYY-MM-DD",
            target_type="string"
        )

        transformer = DataTransformer()
        data = ["2025-08-27", "2025-8-27", "08/27/2025", "invalid"]
        column = pa.array(data)

        result = transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # Check results
        test_passed = (
            result_list[0] == "2025-08-27T00:00:00" and  # Should pass (exact format)
            result_list[1] is None and                   # Should fail (not zero-padded)
            result_list[2] is None and                   # Should fail (wrong format)
            result_list[3] is None                       # Should fail (invalid)
        )

        print(f"   Results: {result_list}")
        print(f"   Expected: ['2025-08-27T00:00:00', None, None, None]")
        print(f"   ✅ PASSED" if test_passed else "   ❌ FAILED")
        return test_passed

    except Exception as e:
        print(f"   ❌ FAILED with exception: {e}")
        return False

def test_microseconds_token():
    """Test microseconds token handling (test_microseconds_token)."""
    print("\n2. Testing microseconds token handling...")

    try:
        result = coerce_datetime("2025-08-27 14:30:00.123", fmt="YYYY-MM-DD HH:MM:SS.fff")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, 123000)

        test_passed = result == expected
        print(f"   Input: '2025-08-27 14:30:00.123' with format 'YYYY-MM-DD HH:MM:SS.fff'")
        print(f"   Result: {result}")
        print(f"   Expected: {expected}")
        print(f"   ✅ PASSED" if test_passed else "   ❌ FAILED")
        return test_passed

    except Exception as e:
        print(f"   ❌ FAILED with exception: {e}")
        return False

def test_write_time_validator_exception_handling():
    """Test write time validator exception handling (test_process_batch_exception_handling)."""
    print("\n3. Testing write time validator exception handling...")

    try:
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

        # Create validator with bad config that should cause exception
        bad_config = WriteTimeConfig(expected_schema="not_a_schema")  # Invalid type
        validator = WriteTimeValidator(bad_config)

        batch, results = validator.process_batch(valid_data)

        # Check if we have the expected error
        has_write_validation_error = any(r.error_code == "WRITE_VALIDATION_ERROR" for r in results)

        print(f"   Number of validation results: {len(results)}")
        if results:
            print(f"   Error codes: {[r.error_code for r in results]}")
        print(f"   Has WRITE_VALIDATION_ERROR: {has_write_validation_error}")
        print(f"   ✅ PASSED" if has_write_validation_error else "   ❌ FAILED")
        return has_write_validation_error

    except Exception as e:
        print(f"   ❌ FAILED with exception: {e}")
        return False

def test_write_time_validator_method_exists():
    """Test that _validate_schema_compliance method exists."""
    print("\n4. Testing write time validator method exists...")

    try:
        config = WriteTimeConfig()
        validator = WriteTimeValidator(config)

        # Check if the method exists
        has_method = hasattr(validator, '_validate_schema_compliance')
        print(f"   Has _validate_schema_compliance method: {has_method}")
        print(f"   ✅ PASSED" if has_method else "   ❌ FAILED")
        return has_method

    except Exception as e:
        print(f"   ❌ FAILED with exception: {e}")
        return False

def test_multi_schema_fwf_path():
    """Test multi-schema FWF path fix (test_multi_schema_fwf)."""
    print("\n5. Testing multi-schema FWF path fix...")

    try:
        # Test the corrected path
        test_dir = Path(__file__).parent / "tests" / "test-files" / "goodfwf"
        schema_path = test_dir / "multi_schema_example.json"
        data_path = test_dir / "multi_schema_example.txt"

        schema_exists = schema_path.exists()
        data_exists = data_path.exists()

        print(f"   Schema path: {schema_path}")
        print(f"   Data path: {data_path}")
        print(f"   Schema exists: {schema_exists}")
        print(f"   Data exists: {data_exists}")

        path_test_passed = schema_exists and data_exists

        if path_test_passed:
            # Try to load the schema
            try:
                config = create_fwf_config_from_schema(schema_path)
                config_loaded = True
                print(f"   Schema config loaded successfully")
            except Exception as e:
                config_loaded = False
                print(f"   Schema config loading failed: {e}")
        else:
            config_loaded = False

        test_passed = path_test_passed and config_loaded
        print(f"   ✅ PASSED" if test_passed else "   ❌ FAILED")
        return test_passed

    except Exception as e:
        print(f"   ❌ FAILED with exception: {e}")
        return False

def main():
    """Run all tests and report results."""
    print("=" * 60)
    print("COMPREHENSIVE FIX VALIDATION")
    print("=" * 60)

    tests = [
        test_datetime_strict_format,
        test_microseconds_token,
        test_write_time_validator_exception_handling,
        test_write_time_validator_method_exists,
        test_multi_schema_fwf_path,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"   ❌ FAILED with unexpected exception: {e}")
            results.append(False)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    test_names = [
        "DateTime strict format enforcement",
        "Microseconds token handling",
        "Write time validator exception handling",
        "Write time validator method exists",
        "Multi-schema FWF path fix",
    ]

    for i, (name, result) in enumerate(zip(test_names, results), 1):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{i}. {name}: {status}")

    passed = sum(results)
    total = len(results)
    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All fixes are working correctly!")
        return True
    else:
        print("⚠️  Some fixes may need additional work.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
