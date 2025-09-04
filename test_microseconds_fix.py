#!/usr/bin/env python3
"""Test script to verify microseconds token fix."""

import sys
from pathlib import Path
import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from forklift.utils.date_parser import coerce_datetime

def test_microseconds_token():
    """Test that microseconds token handling works correctly."""
    print("Testing microseconds token handling...")

    try:
        # Test the exact case that was failing
        result = coerce_datetime("2025-08-27 14:30:00.123", fmt="YYYY-MM-DD HH:MM:SS.fff")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, 123000)

        print(f"Input: '2025-08-27 14:30:00.123'")
        print(f"Format: 'YYYY-MM-DD HH:MM:SS.fff'")
        print(f"Result: {result}")
        print(f"Expected: {expected}")
        print(f"Match: {result == expected}")

        if result == expected:
            print("✅ Microseconds token test PASSED")
            return True
        else:
            print("❌ Microseconds token test FAILED")
            return False

    except Exception as e:
        print(f"❌ Microseconds token test FAILED with exception: {e}")
        return False

def test_various_microseconds_formats():
    """Test various microseconds formats."""
    print("\nTesting various microseconds formats...")

    test_cases = [
        ("2025-08-27 14:30:00.1", "YYYY-MM-DD HH:MM:SS.fff", datetime.datetime(2025, 8, 27, 14, 30, 0, 100000)),
        ("2025-08-27 14:30:00.12", "YYYY-MM-DD HH:MM:SS.fff", datetime.datetime(2025, 8, 27, 14, 30, 0, 120000)),
        ("2025-08-27 14:30:00.123", "YYYY-MM-DD HH:MM:SS.fff", datetime.datetime(2025, 8, 27, 14, 30, 0, 123000)),
        ("2025-08-27 14:30:00.123456", "YYYY-MM-DD HH:MM:SS.ffffff", datetime.datetime(2025, 8, 27, 14, 30, 0, 123456)),
    ]

    all_passed = True

    for i, (input_str, format_str, expected) in enumerate(test_cases, 1):
        try:
            result = coerce_datetime(input_str, fmt=format_str)
            passed = result == expected
            print(f"  Test {i}: {input_str} -> {result} ({'✅ PASS' if passed else '❌ FAIL'})")
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"  Test {i}: {input_str} -> Exception: {e} (❌ FAIL)")
            all_passed = False

    return all_passed

if __name__ == "__main__":
    success1 = test_microseconds_token()
    success2 = test_various_microseconds_formats()

    overall_success = success1 and success2
    print(f"\n{'✅ All tests PASSED' if overall_success else '❌ Some tests FAILED'}")
    sys.exit(0 if overall_success else 1)
