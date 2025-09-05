#!/usr/bin/env python3

import pyarrow as pa
from src.forklift.utils.data_transformations import DataTransformer, StringCleaningConfig

def test_both_fixes():
    """Test both the tab replacement fix and the encoding error fix."""

    # Test 1: Tab replacement fix (original failing test)
    print("=== Test 1: Custom Tab Replacement Fix ===")
    transformer = DataTransformer()
    config = StringCleaningConfig(remove_tabs=False, tab_replacement="    ")

    column = pa.array(["hello\tworld"])
    result = transformer.apply_string_cleaning(column, config)

    expected = ["hello    world"]
    actual = result.to_pylist()

    print(f"Input: {['hello\\tworld']}")
    print(f"Expected: {expected}")
    print(f"Actual: {actual}")
    print(f"Test 1 passed: {actual == expected}")
    print()

    # Test 2: Encoding error fix
    print("=== Test 2: Encoding Error Fix ===")
    config2 = StringCleaningConfig(fix_encoding_errors=True)

    column2 = pa.array(["Donâ€™t"])  # Mojibake for "Don't"
    result2 = transformer.apply_string_cleaning(column2, config2)

    expected2 = ["Don't"]
    actual2 = result2.to_pylist()

    print(f"Input: {['Donâ€™t']}")
    print(f"Expected: {expected2}")
    print(f"Actual: {actual2}")
    print(f"Test 2 passed: {actual2 == expected2}")
    print()

    # Overall result
    both_passed = (actual == expected) and (actual2 == expected2)
    print(f"=== Overall Result ===")
    print(f"Both fixes working: {both_passed}")

    return both_passed

if __name__ == "__main__":
    test_both_fixes()
