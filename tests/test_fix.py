#!/usr/bin/env python3
"""Test script to verify all string cleaning fixes."""

import pyarrow as pa

from forklift.utils.transformations import (DataTransformer,
                                            StringCleaningConfig)


def test_tab_handling_fix():
    """Test that tab replacement works correctly."""
    transformer = DataTransformer()

    # Test tab replacement
    config_replace = StringCleaningConfig(
        remove_tabs=False,
        tab_replacement="    ",  # 4 spaces
        collapse_whitespace=False,
        strip_whitespace=False,
    )

    test_data = ["Hello\tworld\ttabs"]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_replace)
    actual = result.to_pylist()[0]
    expected = "Hello    world    tabs"

    print(f"Tab replacement test:")
    print(f"  Input: {repr(test_data[0])}")
    print(f"  Expected: {repr(expected)}")
    print(f"  Actual: {repr(actual)}")
    print(f"  Passed: {actual == expected}")

    assert actual == expected, f"Expected {repr(expected)}, but got {repr(actual)}"


def test_whitespace_collapse_fix():
    """Test that whitespace collapse works correctly with tabs."""
    transformer = DataTransformer()

    config = StringCleaningConfig(collapse_whitespace=True, strip_whitespace=True)

    test_data = [
        "Hello    world",  # Multiple spaces
        "  Hello   world  ",  # Leading/trailing + multiple
        "Hello\t\tworld",  # Tabs
        "Hello\n\nworld",  # Newlines
    ]
    column = pa.array(test_data)

    result = transformer.apply_string_cleaning(column, config)
    actual_list = result.to_pylist()

    expected = ["Hello world", "Hello world", "Hello world", "Hello world"]

    print(f"Whitespace collapse test:")
    for i, (input_val, expected_val, actual_val) in enumerate(
        zip(test_data, expected, actual_list)
    ):
        print(f"  Test {i+1}:")
        print(f"    Input: {repr(input_val)}")
        print(f"    Expected: {repr(expected_val)}")
        print(f"    Actual: {repr(actual_val)}")
        print(f"    Passed: {actual_val == expected_val}")

    assert actual_list == expected, f"Expected {expected}, but got {actual_list}"


def test_control_character_removal_fix():
    """Test that control character removal works correctly."""
    transformer = DataTransformer()

    config = StringCleaningConfig(
        remove_control_chars=True,
        preserve_newlines=True,
        preserve_tabs=False,
        collapse_whitespace=False,
        strip_whitespace=False,
    )

    test_data = [
        "Hello\x01world",  # Control character
        "Hello\nworld",  # Newline (should be preserved)
        "Hello\tworld",  # Tab (should be removed)
        "Hello\x7fworld",  # DEL character
    ]
    column = pa.array(test_data)

    result = transformer.apply_string_cleaning(column, config)
    actual_list = result.to_pylist()

    expected = ["Helloworld", "Hello\nworld", "Helloworld", "Helloworld"]

    print(f"Control character removal test:")
    for i, (input_val, expected_val, actual_val) in enumerate(
        zip(test_data, expected, actual_list)
    ):
        print(f"  Test {i+1}:")
        print(f"    Input: {repr(input_val)}")
        print(f"    Expected: {repr(expected_val)}")
        print(f"    Actual: {repr(actual_val)}")
        print(f"    Passed: {actual_val == expected_val}")

    assert actual_list == expected, f"Expected {expected}, but got {actual_list}"


if __name__ == "__main__":
    print("Testing all string cleaning fixes...")
    print("=" * 60)

    try:
        test_tab_handling_fix()
        tab_test_passed = True
        print("✅ Tab handling test passed")
    except AssertionError as e:
        tab_test_passed = False
        print(f"❌ Tab handling test failed: {e}")
    print()

    try:
        test_whitespace_collapse_fix()
        whitespace_test_passed = True
        print("✅ Whitespace collapse test passed")
    except AssertionError as e:
        whitespace_test_passed = False
        print(f"❌ Whitespace collapse test failed: {e}")
    print()

    try:
        test_control_character_removal_fix()
        control_test_passed = True
        print("✅ Control character removal test passed")
    except AssertionError as e:
        control_test_passed = False
        print(f"❌ Control character removal test failed: {e}")
    print()

    all_tests_passed = tab_test_passed and whitespace_test_passed and control_test_passed

    if all_tests_passed:
        print("🎉 All tests passed! The fixes are working correctly.")
    else:
        print("⚠️  Some tests failed. The fixes need more work.")
        if not tab_test_passed:
            print("  - Tab handling test failed")
        if not whitespace_test_passed:
            print("  - Whitespace collapse test failed")
        if not control_test_passed:
            print("  - Control character removal test failed")
