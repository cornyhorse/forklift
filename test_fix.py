#!/usr/bin/env python3

import pyarrow as pa
from src.forklift.utils.transformations import DataTransformer, StringCleaningConfig

def test_tab_replacement_fix():
    """Test that custom tab replacement is preserved when collapse_whitespace is True."""

    # Test the exact scenario from the failing test
    transformer = DataTransformer()
    config = StringCleaningConfig(remove_tabs=False, tab_replacement="    ")

    column = pa.array(["hello\tworld"])
    result = transformer.apply_string_cleaning(column, config)

    expected = ["hello    world"]
    actual = result.to_pylist()

    print(f"Input: {['hello\\tworld']}")
    print(f"Expected: {expected}")
    print(f"Actual: {actual}")
    print(f"Test passed: {actual == expected}")

    return actual == expected

if __name__ == "__main__":
    test_tab_replacement_fix()
