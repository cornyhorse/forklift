#!/usr/bin/env python3
"""
Comprehensive test demonstrating the new case transformation features
for string columns in the schema standard.
"""

import pyarrow as pa
from forklift.utils.data_transformations import DataTransformer, StringCleaningConfig

def test_case_transformations():
    """Test all case transformation features."""
    transformer = DataTransformer()

    print("🔤 CASE TRANSFORMATION FEATURES TEST")
    print("=" * 60)

    # Test data with various cases and scenarios
    test_data = [
        "hello world",
        "CALIFORNIA",
        "new york state",
        "the quick brown fox",
        "NASA AND THE FBI",
        "mcdonald's restaurant",
        "state of texas",
        "florida",
        "washington d.c."
    ]

    column = pa.array(test_data)

    print("\n📋 Original Data:")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {repr(item)}")

    # Test 1: Basic uppercase transformation
    print("\n1️⃣ UPPERCASE TRANSFORMATION")
    print("-" * 40)
    config_upper = StringCleaningConfig(
        case_transform='upper',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_upper)
    print("Result:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 2: Basic lowercase transformation
    print("\n2️⃣ LOWERCASE TRANSFORMATION")
    print("-" * 40)
    config_lower = StringCleaningConfig(
        case_transform='lower',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_lower)
    print("Result:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 3: Title case transformation
    print("\n3️⃣ TITLE CASE TRANSFORMATION")
    print("-" * 40)
    config_title = StringCleaningConfig(
        case_transform='title',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_title)
    print("Result:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 4: Proper case transformation
    print("\n4️⃣ PROPER CASE TRANSFORMATION")
    print("-" * 40)
    config_proper = StringCleaningConfig(
        case_transform='proper',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_proper)
    print("Result:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 5: Custom case mapping - State codes (exact matching)
    print("\n5️⃣ CUSTOM CASE MAPPING - STATE CODES (EXACT)")
    print("-" * 50)
    state_codes = {
        "california": "CA",
        "new york state": "NY",
        "texas": "TX",
        "florida": "FL"
    }
    config_states = StringCleaningConfig(
        case_transform='lower',  # First normalize to lowercase
        custom_case_mapping=state_codes,
        case_mapping_mode='exact',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_states)
    print("State code mappings:")
    for key, value in state_codes.items():
        print(f"  '{key}' → '{value}'")
    print("\nResult:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 6: Custom case mapping - Contains mode
    print("\n6️⃣ CUSTOM CASE MAPPING - CONTAINS MODE")
    print("-" * 45)
    restaurant_mapping = {
        "mcdonald": "McDonald",
        "restaurant": "Restaurant"
    }
    config_contains = StringCleaningConfig(
        case_transform='lower',  # First normalize
        custom_case_mapping=restaurant_mapping,
        case_mapping_mode='contains',
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(column, config_contains)
    print("Contains mappings:")
    for key, value in restaurant_mapping.items():
        print(f"  contains '{key}' → replace with '{value}'")
    print("\nResult:")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 7: Legacy fix_case_issues (smart title case with acronyms)
    print("\n7️⃣ LEGACY SMART CASE FIXING (ALL CAPS → Smart Title)")
    print("-" * 55)
    all_caps_data = [
        "NASA AND THE FBI",
        "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG",
        "WELCOME TO THE UNITED STATES OF AMERICA",
        "CEO AND CTO MEETING"
    ]
    caps_column = pa.array(all_caps_data)

    config_fix = StringCleaningConfig(
        fix_case_issues=True,
        collapse_whitespace=True,
        strip_whitespace=True
    )
    result = transformer.apply_string_cleaning(caps_column, config_fix)
    print("Input (ALL CAPS):")
    for i, item in enumerate(all_caps_data):
        print(f"  {i+1}. {repr(item)}")
    print("\nResult (Smart Title Case):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    # Test 8: Combined transformations - State standardization pipeline
    print("\n8️⃣ COMBINED PIPELINE - STATE STANDARDIZATION")
    print("-" * 50)
    messy_states = [
        "  CALIFORNIA  ",
        "new    york",
        "TEXAS STATE",
        "  florida   ",
        "state of washington"
    ]
    messy_column = pa.array(messy_states)

    # Multi-step pipeline for state standardization
    state_standard_mapping = {
        "california": "CA",
        "new york": "NY",
        "texas state": "TX",
        "texas": "TX",
        "florida": "FL",
        "state of washington": "WA",
        "washington": "WA"
    }

    config_pipeline = StringCleaningConfig(
        # First clean up whitespace
        collapse_whitespace=True,
        strip_whitespace=True,
        # Normalize case
        case_transform='lower',
        # Apply state code mapping
        custom_case_mapping=state_standard_mapping,
        case_mapping_mode='exact'
    )

    result = transformer.apply_string_cleaning(messy_column, config_pipeline)
    print("Input (messy state names):")
    for i, item in enumerate(messy_states):
        print(f"  {i+1}. {repr(item)}")
    print("\nResult (standardized state codes):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")

    print("\n✅ All case transformation tests completed!")
    print("\n📝 SUMMARY OF NEW FEATURES:")
    print("   • case_transform: 'upper', 'lower', 'title', 'proper'")
    print("   • custom_case_mapping: Dictionary of custom transformations")
    print("   • case_mapping_mode: 'exact', 'contains', 'startswith', 'endswith'")
    print("   • Enhanced title_case_exceptions for smart title casing")
    print("   • Backward compatible with existing fix_case_issues")


def test_schema_configuration():
    """Test how these features would be used in schema configuration."""
    print("\n\n🔧 SCHEMA CONFIGURATION EXAMPLES")
    print("=" * 50)

    # Example 1: Basic schema configuration for uppercase
    print("\n📄 Example 1: Uppercase transformation in schema")
    schema_config_upper = {
        "type": "string_cleaning",
        "enabled": True,
        "case_transform": "upper",
        "strip_whitespace": True,
        "collapse_whitespace": True
    }
    print("Schema configuration:")
    for key, value in schema_config_upper.items():
        print(f"  {key}: {value}")

    # Example 2: State code standardization schema
    print("\n📄 Example 2: State code standardization in schema")
    schema_config_states = {
        "type": "string_cleaning",
        "enabled": True,
        "case_transform": "lower",
        "custom_case_mapping": {
            "california": "CA",
            "new york": "NY",
            "texas": "TX",
            "florida": "FL",
            "washington": "WA"
        },
        "case_mapping_mode": "exact",
        "strip_whitespace": True,
        "collapse_whitespace": True
    }
    print("Schema configuration:")
    for key, value in schema_config_states.items():
        if key == "custom_case_mapping":
            print(f"  {key}:")
            for k, v in value.items():
                print(f"    {k}: {v}")
        else:
            print(f"  {key}: {value}")

    # Example 3: Title case with custom exceptions
    print("\n📄 Example 3: Smart title case in schema")
    schema_config_title = {
        "type": "string_cleaning",
        "enabled": True,
        "case_transform": "title",
        "title_case_exceptions": ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"],
        "strip_whitespace": True,
        "collapse_whitespace": True
    }
    print("Schema configuration:")
    for key, value in schema_config_title.items():
        if key == "title_case_exceptions":
            print(f"  {key}: {value[:5]}... (and {len(value)-5} more)")
        else:
            print(f"  {key}: {value}")


if __name__ == "__main__":
    test_case_transformations()
    test_schema_configuration()
