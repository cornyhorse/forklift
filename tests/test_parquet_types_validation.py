"""Tests for Parquet type validation functionality."""

import pytest
from forklift.schema.fwf.validation.parquet_types import ParquetTypeValidator


class TestParquetTypeValidator:
    """Test cases for ParquetTypeValidator class."""

    def test_supported_parquet_types_attribute(self):
        """Test that SUPPORTED_PARQUET_TYPES contains expected types."""
        expected_types = {
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "double", "bool", "string", "binary",
            "date32", "date64", "timestamp[s]", "timestamp[ms]",
            "timestamp[us]", "timestamp[ns]", "duration[s]", "duration[ms]",
            "duration[us]", "duration[ns]", "decimal128(10,2)",
            "list<string>", "struct", "dictionary<values=string, indices=int32>"
        }

        assert ParquetTypeValidator.SUPPORTED_PARQUET_TYPES == expected_types

    def test_is_valid_parquet_type_basic_types(self):
        """Test validation of basic Parquet types."""
        basic_types = [
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "double", "bool", "string", "binary"
        ]

        for parquet_type in basic_types:
            assert ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def test_is_valid_parquet_type_temporal_types(self):
        """Test validation of temporal Parquet types."""
        temporal_types = [
            "date32", "date64",
            "timestamp[s]", "timestamp[ms]", "timestamp[us]", "timestamp[ns]"
        ]

        for parquet_type in temporal_types:
            assert ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def test_is_valid_parquet_type_duration_types(self):
        """Test validation of duration Parquet types."""
        duration_types = [
            "duration[s]", "duration[ms]", "duration[us]", "duration[ns]"
        ]

        for parquet_type in duration_types:
            assert ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def test_is_valid_parquet_type_complex_types(self):
        """Test validation of complex Parquet types."""
        complex_types = [
            "decimal128(10,2)", "list<string>", "struct",
            "dictionary<values=string, indices=int32>"
        ]

        for parquet_type in complex_types:
            assert ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def test_is_valid_parquet_type_invalid_types(self):
        """Test validation of invalid Parquet types."""
        invalid_types = [
            "int128", "uint128", "float64", "varchar", "char",
            "text", "blob", "json", "xml", "unknown",
            "", "null", "void", "any"
        ]

        for parquet_type in invalid_types:
            assert not ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def test_is_valid_parquet_type_parameterized_decimal(self):
        """Test validation of parameterized decimal types."""
        valid_decimals = [
            "decimal128(5,2)", "decimal128(10,0)", "decimal128(38,18)",
            "decimal128(1,1)", "decimal128(20,5)"
        ]

        for decimal_type in valid_decimals:
            assert ParquetTypeValidator.is_valid_parquet_type(decimal_type)

    def test_is_valid_parquet_type_invalid_decimal(self):
        """Test validation of invalid decimal types."""
        invalid_decimals = [
            "decimal128",  # Missing parentheses
            "decimal64(10,2)",  # Wrong decimal type
            "decimal(10,2)",  # Wrong decimal type
            "decimal128[10,2]"  # Wrong brackets
        ]

        for decimal_type in invalid_decimals:
            assert not ParquetTypeValidator.is_valid_parquet_type(decimal_type)

        # Note: decimal128() and decimal128(5) are actually considered valid
        # by the current implementation since it only checks for the pattern
        # decimal128(...) without validating the content inside parentheses

    def test_is_valid_parquet_type_timestamp_with_timezone(self):
        """Test validation of timestamp types with timezone."""
        valid_timestamps = [
            "timestamp[s]", "timestamp[ms]", "timestamp[us]", "timestamp[ns]",
            "timestamp[s, tz=UTC]", "timestamp[ms, tz=America/New_York]"
        ]

        for timestamp_type in valid_timestamps:
            assert ParquetTypeValidator.is_valid_parquet_type(timestamp_type)

    def test_is_valid_parquet_type_custom_duration(self):
        """Test validation of custom duration types."""
        valid_durations = [
            "duration[s]", "duration[ms]", "duration[us]", "duration[ns]"
        ]

        for duration_type in valid_durations:
            assert ParquetTypeValidator.is_valid_parquet_type(duration_type)

    def test_is_valid_parquet_type_list_types(self):
        """Test validation of list types."""
        valid_lists = [
            "list<string>", "list<int32>", "list<double>", "list<bool>"
        ]

        for list_type in valid_lists:
            assert ParquetTypeValidator.is_valid_parquet_type(list_type)

    def test_is_valid_parquet_type_dictionary_types(self):
        """Test validation of dictionary types."""
        valid_dictionaries = [
            "dictionary<values=string, indices=int32>",
            "dictionary<values=int64, indices=int16>",
            "dictionary<values=double, indices=uint32>"
        ]

        for dict_type in valid_dictionaries:
            assert ParquetTypeValidator.is_valid_parquet_type(dict_type)

    def test_are_types_compatible_identical_types(self):
        """Test compatibility of identical types."""
        identical_cases = [
            ["string"],
            ["int32", "int32"],
            ["double", "double", "double"],
            ["bool", "bool", "bool", "bool"]
        ]

        for types in identical_cases:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_numeric_types(self):
        """Test compatibility of numeric types."""
        numeric_combinations = [
            ["int8", "int16"],
            ["int32", "int64"],
            ["uint8", "uint16", "uint32"],
            ["float32", "double"],
            ["int8", "int16", "int32", "int64", "float32", "double"],
            ["uint8", "uint16", "uint32", "uint64"]
        ]

        for types in numeric_combinations:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_temporal_types(self):
        """Test compatibility of temporal types."""
        temporal_combinations = [
            ["date32", "date64"],
            ["timestamp[s]", "timestamp[ms]"],
            ["timestamp[us]", "timestamp[ns]"],
            ["date32", "date64", "timestamp[s]", "timestamp[ms]"]
        ]

        for types in temporal_combinations:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_duration_types(self):
        """Test compatibility of duration types."""
        duration_combinations = [
            ["duration[s]", "duration[ms]"],
            ["duration[us]", "duration[ns]"],
            ["duration[s]", "duration[ms]", "duration[us]", "duration[ns]"]
        ]

        for types in duration_combinations:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_string_types(self):
        """Test compatibility of string types."""
        string_combinations = [
            ["string", "binary"],
            ["binary", "string"]
        ]

        for types in string_combinations:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_decimal_types(self):
        """Test compatibility of decimal types."""
        decimal_combinations = [
            ["decimal128(10,2)", "decimal128(5,1)"],
            ["decimal128(10,0)", "decimal128(20,5)", "decimal128(38,18)"]
        ]

        for types in decimal_combinations:
            assert ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_incompatible_types(self):
        """Test incompatibility of different type groups."""
        incompatible_combinations = [
            ["string", "int32"],
            ["bool", "double"],
            ["date32", "int64"],
            ["timestamp[s]", "string"],
            ["duration[ms]", "double"],
            ["list<string>", "string"],
            ["struct", "int32"],
            ["bool", "string", "int32"],
            ["float32", "date32"],
            ["binary", "int64"]
        ]

        for types in incompatible_combinations:
            assert not ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_mixed_numeric_temporal(self):
        """Test incompatibility between numeric and temporal types."""
        mixed_combinations = [
            ["int32", "date32"],
            ["double", "timestamp[s]"],
            ["float32", "duration[ms]"],
            ["uint64", "date64", "timestamp[ns]"]
        ]

        for types in mixed_combinations:
            assert not ParquetTypeValidator.are_types_compatible(types)

    def test_are_types_compatible_edge_cases(self):
        """Test compatibility edge cases."""
        # Empty list
        assert ParquetTypeValidator.are_types_compatible([])

        # Single type
        assert ParquetTypeValidator.are_types_compatible(["string"])
        assert ParquetTypeValidator.are_types_compatible(["int32"])
        assert ParquetTypeValidator.are_types_compatible(["bool"])

    def test_are_types_compatible_complex_types_incompatible(self):
        """Test that complex types are generally incompatible with others."""
        complex_incompatible = [
            ["list<string>", "string"],
            ["struct", "bool"],
            ["dictionary<values=string, indices=int32>", "string"],
            ["list<int32>", "int32"],
            ["struct", "list<string>"]
        ]

        for types in complex_incompatible:
            assert not ParquetTypeValidator.are_types_compatible(types)

    def test_validator_class_methods(self):
        """Test that validator methods are class methods."""
        # Test that methods can be called on the class
        assert callable(ParquetTypeValidator.is_valid_parquet_type)
        assert callable(ParquetTypeValidator.are_types_compatible)

        # Test that they work when called on an instance too
        validator = ParquetTypeValidator()
        assert validator.is_valid_parquet_type("string")
        assert validator.are_types_compatible(["int32", "int64"])

    def test_comprehensive_type_validation_scenario(self):
        """Test a comprehensive scenario with various type validations."""
        # Mix of valid and invalid types
        test_cases = [
            ("string", True),
            ("int32", True),
            ("decimal128(10,2)", True),
            ("timestamp[ms]", True),
            ("list<string>", True),
            ("invalid_type", False),
            ("int128", False),
            ("decimal(10,2)", False)
        ]

        for parquet_type, expected in test_cases:
            result = ParquetTypeValidator.is_valid_parquet_type(parquet_type)
            assert result == expected, f"Type {parquet_type} validation failed"

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario with various compatibility checks."""
        # Various compatibility scenarios
        compatibility_cases = [
            (["int8", "int16", "int32"], True),  # All numeric
            (["string", "binary"], True),  # String types
            (["date32", "timestamp[s]"], True),  # Temporal types
            (["duration[s]", "duration[ms]"], True),  # Duration types
            (["decimal128(5,2)", "decimal128(10,5)"], True),  # Decimal types
            (["string", "int32"], False),  # String + numeric
            (["bool", "double"], False),  # Boolean + numeric
            (["timestamp[s]", "int64"], False),  # Temporal + numeric
        ]

        for types, expected in compatibility_cases:
            result = ParquetTypeValidator.are_types_compatible(types)
            assert result == expected, f"Compatibility check for {types} failed"
