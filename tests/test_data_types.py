"""Tests for data type conversion utilities."""

import pytest
import pyarrow as pa

from forklift.schema.types.data_types import DataTypeConverter


class TestDataTypeConverter:
    """Test cases for DataTypeConverter class."""

    def test_arrow_to_json_schema_type_integer(self):
        """Test conversion of PyArrow integer types to JSON Schema."""
        # Test various integer types
        int_types = [pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                     pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]

        for arrow_type in int_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "integer"}

    def test_arrow_to_json_schema_type_float(self):
        """Test conversion of PyArrow floating point types to JSON Schema."""
        # Test various float types
        float_types = [pa.float16(), pa.float32(), pa.float64()]

        for arrow_type in float_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "number"}

    def test_arrow_to_json_schema_type_boolean(self):
        """Test conversion of PyArrow boolean type to JSON Schema."""
        result = DataTypeConverter.arrow_to_json_schema_type(pa.bool_())
        assert result == {"type": "boolean"}

    def test_arrow_to_json_schema_type_string(self):
        """Test conversion of PyArrow string types to JSON Schema."""
        # Test regular and large string types
        string_types = [pa.string(), pa.large_string()]

        for arrow_type in string_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "string"}

    def test_arrow_to_json_schema_type_date(self):
        """Test conversion of PyArrow date types to JSON Schema."""
        # Test various date types
        date_types = [pa.date32(), pa.date64()]

        for arrow_type in date_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "string", "format": "date"}

    def test_arrow_to_json_schema_type_timestamp(self):
        """Test conversion of PyArrow timestamp types to JSON Schema."""
        # Test various timestamp types
        timestamp_types = [
            pa.timestamp('s'),
            pa.timestamp('ms'),
            pa.timestamp('us'),
            pa.timestamp('ns')
        ]

        for arrow_type in timestamp_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "string", "format": "date-time"}

    def test_arrow_to_json_schema_type_time(self):
        """Test conversion of PyArrow time types to JSON Schema."""
        # Test various time types
        time_types = [pa.time32('s'), pa.time32('ms'), pa.time64('us'), pa.time64('ns')]

        for arrow_type in time_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "string", "format": "time"}

    def test_arrow_to_json_schema_type_binary(self):
        """Test conversion of PyArrow binary types to JSON Schema."""
        # Test regular and large binary types
        binary_types = [pa.binary(), pa.large_binary()]

        for arrow_type in binary_types:
            result = DataTypeConverter.arrow_to_json_schema_type(arrow_type)
            assert result == {"type": "string", "contentEncoding": "base64"}

    def test_arrow_to_json_schema_type_list(self):
        """Test conversion of PyArrow list types to JSON Schema."""
        # Test list with string value type
        list_type = pa.list_(pa.string())
        result = DataTypeConverter.arrow_to_json_schema_type(list_type)

        expected = {
            "type": "array",
            "items": {"type": "string"}
        }
        assert result == expected

    def test_arrow_to_json_schema_type_large_list(self):
        """Test conversion of PyArrow large list types to JSON Schema."""
        # Test large list with integer value type
        large_list_type = pa.large_list(pa.int32())
        result = DataTypeConverter.arrow_to_json_schema_type(large_list_type)

        expected = {
            "type": "array",
            "items": {"type": "integer"}
        }
        assert result == expected

    def test_arrow_to_json_schema_type_list_without_value_type(self):
        """Test conversion of PyArrow list types without value_type attribute."""
        # Create a mock list type without value_type
        class MockListType:
            def __init__(self):
                self.id = 1  # Add id attribute that PyArrow expects

        mock_type = MockListType()

        # Mock the pa.types functions to return appropriate values for our mock type
        import pyarrow.types as pa_types
        original_functions = {}

        # Store all original type checking functions
        type_check_functions = [
            'is_integer', 'is_floating', 'is_boolean', 'is_string', 'is_large_string',
            'is_date', 'is_timestamp', 'is_time', 'is_binary', 'is_large_binary',
            'is_list', 'is_large_list', 'is_struct', 'is_dictionary'
        ]

        for func_name in type_check_functions:
            original_functions[func_name] = getattr(pa_types, func_name)

        # Create mock functions that return True only for is_list with our mock type
        def mock_is_list(t):
            return t is mock_type or original_functions['is_list'](t)

        def create_false_func(original_func):
            def mock_func(t):
                if t is mock_type:
                    return False
                return original_func(t)
            return mock_func

        # Apply mocks
        pa_types.is_list = mock_is_list
        for func_name in type_check_functions:
            if func_name != 'is_list':
                setattr(pa_types, func_name, create_false_func(original_functions[func_name]))

        try:
            result = DataTypeConverter.arrow_to_json_schema_type(mock_type)
            expected = {
                "type": "array",
                "items": {"type": "string"}
            }
            assert result == expected
        finally:
            # Restore original functions
            for func_name, original_func in original_functions.items():
                setattr(pa_types, func_name, original_func)

    def test_arrow_to_json_schema_type_struct(self):
        """Test conversion of PyArrow struct types to JSON Schema."""
        struct_type = pa.struct([
            pa.field('field1', pa.string()),
            pa.field('field2', pa.int32())
        ])

        result = DataTypeConverter.arrow_to_json_schema_type(struct_type)
        assert result == {"type": "object", "additionalProperties": True}

    def test_arrow_to_json_schema_type_dictionary(self):
        """Test conversion of PyArrow dictionary types to JSON Schema."""
        dict_type = pa.dictionary(pa.int32(), pa.string())
        result = DataTypeConverter.arrow_to_json_schema_type(dict_type)
        assert result == {"type": "string"}

    def test_arrow_to_json_schema_type_unknown(self):
        """Test conversion of unknown PyArrow types to JSON Schema."""
        # Create a custom type that doesn't match any known patterns
        class UnknownType:
            pass

        unknown_type = UnknownType()

        # Mock all the type checking functions to return False
        import pyarrow.types as pa_types
        original_functions = {}
        type_check_functions = [
            'is_integer', 'is_floating', 'is_boolean', 'is_string', 'is_large_string',
            'is_date', 'is_timestamp', 'is_time', 'is_binary', 'is_large_binary',
            'is_list', 'is_large_list', 'is_struct', 'is_dictionary'
        ]

        for func_name in type_check_functions:
            original_functions[func_name] = getattr(pa_types, func_name)
            setattr(pa_types, func_name, lambda t: False)

        try:
            result = DataTypeConverter.arrow_to_json_schema_type(unknown_type)
            assert result == {"type": "string"}
        finally:
            # Restore original functions
            for func_name, original_func in original_functions.items():
                setattr(pa_types, func_name, original_func)

    def test_detect_numeric_patterns_empty_list(self):
        """Test numeric pattern detection with empty sample values."""
        result = DataTypeConverter.detect_numeric_patterns([])

        expected = {
            'has_thousands_separator': False,
            'has_decimal_separator': False,
            'has_currency_symbols': False,
            'has_parentheses_negative': False
        }
        assert result == expected

    def test_detect_numeric_patterns_currency_symbols(self):
        """Test detection of currency symbols in numeric data."""
        sample_values = ['$100', '€50', '£75', '¥1000', '₹500', '₽200', '¢25']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        assert result['has_currency_symbols'] == True
        assert result['has_thousands_separator'] == False
        assert result['has_decimal_separator'] == False
        assert result['has_parentheses_negative'] == False

    def test_detect_numeric_patterns_thousands_separator(self):
        """Test detection of thousands separators in numeric data."""
        sample_values = ['1,000', '10,500', '1,234,567']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        assert result['has_thousands_separator'] == True
        assert result['has_currency_symbols'] == False
        assert result['has_decimal_separator'] == False
        assert result['has_parentheses_negative'] == False

    def test_detect_numeric_patterns_decimal_separator(self):
        """Test detection of decimal separators in numeric data."""
        sample_values = ['100.50', '3.14', '0.99']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        assert result['has_decimal_separator'] == True
        assert result['has_currency_symbols'] == False
        assert result['has_thousands_separator'] == False
        assert result['has_parentheses_negative'] == False

    def test_detect_numeric_patterns_parentheses_negative(self):
        """Test detection of parentheses for negative numbers."""
        sample_values = ['(100)', '(50.25)', '(1,000)']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        assert result['has_parentheses_negative'] == True
        assert result['has_thousands_separator'] == True  # (1,000) also has thousands separator
        assert result['has_decimal_separator'] == True   # (50.25) also has decimal separator

    def test_detect_numeric_patterns_combined(self):
        """Test detection of multiple patterns combined."""
        sample_values = ['$1,000.50', '€(500.25)', '£10,000', '¥1,234.56']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        assert result['has_currency_symbols'] == True
        assert result['has_thousands_separator'] == True
        assert result['has_decimal_separator'] == True
        assert result['has_parentheses_negative'] == True

    def test_detect_numeric_patterns_no_patterns(self):
        """Test detection with no special numeric patterns."""
        sample_values = ['100', '200', '300', 'abc', 'def']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        expected = {
            'has_thousands_separator': False,
            'has_decimal_separator': False,
            'has_currency_symbols': False,
            'has_parentheses_negative': False
        }
        assert result == expected

    def test_detect_numeric_patterns_limit_sample_size(self):
        """Test that pattern detection only checks first 10 values."""
        # Create list with more than 10 values
        sample_values = ['100'] * 15  # 15 simple values
        sample_values[12] = '$1,000.50'  # Pattern at index 12 (beyond first 10)

        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        # Should not detect the pattern since it's beyond the first 10 values
        expected = {
            'has_thousands_separator': False,
            'has_decimal_separator': False,
            'has_currency_symbols': False,
            'has_parentheses_negative': False
        }
        assert result == expected

    def test_detect_numeric_patterns_with_none_values(self):
        """Test pattern detection with None values in the sample."""
        sample_values = [None, '100', None, '$50', None]
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        # Should handle None values gracefully and still detect patterns
        assert result['has_currency_symbols'] == True

    def test_detect_numeric_patterns_mixed_types(self):
        """Test pattern detection with mixed data types."""
        sample_values = [100, '$50.25', 3.14, '(200)', True, '1,000']
        result = DataTypeConverter.detect_numeric_patterns(sample_values)

        # Should convert all values to strings and detect patterns
        assert result['has_currency_symbols'] == True
        assert result['has_parentheses_negative'] == True
        assert result['has_thousands_separator'] == True
