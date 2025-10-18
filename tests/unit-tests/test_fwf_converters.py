"""Tests for FWF type converter functionality."""

from unittest.mock import Mock

import pyarrow as pa
import pytest

from forklift.inputs.fwf.converters import FwfTypeConverter


class TestFwfTypeConverter:
    """Test cases for FWF type converter."""

    def test_get_arrow_type_basic_types(self):
        """Test basic type conversions."""
        # Integer types
        assert FwfTypeConverter.get_arrow_type("int8") == pa.int8()
        assert FwfTypeConverter.get_arrow_type("int16") == pa.int16()
        assert FwfTypeConverter.get_arrow_type("int32") == pa.int32()
        assert FwfTypeConverter.get_arrow_type("int64") == pa.int64()

        # Unsigned integer types
        assert FwfTypeConverter.get_arrow_type("uint8") == pa.uint8()
        assert FwfTypeConverter.get_arrow_type("uint16") == pa.uint16()
        assert FwfTypeConverter.get_arrow_type("uint32") == pa.uint32()
        assert FwfTypeConverter.get_arrow_type("uint64") == pa.uint64()

        # Float types
        assert FwfTypeConverter.get_arrow_type("float32") == pa.float32()
        assert FwfTypeConverter.get_arrow_type("float64") == pa.float64()
        assert FwfTypeConverter.get_arrow_type("double") == pa.float64()

        # Boolean type
        assert FwfTypeConverter.get_arrow_type("bool") == pa.bool_()

        # String types
        assert FwfTypeConverter.get_arrow_type("string") == pa.string()
        assert FwfTypeConverter.get_arrow_type("utf8") == pa.string()

        # Binary type
        assert FwfTypeConverter.get_arrow_type("binary") == pa.binary()

        # Date types
        assert FwfTypeConverter.get_arrow_type("date32") == pa.date32()
        assert FwfTypeConverter.get_arrow_type("date64") == pa.date64()

        # Basic timestamp
        assert FwfTypeConverter.get_arrow_type("timestamp") == pa.timestamp("ns")

    def test_get_arrow_type_timestamp_with_units(self):
        """Test timestamp type conversions with specific units."""
        assert FwfTypeConverter.get_arrow_type("timestamp[s]") == pa.timestamp("s")
        assert FwfTypeConverter.get_arrow_type("timestamp[ms]") == pa.timestamp("ms")
        assert FwfTypeConverter.get_arrow_type("timestamp[us]") == pa.timestamp("us")
        assert FwfTypeConverter.get_arrow_type("timestamp[ns]") == pa.timestamp("ns")

    def test_get_arrow_type_duration_with_units(self):
        """Test duration type conversions with specific units."""
        assert FwfTypeConverter.get_arrow_type("duration[s]") == pa.duration("s")
        assert FwfTypeConverter.get_arrow_type("duration[ms]") == pa.duration("ms")
        assert FwfTypeConverter.get_arrow_type("duration[us]") == pa.duration("us")
        assert FwfTypeConverter.get_arrow_type("duration[ns]") == pa.duration("ns")

    def test_get_arrow_type_unknown_type_defaults_to_string(self):
        """Test handling of unknown types defaults to string."""
        # Unknown types should default to string type
        assert FwfTypeConverter.get_arrow_type("unknown_type") == pa.string()
        assert FwfTypeConverter.get_arrow_type("UNKNOWN") == pa.string()
        assert FwfTypeConverter.get_arrow_type("") == pa.string()

    def test_get_arrow_type_case_sensitivity(self):
        """Test that type conversion is case sensitive."""
        # This should work
        assert FwfTypeConverter.get_arrow_type("string") == pa.string()

        # These should default to string (case sensitive)
        assert FwfTypeConverter.get_arrow_type("STRING") == pa.string()
        assert FwfTypeConverter.get_arrow_type("String") == pa.string()

    def test_get_arrow_type_list_types(self):
        """Test list type conversions."""
        assert FwfTypeConverter.get_arrow_type("list<int32>") == pa.list_(pa.int32())
        assert FwfTypeConverter.get_arrow_type("list<string>") == pa.list_(pa.string())
        assert FwfTypeConverter.get_arrow_type("list<float64>") == pa.list_(pa.float64())

    def test_get_arrow_type_decimal_types(self):
        """Test decimal type conversions."""
        # Basic decimal
        assert FwfTypeConverter.get_arrow_type("decimal") == pa.decimal128(10, 2)

        # Decimal with precision only
        assert FwfTypeConverter.get_arrow_type("decimal(15)") == pa.decimal128(15, 2)

        # Decimal with precision and scale
        assert FwfTypeConverter.get_arrow_type("decimal(18,4)") == pa.decimal128(18, 4)

    def test_convert_value_integer_types(self):
        """Test value conversion for integer types."""
        assert FwfTypeConverter.convert_value("123", "int32") == 123
        assert FwfTypeConverter.convert_value("456", "int64") == 456
        assert FwfTypeConverter.convert_value("789", "uint32") == 789

    def test_convert_value_float_types(self):
        """Test value conversion for float types."""
        assert FwfTypeConverter.convert_value("123.45", "float32") == 123.45
        assert FwfTypeConverter.convert_value("678.90", "float64") == 678.90
        assert FwfTypeConverter.convert_value("999.99", "double") == 999.99

    def test_convert_value_boolean_types(self):
        """Test value conversion for boolean types."""
        # True values
        assert FwfTypeConverter.convert_value("true", "bool") == True
        assert FwfTypeConverter.convert_value("True", "bool") == True
        assert FwfTypeConverter.convert_value("1", "bool") == True
        assert FwfTypeConverter.convert_value("yes", "bool") == True
        assert FwfTypeConverter.convert_value("y", "bool") == True
        assert FwfTypeConverter.convert_value("t", "bool") == True

        # False values
        assert FwfTypeConverter.convert_value("false", "bool") == False
        assert FwfTypeConverter.convert_value("0", "bool") == False
        assert FwfTypeConverter.convert_value("no", "bool") == False

    def test_convert_value_string_types(self):
        """Test value conversion for string types."""
        assert FwfTypeConverter.convert_value("hello", "string") == "hello"
        assert FwfTypeConverter.convert_value("world", "utf8") == "world"

    def test_convert_value_empty_values(self):
        """Test value conversion for empty values."""
        assert FwfTypeConverter.convert_value("", "int32") is None
        assert FwfTypeConverter.convert_value("", "float64") is None
        assert FwfTypeConverter.convert_value("", "string") is None
        assert FwfTypeConverter.convert_value(None, "int32") is None

    def test_convert_value_decimal_types(self):
        """Test value conversion for decimal types."""
        assert FwfTypeConverter.convert_value("123.45", "decimal") == 123.45
        assert FwfTypeConverter.convert_value("678.90", "decimal(10,2)") == 678.90
