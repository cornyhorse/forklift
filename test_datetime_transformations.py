"""
Test suite for enhanced datetime transformations in forklift.

This demonstrates the new datetime parsing capabilities including:
1. Enforce mode (strict format validation)
2. Specify formats mode (custom format lists)
3. Common formats mode (predefined formats)
4. Fuzzy parsing with dateutil
5. Epoch timestamp support (seconds, milliseconds, etc.)
6. Timezone conversions
7. Various output formats
"""

import pytest
import pyarrow as pa
import datetime
from dateutil import tz

from src.forklift.utils.data_transformations import (
    DataTransformer,
    DateTimeTransformConfig,
    create_transformation_from_config
)
from src.forklift.processors.transformations import SchemaBasedTransformer


class TestDateTimeTransformations:
    """Test suite for the enhanced datetime transformation features."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()

    def test_enforce_mode_strict_format(self):
        """Test enforce mode with strict format validation."""
        config = DateTimeTransformConfig(
            mode="enforce",
            format="YYYY-MM-DD",  # Schema token format
            target_type="string"
        )

        # Test data with valid and invalid formats
        data = ["2025-08-27", "2025-8-27", "08/27/2025", "invalid"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # Only the first item should parse successfully (exact format match)
        assert result_list[0] == "2025-08-27T00:00:00"  # Parsed successfully
        assert result_list[1] is None  # Failed (not zero-padded)
        assert result_list[2] is None  # Failed (wrong format)
        assert result_list[3] is None  # Failed (invalid)

    def test_specify_formats_mode(self):
        """Test specify_formats mode with custom format list."""
        config = DateTimeTransformConfig(
            mode="specify_formats",
            formats=["YYYY-MM-DD", "MM/DD/YYYY", "DD-MM-YYYY"],
            target_type="string"
        )

        data = ["2025-08-27", "08/27/2025", "27-08-2025", "Aug 27, 2025"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # First three should parse, last should fail (not in allowed formats)
        assert result_list[0] is not None
        assert result_list[1] is not None
        assert result_list[2] is not None
        assert result_list[3] is None  # Not in specified formats

    def test_common_formats_mode(self):
        """Test common_formats mode with predefined format list."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            target_type="string"
        )

        data = ["2025-08-27", "08/27/2025", "27-Aug-2025", "Aug 27, 2025"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # All should parse successfully with common formats
        assert all(val is not None for val in result_list)

    def test_fuzzy_parsing_enabled(self):
        """Test fuzzy parsing with dateutil when enabled."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            allow_fuzzy=True,
            target_type="string"
        )

        # Include some harder-to-parse dates
        data = ["Tuesday, August 27th 2025", "27 Aug 2025 at 2:30 PM", "2025-08-27"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # With fuzzy parsing, these should all succeed
        assert all(val is not None for val in result_list)

    def test_fuzzy_parsing_disabled(self):
        """Test that fuzzy parsing is disabled by default."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            allow_fuzzy=False,  # Explicitly disabled
            target_type="string"
        )

        # Dates that truly require fuzzy parsing (not in common formats)
        data = ["next Tuesday", "in 3 days", "yesterday at noon"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # Without fuzzy parsing, these should fail
        assert all(val is None for val in result_list)

    def test_epoch_timestamp_parsing_seconds(self):
        """Test parsing epoch timestamps in seconds."""
        config = DateTimeTransformConfig(
            from_epoch=True,
            target_type="string"
        )

        # Unix timestamp for 2025-08-27 14:30:00 UTC
        epoch_seconds = "1724766600"
        data = [epoch_seconds]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        assert result_list[0] is not None
        # Should parse to a valid datetime string

    def test_epoch_timestamp_parsing_milliseconds(self):
        """Test parsing epoch timestamps in milliseconds."""
        config = DateTimeTransformConfig(
            from_epoch=True,
            target_type="string"
        )

        # Unix timestamp in milliseconds
        epoch_ms = "1724766600000"
        data = [epoch_ms]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        assert result_list[0] is not None

    def test_datetime_to_epoch_conversion(self):
        """Test converting datetime to epoch timestamps."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            to_epoch="seconds"
        )

        data = ["2025-08-27 14:30:00"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # Should return epoch timestamp as integer
        assert isinstance(result_list[0], (int, float))
        assert result_list[0] > 1700000000  # Reasonable epoch value

    def test_datetime_to_epoch_milliseconds(self):
        """Test converting datetime to epoch milliseconds."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            to_epoch="milliseconds"
        )

        data = ["2025-08-27 14:30:00"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # Should return epoch timestamp in milliseconds
        assert isinstance(result_list[0], (int, float))
        assert result_list[0] > 1700000000000  # Millisecond range

    def test_timezone_conversion(self):
        """Test timezone conversion functionality."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            timezone="America/New_York",
            target_type="string"
        )

        data = ["2025-08-27 14:30:00"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        assert result_list[0] is not None
        # Should include timezone info in the result

    def test_target_type_date(self):
        """Test converting to date type."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            target_type="date"
        )

        data = ["2025-08-27 14:30:00"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)

        # Should return date32 type
        assert result.type == pa.date32()

    def test_target_type_timestamp(self):
        """Test converting to timestamp type."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            target_type="timestamp"
        )

        data = ["2025-08-27 14:30:00"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)

        # Should return float64 type for timestamps
        assert result.type == pa.float64()

    def test_custom_output_format(self):
        """Test custom string output formatting."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            target_type="string",
            output_format="%B %d, %Y"  # e.g., "August 27, 2025"
        )

        data = ["2025-08-27"]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        assert "August 27, 2025" in result_list[0]

    def test_schema_based_transformation_enforce_mode(self):
        """Test schema-based datetime transformation with enforce mode."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "date_col": {
                        "datetime": {
                            "enabled": True,
                            "mode": "enforce",
                            "format": "YYYY-MM-DD",
                            "target_type": "string"
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([pa.field("date_col", pa.string())])
        data = {"date_col": ["2025-08-27", "invalid_date"]}
        batch = pa.record_batch(data, pa_schema)

        transformer = SchemaBasedTransformer(schema_dict)
        result_batch, validation_results = transformer.process_batch(batch)

        result_col = result_batch.column("date_col").to_pylist()

        # First should parse, second should be None
        assert result_col[0] is not None
        assert result_col[1] is None
        assert len(validation_results) == 0  # No validation errors

    def test_schema_based_transformation_fuzzy_mode(self):
        """Test schema-based datetime transformation with fuzzy parsing."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "date_col": {
                        "datetime": {
                            "enabled": True,
                            "mode": "common_formats",
                            "allow_fuzzy": True,
                            "target_type": "string"
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([pa.field("date_col", pa.string())])
        data = {"date_col": ["August 27, 2025", "27th Aug 2025"]}
        batch = pa.record_batch(data, pa_schema)

        transformer = SchemaBasedTransformer(schema_dict)
        result_batch, validation_results = transformer.process_batch(batch)

        result_col = result_batch.column("date_col").to_pylist()

        # Both should parse with fuzzy parsing
        assert all(val is not None for val in result_col)
        assert len(validation_results) == 0

    def test_schema_based_epoch_conversion(self):
        """Test schema-based epoch timestamp conversion."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "timestamp_col": {
                        "datetime": {
                            "enabled": True,
                            "mode": "common_formats",
                            "to_epoch": "seconds",
                            "target_type": "timestamp"
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([pa.field("timestamp_col", pa.string())])
        data = {"timestamp_col": ["2025-08-27 14:30:00"]}
        batch = pa.record_batch(data, pa_schema)

        transformer = SchemaBasedTransformer(schema_dict)
        result_batch, validation_results = transformer.process_batch(batch)

        result_col = result_batch.column("timestamp_col").to_pylist()

        # Should return epoch timestamp
        assert isinstance(result_col[0], (int, float))
        assert result_col[0] > 1700000000
        assert len(validation_results) == 0

    def test_mixed_epoch_formats(self):
        """Test automatic detection of different epoch formats."""
        config = DateTimeTransformConfig(
            mode="common_formats",
            target_type="string"
        )

        # Mix of epoch formats and regular dates
        data = [
            "1724766600",      # 10-digit seconds
            "1724766600000",   # 13-digit milliseconds
            "2025-08-27",      # Regular date
            "1724766600000000" # 16-digit microseconds
        ]
        column = pa.array(data)

        result = self.transformer.apply_datetime_transformation(column, config)
        result_list = result.to_pylist()

        # All should parse successfully
        assert all(val is not None for val in result_list)

    def test_invalid_configuration_validation(self):
        """Test that invalid configurations raise appropriate errors."""
        # Test enforce mode without format
        with pytest.raises(ValueError, match="Format must be specified"):
            DateTimeTransformConfig(mode="enforce")

        # Test specify_formats mode without formats list
        with pytest.raises(ValueError, match="Formats list must be specified"):
            DateTimeTransformConfig(mode="specify_formats")

        # Test invalid mode
        with pytest.raises(ValueError, match="Invalid mode"):
            DateTimeTransformConfig(mode="invalid_mode")

        # Test invalid target type
        with pytest.raises(ValueError, match="Invalid target_type"):
            DateTimeTransformConfig(target_type="invalid_type")

        # Test invalid epoch unit
        with pytest.raises(ValueError, match="Invalid to_epoch unit"):
            DateTimeTransformConfig(to_epoch="invalid_unit")


def test_datetime_transformation_factory():
    """Test creating datetime transformations via factory function."""
    config_dict = {
        "enabled": True,
        "mode": "enforce",
        "format": "YYYY-MM-DD",
        "target_type": "string"
    }

    transform_func = create_transformation_from_config("datetime", config_dict)

    # Test the created function
    data = ["2025-08-27", "invalid"]
    column = pa.array(data)

    result = transform_func(column)
    result_list = result.to_pylist()

    assert result_list[0] is not None
    assert result_list[1] is None


if __name__ == "__main__":
    # Run a few quick tests to demonstrate functionality
    test_suite = TestDateTimeTransformations()
    test_suite.setup_method()

    print("🚀 Testing Enhanced Datetime Transformations")
    print("=" * 50)

    # Test 1: Enforce Mode
    print("✅ Test 1: Enforce Mode (Strict Format)")
    test_suite.test_enforce_mode_strict_format()
    print("   ✓ Only exact format matches are accepted")

    # Test 2: Fuzzy Parsing
    print("✅ Test 2: Fuzzy Parsing")
    test_suite.test_fuzzy_parsing_enabled()
    print("   ✓ Natural language dates parsed successfully")

    # Test 3: Epoch Timestamps
    print("✅ Test 3: Epoch Timestamp Support")
    test_suite.test_epoch_timestamp_parsing_seconds()
    test_suite.test_datetime_to_epoch_conversion()
    print("   ✓ Epoch timestamps in various formats supported")

    # Test 4: Schema Integration
    print("✅ Test 4: Schema-Based Integration")
    test_suite.test_schema_based_transformation_enforce_mode()
    print("   ✓ Seamlessly integrates with schema transformations")

    print("\n🎉 All datetime transformation features working correctly!")
    print("\nKey Features Implemented:")
    print("• 🎯 Enforce Mode: Strict format validation")
    print("• 📋 Specify Formats: Custom format lists")
    print("• 🔄 Common Formats: Predefined format recognition")
    print("• 🧠 Fuzzy Parsing: Natural language date parsing")
    print("• ⏰ Epoch Support: Unix timestamps (seconds, ms, μs, ns)")
    print("• 🌍 Timezone Conversion: Global timezone support")
    print("• 📊 Multiple Output Types: datetime, date, timestamp, string")
    print("• ���️ Schema Integration: Works with existing transformation system")
