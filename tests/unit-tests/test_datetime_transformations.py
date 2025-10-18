"""Tests for datetime transformation utilities."""

import datetime
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pyarrow as pa
import pytest
import pytz

from forklift.utils.transformations.configs import DateTimeTransformConfig
from forklift.utils.transformations.datetime_transformations import DateTimeTransformer


class TestDateTimeTransformer:
    """Test cases for DateTimeTransformer."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DateTimeTransformer()

    def test_timezone_conversion_with_mock_object(self):
        """Test timezone conversion logic with Mock objects (lines 78-86)."""
        # Create a mock datetime object that has astimezone method
        mock_dt = Mock()
        mock_dt._mock_name = "mock_datetime"

        # Create a proper datetime for the astimezone return value
        converted_dt = datetime.datetime(2023, 1, 1, 7, 0, 0)  # EST equivalent of noon UTC
        mock_dt.astimezone = Mock(return_value=converted_dt)

        # Create test data with string (not mock object)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", timezone="America/New_York")

        # Mock the coerce_datetime to return our mock object
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        # Verify mock's astimezone was called
        mock_dt.astimezone.assert_called_once()
        assert result is not None

    def test_timezone_conversion_without_astimezone_mock(self):
        """Test timezone conversion with Mock object without astimezone method."""
        # Create a mock that doesn't have astimezone
        mock_dt = Mock()
        mock_dt._mock_name = "mock_datetime"
        # Don't add astimezone method

        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", timezone="America/New_York")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        # Should handle gracefully without calling astimezone
        assert result is not None

    def test_timezone_aware_datetime_conversion(self):
        """Test timezone conversion with real timezone-aware datetime (line 90)."""
        # Create a timezone-aware datetime
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        test_data = pa.array([dt.isoformat()])

        config = DateTimeTransformConfig(mode="common_formats", timezone="America/New_York")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result is not None

    def test_target_type_date_conversion(self):
        """Test conversion to date target type (line 95)."""
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="date")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.date32()

    def test_target_type_date_with_non_datetime(self):
        """Test date conversion when parsed value is not datetime."""
        # Mock a non-datetime return value
        mock_date = Mock()
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="date")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_date,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.date32()

    def test_target_type_timestamp_conversion(self):
        """Test conversion to timestamp target type (lines 100-103)."""
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="timestamp")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.float64()

    def test_target_type_timestamp_with_non_datetime(self):
        """Test timestamp conversion when parsed value is not datetime."""
        mock_timestamp = 1672574400.0  # Mock timestamp value
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="timestamp")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_timestamp,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.float64()

    def test_target_type_string_with_output_format_datetime(self):
        """Test string conversion with output_format for datetime (lines 107-110)."""
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(
            mode="common_formats", target_type="string", output_format="%Y-%m-%d %H:%M:%S"
        )

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()
        # Check that strftime was used by verifying the result format
        result_pandas = result.to_pandas()
        assert "2023-01-01 12:00:00" in str(result_pandas[0])

    def test_target_type_string_with_output_format_date(self):
        """Test string conversion with output_format for date object."""
        dt = datetime.date(2023, 1, 1)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(
            mode="common_formats", target_type="string", output_format="%Y-%m-%d"
        )

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()

    def test_target_type_string_with_output_format_other(self):
        """Test string conversion with output_format for non-datetime/date object."""
        mock_obj = Mock()
        test_data = pa.array(["test"])

        config = DateTimeTransformConfig(
            mode="common_formats", target_type="string", output_format="%Y-%m-%d"
        )

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_obj,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()

    def test_pyarrow_array_creation_fallback_with_mock_objects(self):
        """Test fallback PyArrow array creation with Mock objects (lines 133-144)."""
        # Create mock objects that will cause PyArrow type errors
        mock_obj1 = Mock()
        mock_obj1._mock_name = "mock1"

        mock_obj2 = Mock()
        mock_obj2._mock_methods = ["some_method"]

        # Create a mock that looks like unittest.mock
        mock_obj3 = Mock()
        mock_obj3.__class__.__module__ = "unittest.mock"

        test_data = pa.array(["2023-01-01"])  # Use string instead of mock

        config = DateTimeTransformConfig(mode="common_formats", target_type="datetime")

        # Mock coerce_datetime to return problematic mock objects
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_obj1,
        ):
            # Mock pa.array to raise ArrowTypeError on first call, succeed on second
            original_array = pa.array
            call_count = 0

            def mock_array(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise pa.ArrowTypeError("Mock conversion error")
                return original_array(
                    [None], type=kwargs.get("type", pa.timestamp("us", tz="UTC"))
                )

            with patch("pyarrow.array", side_effect=mock_array):
                result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result is not None

    def test_pyarrow_array_creation_fallback_with_typeerror(self):
        """Test fallback PyArrow array creation with TypeError."""
        mock_obj = Mock()
        mock_obj._mock_name = "problematic_mock"

        test_data = pa.array(["2023-01-01"])  # Use string instead of mock

        config = DateTimeTransformConfig(mode="common_formats", target_type="datetime")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_obj,
        ):
            original_array = pa.array
            call_count = 0

            def mock_array(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise TypeError("Type conversion error")
                return original_array(
                    [None], type=kwargs.get("type", pa.timestamp("us", tz="UTC"))
                )

            with patch("pyarrow.array", side_effect=mock_array):
                result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result is not None

    def test_mock_object_detection_various_types(self):
        """Test detection and filtering of various Mock object types."""
        # Test different ways a Mock object can be identified
        mock1 = Mock()
        mock1._mock_name = "test_mock"

        mock2 = Mock()
        mock2._mock_methods = ["test_method"]

        # Mock with unittest.mock in type string
        mock3 = MagicMock()

        test_data = pa.array(["2023-01-01"])  # Use string instead of mock

        config = DateTimeTransformConfig(mode="common_formats", target_type="datetime")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock1,
        ):
            original_array = pa.array
            call_count = 0

            def mock_array(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise pa.ArrowTypeError("Mock error")
                # Check that Mock objects were filtered to None
                values = args[0]
                assert all(v is None for v in values), f"Expected all None values, got {values}"
                return original_array(
                    values, type=kwargs.get("type", pa.timestamp("us", tz="UTC"))
                )

            with patch("pyarrow.array", side_effect=mock_array):
                result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result is not None

    def test_to_epoch_milliseconds_conversion(self):
        """Test conversion to epoch with milliseconds unit."""
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", to_epoch="milliseconds")

        epoch_value = 1672531200000  # milliseconds since epoch
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=epoch_value,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.int64()

    def test_to_epoch_microseconds_conversion(self):
        """Test conversion to epoch with microseconds unit."""
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", to_epoch="microseconds")

        epoch_value = 1672531200000000  # microseconds since epoch
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=epoch_value,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.int64()

    def test_to_epoch_nanoseconds_conversion(self):
        """Test conversion to epoch with nanoseconds unit."""
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", to_epoch="nanoseconds")

        epoch_value = 1672531200000000000  # nanoseconds since epoch
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=epoch_value,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.int64()

    def test_to_epoch_seconds_conversion(self):
        """Test conversion to epoch with seconds unit (should use float64)."""
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", to_epoch="seconds")

        epoch_value = 1672531200.0  # seconds since epoch
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=epoch_value,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.float64()

    def test_string_without_output_format_datetime(self):
        """Test string conversion without output_format for datetime."""
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="string")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()

    def test_string_without_output_format_date(self):
        """Test string conversion without output_format for date."""
        dt = datetime.date(2023, 1, 1)
        test_data = pa.array(["2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="string")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=dt,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()

    def test_string_without_output_format_other(self):
        """Test string conversion without output_format for other objects."""
        mock_obj = Mock()
        test_data = pa.array(["test"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="string")

        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            return_value=mock_obj,
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)

        assert result.type == pa.string()

    def test_null_and_empty_values(self):
        """Test handling of null and empty values."""
        test_data = pa.array([None, "", "  ", "2023-01-01"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="string")

        result = self.transformer.apply_datetime_transformation(test_data, config)
        result_list = result.to_pylist()

        # First three should be None, last should be parsed
        assert result_list[0] is None
        assert result_list[1] is None
        assert result_list[2] is None
        assert result_list[3] is not None

    def test_exception_handling(self):
        """Test exception handling during parsing."""
        test_data = pa.array(["invalid_date"])

        config = DateTimeTransformConfig(mode="common_formats", target_type="string")

        # Mock coerce_datetime to raise an exception
        with patch(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime",
            side_effect=ValueError("Parse error"),
        ):
            result = self.transformer.apply_datetime_transformation(test_data, config)
            result_list = result.to_pylist()

        # Should handle exception and return None
        assert result_list[0] is None
