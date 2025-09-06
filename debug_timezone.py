#!/usr/bin/env python3

import datetime
from unittest.mock import Mock, patch
import pyarrow as pa
from src.forklift.utils.transformations import DataTransformer, DateTimeTransformConfig

def debug_timezone_test():
    """Debug the timezone handling to understand why pytz.timezone isn't being called."""

    with patch('forklift.utils.data_transformations.coerce_datetime') as mock_coerce, \
         patch('pytz.timezone') as mock_timezone:

        transformer = DataTransformer()
        config = DateTimeTransformConfig(timezone="America/New_York")

        # Create a mock datetime object
        mock_dt = Mock()
        mock_dt.tzinfo = None
        mock_coerce.return_value = mock_dt

        # Mock the timezone object
        mock_tz = Mock()
        mock_timezone.return_value = mock_tz
        mock_dt_converted = datetime.datetime(2023, 1, 1, 7, 0, 0)
        mock_dt.astimezone.return_value = mock_dt_converted

        print("=== Debug Info ===")
        print(f"mock_dt type: {type(mock_dt)}")
        print(f"isinstance(mock_dt, datetime.datetime): {isinstance(mock_dt, datetime.datetime)}")
        print(f"hasattr(mock_dt, '_mock_name'): {hasattr(mock_dt, '_mock_name')}")
        print(f"'Mock' in str(type(mock_dt)): {'Mock' in str(type(mock_dt))}")
        print(f"hasattr(mock_dt, '_mock_methods'): {hasattr(mock_dt, '_mock_methods')}")

        column = pa.array(["2023-01-01"])
        result = transformer.apply_datetime_transformation(column, config)

        print(f"mock_timezone.called: {mock_timezone.called}")
        print(f"mock_timezone.call_count: {mock_timezone.call_count}")
        if mock_timezone.call_args:
            print(f"mock_timezone.call_args: {mock_timezone.call_args}")
        else:
            print("mock_timezone was never called")

if __name__ == "__main__":
    debug_timezone_test()
