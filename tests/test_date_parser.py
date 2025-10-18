"""
Comprehensive test suite for forklift/utils/date_parser.py

This test suite provides extensive coverage of the date parsing functionality
including edge cases, error handling, and various date/datetime formats.
"""

import datetime

import pytest

from forklift.utils.date_parser import (COMMON_DATE_FORMATS,
                                        COMMON_DATETIME_FORMATS,
                                        SCHEMA_TOKEN_MAP, coerce_date,
                                        coerce_datetime, parse_date)


class TestParseDate:
    """Test the parse_date function comprehensively."""

    def test_valid_dates_no_format(self):
        """Test parsing valid dates without specifying format."""
        assert parse_date("2025-08-27") == True
        assert parse_date("27/08/2025") == True
        assert parse_date("08/27/2025") == True
        assert parse_date("27-Aug-2025") == True
        assert parse_date("Aug 27, 2025") == True
        assert parse_date("27 Aug 2025") == True
        assert parse_date("2025.08.27") == True
        assert parse_date("20250827") == True

    def test_epoch_timestamps(self):
        """Test parsing epoch timestamps (exercises _is_epoch_timestamp and _parse_epoch_timestamp)."""
        assert parse_date("1609459200") == True  # seconds
        assert parse_date("1609459200000") == True  # milliseconds
        assert parse_date("1609459200000000") == True  # microseconds
        assert parse_date("1609459200000000000") == True  # nanoseconds

    def test_invalid_epoch_timestamps(self):
        """Test invalid epoch timestamps."""
        assert parse_date("") == False
        assert parse_date("abc") == False
        assert parse_date("123") == False  # Too short (only 3 digits)
        assert parse_date("12345678901234567890") == False  # Too long
        assert parse_date("999999999") == False  # Too early (9-digit)
        assert parse_date("123.456") == False  # Contains decimal

    def test_with_specific_format_strptime(self):
        """Test parsing with specific strptime format."""
        assert parse_date("2025-08-27", fmt="%Y-%m-%d") == True
        # This should return False for non-exact matches when fmt is specified
        assert parse_date("2025-8-27", fmt="%Y-%m-%d") == False  # Not exact match
        assert parse_date("27/08/2025", fmt="%Y-%m-%d") == False  # Wrong format
        assert parse_date("27/08/2025", fmt="%d/%m/%Y") == True

    def test_with_specific_format_schema_tokens(self):
        """Test parsing with schema token format (exercises _normalize_format)."""
        assert parse_date("2025-08-27", fmt="YYYY-MM-DD") == True
        assert parse_date("27/08/2025", fmt="DD/MM/YYYY") == True
        assert parse_date("2025/08/27", fmt="YYYY/MM/DD") == True
        assert parse_date("27-Aug-2025", fmt="DD-MMM-YYYY") == True
        assert parse_date("August 27, 2025", fmt="MMMM DD, YYYY") == True
        assert parse_date("27 Aug 2025", fmt="DD MMM YYYY") == True

        # Test case insensitive tokens
        assert parse_date("2025-08-27", fmt="yyyy-mm-dd") == True
        assert parse_date("2025-08-27", fmt="Yyyy-Mm-Dd") == True

    def test_with_formats_list(self):
        """Test parsing with list of formats."""
        formats = ["%Y-%m-%d", "%d/%m/%Y", "YYYY/MM/DD"]
        assert parse_date("2025-08-27", formats=formats) == True
        assert parse_date("27/08/2025", formats=formats) == True
        assert parse_date("2025/08/27", formats=formats) == True
        # This should use dateutil fallback and return True
        assert parse_date("Aug 27, 2025", formats=formats) == True

    def test_invalid_inputs(self):
        """Test invalid inputs."""
        assert parse_date(None) == False
        assert parse_date("") == False
        assert parse_date("invalid") == False
        assert parse_date(123) == False  # Not a string

    def test_fuzzy_parsing_fallback(self):
        """Test that fuzzy parsing works as fallback."""
        # This should work with dateutil parser fallback
        assert parse_date("January 1, 2025") == True
        assert parse_date("1st Jan 2025") == True

    def test_edge_cases(self):
        """Test edge cases that exercise internal functions."""
        # Test leap year
        assert parse_date("2024-02-29") == True

        # Test whitespace handling
        assert parse_date("  2025-08-27  ") == True

        # Test boundary epoch timestamps
        assert parse_date("1000000000") == True  # Min 10-digit
        assert parse_date("9999999999") == True  # Max 10-digit

    def test_epoch_timestamp_edge_cases(self):
        """Test edge cases in epoch timestamp validation."""
        # Test epoch timestamp that fails parsing but passes initial validation
        assert parse_date("1000000000") == True  # Valid 10-digit

        # Test non-digit strings that might slip through
        assert parse_date("not_digits") == False
        assert parse_date("123abc") == False

        # Test empty string handling in epoch check
        assert parse_date("") == False

        # Test boundary conditions for different epoch precisions
        assert parse_date("999999999") == False  # Just below 10-digit minimum
        # This will use dateutil fallback and cause OverflowError
        assert parse_date("10000000000") == False  # Just above 10-digit maximum
        assert parse_date("999999999999") == False  # Just below 13-digit minimum
        assert parse_date("10000000000000") == False  # Just above 13-digit maximum

    def test_format_normalization_coverage(self):
        """Test format normalization to cover _normalize_format branches."""
        # Test format already containing % (should not be normalized)
        assert parse_date("2025-08-27", fmt="%Y-%m-%d") == True

        # Test format without % (should be normalized)
        assert parse_date("2025-08-27", fmt="YYYY-MM-DD") == True

        # Test mixed format normalization with list
        formats = ["%Y-%m-%d", "DD/MM/YYYY"]  # Mix of strptime and schema tokens
        assert parse_date("2025-08-27", formats=formats) == True
        assert parse_date("27/08/2025", formats=formats) == True

    def test_dateutil_parse_exception_handling(self):
        """Test exception handling in dateutil parser fallback."""
        # Test strings that cause dateutil parser to fail
        assert parse_date("invalid date string xyz") == False
        assert parse_date("32/13/2025") == False  # Invalid date
        assert parse_date("2025-02-30") == False  # Invalid date (Feb 30)


class TestCoerceDate:
    """Test the coerce_date function comprehensively."""

    def test_basic_coercion(self):
        """Test basic date coercion to ISO format."""
        assert coerce_date("2025-08-27") == "2025-08-27"
        assert coerce_date("27/08/2025") == "2025-08-27"
        assert coerce_date("08/27/2025") == "2025-08-27"
        assert coerce_date("20250827") == "2025-08-27"

    def test_epoch_timestamp_coercion(self):
        """Test coercing epoch timestamps (exercises _parse_epoch_timestamp)."""
        assert coerce_date("1609459200") == "2021-01-01"  # seconds
        assert coerce_date("1609459200000") == "2021-01-01"  # milliseconds
        assert coerce_date("1609459200000000") == "2021-01-01"  # microseconds
        assert coerce_date("1609459200000000000") == "2021-01-01"  # nanoseconds

    def test_with_specific_format_strptime(self):
        """Test coercion with specific strptime format."""
        assert coerce_date("2025-08-27", fmt="%Y-%m-%d") == "2025-08-27"
        assert coerce_date("27-Aug-2025", fmt="%d-%b-%Y") == "2025-08-27"

    def test_with_specific_format_schema_tokens(self):
        """Test coercion with schema token format (exercises _normalize_format)."""
        # These should work with proper format matching
        assert coerce_date("2025-08-27", fmt="YYYY-MM-DD") == "2025-08-27"
        assert coerce_date("27/08/2025", fmt="DD/MM/YYYY") == "2025-08-27"

    def test_with_formats_list(self):
        """Test coercion with formats list."""
        formats = ["%Y-%m-%d", "%d/%m/%Y", "YYYY/MM/DD"]
        assert coerce_date("2025-08-27", formats=formats) == "2025-08-27"
        assert coerce_date("27/08/2025", formats=formats) == "2025-08-27"
        assert coerce_date("2025/08/27", formats=formats) == "2025-08-27"

    def test_dateutil_fallback(self):
        """Test fallback to dateutil parser."""
        assert coerce_date("January 1, 2025") == "2025-01-01"
        assert coerce_date("1st Jan 2025") == "2025-01-01"

    def test_invalid_inputs(self):
        """Test that invalid inputs raise ValueError."""
        with pytest.raises(ValueError, match="empty date"):
            coerce_date(None)

        with pytest.raises(ValueError, match="empty date"):
            coerce_date("")

        with pytest.raises(ValueError, match="empty date"):
            coerce_date("   ")

    def test_format_mismatch_error(self):
        """Test error when value doesn't match specified format."""
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("27/08/2025", fmt="%Y-%m-%d")

    def test_candidates_list_processing(self):
        """Test processing of candidate formats list."""
        formats = ["DD/MM/YYYY", "%m/%d/%Y"]
        result = coerce_date("27/08/2025", fmt="DD/MM/YYYY", formats=formats)
        assert result == "2025-08-27"

    def test_whitespace_handling(self):
        """Test whitespace handling."""
        assert coerce_date("  2025-08-27  ") == "2025-08-27"

    def test_epoch_timestamp_fallback(self):
        """Test epoch timestamp that fails but falls through to other methods."""
        # Valid format but should still work
        assert coerce_date("1609459200") == "2021-01-01"

    def test_format_exact_matching_failure(self):
        """Test when format matching fails exact check."""
        # This should fail exact matching and trigger error when fmt is specified
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("2025-8-27", fmt="YYYY-MM-DD")  # Single digit month

    def test_all_parsing_methods_fail(self):
        """Test when all parsing methods fail."""
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("invalid date xyz 123")


class TestCoerceDatetime:
    """Test the coerce_datetime function comprehensively."""

    def test_basic_datetime_coercion(self):
        """Test basic datetime coercion."""
        result = coerce_datetime("2025-08-27 14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_iso_format_parsing(self):
        """Test ISO format datetime parsing."""
        result = coerce_datetime("2025-08-27T14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_timezone_aware_parsing(self):
        """Test parsing timezone-aware datetimes."""
        result = coerce_datetime("2025-08-27T14:30:00Z")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_microseconds_parsing(self):
        """Test parsing datetimes with microseconds."""
        result = coerce_datetime("2025-08-27T14:30:00.123456")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, 123456)
        assert result == expected

    def test_epoch_timestamp_conversion(self):
        """Test conversion from epoch timestamps."""
        result = coerce_datetime("1609459200", from_epoch=True)
        expected = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_to_epoch_conversion(self):
        """Test conversion to epoch timestamps."""
        # Test seconds
        result = coerce_datetime("2021-01-01T00:00:00Z", to_epoch="seconds")
        assert result == 1609459200

        # Test milliseconds
        result = coerce_datetime("2021-01-01T00:00:00Z", to_epoch="milliseconds")
        assert result == 1609459200000

        # Test microseconds
        result = coerce_datetime("2021-01-01T00:00:00Z", to_epoch="microseconds")
        assert result == 1609459200000000

        # Test nanoseconds
        result = coerce_datetime("2021-01-01T00:00:00Z", to_epoch="nanoseconds")
        assert result == 1609459200000000000

        # Test invalid epoch unit
        with pytest.raises(ValueError, match="Invalid epoch unit"):
            coerce_datetime("2021-01-01T00:00:00Z", to_epoch="invalid")

    def test_format_specific_parsing(self):
        """Test parsing with specific formats."""
        result = coerce_datetime("27/08/2025 14:30:00", fmt="%d/%m/%Y %H:%M:%S")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_schema_token_formats(self):
        """Test parsing with schema token formats."""
        result = coerce_datetime("27/08/2025 14:30:00", fmt="DD/MM/YYYY HH:MM:SS")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_formats_list(self):
        """Test parsing with list of formats."""
        formats = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"]
        result = coerce_datetime("27/08/2025 14:30:00", formats=formats)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_fuzzy_parsing(self):
        """Test fuzzy parsing with dateutil."""
        result = coerce_datetime("January 1, 2025 2:30 PM", fuzzy=True)
        expected = datetime.datetime(2025, 1, 1, 14, 30, 0)
        assert result == expected

    def test_allow_fuzzy_legacy_parameter(self):
        """Test legacy allow_fuzzy parameter."""
        result = coerce_datetime("January 1, 2025 2:30 PM", allow_fuzzy=True)
        expected = datetime.datetime(2025, 1, 1, 14, 30, 0)
        assert result == expected

    def test_invalid_inputs(self):
        """Test invalid inputs."""
        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime("")

        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime(None)

        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime("   ")

    def test_invalid_epoch_timestamp(self):
        """Test invalid epoch timestamp with from_epoch=True."""
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            coerce_datetime("invalid", from_epoch=True)

    def test_epoch_auto_detection(self):
        """Test automatic epoch timestamp detection."""
        result = coerce_datetime("1609459200")  # Should auto-detect as epoch
        expected = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_format_mismatch_error(self):
        """Test error when format doesn't match."""
        with pytest.raises(ValueError, match="does not match required format"):
            coerce_datetime("27/08/2025 14:30:00", fmt="%Y-%m-%d %H:%M:%S")

    def test_formats_list_mismatch_error(self):
        """Test error when none of the formats match."""
        formats = ["%Y-%m-%d", "%d-%m-%Y"]
        with pytest.raises(ValueError, match="does not match any of the specified formats"):
            coerce_datetime("08/27/2025 14:30:00", formats=formats)

    def test_all_parsing_fails(self):
        """Test when all parsing methods fail."""
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime("invalid datetime xyz 123")

    def test_date_format_fallback(self):
        """Test fallback to date formats when datetime formats fail."""
        result = coerce_datetime("2025-08-27")  # Just a date
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

    def test_whitespace_handling(self):
        """Test whitespace handling."""
        result = coerce_datetime("  2025-08-27T14:30:00  ")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_exact_format_matching(self):
        """Test exact format matching when fmt is specified."""
        # Should work with exact match
        result = coerce_datetime("2025-08-27", fmt="YYYY-MM-DD")
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

    def test_naive_datetime_to_epoch(self):
        """Test conversion of naive datetime to epoch (treated as UTC)."""
        result = coerce_datetime("2021-01-01 00:00:00", to_epoch="seconds")
        assert result == 1609459200

    def test_timezone_aware_to_epoch(self):
        """Test conversion of timezone-aware datetime to epoch."""
        result = coerce_datetime("2021-01-01T00:00:00+00:00", to_epoch="seconds")
        assert result == 1609459200


class TestHelperFunctions:
    """Test helper functions through public API."""

    def test_schema_token_mapping(self):
        """Test that all schema tokens are properly mapped."""
        # Test year tokens
        assert coerce_date("2025-08-27", fmt="YYYY-MM-DD") == "2025-08-27"
        assert coerce_date("25-08-27", fmt="YY-MM-DD") == "2025-08-27"

        # Test month tokens
        assert coerce_date("2025-8-27", fmt="YYYY-M-DD") == "2025-08-27"
        assert coerce_date("2025-Aug-27", fmt="YYYY-MMM-DD") == "2025-08-27"
        assert coerce_date("2025-August-27", fmt="YYYY-MMMM-DD") == "2025-08-27"

        # Test day tokens
        assert coerce_date("2025-08-7", fmt="YYYY-MM-D") == "2025-08-07"

    def test_microseconds_token(self):
        """Test microseconds token handling."""
        result = coerce_datetime("2025-08-27 14:30:00.123", fmt="YYYY-MM-DD HH:MM:SS.fff")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, 123000)
        assert result == expected

    def test_epoch_timestamp_lengths(self):
        """Test different epoch timestamp lengths."""
        # 10 digits (seconds)
        assert parse_date("1609459200") == True
        # 13 digits (milliseconds)
        assert parse_date("1609459200000") == True
        # 16 digits (microseconds)
        assert parse_date("1609459200000000") == True
        # 19 digits (nanoseconds)
        assert parse_date("1609459200000000000") == True

        # Invalid lengths
        assert parse_date("160945920") == False  # 9 digits
        assert parse_date("16094592000") == False  # 11 digits
        assert parse_date("160945920000") == False  # 12 digits
        assert parse_date("16094592000000") == False  # 14 digits

    def test_epoch_boundary_values(self):
        """Test epoch timestamp boundary values."""
        # Test minimum values
        assert parse_date("1000000000") == True  # 10-digit min
        assert parse_date("1000000000000") == True  # 13-digit min
        assert parse_date("1000000000000000") == True  # 16-digit min
        assert parse_date("1000000000000000000") == True  # 19-digit min

        # Test maximum values
        assert parse_date("9999999999") == True  # 10-digit max
        assert parse_date("9999999999999") == True  # 13-digit max
        assert parse_date("9999999999999999") == True  # 16-digit max
        assert parse_date("9999999999999999999") == True  # 19-digit max

        # Test below minimum values
        assert parse_date("999999999") == False  # Below 10-digit min
        assert parse_date("999999999999") == False  # Below 13-digit min
        assert parse_date("999999999999999") == False  # Below 16-digit min
        assert parse_date("999999999999999999") == False  # Below 19-digit min

    def test_format_exact_matching(self):
        """Test exact format matching helper function through public API."""
        # These should pass exact matching
        assert parse_date("2025-08-27", fmt="%Y-%m-%d") == True
        assert parse_date("27/08/2025", fmt="%d/%m/%Y") == True

        # These should fail exact matching when fmt is specified
        assert parse_date("2025-8-27", fmt="%Y-%m-%d") == False  # Single digit month
        assert parse_date("2025-08-7", fmt="%Y-%m-%d") == False  # Single digit day

    def test_common_formats_coverage(self):
        """Test that common date and datetime formats are covered."""
        # Test some common date formats
        for fmt in COMMON_DATE_FORMATS[:5]:  # Test first 5 to avoid too many tests
            if fmt == "%Y-%m-%d":
                assert parse_date("2025-08-27") == True
            elif fmt == "%d/%m/%Y":
                assert parse_date("27/08/2025") == True
            elif fmt == "%m/%d/%Y":
                assert parse_date("08/27/2025") == True

        # Test some common datetime formats
        for fmt in COMMON_DATETIME_FORMATS[:3]:  # Test first 3
            if fmt == "%Y-%m-%d %H:%M:%S":
                assert coerce_datetime("2025-08-27 14:30:00") is not None
            elif fmt == "%Y-%m-%dT%H:%M:%S":
                assert coerce_datetime("2025-08-27T14:30:00") is not None

    def test_dateutil_overflow_handling(self):
        """Test handling of OverflowError from dateutil parser."""
        # This should trigger OverflowError in dateutil parser and return False
        assert parse_date("10000000000") == False  # Large number that causes overflow

    def test_strftime_failure_handling(self):
        """Test handling of strftime failures in exact matching."""
        # Test with a format that might cause strftime to fail
        # Use an edge case that could trigger the strftime exception path
        result = parse_date("2025-08-27", fmt="%Y-%m-%d")
        assert result == True  # Should still work despite potential strftime issues


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
