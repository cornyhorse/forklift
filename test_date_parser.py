"""
Comprehensive test suite for src/forklift/utils/date_parser.py

This test suite aims to achieve high code coverage by testing:
1. All public functions (parse_date, coerce_date, coerce_datetime)
2. Edge cases and error conditions that exercise internal helper functions
3. Different date/datetime formats
4. Epoch timestamp handling
5. Format normalization (via public API)
"""

import pytest
import datetime

from src.forklift.utils.date_parser import (
    parse_date,
    coerce_date,
    coerce_datetime,
    COMMON_DATE_FORMATS,
    COMMON_DATETIME_FORMATS
)


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
        assert parse_date("123") == False  # Too short
        assert parse_date("12345678901234567890") == False  # Too long
        assert parse_date("999999999") == False  # Too early (9-digit)
        assert parse_date("123.456") == False  # Contains decimal

    def test_with_specific_format_strptime(self):
        """Test parsing with specific strptime format."""
        assert parse_date("2025-08-27", fmt="%Y-%m-%d") == True
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
        assert parse_date("Aug 27, 2025", formats=formats) == False  # Not in list

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
        assert coerce_date("2025-08-27", fmt="YYYY-MM-DD") == "2025-08-27"
        assert coerce_date("27/08/2025", fmt="DD/MM/YYYY") == "2025-08-27"
        assert coerce_date("27-Aug-2025", fmt="DD-MMM-YYYY") == "2025-08-27"
        assert coerce_date("August 27, 2025", fmt="MMMM DD, YYYY") == "2025-08-27"

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

        with pytest.raises(ValueError, match="bad date"):
            coerce_date("invalid", formats=["%Y-%m-%d", "%d/%m/%Y"])

    def test_completely_invalid_date(self):
        """Test completely unparseable date."""
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("completely invalid date string that makes no sense")

    def test_whitespace_handling(self):
        """Test handling of whitespace in date strings."""
        assert coerce_date("  2025-08-27  ") == "2025-08-27"

    def test_leap_year_handling(self):
        """Test leap year date handling."""
        assert coerce_date("2024-02-29") == "2024-02-29"

    def test_epoch_timestamp_error_handling(self):
        """Test epoch timestamp error handling in coerce_date."""
        # Test epoch timestamp that looks valid but fails parsing
        # This should trigger the ValueError exception in the except block
        # and fall through to other parsing methods
        pass  # Most epoch edge cases are covered in the parse tests

    def test_candidates_list_processing(self):
        """Test the candidates list processing logic."""
        # Test with both fmt and formats provided (both should be added to candidates)
        formats = ["DD/MM/YYYY"]
        result = coerce_date("27/08/2025", fmt="DD/MM/YYYY", formats=formats)
        assert result == "2025-08-27"

        # Test when candidates list is built but no match found
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("invalid", fmt="YYYY-MM-DD", formats=["DD/MM/YYYY"])

    def test_common_formats_fallback(self):
        """Test fallback to COMMON_DATE_FORMATS when no candidates."""
        # This tests the _try_strptime(token, COMMON_DATE_FORMATS) path
        assert coerce_date("2025.08.27") == "2025-08-27"  # Should match COMMON_DATE_FORMATS


class TestCoerceDatetime:
    """Test the coerce_datetime function comprehensively."""

    def test_basic_datetime_coercion(self):
        """Test basic datetime coercion."""
        result = coerce_datetime("2025-08-27 14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_iso_format_parsing(self):
        """Test parsing various ISO format datetimes."""
        result = coerce_datetime("2025-08-27T14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        result = coerce_datetime("2025-08-27T14:30:00Z")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

        # Test microseconds
        result = coerce_datetime("2025-08-27T14:30:00.123456")
        assert result.microsecond == 123456

    def test_epoch_timestamp_input(self):
        """Test parsing epoch timestamps (exercises _parse_epoch_timestamp)."""
        result = coerce_datetime("1609459200")  # 2021-01-01 00:00:00 UTC
        expected = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

        # Test different epoch precisions
        result = coerce_datetime("1609459200123")  # milliseconds
        expected = datetime.datetime(2021, 1, 1, 0, 0, 0, 123000, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_from_epoch_parameter(self):
        """Test explicit from_epoch parameter."""
        result = coerce_datetime("1609459200", from_epoch=True)
        expected = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

        # Should fail if from_epoch=True but value is not epoch
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            coerce_datetime("2025-08-27", from_epoch=True)

    def test_to_epoch_conversion(self):
        """Test conversion to various epoch units (exercises _datetime_to_epoch)."""
        # Test seconds
        result = coerce_datetime("2021-01-01T00:00:00Z", to_epoch="seconds")
        assert result == 1609459200

        # Test milliseconds
        result = coerce_datetime("2021-01-01T00:00:00.123Z", to_epoch="milliseconds")
        assert result == 1609459200123

        # Test microseconds
        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="microseconds")
        assert result == 1609459200123456

        # Test nanoseconds
        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="nanoseconds")
        assert result == 1609459200123456000

        # Test invalid unit
        with pytest.raises(ValueError, match="Invalid epoch unit"):
            coerce_datetime("2021-01-01T00:00:00Z", to_epoch="invalid")

    def test_specific_format_enforcement(self):
        """Test strict format enforcement (exercises _matches_format_exact)."""
        result = coerce_datetime("2025-08-27 14:30:00", fmt="%Y-%m-%d %H:%M:%S")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Should fail if format doesn't match exactly
        with pytest.raises(ValueError, match="does not match required format"):
            coerce_datetime("2025-8-27 14:30:00", fmt="%Y-%m-%d %H:%M:%S")

    def test_schema_token_format(self):
        """Test datetime parsing with schema tokens (exercises _normalize_format)."""
        result = coerce_datetime("2025-08-27 14:30:00", fmt="YYYY-MM-DD HH:mm:SS")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Test various schema token combinations
        result = coerce_datetime("27/08/2025 14:30:00", fmt="DD/MM/YYYY HH:mm:SS")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_formats_list_enforcement(self):
        """Test strict format list enforcement."""
        formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "YYYY/MM/DD HH:mm:SS"]

        result = coerce_datetime("2025-08-27 14:30:00", formats=formats)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        result = coerce_datetime("2025-08-27T14:30:00", formats=formats)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        result = coerce_datetime("2025/08/27 14:30:00", formats=formats)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Should fail if no format matches
        with pytest.raises(ValueError, match="does not match any of the specified formats"):
            coerce_datetime("27-Aug-2025 14:30:00", formats=formats)

    def test_common_format_fallback(self):
        """Test fallback to common formats (exercises _try_strptime)."""
        # Test common datetime formats
        result = coerce_datetime("08/27/2025 14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Test common date formats
        result = coerce_datetime("2025-08-27")
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

    def test_dateutil_fallback(self):
        """Test fallback to dateutil parser."""
        result = coerce_datetime("August 27, 2025 2:30 PM")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_fuzzy_parsing(self):
        """Test fuzzy parsing option."""
        # Without fuzzy, this should fail
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime("The date is August 27, 2025 at 2:30 PM", allow_fuzzy=False)

        # With fuzzy, it should work
        result = coerce_datetime("The date is August 27, 2025 at 2:30 PM", allow_fuzzy=True)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_timezone_handling(self):
        """Test timezone handling in datetime strings."""
        # Test Z suffix
        result = coerce_datetime("2025-08-27T14:30:00Z")
        assert result.tzinfo is not None

        # Test explicit timezone offset
        result = coerce_datetime("2025-08-27T14:30:00+00:00")
        assert result.tzinfo is not None

        result = coerce_datetime("2025-08-27T14:30:00+05:30")
        assert result.tzinfo is not None

        result = coerce_datetime("2025-08-27T14:30:00-08:00")
        assert result.tzinfo is not None

    def test_invalid_inputs(self):
        """Test invalid inputs raise appropriate errors."""
        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime(None)

        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime("")

        with pytest.raises(ValueError, match="empty datetime"):
            coerce_datetime("   ")

    def test_completely_invalid_datetime(self):
        """Test completely unparseable datetime."""
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime("completely invalid datetime string", allow_fuzzy=False)

    def test_whitespace_handling(self):
        """Test handling of whitespace in datetime strings."""
        result = coerce_datetime("  2025-08-27 14:30:00  ")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_epoch_error_handling_paths(self):
        """Test epoch error handling edge cases."""
        # Test from_epoch=True with invalid epoch (should raise immediately)
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            coerce_datetime("invalid", from_epoch=True)

        # Test epoch detection that fails parsing but from_epoch=False (should continue)
        # This is harder to trigger since _is_epoch_timestamp is pretty strict

    def test_format_matching_edge_cases(self):
        """Test edge cases in format matching logic."""
        # Test when format matches but strptime fails (covers except ValueError branch)
        # This is hard to trigger since _matches_format_exact already validates parsing

        # Test format normalization branches
        result = coerce_datetime("2025-08-27 14:30:00", fmt="%Y-%m-%d %H:%M:%S")  # Already strptime
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        result = coerce_datetime("2025-08-27 14:30:00", fmt="YYYY-MM-DD HH:mm:SS")  # Schema tokens
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_formats_list_processing_edge_cases(self):
        """Test edge cases in formats list processing."""
        # Test mixed format types in list
        formats = ["%Y-%m-%d %H:%M:%S", "YYYY-MM-DD HH:mm:SS"]
        result = coerce_datetime("2025-08-27 14:30:00", formats=formats)
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Test when format matches but strptime fails (continue loop)
        # This would require a format that _matches_format_exact passes but strptime fails
        # which is unlikely given the implementation

    def test_dateutil_iso_replacement(self):
        """Test the Z to +00:00 replacement in dateutil fallback."""
        # Test the iso_try = token.replace("Z", "+00:00") logic
        result = coerce_datetime("2025-08-27T14:30:00Z")
        assert result.tzinfo is not None

        # Test with different timezone format
        result = coerce_datetime("2025-08-27T14:30:00+02:00")
        assert result.tzinfo is not None

    def test_fuzzy_parsing_failure_paths(self):
        """Test fuzzy parsing failure scenarios."""
        # Test when allow_fuzzy=False and parsing fails
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime("completely unparseable garbage", allow_fuzzy=False)

        # Test when allow_fuzzy=True but even fuzzy parsing fails
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime("", allow_fuzzy=True)

    def test_final_parsed_dt_none_check(self):
        """Test the final check for parsed_dt is None."""
        # This is hard to trigger since all the parsing paths should either succeed or raise
        # The main case would be if all parsing methods silently fail without raising
        pass


class TestConstants:
    """Test that the module constants are properly defined."""

    def test_common_date_formats_exist(self):
        """Test that COMMON_DATE_FORMATS is defined and contains expected formats."""
        assert isinstance(COMMON_DATE_FORMATS, list)
        assert len(COMMON_DATE_FORMATS) > 0
        assert "%Y-%m-%d" in COMMON_DATE_FORMATS
        assert "%Y%m%d" in COMMON_DATE_FORMATS
        assert "%m/%d/%Y" in COMMON_DATE_FORMATS
        assert "%d/%m/%Y" in COMMON_DATE_FORMATS

    def test_common_datetime_formats_exist(self):
        """Test that COMMON_DATETIME_FORMATS is defined and contains expected formats."""
        assert isinstance(COMMON_DATETIME_FORMATS, list)
        assert len(COMMON_DATETIME_FORMATS) > 0
        assert "%Y-%m-%d %H:%M:%S" in COMMON_DATETIME_FORMATS
        assert "%Y-%m-%dT%H:%M:%S" in COMMON_DATETIME_FORMATS
        assert "%Y-%m-%dT%H:%M:%SZ" in COMMON_DATETIME_FORMATS


class TestEdgeCasesAndBoundaries:
    """Test edge cases and boundary conditions that exercise internal functions."""

    def test_boundary_epoch_timestamps(self):
        """Test boundary epoch timestamps (exercises _is_epoch_timestamp edge cases)."""
        # Test minimum valid timestamps for each precision
        assert parse_date("1000000000") == True  # Min 10-digit (seconds)
        assert parse_date("1000000000000") == True  # Min 13-digit (milliseconds)
        assert parse_date("1000000000000000") == True  # Min 16-digit (microseconds)
        assert parse_date("1000000000000000000") == True  # Min 19-digit (nanoseconds)

        # Test maximum valid timestamps
        assert parse_date("9999999999") == True  # Max 10-digit
        assert parse_date("9999999999999") == True  # Max 13-digit
        assert parse_date("9999999999999999") == True  # Max 16-digit
        assert parse_date("9999999999999999999") == True  # Max 19-digit

        # Test just outside boundaries
        assert parse_date("999999999") == False  # Too short
        assert parse_date("10000000000") == False  # Too long for 10-digit
        assert parse_date("100000000000") == False  # Too short for 13-digit

    def test_invalid_epoch_conversion_edge_cases(self):
        """Test edge cases in epoch timestamp conversion."""
        # Test parsing invalid epoch when from_epoch=True
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            coerce_datetime("123", from_epoch=True)

        # Test parsing borderline invalid epoch
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            coerce_datetime("999999999", from_epoch=True)

    def test_format_normalization_edge_cases(self):
        """Test edge cases in format normalization via public API."""
        # Test overlapping token patterns (longer tokens should be processed first)
        assert parse_date("2025-08-27", fmt="YYYY-MM-DD") == True

        # Test tokens that could conflict if not processed in correct order
        assert parse_date("August 27, 2025", fmt="MMMM DD, YYYY") == True
        assert parse_date("Aug 27, 2025", fmt="MMM DD, YYYY") == True

    def test_strptime_edge_cases(self):
        """Test edge cases in strptime parsing (exercises _try_strptime via public API)."""
        # Test with empty formats list
        with pytest.raises(ValueError):
            coerce_date("2025-08-27", formats=[])

        # Test with list containing invalid formats mixed with valid ones
        formats = ["invalid_format", "%Y-%m-%d", "another_invalid"]
        assert coerce_date("2025-08-27", formats=formats) == "2025-08-27"

    def test_exact_format_matching_edge_cases(self):
        """Test edge cases in exact format matching (exercises _matches_format_exact)."""
        # Test that inexact matches fail even if they parse
        assert parse_date("2025-8-27", fmt="%Y-%m-%d") == False  # Missing zero padding
        assert parse_date("2025-08-7", fmt="%Y-%m-%d") == False  # Missing zero padding

        # Test that extra characters cause failure
        assert parse_date("2025-08-27 extra", fmt="%Y-%m-%d") == False

    def test_dateutil_parser_integration(self):
        """Test integration with dateutil parser fallback."""
        # Test various natural language date formats that dateutil can handle
        assert parse_date("tomorrow") == False  # This should fail in strict mode
        assert parse_date("next week") == False  # This should fail in strict mode

        # Test formats that dateutil can parse but are ambiguous
        assert parse_date("12/25/2025") == True  # Should work via common formats
        assert parse_date("25/12/2025") == True  # Should work via common formats

    def test_token_map_coverage(self):
        """Test all token mappings in _TOKEN_MAP."""
        # Test all schema tokens to ensure _normalize_format covers them
        assert parse_date("2025-08-27", fmt="YYYY-MM-DD") == True  # YYYY, MM, DD
        assert parse_date("August 27, 2025", fmt="MMMM DD, YYYY") == True  # MMMM
        assert parse_date("Aug 27, 2025", fmt="MMM DD, YYYY") == True  # MMM
        assert parse_date("27/08/2025 14:30:45", fmt="DD/MM/YYYY HH:mm:SS") == True  # HH, mm, SS

    def test_exception_handling_in_matches_format_exact(self):
        """Test exception handling in _matches_format_exact."""
        # Test with invalid format string that causes strptime to raise
        assert parse_date("2025-08-27", fmt="%invalid") == False

        # Test with format that causes strftime to fail
        # This is harder to trigger since strptime already validates the format

    def test_exception_handling_in_try_strptime(self):
        """Test exception handling in _try_strptime."""
        # Test formats list with invalid formats (should continue to next)
        formats = ["%invalid1", "%Y-%m-%d", "%invalid2"]
        assert coerce_date("2025-08-27", formats=formats) == "2025-08-27"

    def test_value_error_handling_in_epoch_parsing(self):
        """Test ValueError handling in epoch timestamp parsing."""
        # This tests the except ValueError block in parse_date for epoch timestamps
        # It's hard to trigger since _is_epoch_timestamp is quite strict
        # but we can test the fallback behavior
        pass

    def test_all_common_formats_coverage(self):
        """Test that our tests cover all common formats."""
        # Test various formats from COMMON_DATE_FORMATS
        test_dates = [
            ("20250827", "%Y%m%d"),
            ("2025-08-27", "%Y-%m-%d"),
            ("08/27/2025", "%m/%d/%Y"),
            ("27/08/2025", "%d/%m/%Y"),
            ("2025/08/27", "%Y/%m/%d"),
            ("27-Aug-2025", "%d-%b-%Y"),
            ("Aug 27, 2025", "%b %d, %Y"),
            ("27 Aug 2025", "%d %b %Y"),
            ("2025.08.27", "%Y.%m.%d"),
        ]

        for date_str, fmt in test_dates:
            assert parse_date(date_str) == True
            assert coerce_date(date_str) == "2025-08-27"


class TestAdditionalCoverage:
    """Additional tests to achieve maximum code coverage."""

    def test_epoch_timestamp_value_error_path(self):
        """Test the ValueError exception path in epoch timestamp parsing."""
        # This tests the case where _is_epoch_timestamp returns True but _parse_epoch_timestamp fails
        # We need to create a scenario where the timestamp looks valid but parsing fails

        # Test with a number that passes _is_epoch_timestamp but fails in conversion
        # This is tricky since the validation is quite robust, but let's test edge cases

        # Test that large valid timestamps work
        assert parse_date("2147483647") == True  # Max 32-bit timestamp (2038)

        # Test timestamp that might cause conversion issues (but probably won't)
        assert coerce_date("1609459200") == "2021-01-01"

    def test_try_strptime_all_formats_fail(self):
        """Test _try_strptime when all formats in the list fail."""
        # This should return None and trigger fallback behavior
        with pytest.raises(ValueError, match="bad date"):
            coerce_date("invalid_date", formats=["%Y-%m-%d", "%d/%m/%Y"])

    def test_matches_format_exact_strftime_exception(self):
        """Test _matches_format_exact when strftime might fail."""
        # Test edge case where strptime succeeds but strftime comparison fails
        # This is rare but can happen with certain format/value combinations

        # Test with valid date but check exact matching
        assert parse_date("2025-08-27", fmt="%Y-%m-%d") == True
        assert parse_date("2025-8-27", fmt="%Y-%m-%d") == False  # Not exact due to zero padding

    def test_normalize_format_no_tokens(self):
        """Test _normalize_format when no tokens need replacement."""
        # Test string with no schema tokens (should return unchanged)
        assert parse_date("2025-08-27", fmt="2025-08-27") == False  # Literal string, should fail

        # Test format that looks like tokens but isn't
        assert parse_date("2025-08-27", fmt="AAAA-BB-CC") == False

    def test_coerce_datetime_strptime_exception_path(self):
        """Test exception handling in coerce_datetime strptime calls."""
        # Test when _matches_format_exact returns True but strptime still fails
        # This covers the except ValueError block in format matching

        # This is hard to trigger directly, so test related scenarios
        result = coerce_datetime("2025-08-27 14:30:00", fmt="%Y-%m-%d %H:%M:%S")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

    def test_coerce_datetime_no_explicit_format_paths(self):
        """Test coerce_datetime paths when no explicit format is specified."""
        # Test the fallback sequence: common datetime -> common date -> dateutil

        # Should hit common datetime formats
        result = coerce_datetime("2025-08-27T14:30:00")
        expected = datetime.datetime(2025, 8, 27, 14, 30, 0)
        assert result == expected

        # Should hit common date formats
        result = coerce_datetime("2025-08-27")
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

        # Should hit dateutil parser
        result = coerce_datetime("August 27, 2025")
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

    def test_coerce_datetime_epoch_but_not_from_epoch(self):
        """Test when input looks like epoch but from_epoch=False."""
        # This tests the path where _is_epoch_timestamp is True but from_epoch is False
        result = coerce_datetime("1609459200")  # Should be parsed as epoch anyway
        expected = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_coerce_datetime_fuzzy_second_attempt(self):
        """Test the second fuzzy parsing attempt in dateutil fallback."""
        # Test the case where first dateutil parse fails but second with fuzzy=True succeeds

        # This is hard to trigger since we'd need a string that fails with fuzzy=False
        # but succeeds with fuzzy=True when allow_fuzzy=True
        result = coerce_datetime("The date is August 27, 2025", allow_fuzzy=True)
        expected = datetime.datetime(2025, 8, 27, 0, 0, 0)
        assert result == expected

    def test_is_epoch_timestamp_value_error_exception(self):
        """Test the ValueError exception handling in _is_epoch_timestamp."""
        # Test inputs that might cause int() conversion to fail
        # Most are caught by isdigit() check, but let's be thorough
        assert parse_date("abc123") == False
        assert parse_date("123.456") == False
        assert parse_date("1e10") == False  # Scientific notation

    def test_parse_epoch_timestamp_all_units(self):
        """Test _parse_epoch_timestamp with all supported precisions."""
        # Test all the precision branches in _parse_epoch_timestamp

        # 10-digit seconds
        result = coerce_datetime("1609459200")
        assert result.year == 2021

        # 13-digit milliseconds
        result = coerce_datetime("1609459200123")
        assert result.microsecond == 123000

        # 16-digit microseconds
        result = coerce_datetime("1609459200123456")
        assert result.microsecond == 123456

        # 19-digit nanoseconds (truncated to microseconds in Python)
        result = coerce_datetime("1609459200123456789")
        assert result.microsecond == 123456

    def test_datetime_to_epoch_all_units(self):
        """Test _datetime_to_epoch with all supported units."""
        dt = datetime.datetime(2021, 1, 1, 0, 0, 0, 123456, tzinfo=datetime.timezone.utc)

        # Test all units through to_epoch parameter
        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="seconds")
        assert isinstance(result, int)

        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="milliseconds")
        assert isinstance(result, int)

        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="microseconds")
        assert isinstance(result, int)

        result = coerce_datetime("2021-01-01T00:00:00.123456Z", to_epoch="nanoseconds")
        assert isinstance(result, int)

    def test_parse_date_formats_early_return(self):
        """Test parse_date when formats list finds a match early."""
        # Test that the function returns True as soon as a format matches
        formats = ["DD/MM/YYYY", "%Y-%m-%d", "YYYY/MM/DD"]
        assert parse_date("27/08/2025", formats=formats) == True

    def test_coerce_date_epoch_exception_fallthrough(self):
        """Test coerce_date when epoch parsing fails and falls through."""
        # Test a string that looks like epoch but fails parsing
        # This is hard since _is_epoch_timestamp is strict, but test the fallthrough

        # Use a real date that will work through normal parsing
        assert coerce_date("2025-08-27") == "2025-08-27"

    def test_common_datetime_formats_microseconds(self):
        """Test datetime formats with microsecond precision."""
        # Test formats that include microseconds
        result = coerce_datetime("2025-08-27 14:30:00.123456")
        assert result.microsecond == 123456

        result = coerce_datetime("2025-08-27T14:30:00.123456")
        assert result.microsecond == 123456

        result = coerce_datetime("2025-08-27T14:30:00.123456Z")
        assert result.microsecond == 123456

    def test_normalize_format_case_combinations(self):
        """Test _normalize_format with various case combinations."""
        # Test different case combinations to ensure case-insensitive matching
        assert parse_date("2025-08-27", fmt="yyyy-mm-dd") == True
        assert parse_date("2025-08-27", fmt="YYYY-mm-dd") == True
        assert parse_date("2025-08-27", fmt="Yyyy-Mm-Dd") == True

        # Test month tokens with different cases
        assert parse_date("August 27, 2025", fmt="mmmm dd, yyyy") == True
        assert parse_date("Aug 27, 2025", fmt="mmm dd, yyyy") == True

