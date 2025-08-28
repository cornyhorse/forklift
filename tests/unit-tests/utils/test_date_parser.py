import pytest
from forklift.utils.date_parser import parse_date, COMMON_DATE_FORMATS, _normalize_format, _matches_format_exact

def test_parse_date_valid_common_formats():
    cases = [
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
    for value, fmt in cases:
        assert parse_date(value, fmt)
        assert parse_date(value)  # Should work with default formats

def test_parse_date_invalid_common_formats():
    cases = [
        ("2025-13-27", "%Y-%m-%d"),  # invalid month
        ("99/99/9999", "%m/%d/%Y"),  # invalid date
        ("2025/32/08", "%Y/%d/%m"),  # truly invalid day
        ("Aug 32, 2025", "%b %d, %Y"),  # invalid day
        ("", "%Y%m%d"),  # empty string
        (None, "%Y%m%d"),  # None
        ("notadate", "%Y%m%d"),  # not a date
    ]
    for value, fmt in cases:
        assert not parse_date(value, fmt)
        assert not parse_date(value)

def test_parse_date_custom_format():
    assert parse_date("2025|08|27", "%Y|%m|%d")
    assert not parse_date("2025|8|27", "%Y|%m|%d")  # month must be 2 digits

def test_parse_date_multiple_formats():
    formats = ["%Y-%m-%d", "%d/%m/%Y"]
    assert parse_date("2025-08-27", formats=formats)
    assert parse_date("27/08/2025", formats=formats)
    assert not parse_date("2025/08/27", formats=formats)

def test_parse_date_wrong_type():
    assert not parse_date(20250827, "%Y%m%d")
    assert not parse_date([], "%Y%m%d")
    assert not parse_date({}, "%Y%m%d")

def test_normalize_format_basic():
    assert _normalize_format("YYYY-MM-DD") == "%Y-%m-%d"
    assert _normalize_format("DD/MMMM/YYYY") == "%d/%B/%Y"
    assert _normalize_format("MMM DD, YYYY") == "%b %d, %Y"
    assert _normalize_format("YYYY.MM.DD") == "%Y.%m.%d"
    assert _normalize_format("YYYY/MM/DD") == "%Y/%m/%d"

def test_normalize_format_no_tokens():
    # Should return unchanged if no schema tokens
    assert _normalize_format("%Y-%m-%d") == "%Y-%m-%d"
    assert _normalize_format("%d/%m/%Y") == "%d/%m/%Y"

def test_matches_format_exact_strict():
    # Exact match required
    assert _matches_format_exact("2025-08-27", "%Y-%m-%d")
    assert not _matches_format_exact("2025-8-27", "%Y-%m-%d")  # month not zero-padded
    assert not _matches_format_exact("2025-08-27 ", "%Y-%m-%d")  # trailing space
    assert not _matches_format_exact("2025-08-27T00:00:00", "%Y-%m-%d")  # extra literal

def test_matches_format_exact_invalid():
    # Should return False for invalid dates
    assert not _matches_format_exact("2025-13-27", "%Y-%m-%d")
    assert not _matches_format_exact("notadate", "%Y-%m-%d")
    assert not _matches_format_exact("", "%Y-%m-%d")

def test_parse_date_fallback_dateutil():
    # This string is parseable by dateutil, but not by any of the listed formats
    value = "27th of August, 2025"
    assert parse_date(value)
