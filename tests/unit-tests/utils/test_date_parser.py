import pytest
from datetime import datetime

from forklift.utils.date_parser import (
    parse_date,
    coerce_date,
    coerce_datetime,
    COMMON_DATE_FORMATS,
)

# parse_date tests

def test_parse_date_none_and_non_string():
    assert parse_date(None) is False
    assert parse_date(123) is False  # type: ignore


def test_parse_date_with_explicit_schema_token_format_success_and_failure():
    # Schema tokens YYYY MM DD
    assert parse_date("2025-08-29", fmt="YYYY-MM-DD") is True
    # Mismatch (wrong zero padding or different literal) returns False
    assert parse_date("2025/08-29", fmt="YYYY-MM-DD") is False


def test_parse_date_with_formats_list():
    # Provide custom list (token + strptime mixed)
    fmts = ["DD|MM|YYYY", "%Y*%m*%d"]
    assert parse_date("29|08|2025", formats=fmts) is True
    assert parse_date("2025*08*29", formats=fmts) is True
    # No match
    assert parse_date("29-08-2025", formats=fmts) is False


def test_parse_date_common_formats_iteration():
    # Pick one definitely in COMMON_DATE_FORMATS
    assert "%Y.%m.%d" in COMMON_DATE_FORMATS
    assert parse_date("2025.08.29") is True


def test_parse_date_falls_back_to_dateutil():
    # "March 05 2025" not in COMMON_DATE_FORMATS list exactly
    assert parse_date("March 05 2025") is True


def test_parse_date_unparseable_returns_false():
    assert parse_date("not-a-date") is False

# coerce_date tests

def test_coerce_date_empty_and_none_raise():
    with pytest.raises(ValueError):
        coerce_date(None)  # type: ignore
    with pytest.raises(ValueError):
        coerce_date("   ")


def test_coerce_date_with_explicit_fmt_tokens_success():
    assert coerce_date("2025-08-29", fmt="YYYY-MM-DD") == "2025-08-29"


def test_coerce_date_with_explicit_fmt_tokens_failure():
    with pytest.raises(ValueError):
        coerce_date("2025/08/29", fmt="YYYY-MM-DD")


def test_coerce_date_with_formats_list_success_first_and_failure():
    fmts = ["DD|MM|YYYY", "YYYY/MM/DD"]
    assert coerce_date("29|08|2025", formats=fmts) == "2025-08-29"
    assert coerce_date("2025/08/29", formats=fmts) == "2025-08-29"
    with pytest.raises(ValueError):
        coerce_date("2025*08*29", formats=fmts)


def test_coerce_date_common_formats_path():
    assert coerce_date("20250829") == "2025-08-29"  # %Y%m%d in common list


def test_coerce_date_fallback_dateutil_path():
    # Not covered by earlier patterns, but dateutil can parse
    assert coerce_date("March 5 2025") == "2025-03-05"


def test_coerce_date_bad_date_after_all_paths():
    with pytest.raises(ValueError):
        coerce_date("bad-date-value")

# coerce_datetime tests

def test_coerce_datetime_empty_and_none():
    with pytest.raises(ValueError):
        coerce_datetime(None)  # type: ignore
    with pytest.raises(ValueError):
        coerce_datetime("   ")


def test_coerce_datetime_iso_z():
    dt = coerce_datetime("2024-03-01T12:34:56Z")
    assert dt.year == 2024 and dt.minute == 34


def test_coerce_datetime_with_dateutil_fallback_strptime(monkeypatch):
    # Force dateutil parser path to raise so strptime fallback is used
    from forklift.utils import date_parser as dp

    original_parse = dp.parser.parse

    def fail_parse(value, fuzzy=False):  # mimic signature
        raise ValueError("forced fail")

    try:
        monkeypatch.setattr(dp.parser, "parse", fail_parse)
        dt = dp.coerce_datetime("2024-03-02 01:02:03")  # matches first fallback format
        assert dt.hour == 1 and dt.second == 3
    finally:
        # restore for safety (monkeypatch auto restores, but explicit is fine)
        monkeypatch.setattr(dp.parser, "parse", original_parse)


def test_coerce_datetime_invalid_raises():
    with pytest.raises(ValueError):
        coerce_datetime("not-a-datetime")

