"""Schema-token conversion and exact format matching in ``date_parser.format_utils``."""

import datetime
import types

import pytest

from forklift.utils.date_parser import format_utils
from forklift.utils.date_parser.format_utils import (
    format_accepts_unpadded,
    matches_format_exact,
    normalize_format,
)


class TestMonthOrMinute:
    def test_hour_far_away_from_mm_leaves_it_a_month(self):
        # More than two separator characters between HH and MM: not "HH:mm"
        assert normalize_format("HH -- MM/DD/YYYY") == "%H -- %m/%d/%Y"

    def test_mm_before_seconds_with_a_colon_is_the_minute(self):
        assert normalize_format("mm:ss") == "%M:%S"

    def test_mm_before_seconds_without_a_colon_is_the_month(self):
        assert normalize_format("MM-SS") == "%m-%S"

    def test_mm_far_away_from_seconds_is_the_month(self):
        assert normalize_format("MM -- ss") == "%m -- %S"


class TestFormatAcceptsUnpadded:
    def test_strptime_formats_always_require_padding(self):
        assert format_accepts_unpadded("%Y-%m-%d") is False

    def test_single_letter_tokens_accept_unpadded_values(self):
        assert format_accepts_unpadded("YYYY-M-D") is True


class TestMatchesFormatExact:
    def test_value_strptime_rejects_does_not_match(self):
        assert matches_format_exact("not a date", "%Y-%m-%d") is False

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("2025-01-01 3", True),  # 1 January 2025 is a Wednesday (%w == 3)
            ("2025-01-01 5", False),  # strptime ignores the weekday; the round trip does not
        ],
    )
    def test_directive_without_strict_pattern_is_checked_by_round_trip(self, value, expected):
        assert matches_format_exact(value, "%Y-%m-%d %w") is expected

    def test_value_whose_round_trip_rendering_fails_does_not_match(self, monkeypatch):
        # strftime fails for some formats depending on platform and Python version (for example
        # a lone surrogate on 3.12); simulate that with a datetime whose strftime raises.
        class UnrenderableDatetime(datetime.datetime):
            def strftime(self, fmt):
                raise ValueError("cannot render this format")

        monkeypatch.setattr(
            format_utils, "datetime", types.SimpleNamespace(datetime=UnrenderableDatetime)
        )

        assert matches_format_exact("2025-01-01 3", "%Y-%m-%d %w") is False
