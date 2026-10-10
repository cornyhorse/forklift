"""Input validation and range of ``date_parser.epoch``."""

import datetime

import pytest

from forklift.utils.date_parser.epoch import is_epoch_timestamp, parse_epoch_timestamp

UTC = datetime.timezone.utc


class TestIsEpochTimestamp:
    def test_empty_text_is_not_an_epoch(self):
        assert is_epoch_timestamp("") is False


class TestParseEpochTimestamp:
    @pytest.mark.parametrize("value", ["12345", "17000000OO", ""])
    def test_text_that_is_not_an_epoch_raises(self, value):
        with pytest.raises(ValueError, match="Invalid epoch timestamp"):
            parse_epoch_timestamp(value)

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("9999999999", datetime.datetime(2286, 11, 20, 17, 46, 39, tzinfo=UTC)),
            ("9999999999999", datetime.datetime(2286, 11, 20, 17, 46, 39, 999000, tzinfo=UTC)),
            (
                "9999999999999999",
                datetime.datetime(2286, 11, 20, 17, 46, 39, 999999, tzinfo=UTC),
            ),
            (
                "9999999999999999999",
                datetime.datetime(2286, 11, 20, 17, 46, 39, 999999, tzinfo=UTC),
            ),
        ],
    )
    def test_largest_epoch_of_each_precision_is_in_range(self, value, expected):
        assert parse_epoch_timestamp(value) == expected
