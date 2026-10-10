"""Timezone-designated text in ``date_parser.parsing`` (the default resolution order)."""

import datetime

import pytest

from forklift.utils.date_parser.parsing import (
    coerce_datetime_value,
    parse_date_value,
)


class TestTimeWithZoneButNoDate:
    def test_time_with_zone_but_no_date_is_rejected(self):
        # dateutil would fill the date from its default; the other resolvers find no date either
        with pytest.raises(ValueError, match="bad datetime"):
            coerce_datetime_value("10:30Z")

    def test_time_with_zone_but_no_date_is_not_a_date(self):
        assert parse_date_value("10:30Z") is False

    def test_full_timestamp_with_zone_keeps_its_offset(self):
        parsed = coerce_datetime_value("2024-06-01 10:30+02:00")

        assert parsed == datetime.datetime(
            2024, 6, 1, 10, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        )
        assert parsed.utcoffset() == datetime.timedelta(hours=2)


class TestEpochAutoDetection:
    def test_ten_digit_text_is_read_as_epoch_seconds(self):
        assert coerce_datetime_value("1700000000") == datetime.datetime(
            2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc
        )
