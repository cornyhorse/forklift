"""Name validation of ``transformations._timezones.resolve_timezone``."""

import datetime

import pytest

from forklift.utils.transformations._timezones import resolve_timezone, validate_timezone


class TestResolveTimezoneNames:
    @pytest.mark.parametrize("name", ["", "   ", None, 5])
    def test_missing_or_blank_name_is_rejected(self, name):
        with pytest.raises(ValueError, match="timezone must be a non-empty IANA time zone name"):
            resolve_timezone(name)

    def test_known_name_resolves_to_a_tzinfo(self):
        zone = resolve_timezone("Europe/Berlin")

        assert isinstance(zone, datetime.tzinfo)
        assert datetime.datetime(2024, 1, 1, tzinfo=zone).utcoffset() == datetime.timedelta(
            hours=1
        )


class TestValidateTimezone:
    @pytest.mark.parametrize("name", [None, ""])
    def test_unset_name_is_accepted(self, name):
        assert validate_timezone(name) is None

    def test_blank_name_is_rejected(self):
        with pytest.raises(ValueError, match="non-empty"):
            validate_timezone(" ")
