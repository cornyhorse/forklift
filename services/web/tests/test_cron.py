"""Cron expressions: fields, names, macros, the day rule, time zones and DST transitions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from forklift_web import cron

UTC = timezone.utc
BERLIN = ZoneInfo("Europe/Berlin")
NEW_YORK = ZoneInfo("America/New_York")


def runs(expression: str, zone: str, after: datetime, count: int = 5) -> list:
    """The next runs as wall-clock strings with their UTC offset, in ``zone``."""
    return [
        run.astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d %H:%M%z")
        for run in cron.next_runs(expression, zone, after, count)
    ]


def error(expression: str, zone: str = "UTC") -> cron.CronError:
    with pytest.raises(cron.CronError) as caught:
        cron.validate(expression, zone, now=datetime(2026, 1, 1, tzinfo=UTC))
    return caught.value


# --------------------------------------------------------------------------- fields


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("*/15 * * * *", ["00:15", "00:30", "00:45", "01:00"]),
        ("5,10-12 * * * *", ["00:05", "00:10", "00:11", "00:12"]),
        ("10-40/10 9 * * *", ["09:10", "09:20", "09:30", "09:40"]),
        ("0 22-23,0-1 * * *", ["01:00", "22:00", "23:00", "00:00"]),
        ("0 */8 * * *", ["08:00", "16:00", "00:00", "08:00"]),
    ],
)
def test_minutes_and_hours(expression, expected):
    found = cron.next_runs(expression, "UTC", datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 4)
    assert [run.strftime("%H:%M") for run in found] == expected


def test_runs_are_strictly_after_and_in_utc():
    after = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
    first = cron.next_after("0 9 * * *", "UTC", after)
    assert first == datetime(2026, 1, 2, 9, 0, tzinfo=UTC) and first.tzinfo is UTC
    assert cron.next_after("0 9 * * *", "UTC", after - timedelta(seconds=1)) == after
    assert cron.next_after("* * * * *", "UTC", after + timedelta(seconds=30)) == after.replace(
        minute=1
    )


def test_month_and_weekday_names_and_steps_on_ranges():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    assert runs("0 0 1 jan,JUL *", "UTC", start, 3) == [
        "2026-07-01 00:00+0000",
        "2027-01-01 00:00+0000",
        "2027-07-01 00:00+0000",
    ]
    # 2026-01-01 is a Thursday
    assert runs("0 0 * * mon-wed", "UTC", start, 3) == [
        "2026-01-05 00:00+0000",
        "2026-01-06 00:00+0000",
        "2026-01-07 00:00+0000",
    ]
    assert runs("0 0 1 feb-dec/3 *", "UTC", start, 4) == [
        "2026-02-01 00:00+0000",
        "2026-05-01 00:00+0000",
        "2026-08-01 00:00+0000",
        "2026-11-01 00:00+0000",
    ]


@pytest.mark.parametrize("sunday", ["0", "7", "sun", "SUN", "*/7", "5-7"])
def test_sunday_is_0_and_7(sunday):
    found = cron.next_after(f"0 12 * * {sunday}", "UTC", datetime(2026, 1, 5, tzinfo=UTC))
    assert found == datetime(2026, 1, 9 if sunday == "5-7" else 11, 12, 0, tzinfo=UTC)


def test_either_day_field_matches_when_both_are_restricted():
    start = datetime(2026, 6, 1, tzinfo=UTC)  # a Monday
    # Vixie cron: the 1st of the month or any Monday
    assert runs("0 0 1 * mon", "UTC", start, 6) == [
        "2026-06-08 00:00+0000",
        "2026-06-15 00:00+0000",
        "2026-06-22 00:00+0000",
        "2026-06-29 00:00+0000",
        "2026-07-01 00:00+0000",
        "2026-07-06 00:00+0000",
    ]
    # A field that starts with * is not restricted, so both have to match: odd days that are
    # Mondays (as in Vixie cron, where */2 counts as starting with *)
    assert runs("0 0 */2 * mon", "UTC", start, 3) == [
        "2026-06-15 00:00+0000",
        "2026-06-29 00:00+0000",
        "2026-07-13 00:00+0000",
    ]
    assert runs("0 0 13 * *", "UTC", start, 1) == ["2026-06-13 00:00+0000"]


@pytest.mark.parametrize(
    "macro,expression",
    [
        ("@yearly", "0 0 1 1 *"),
        ("@annually", "0 0 1 1 *"),
        ("@monthly", "0 0 1 * *"),
        ("@weekly", "0 0 * * 0"),
        ("@daily", "0 0 * * *"),
        ("@midnight", "0 0 * * *"),
        ("@HOURLY", "0 * * * *"),
    ],
)
def test_macros(macro, expression):
    start = datetime(2026, 3, 4, 5, 6, tzinfo=UTC)
    assert cron.next_runs(macro, "UTC", start, 3) == cron.next_runs(expression, "UTC", start, 3)
    assert cron.parse(macro).expression == macro


def test_whitespace_is_normalised():
    assert cron.validate("  0   2 *\t* * ", "UTC", now=datetime(2026, 1, 1, tzinfo=UTC)) == (
        "0 2 * * *"
    )


# --------------------------------------------------------------------------- leap days


def test_leap_days():
    assert runs("0 0 29 2 *", "UTC", datetime(2026, 1, 1, tzinfo=UTC), 2) == [
        "2028-02-29 00:00+0000",
        "2032-02-29 00:00+0000",
    ]
    # 2100 is not a leap year
    assert runs("0 0 29 2 *", "UTC", datetime(2096, 3, 1, tzinfo=UTC), 1) == [
        "2104-02-29 00:00+0000"
    ]
    # February 29th that is also a Sunday (the day-of-week field starts with *, so both match)
    assert runs("0 0 29 2 */7", "UTC", datetime(2026, 1, 1, tzinfo=UTC), 2) == [
        "2032-02-29 00:00+0000",
        "2060-02-29 00:00+0000",
    ]


# --------------------------------------------------------------------------- DST


def test_a_time_the_clocks_skip_runs_once_at_the_end_of_the_gap_berlin():
    # 2026-03-29: 02:00 CET becomes 03:00 CEST
    before = datetime(2026, 3, 29, 1, 30, tzinfo=BERLIN)
    assert runs("*/15 * * * *", "Europe/Berlin", before, 4) == [
        "2026-03-29 01:45+0100",
        "2026-03-29 03:00+0200",  # 02:00 .. 02:45 do not exist: one run, at the end of the gap
        "2026-03-29 03:15+0200",
        "2026-03-29 03:30+0200",
    ]
    assert runs("30 2 * * *", "Europe/Berlin", datetime(2026, 3, 28, 12, tzinfo=BERLIN), 3) == [
        "2026-03-29 03:00+0200",
        "2026-03-30 02:30+0200",
        "2026-03-31 02:30+0200",
    ]
    gap_end = cron.next_after("30 2 * * *", "Europe/Berlin", before)
    assert gap_end == datetime(2026, 3, 29, 1, 0, tzinfo=UTC)


def test_a_time_the_clocks_skip_runs_once_at_the_end_of_the_gap_new_york():
    # 2026-03-08: 02:00 EST becomes 03:00 EDT
    after = datetime(2026, 3, 7, 12, tzinfo=NEW_YORK)
    assert runs("30 2 * * *", "America/New_York", after, 3) == [
        "2026-03-08 03:00-0400",
        "2026-03-09 02:30-0400",
        "2026-03-10 02:30-0400",
    ]


def test_a_time_the_clocks_pass_twice_runs_once_at_its_first_occurrence_berlin():
    # 2026-10-25: 03:00 CEST becomes 02:00 CET, so 02:00 .. 02:59 happen twice
    after = datetime(2026, 10, 25, 1, 0, tzinfo=BERLIN)
    assert runs("*/30 * * * *", "Europe/Berlin", after, 5) == [
        "2026-10-25 01:30+0200",
        "2026-10-25 02:00+0200",
        "2026-10-25 02:30+0200",
        "2026-10-25 03:00+0100",  # 02:00 and 02:30 CET (the second time round) do not run
        "2026-10-25 03:30+0100",
    ]
    assert runs("30 2 * * *", "Europe/Berlin", datetime(2026, 10, 24, 12, tzinfo=BERLIN), 2) == [
        "2026-10-25 02:30+0200",
        "2026-10-26 02:30+0100",
    ]
    # Asked from inside the repeated hour (its second occurrence): the next new time
    second = datetime(2026, 10, 25, 2, 15, tzinfo=BERLIN, fold=1)
    assert cron.next_after("*/30 * * * *", "Europe/Berlin", second) == datetime(
        2026, 10, 25, 2, 0, tzinfo=UTC
    )


def test_a_time_the_clocks_pass_twice_runs_once_at_its_first_occurrence_new_york():
    # 2026-11-01: 02:00 EDT becomes 01:00 EST
    after = datetime(2026, 10, 31, 12, tzinfo=NEW_YORK)
    assert runs("30 1 * * *", "America/New_York", after, 3) == [
        "2026-11-01 01:30-0400",
        "2026-11-02 01:30-0500",
        "2026-11-03 01:30-0500",
    ]
    assert runs(
        "0 * * * *", "America/New_York", datetime(2026, 11, 1, 0, 30, tzinfo=NEW_YORK)
    ) == [
        "2026-11-01 01:00-0400",
        "2026-11-01 02:00-0500",  # 01:00 EST (the second time round) does not run
        "2026-11-01 03:00-0500",
        "2026-11-01 04:00-0500",
        "2026-11-01 05:00-0500",
    ]


def test_a_gap_of_half_an_hour():
    # Lord Howe Island moves its clocks by 30 minutes: 2026-10-04 02:00 becomes 02:30
    assert runs("15 2 * * *", "Australia/Lord_Howe", datetime(2026, 10, 3, tzinfo=UTC), 2) == [
        "2026-10-04 02:30+1100",
        "2026-10-05 02:15+1100",
    ]


def test_runs_in_other_time_zones():
    after = datetime(2026, 1, 1, tzinfo=UTC)
    assert cron.next_after("0 9 * * *", "Asia/Kolkata", after) == datetime(
        2026, 1, 1, 3, 30, tzinfo=UTC
    )
    assert cron.next_after("0 9 * * *", "UTC", datetime(2026, 1, 1, 10, tzinfo=BERLIN)) == (
        datetime(2026, 1, 2, 9, tzinfo=UTC)
    )


# --------------------------------------------------------------------------- errors


@pytest.mark.parametrize(
    "expression,message",
    [
        ("61 * * * *", "minute field '61': must be 0-59."),
        ("* 24 * * *", "hour field '24': must be 0-23."),
        ("* * 0 * *", "day-of-month field '0': must be 1-31."),
        ("* * * 13 *", "month field '13': must be 1-12 or jan-dec."),
        ("* * * * 8", "day-of-week field '8': must be 0-7 or sun-sat."),
        ("* * * foo *", "month field 'foo': 'foo' is not a number or name."),
        ("mon * * * *", "minute field 'mon': 'mon' is not a number or name."),
        ("1-5-7 * * * *", "minute field '1-5-7': '5-7' is not a number or name."),
        ("1, 2 * * * *", "has 6."),
        ("1,,2 * * * *", "minute field '1,,2': the list has an empty item."),
        ("* 5-2 * * *", "hour field '5-2': the range runs backwards."),
        ("* * * * fri-mon", "day-of-week field 'fri-mon': the range runs backwards."),
        ("*/0 * * * *", "minute field '*/0': the step must be 1 or more."),
        ("*/x * * * *", "minute field '*/x': the step must be 1 or more."),
        (
            "5/15 * * * *",
            "minute field '5/15': a step needs a range or '*' (for example '5-59/15').",
        ),
        ("* * *", "or is a macro such as @daily; '* * *' has 3."),
        ("", "has 0."),
        ("@reboot", "Unknown cron macro '@reboot'; macros: @yearly, @annually"),
        ("0 0 31 2 *", "The cron expression '0 0 31 2 *' never runs: no date matches it."),
        ("0 0 30,31 2 *", "never runs"),
        ("0 0 31 4,6,9,11 *", "never runs"),
    ],
)
def test_errors_name_the_field_and_what_is_wrong(expression, message):
    found = error(expression)
    assert message in found.message and found.field == "cron"
    assert str(found) == found.message


def test_an_impossible_date_is_possible_with_a_weekday():
    # Day 31 of February never comes, but with both day fields restricted Mondays still do
    assert cron.validate("0 0 31 2 mon", "UTC", now=datetime(2026, 1, 1, tzinfo=UTC))


@pytest.mark.parametrize("name", ["Mars/Base", "../../etc/passwd", "", "utc", "Europe/berlin"])
def test_unknown_time_zones(name):
    found = error("0 0 * * *", name)
    assert found.field == "timezone"
    assert found.message == (
        f"Unknown time zone {name!r}: use an IANA name such as 'UTC' or 'Europe/Berlin'."
    )


def test_a_listed_time_zone_that_cannot_be_loaded(monkeypatch):
    monkeypatch.setattr(cron, "time_zones", lambda: frozenset({"Nowhere/Gone"}))
    found = error("0 0 * * *", "Nowhere/Gone")
    assert (found.field, found.message) == (
        "timezone",
        "The time zone 'Nowhere/Gone' cannot be loaded.",
    )


def test_time_zones_include_the_tzdata_names():
    names = cron.time_zones()
    assert {"UTC", "Europe/Berlin", "America/New_York", "Australia/Lord_Howe"} <= names


def test_after_must_be_aware():
    with pytest.raises(ValueError, match="aware"):
        cron.next_after("* * * * *", "UTC", datetime(2026, 1, 1))


def test_the_search_ends_with_the_calendar():
    # Near the end of what datetime can hold there is nothing left to find
    with pytest.raises(cron.CronError, match="never runs"):
        cron.next_after("0 0 1 1 *", "UTC", datetime(9999, 6, 1, tzinfo=UTC))
