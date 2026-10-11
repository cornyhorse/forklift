"""Cron expressions: when a schedule runs.

Five fields, ``minute hour day-of-month month day-of-week``, each ``*``, a number, a range
(``1-5``) or a list of those (``1,15,30-35``), with an optional step after ``*`` or a range
(``*/15``, ``8-18/2``). Months and weekdays may be names (``jan``, ``mon-fri``); weekday 0 and 7
are both Sunday. As in Vixie cron, when both day fields are restricted (neither starts with
``*``) a day matches if either does: ``0 0 1 * mon`` runs on the 1st and on every Monday. The
macros ``@hourly``, ``@daily`` (``@midnight``), ``@weekly``, ``@monthly`` and ``@yearly``
(``@annually``) stand for their usual expressions.

Times are wall-clock times in the schedule's IANA time zone. A time the clocks skip (spring
forward) runs once, at the first instant after the gap; a time they pass twice (fall back) runs
once, at its first occurrence.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
WEEKDAYS = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
# The Gregorian calendar repeats (weekdays included) every 400 years, so a day that matches
# exists within that span or never.
SEARCH_YEARS = 400


class CronError(ValueError):
    """An expression or time zone that cannot be used; ``field`` is ``cron`` or ``timezone``."""

    def __init__(self, message: str, *, field: str = "cron"):
        super().__init__(message)
        self.message = message
        self.field = field


@dataclass(frozen=True)
class _Field:
    name: str
    low: int
    high: int
    names: tuple = ()
    first_name: int = 0  # the value of names[0]

    def describe(self) -> str:
        span = f"{self.low}-{self.high}"
        if self.names:
            span += f" or {self.names[0]}-{self.names[-1]}"
        return span

    def number(self, text: str, item: str) -> int:
        lowered = text.lower()
        if lowered in self.names:
            return self.names.index(lowered) + self.first_name
        if not text.isdigit():
            raise CronError(f"{self.name} field {item!r}: {text!r} is not a number or name.")
        value = int(text)
        if not self.low <= value <= self.high:
            raise CronError(f"{self.name} field {item!r}: must be {self.describe()}.")
        return value

    def values(self, text: str) -> frozenset:
        found: set = set()
        for item in text.split(","):
            if not item:
                raise CronError(f"{self.name} field {text!r}: the list has an empty item.")
            found.update(self._item(item))
        return frozenset(found)

    def _item(self, item: str) -> range:
        base, slash, step_text = item.partition("/")
        step = 1
        if slash:
            if not step_text.isdigit() or int(step_text) == 0:
                raise CronError(f"{self.name} field {item!r}: the step must be 1 or more.")
            step = int(step_text)
        if base == "*":
            return range(self.low, self.high + 1, step)
        start_text, dash, end_text = base.partition("-")
        start = self.number(start_text, item)
        if not dash:
            if slash:
                raise CronError(
                    f"{self.name} field {item!r}: a step needs a range or '*' (for example "
                    f"'{start_text}-{self.high}/{step_text}')."
                )
            return range(start, start + 1)
        end = self.number(end_text, item)
        if end < start:
            raise CronError(f"{self.name} field {item!r}: the range runs backwards.")
        return range(start, end + 1, step)


MINUTE = _Field("minute", 0, 59)
HOUR = _Field("hour", 0, 23)
DAY = _Field("day-of-month", 1, 31)
MONTH = _Field("month", 1, 12, MONTHS, 1)
WEEKDAY = _Field("day-of-week", 0, 7, WEEKDAYS, 0)


@dataclass(frozen=True)
class Cron:
    """A parsed expression; :meth:`next_after` finds its runs."""

    expression: str
    minutes: tuple
    hours: tuple
    days: frozenset
    months: frozenset
    weekdays: frozenset  # 0 = Sunday ... 6 = Saturday
    either_day: bool  # both day fields restricted: a day matches if either does

    def day_matches(self, day: date) -> bool:
        by_day = day.day in self.days
        by_weekday = day.isoweekday() % 7 in self.weekdays
        return (by_day or by_weekday) if self.either_day else (by_day and by_weekday)

    def wall_times(self, start: datetime) -> Iterator[datetime]:
        """Matching wall-clock times from ``start`` (naive, whole minutes) on, in order, for
        at most :data:`SEARCH_YEARS` years."""
        first = day = start.date()
        last = date(min(start.year + SEARCH_YEARS, 9999), 1, 1)
        while day < last:
            if day.month not in self.months:
                day = (day.replace(day=1) + timedelta(days=31)).replace(day=1)
                continue
            if self.day_matches(day):
                hours = self.hours if day != first else [h for h in self.hours if h >= start.hour]
                for hour in hours:
                    for minute in self.minutes:
                        candidate = datetime.combine(day, time(hour, minute))
                        if candidate >= start:
                            yield candidate
            day += timedelta(days=1)

    def next_after(self, zone: tzinfo, after: datetime) -> datetime:
        """The first run strictly after ``after`` (an aware datetime), in UTC."""
        if after.tzinfo is None:
            raise ValueError("after must be an aware datetime.")
        # In UTC throughout: aware datetimes of one time zone compare by wall-clock time.
        after = after.astimezone(timezone.utc)
        local = after.astimezone(zone).replace(tzinfo=None)
        start = local.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for candidate in self.wall_times(start):
            instant = _instant(candidate, zone)
            # A time passed twice runs at its first occurrence only, which lies before `after`
            # when `after` is in the repeated hour.
            if instant > after:
                return instant
        raise CronError(f"The cron expression {self.expression!r} never runs: no date matches it.")


def _exists(wall: datetime, zone: tzinfo) -> bool:
    aware = wall.replace(tzinfo=zone)
    return aware.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == wall


def _instant(wall: datetime, zone: tzinfo) -> datetime:
    """The instant (in UTC) a wall-clock time runs at: its first occurrence, or for a time the
    clocks skip, the end of the gap."""
    if _exists(wall, zone):
        return wall.replace(tzinfo=zone, fold=0).astimezone(timezone.utc)
    # In a gap, fold=0 reads the time with the offset before the transition (an instant after
    # the gap) and fold=1 with the offset after it (an instant before the gap); the transition
    # lies between the two.
    before = wall.replace(tzinfo=zone, fold=1).astimezone(timezone.utc)
    after = wall.replace(tzinfo=zone, fold=0).astimezone(timezone.utc)
    offset = before.astimezone(zone).utcoffset()
    while after - before > timedelta(seconds=1):
        middle = before + (after - before) / 2
        if middle.astimezone(zone).utcoffset() == offset:
            before = middle
        else:
            after = middle
    return after.replace(microsecond=0)


@functools.lru_cache(maxsize=1)
def time_zones() -> frozenset:
    """The IANA time zone names zoneinfo knows (from the tzdata package where needed)."""
    return frozenset(available_timezones())


def zone(name: str) -> ZoneInfo:
    """The time zone named ``name``; raises CronError for anything that is not an IANA name."""
    if name not in time_zones():
        raise CronError(
            f"Unknown time zone {name!r}: use an IANA name such as 'UTC' or 'Europe/Berlin'.",
            field="timezone",
        )
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:  # listed, but its file cannot be read
        raise CronError(f"The time zone {name!r} cannot be loaded.", field="timezone") from None


@functools.lru_cache(maxsize=256)
def parse(expression: str) -> Cron:
    """Parse ``expression`` (raises CronError naming the field and what is wrong)."""
    text = " ".join(expression.split())
    if text.startswith("@"):
        if text.lower() not in MACROS:
            raise CronError(f"Unknown cron macro {text!r}; macros: {', '.join(MACROS)}.")
        fields = MACROS[text.lower()].split()
    else:
        fields = text.split()
    if len(fields) != 5:
        raise CronError(
            f"A cron expression has five fields (minute hour day-of-month month day-of-week) "
            f"or is a macro such as @daily; {text!r} has {len(fields)}."
        )
    minute, hour, day, month, weekday = fields
    return Cron(
        expression=text,
        minutes=tuple(sorted(MINUTE.values(minute))),
        hours=tuple(sorted(HOUR.values(hour))),
        days=DAY.values(day),
        months=MONTH.values(month),
        weekdays=frozenset(value % 7 for value in WEEKDAY.values(weekday)),
        either_day=not day.startswith("*") and not weekday.startswith("*"),
    )


def validate(expression: str, time_zone: str, *, now: datetime) -> str:
    """Check that ``expression`` parses and runs at some time after ``now`` in ``time_zone``;
    returns the expression with its whitespace normalised."""
    cron = parse(expression)
    cron.next_after(zone(time_zone), now)
    return cron.expression


def next_after(expression: str, time_zone: str, after: datetime) -> datetime:
    """The first run of ``expression`` in ``time_zone`` strictly after ``after``, in UTC."""
    return parse(expression).next_after(zone(time_zone), after)


def next_runs(expression: str, time_zone: str, after: datetime, count: int) -> list:
    """The next ``count`` runs strictly after ``after``, in UTC."""
    cron, tz = parse(expression), zone(time_zone)
    runs = []
    for _ in range(count):
        after = cron.next_after(tz, after)
        runs.append(after)
    return runs
