"""Core parsing utilities for date and datetime parsing.

``coerce_date_value``, ``coerce_datetime_value`` and ``parse_date_value`` share one resolution
order so they cannot disagree about the same text:

1. ``from_epoch=True``: the value must be an epoch timestamp.
2. An explicit ``fmt`` / ``formats`` list: only those formats are tried. Epoch auto-detection and
   the common-format fallbacks are *not* used, so ``fmt='%Y%m%d%H'`` really parses ``2024010112``
   and a 10-digit phone-like ID is never reinterpreted as an epoch when a format was requested.
3. Otherwise: conservative epoch auto-detection (10/13/16/19 plain digits), timezone-aware text
   through dateutil, the common datetime formats, the common date formats, and finally dateutil.
   The dateutil fallback only accepts text that carries a full year, month and day; it never fills
   missing parts from today's date.

Ambiguous numeric dates such as ``03-04-2024`` follow ``dayfirst`` (default True: 3 April).

Error messages never contain the offending value (it may be personal data).
"""

import datetime
import re
from typing import Any, List, Optional, Union

from dateutil import parser as dateutil_parser

from .constants import COMMON_DATE_FORMATS, COMMON_DATETIME_FORMATS
from .epoch import datetime_to_epoch, is_epoch_timestamp, parse_epoch_timestamp
from .format_utils import (
    format_accepts_unpadded,
    matches_format_exact,
    normalize_format,
    ordered_formats,
    try_strptime,
)

# Text that ends in a time followed by a timezone designator (Z, +05:00, -0800, UTC, GMT). A bare
# trailing "-2024" in "03-04-2024" is a year, not an offset, hence the required time component.
_TZ_AWARE = re.compile(
    r"\d:\d{2}(?::\d{2}(?:[.,]\d+)?)?\s*(?:Z|[+-]\d{2}(?::?\d{2})?|UTC|GMT)$", re.IGNORECASE
)

# Text that starts with a four-digit year (ISO-style YYYY-MM-DD ...)
_YEAR_FIRST = re.compile(r"\s*\d{4}(?!\d)")

# Two different defaults reveal which date parts dateutil had to invent (see _dateutil_complete)
_DEFAULT_A = datetime.datetime(1904, 1, 1)
_DEFAULT_B = datetime.datetime(1905, 2, 2)


def _dateutil_complete(
    value: str, fuzzy: bool = False, dayfirst: bool = True
) -> Optional[datetime.datetime]:
    """Parse with dateutil, but only if year, month and day are all present in the text.

    dateutil fills whatever is missing from a default date (today by default), which turns
    ``'12'``, ``'Mon'``, ``'Mar'``, ``'2024'`` or ``'10:30'`` into plausible-looking dates.
    The text is parsed with two different defaults; if the date part differs between the two
    results, a component came from the default and the value is rejected.
    """
    if dayfirst and _YEAR_FIRST.match(value):
        # dateutil reads "2024-06-01" as year-day-month when dayfirst=True; a leading four-digit
        # year is unambiguous, so the day-first preference must not apply to it.
        dayfirst = False
    try:
        first = dateutil_parser.parse(value, default=_DEFAULT_A, fuzzy=fuzzy, dayfirst=dayfirst)
        second = dateutil_parser.parse(value, default=_DEFAULT_B, fuzzy=fuzzy, dayfirst=dayfirst)
    except (ValueError, TypeError, OverflowError):
        return None
    if (first.year, first.month, first.day) != (second.year, second.month, second.day):
        return None
    return first


def _parse_explicit(
    value: str, fmt: Optional[str], formats: Optional[List[str]]
) -> Optional[datetime.datetime]:
    """Parse ``value`` with the explicitly requested format(s); None if none matches.

    ``fmt`` is enforced exactly (zero-padded fields) unless it is a schema-token format with
    single-letter tokens (``YYYY-M-D``). Entries of ``formats`` use plain strptime matching.
    """
    candidates = []
    if fmt:
        strict = "%" in fmt or not format_accepts_unpadded(fmt)
        candidates.append((normalize_format(fmt), strict))
    if formats:
        candidates.extend((normalize_format(f), False) for f in formats)

    for candidate, strict in candidates:
        try:
            parsed = datetime.datetime.strptime(value, candidate)
        except (ValueError, TypeError, re.error):
            continue  # re.error: a malformed format is "no match", never an escaping exception
        if strict and not matches_format_exact(value, candidate):
            continue
        return parsed
    return None


def _resolve_default(value: str, fuzzy: bool, dayfirst: bool) -> Optional[datetime.datetime]:
    """Resolve text without an explicit format (shared by date and datetime coercion)."""
    if _TZ_AWARE.search(value):
        # Keep the offset: strptime with a literal "Z" would return a naive datetime
        parsed = _dateutil_complete(value, fuzzy=fuzzy, dayfirst=dayfirst)
        if parsed is not None:
            return parsed

    parsed = try_strptime(value, ordered_formats(COMMON_DATETIME_FORMATS, dayfirst))
    if parsed is None:
        parsed = try_strptime(value, ordered_formats(COMMON_DATE_FORMATS, dayfirst))
    if parsed is None:
        parsed = _dateutil_complete(value, fuzzy=fuzzy, dayfirst=dayfirst)
    return parsed


def _parse(
    value: str,
    fmt: Optional[str],
    formats: Optional[List[str]],
    from_epoch: bool,
    fuzzy: bool,
    dayfirst: bool,
    kind: str,
) -> datetime.datetime:
    """Parse a stripped, non-empty string to a datetime (ValueError if impossible)."""
    if from_epoch:
        if not is_epoch_timestamp(value):
            raise ValueError("Invalid epoch timestamp")
        return parse_epoch_timestamp(value)

    if fmt or formats:
        parsed = _parse_explicit(value, fmt, formats)
        if parsed is None:
            if kind == "datetime" and fmt:
                raise ValueError(f"Value does not match required format '{fmt}'")
            if kind == "datetime":
                raise ValueError("Value does not match any of the specified formats")
            raise ValueError(f"bad {kind}")
        return parsed

    if is_epoch_timestamp(value):
        try:
            return parse_epoch_timestamp(value)
        except ValueError:
            pass  # fall through to the other parsing methods

    parsed = _resolve_default(value, fuzzy, dayfirst)
    if parsed is None:
        raise ValueError(f"bad {kind}")
    return parsed


def parse_date_value(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    dayfirst: bool = True,
) -> bool:
    """Check if a value can be parsed as a date.

    Answers exactly the question "would ``coerce_date_value`` succeed?" (same rules, same
    resolution order), so the two can never disagree, including when ``formats`` is given.

    Args:
        value: Value to check (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try
        dayfirst: Resolve ambiguous dates such as 03-04-2024 as day-month-year

    Returns:
        True if value can be parsed as a date, False otherwise
    """
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        coerce_date_value(value, fmt, formats, dayfirst)
    except ValueError:
        return False
    return True


def coerce_date_value(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    dayfirst: bool = True,
) -> str:
    """Coerce a value to ISO date format (YYYY-MM-DD).

    Args:
        value: Value to coerce (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try
        dayfirst: Resolve ambiguous dates such as 03-04-2024 as day-month-year (default) instead
            of month-day-year

    Returns:
        ISO formatted date string (YYYY-MM-DD)

    Raises:
        ValueError: If value cannot be parsed as a date
    """
    if not isinstance(value, str) or not value or not value.strip():
        raise ValueError("empty date")

    parsed = _parse(value.strip(), fmt, formats, False, False, dayfirst, "date")
    return parsed.date().isoformat()


def coerce_datetime_value(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    from_epoch: bool = False,
    to_epoch: Optional[str] = None,
    fuzzy: bool = False,
    allow_fuzzy: Optional[bool] = None,
    dayfirst: bool = True,
) -> Union[datetime.datetime, int]:
    """Coerce a value to datetime object or epoch timestamp.

    Args:
        value: Value to coerce (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try
        from_epoch: If True, treat value as epoch timestamp
        to_epoch: If specified, return epoch in this unit
                 ('seconds', 'milliseconds', 'microseconds', 'nanoseconds')
        fuzzy: If True, allow fuzzy parsing with dateutil
        allow_fuzzy: Legacy parameter, same as fuzzy
        dayfirst: Resolve ambiguous dates such as 03-04-2024 as day-month-year (default) instead
            of month-day-year

    Returns:
        Datetime object or epoch timestamp (int)

    Raises:
        ValueError: If value cannot be parsed as a datetime
    """
    if not isinstance(value, str) or not value or not value.strip():
        raise ValueError("empty datetime")

    # Handle allow_fuzzy parameter (legacy support)
    if allow_fuzzy is not None:
        fuzzy = allow_fuzzy

    parsed_dt = _parse(value.strip(), fmt, formats, from_epoch, fuzzy, dayfirst, "datetime")

    # Convert to epoch if requested
    if to_epoch:
        return datetime_to_epoch(parsed_dt, to_epoch)

    return parsed_dt
