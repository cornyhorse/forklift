"""Core public API for date parsing utilities.

This module provides the main public functions that maintain backward compatibility
with the original date_parser.py interface.
"""

import datetime
from typing import Any, List, Optional, Union

from .parsing import coerce_date_value, coerce_datetime_value, parse_date_value


def parse_date(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    dayfirst: bool = True,
) -> bool:
    """Check if a value can be parsed as a date.

    Returns True exactly when ``coerce_date`` with the same arguments would succeed.

    Args:
        value: Value to check (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try
        dayfirst: Resolve ambiguous dates such as 03-04-2024 as day-month-year (default) instead
            of month-day-year

    Returns:
        True if value can be parsed as a date, False otherwise
    """
    return parse_date_value(value, fmt, formats, dayfirst)


def coerce_date(
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
    return coerce_date_value(value, fmt, formats, dayfirst)


def coerce_datetime(
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
    return coerce_datetime_value(
        value, fmt, formats, from_epoch, to_epoch, fuzzy, allow_fuzzy, dayfirst
    )
