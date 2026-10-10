"""Epoch timestamp parsing utilities.

All conversions use integer arithmetic. Going through ``float`` (``timestamp / 1e9``,
``dt.timestamp() * 1e9``) silently loses digits for microsecond and nanosecond epochs.
"""

import datetime

_UTC = datetime.timezone.utc
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=_UTC)

# Number of digits -> ticks per second (10: seconds, 13: ms, 16: microseconds, 19: nanoseconds)
_TICKS_PER_SECOND = {10: 1, 13: 1_000, 16: 1_000_000, 19: 1_000_000_000}

_MICROSECONDS_PER_UNIT = {
    "seconds": 1_000_000,
    "milliseconds": 1_000,
    "microseconds": 1,
}


def is_epoch_timestamp(value: str) -> bool:
    """Check if a string represents an epoch timestamp.

    Auto-detection is deliberately conservative: only plain ASCII digit strings of exactly 10,
    13, 16 or 19 digits (no leading zero) qualify. Callers must not use this when an explicit
    format was requested; the format always wins.

    Args:
        value: String to check

    Returns:
        True if value appears to be an epoch timestamp
    """
    if not value:
        return False

    # Only ASCII digits: str.isdigit() also accepts e.g. Arabic-Indic digits and superscripts
    if not value.isascii() or not value.isdigit():
        return False

    # Only accept specific valid lengths
    # 10 digits: seconds since epoch (2001-2286 range)
    # 13 digits: milliseconds since epoch
    # 16 digits: microseconds since epoch
    # 19 digits: nanoseconds since epoch
    length = len(value)
    if length not in _TICKS_PER_SECOND:
        return False

    # A leading zero means fewer significant digits, i.e. not a timestamp of that precision
    return value[0] != "0"


def parse_epoch_timestamp(value: str) -> datetime.datetime:
    """Parse an epoch timestamp string to datetime.

    Args:
        value: Epoch timestamp string

    Returns:
        Parsed datetime object (always UTC timezone)

    Raises:
        ValueError: If timestamp cannot be parsed
    """
    if not is_epoch_timestamp(value):
        raise ValueError("Invalid epoch timestamp")

    ticks = int(value)
    ticks_per_second = _TICKS_PER_SECOND[len(value)]
    seconds, remainder = divmod(ticks, ticks_per_second)
    # datetime resolution is one microsecond; sub-microsecond digits are truncated
    microseconds = remainder * 1_000_000 // ticks_per_second

    # At most 19 digits (9999999999 seconds at any precision): the result is before the year
    # 2287, far inside datetime's range, so this cannot overflow
    return _EPOCH + datetime.timedelta(seconds=seconds, microseconds=microseconds)


def datetime_to_epoch(dt: datetime.datetime, unit: str) -> int:
    """Convert datetime to epoch timestamp.

    Args:
        dt: Datetime object (naive datetimes are treated as UTC)
        unit: Target unit ('seconds', 'milliseconds', 'microseconds', 'nanoseconds')

    Returns:
        Epoch timestamp as integer (truncated toward zero), computed exactly with integers
    """
    if unit not in ("seconds", "milliseconds", "microseconds", "nanoseconds"):
        raise ValueError(f"Invalid epoch unit: {unit}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)

    delta = dt - _EPOCH
    total_microseconds = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    if unit == "nanoseconds":
        return total_microseconds * 1_000

    per_unit = _MICROSECONDS_PER_UNIT[unit]
    quotient = abs(total_microseconds) // per_unit
    return quotient if total_microseconds >= 0 else -quotient
