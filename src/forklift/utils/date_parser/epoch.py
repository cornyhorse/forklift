"""Epoch timestamp parsing utilities."""

import datetime


def is_epoch_timestamp(value: str) -> bool:
    """Check if a string represents an epoch timestamp.

    Args:
        value: String to check

    Returns:
        True if value appears to be an epoch timestamp
    """
    if not value:
        return False

    # Check if all characters are digits (no decimals or other characters)
    if not value.isdigit():
        return False

    # Check for valid epoch timestamp lengths:
    # 10 digits: seconds since epoch (1970-2038 range)
    # 13 digits: milliseconds since epoch
    # 16 digits: microseconds since epoch
    # 19 digits: nanoseconds since epoch
    length = len(value)

    # Only accept specific valid lengths
    if length not in [10, 13, 16, 19]:
        return False

    try:
        timestamp = int(value)
    except ValueError:
        return False

    if length == 10:
        # Validate it's in reasonable range (after 2001, before 2286)
        return 1000000000 <= timestamp <= 9999999999
    elif length == 13:
        # Milliseconds - validate reasonable range
        return 1000000000000 <= timestamp <= 9999999999999
    elif length == 16:
        # Microseconds - validate reasonable range
        return 1000000000000000 <= timestamp <= 9999999999999999
    elif length == 19:
        # Nanoseconds - validate reasonable range
        return 1000000000000000000 <= timestamp <= 9999999999999999999

    return False


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
        raise ValueError(f"Invalid epoch timestamp: {value}")

    try:
        timestamp = int(value)
    except ValueError:
        raise ValueError(f"Invalid epoch timestamp: {value}")

    length = len(value)

    try:
        if length == 10:
            # Seconds
            dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        elif length == 13:
            # Milliseconds
            dt = datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.timezone.utc)
        elif length == 16:
            # Microseconds
            dt = datetime.datetime.fromtimestamp(timestamp / 1000000, tz=datetime.timezone.utc)
        elif length == 19:
            # Nanoseconds
            dt = datetime.datetime.fromtimestamp(timestamp / 1000000000, tz=datetime.timezone.utc)
        else:
            raise ValueError(f"Unsupported epoch timestamp length: {length}")
    except (ValueError, OSError, OverflowError) as e:
        raise ValueError(f"Invalid epoch timestamp: {value}") from e

    return dt


def datetime_to_epoch(dt: datetime.datetime, unit: str) -> int:
    """Convert datetime to epoch timestamp.

    Args:
        dt: Datetime object
        unit: Target unit ('seconds', 'milliseconds', 'microseconds', 'nanoseconds')

    Returns:
        Epoch timestamp as integer
    """
    # Convert to UTC if timezone-aware
    if dt.tzinfo is not None:
        epoch_seconds = dt.timestamp()
    else:
        # Treat naive datetime as UTC
        epoch_seconds = dt.replace(tzinfo=datetime.timezone.utc).timestamp()

    if unit == "seconds":
        return int(epoch_seconds)
    elif unit == "milliseconds":
        return int(epoch_seconds * 1000)
    elif unit == "microseconds":
        return int(epoch_seconds * 1000000)
    elif unit == "nanoseconds":
        return int(epoch_seconds * 1000000000)
    else:
        raise ValueError(f"Invalid epoch unit: {unit}")
