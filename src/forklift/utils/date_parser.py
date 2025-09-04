"""Date parsing utilities for forklift data processing.

This module provides comprehensive date and datetime parsing functionality with support for:
- Multiple date/datetime formats
- Epoch timestamp parsing
- Schema token format normalization
- Fuzzy parsing fallback using dateutil
"""

import datetime
import re
from typing import List, Optional, Union, Any
from dateutil import parser as dateutil_parser


# Common date formats for fallback parsing
COMMON_DATE_FORMATS = [
    "%Y-%m-%d",        # ISO format
    "%d/%m/%Y",        # European format
    "%m/%d/%Y",        # US format
    "%Y/%m/%d",        # Alternative ISO
    "%d-%m-%Y",        # European with dashes
    "%m-%d-%Y",        # US with dashes
    "%Y.%m.%d",        # Dotted format
    "%d.%m.%Y",        # European dotted
    "%Y%m%d",          # Compact format
    "%d-%b-%Y",        # Day-Month-Year with abbreviated month
    "%b %d, %Y",       # Month Day, Year
    "%d %b %Y",        # Day Month Year
]

# Common datetime formats for fallback parsing
COMMON_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",           # ISO datetime
    "%Y-%m-%dT%H:%M:%S",           # ISO with T separator
    "%Y-%m-%d %H:%M:%S.%f",        # ISO with microseconds
    "%Y-%m-%dT%H:%M:%S.%f",        # ISO T with microseconds
    "%Y-%m-%dT%H:%M:%SZ",          # ISO with Z suffix
    "%Y-%m-%dT%H:%M:%S.%fZ",       # ISO with microseconds and Z
    "%Y-%m-%dT%H:%M:%S%z",         # ISO with timezone offset
    "%Y-%m-%dT%H:%M:%S.%f%z",      # ISO with microseconds and timezone
    "%d/%m/%Y %H:%M:%S",           # European datetime
    "%m/%d/%Y %H:%M:%S",           # US datetime
    "%Y/%m/%d %H:%M:%S",           # Alternative ISO datetime
]

# Schema token to strptime format mapping
SCHEMA_TOKEN_MAP = {
    'YYYY': '%Y', 'yyyy': '%Y', 'Yyyy': '%Y',
    'YY': '%y', 'yy': '%y', 'Yy': '%y',
    'MM': '%m', 'mm': '%m', 'Mm': '%m',
    'M': '%m', 'm': '%m',
    'DD': '%d', 'dd': '%d', 'Dd': '%d',
    'D': '%d', 'd': '%d',
    'HH': '%H', 'hh': '%H', 'Hh': '%H',
    'H': '%H', 'h': '%H',
    'mm': '%M',  # Note: minutes, not month when after hour
    'SS': '%S', 'ss': '%S', 'Ss': '%S',
    'S': '%S', 's': '%S',
    'MMM': '%b', 'mmm': '%b', 'Mmm': '%b',
    'MMMM': '%B', 'mmmm': '%B', 'Mmmm': '%B',
    'fff': '%f', 'ffffff': '%f',  # Microseconds
}


def _is_epoch_timestamp(value: str) -> bool:
    """Check if a string represents an epoch timestamp.

    Args:
        value: String to check

    Returns:
        True if value appears to be an epoch timestamp
    """
    if not value:
        return False

    # Check if all characters are digits
    if not value.isdigit():
        return False

    # Check for valid epoch timestamp lengths:
    # 10 digits: seconds since epoch (1970-2038 range)
    # 13 digits: milliseconds since epoch
    # 16 digits: microseconds since epoch
    # 19 digits: nanoseconds since epoch
    length = len(value)

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


def _parse_epoch_timestamp(value: str) -> datetime.datetime:
    """Parse an epoch timestamp string to datetime.

    Args:
        value: Epoch timestamp string

    Returns:
        Parsed datetime object (always UTC timezone)

    Raises:
        ValueError: If timestamp cannot be parsed
    """
    if not _is_epoch_timestamp(value):
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


def _normalize_format(fmt: str) -> str:
    """Normalize schema tokens to strptime format.

    Args:
        fmt: Format string (either strptime or schema tokens)

    Returns:
        Normalized strptime format string
    """
    # If already contains %, assume it's strptime format
    if '%' in fmt:
        return fmt

    # Replace schema tokens with strptime equivalents
    normalized = fmt

    # Sort tokens by length (longest first) to avoid partial replacements
    sorted_tokens = sorted(SCHEMA_TOKEN_MAP.items(), key=lambda x: len(x[0]), reverse=True)

    for token, strptime_equiv in sorted_tokens:
        normalized = normalized.replace(token, strptime_equiv)

    return normalized


def _matches_format_exact(value: str, fmt: str) -> bool:
    """Check if a value matches a format exactly.

    Args:
        value: String to check
        fmt: Format string (strptime format)

    Returns:
        True if value matches format exactly
    """
    try:
        # Parse the value with the format
        parsed = datetime.datetime.strptime(value, fmt)

        # Format it back and compare
        try:
            reformatted = parsed.strftime(fmt)
            return reformatted == value
        except (ValueError, TypeError):
            # strftime can fail for some formats/values
            return False

    except (ValueError, TypeError):
        return False


def _try_strptime(value: str, formats: List[str]) -> Optional[datetime.datetime]:
    """Try to parse a string using multiple strptime formats.

    Args:
        value: String to parse
        formats: List of strptime format strings

    Returns:
        Parsed datetime or None if no format matches
    """
    for fmt in formats:
        try:
            return datetime.datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _datetime_to_epoch(dt: datetime.datetime, unit: str) -> int:
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
        raise ValueError(f"Unsupported epoch unit: {unit}")


def parse_date(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None
) -> bool:
    """Check if a value can be parsed as a date.

    Args:
        value: Value to check (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try

    Returns:
        True if value can be parsed as a date, False otherwise
    """
    if not isinstance(value, str) or not value:
        return False

    # Clean whitespace
    value = value.strip()
    if not value:
        return False

    # Check if it's an epoch timestamp
    if _is_epoch_timestamp(value):
        try:
            _parse_epoch_timestamp(value)
            return True
        except ValueError:
            return False

    # Try specific format if provided
    if fmt:
        normalized_fmt = _normalize_format(fmt)
        try:
            datetime.datetime.strptime(value, normalized_fmt)
            return True
        except (ValueError, TypeError):
            pass

    # Try list of formats if provided
    if formats:
        normalized_formats = [_normalize_format(f) for f in formats]
        if _try_strptime(value, normalized_formats):
            return True

    # Try common date formats
    if _try_strptime(value, COMMON_DATE_FORMATS):
        return True

    # Fallback to dateutil parser
    try:
        dateutil_parser.parse(value)
        return True
    except (ValueError, TypeError):
        return False


def coerce_date(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None
) -> str:
    """Coerce a value to ISO date format (YYYY-MM-DD).

    Args:
        value: Value to coerce (typically a string)
        fmt: Specific format to use (strptime or schema tokens)
        formats: List of formats to try

    Returns:
        ISO formatted date string (YYYY-MM-DD)

    Raises:
        ValueError: If value cannot be parsed as a date
    """
    if not isinstance(value, str) or not value or not value.strip():
        raise ValueError("empty date")

    # Clean whitespace
    value = value.strip()

    # Check if it's an epoch timestamp
    if _is_epoch_timestamp(value):
        try:
            dt = _parse_epoch_timestamp(value)
            return dt.date().isoformat()
        except ValueError:
            pass  # Fall through to other parsing methods

    # Build list of format candidates
    candidates = []

    if fmt:
        candidates.append(_normalize_format(fmt))

    if formats:
        candidates.extend(_normalize_format(f) for f in formats)

    # Try candidate formats first
    if candidates:
        parsed_dt = _try_strptime(value, candidates)
        if parsed_dt:
            return parsed_dt.date().isoformat()

    # Try common date formats
    parsed_dt = _try_strptime(value, COMMON_DATE_FORMATS)
    if parsed_dt:
        return parsed_dt.date().isoformat()

    # Fallback to dateutil parser
    try:
        dt = dateutil_parser.parse(value)
        return dt.date().isoformat()
    except (ValueError, TypeError):
        pass

    raise ValueError(f"bad date: {value}")


def coerce_datetime(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    from_epoch: bool = False,
    to_epoch: Optional[str] = None,
    fuzzy: bool = False
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

    Returns:
        Datetime object or epoch timestamp (int)

    Raises:
        ValueError: If value cannot be parsed as a datetime
    """
    if not isinstance(value, str) or not value or not value.strip():
        raise ValueError("empty datetime")

    # Clean whitespace
    value = value.strip()

    parsed_dt = None

    # Handle explicit epoch conversion
    if from_epoch:
        if not _is_epoch_timestamp(value):
            raise ValueError(f"Invalid epoch timestamp: {value}")
        parsed_dt = _parse_epoch_timestamp(value)
    else:
        # Check if it's an epoch timestamp (auto-detect)
        if _is_epoch_timestamp(value):
            try:
                parsed_dt = _parse_epoch_timestamp(value)
            except ValueError:
                pass  # Fall through to other parsing methods

        if not parsed_dt:
            # Build list of format candidates
            candidates = []

            if fmt:
                candidates.append(_normalize_format(fmt))

            if formats:
                candidates.extend(_normalize_format(f) for f in formats)

            # Try candidate formats first with exact matching
            if candidates:
                for candidate_fmt in candidates:
                    try:
                        parsed_dt = datetime.datetime.strptime(value, candidate_fmt)
                        # For strict format enforcement, check exact match
                        if fmt and not _matches_format_exact(value, candidate_fmt):
                            parsed_dt = None
                            continue
                        break
                    except (ValueError, TypeError):
                        continue

            # Try common datetime formats
            if not parsed_dt:
                parsed_dt = _try_strptime(value, COMMON_DATETIME_FORMATS)

            # Try common date formats (will give time 00:00:00)
            if not parsed_dt:
                parsed_dt = _try_strptime(value, COMMON_DATE_FORMATS)

            # Fallback to dateutil parser
            if not parsed_dt:
                try:
                    parsed_dt = dateutil_parser.parse(value, fuzzy=fuzzy)
                except (ValueError, TypeError):
                    pass

    if not parsed_dt:
        raise ValueError(f"bad datetime: {value}")

    # Convert to epoch if requested
    if to_epoch:
        return _datetime_to_epoch(parsed_dt, to_epoch)

    return parsed_dt


__all__ = [
    'parse_date',
    'coerce_date',
    'coerce_datetime',
    'COMMON_DATE_FORMATS',
    'COMMON_DATETIME_FORMATS'
]
