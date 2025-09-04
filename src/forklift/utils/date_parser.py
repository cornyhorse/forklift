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
    'MM': '%m', 'mm': '%m', 'Mm': '%m',  # Months (default)
    'M': '%m', 'm': '%m',
    'DD': '%d', 'dd': '%d', 'Dd': '%d',
    'D': '%d', 'd': '%d',
    'HH': '%H', 'hh': '%H', 'Hh': '%H',  # Hours (24-hour)
    'H': '%H', 'h': '%H',
    'SS': '%S', 'ss': '%S', 'Ss': '%S',  # Seconds
    'S': '%S', 's': '%S',
    'MMM': '%b', 'mmm': '%b', 'Mmm': '%b',  # Month abbreviations
    'MMMM': '%B', 'mmmm': '%B', 'Mmmm': '%B',  # Full month names
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

    # Start with the input format
    result = fmt

    # Replace tokens in order of specificity (longest first to avoid conflicts)
    # Year tokens (longest first)
    result = result.replace('YYYY', '%Y')
    result = result.replace('yyyy', '%Y')
    result = result.replace('Yyyy', '%Y')
    result = result.replace('YY', '%y')
    result = result.replace('yy', '%y')
    result = result.replace('Yy', '%y')

    # Month name tokens (longest first to avoid conflicts with MM)
    result = result.replace('MMMM', '%B')
    result = result.replace('mmmm', '%B')
    result = result.replace('Mmmm', '%B')
    result = result.replace('MMM', '%b')
    result = result.replace('mmm', '%b')
    result = result.replace('Mmm', '%b')

    # Microsecond tokens (before other tokens that might conflict)
    result = result.replace('ffffff', '%f')
    result = result.replace('fff', '%f')

    # Handle MM/mm tokens with context awareness
    # Split the format into date and time parts to handle MM differently
    # Common time separators that indicate the time part
    time_separators = [' ', 'T']
    time_part_start = -1

    for sep in time_separators:
        if sep in result:
            # Find where time part likely starts (after date part)
            parts = result.split(sep)
            if len(parts) >= 2:
                # Check if the part after separator contains time-like tokens
                time_part = sep.join(parts[1:])
                if any(token in time_part for token in ['HH', 'hh', 'H:', 'h:', 'SS', 'ss', 'S:', 's:']):
                    time_part_start = result.find(sep + time_part)
                    break

    if time_part_start > 0:
        # Split into date and time parts
        date_part = result[:time_part_start]
        time_part = result[time_part_start:]

        # Process date part: MM should be months
        date_part = date_part.replace('MM', '%m')
        date_part = date_part.replace('mm', '%m')
        date_part = date_part.replace('Mm', '%m')

        # Process time part: MM should be minutes
        time_part = time_part.replace('MM', '%M')
        time_part = time_part.replace('mm', '%M')
        time_part = time_part.replace('Mm', '%M')

        result = date_part + time_part
    else:
        # No clear time part, treat as date-only format
        result = result.replace('MM', '%m')
        result = result.replace('mm', '%m')
        result = result.replace('Mm', '%m')

    # Handle single M/m tokens with regex to avoid conflicts with existing % codes
    # Only replace standalone M/m that are not preceded by % and not followed by letters
    result = re.sub(r'(?<!%)M(?![a-zA-Z%])', '%m', result)
    result = re.sub(r'(?<!%)m(?![a-zA-Z%])', '%m', result)

    # Day tokens
    result = result.replace('DD', '%d')
    result = result.replace('dd', '%d')
    result = result.replace('Dd', '%d')
    # Single D/d
    result = re.sub(r'(?<!%)D(?![a-zA-Z%])', '%d', result)
    result = re.sub(r'(?<!%)d(?![a-zA-Z%])', '%d', result)

    # Hour tokens
    result = result.replace('HH', '%H')
    result = result.replace('hh', '%H')
    result = result.replace('Hh', '%H')
    # Single H/h
    result = re.sub(r'(?<!%)H(?![a-zA-Z%])', '%H', result)
    result = re.sub(r'(?<!%)h(?![a-zA-Z%])', '%H', result)

    # Second tokens
    result = result.replace('SS', '%S')
    result = result.replace('ss', '%S')
    result = result.replace('Ss', '%S')
    # Single S/s
    result = re.sub(r'(?<!%)S(?![a-zA-Z%])', '%S', result)
    result = re.sub(r'(?<!%)s(?![a-zA-Z%])', '%S', result)

    return result


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
        raise ValueError(f"Invalid epoch unit: {unit}")


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

    # Try specific format if provided (strict matching)
    if fmt:
        normalized_fmt = _normalize_format(fmt)
        try:
            datetime.datetime.strptime(value, normalized_fmt)
            # For strict format enforcement, check exact match
            return _matches_format_exact(value, normalized_fmt)
        except (ValueError, TypeError):
            return False

    # Try list of formats if provided (strict matching)
    if formats:
        normalized_formats = [_normalize_format(f) for f in formats]
        for normalized_fmt in normalized_formats:
            try:
                datetime.datetime.strptime(value, normalized_fmt)
                return True
            except (ValueError, TypeError):
                continue
        # If formats list was provided but none matched, still try fallback parsing
        # (This allows for more flexible parsing when a formats list is provided)

    # Try common date formats
    if _try_strptime(value, COMMON_DATE_FORMATS):
        return True

    # Fallback to dateutil parser, but be more restrictive
    # Reject obviously invalid inputs that dateutil might accept
    if value.isdigit() and len(value) < 4:
        # Reject pure numeric values that are too short to be reasonable years
        return False

    try:
        parsed = dateutil_parser.parse(value)
        # Additional validation: reject years that are unreasonably old or future
        if parsed.year < 1000 or parsed.year > 9999:
            return False
        return True
    except (ValueError, TypeError, OverflowError):
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
        normalized_fmt = _normalize_format(fmt)
        candidates.append(normalized_fmt)

        # For schema token formats with single character tokens (M, D, H, S),
        # also try variations that account for both zero-padded and non-zero-padded values
        if '%' not in fmt:  # Original was schema tokens, not strptime
            # Check if format contains single character tokens that need flexible parsing
            has_single_chars = any(
                token in fmt and token*2 not in fmt
                for token in ['M', 'D', 'H', 'S', 'm', 'd', 'h', 's']
            )

            if has_single_chars:
                # Create additional format variations for flexible parsing
                # Replace single digit patterns with flexible alternatives
                flexible_fmt = normalized_fmt
                # For single digit months/days/hours/seconds, try both padded and unpadded
                flexible_fmt = re.sub(r'(?<!%)(%[mdhs])(?![a-zA-Z])', r'(?:\1|%\1)', flexible_fmt)
                # This doesn't work with strptime, so we'll handle it differently

                # Instead, we'll just be more lenient with exact matching for single char formats
                pass

    if formats:
        candidates.extend(_normalize_format(f) for f in formats)

    # Try candidate formats first with strict matching
    if candidates:
        for candidate_fmt in candidates:
            try:
                parsed_dt = datetime.datetime.strptime(value, candidate_fmt)
                # For strict format enforcement when fmt is specified, check exact match
                if fmt and '%' in fmt:
                    # Original format was strptime - always check exact match
                    if not _matches_format_exact(value, candidate_fmt):
                        continue
                elif fmt:
                    # Original format was schema tokens - need precise matching logic
                    # For formats like "YYYY-MM-DD", the MM requires zero-padding
                    # For formats like "YYYY-M-DD", the M allows flexible padding
                    
                    has_single_tokens = any(
                        token in fmt and token*2 not in fmt
                        for token in ['M', 'D', 'H', 'S', 'm', 'd', 'h', 's']
                    )
                    
                    if has_single_tokens:
                        # Format has single character tokens - allow flexible parsing
                        # Only require exact match if parsing failed completely
                        pass
                    else:
                        # Format uses only double character tokens - require exact match
                        if not _matches_format_exact(value, candidate_fmt):
                            continue
                return parsed_dt.date().isoformat()
            except (ValueError, TypeError):
                continue

    # If specific formats were provided but none matched, raise error
    if fmt or formats:
        raise ValueError(f"bad date: {value}")

    # Try common date formats
    parsed_dt = _try_strptime(value, COMMON_DATE_FORMATS)
    if parsed_dt:
        return parsed_dt.date().isoformat()

    # Fallback to dateutil parser
    try:
        dt = dateutil_parser.parse(value)
        return dt.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        pass

    raise ValueError(f"bad date: {value}")


def coerce_datetime(
    value: Any,
    fmt: Optional[str] = None,
    formats: Optional[List[str]] = None,
    from_epoch: bool = False,
    to_epoch: Optional[str] = None,
    fuzzy: bool = False,
    allow_fuzzy: Optional[bool] = None
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
            # Check if the string appears to be timezone-aware
            # If so, use dateutil parser to preserve timezone info
            is_timezone_aware = (
                value.endswith('Z') or  # UTC indicator
                '+' in value[-6:] or    # Timezone offset like +05:00
                '-' in value[-6:] or    # Timezone offset like -05:00
                value.endswith(('UTC', 'GMT'))  # Named timezones
            )

            if is_timezone_aware and not fmt and not formats:
                # For timezone-aware strings without explicit format requirements,
                # use dateutil parser to preserve timezone information
                try:
                    parsed_dt = dateutil_parser.parse(value, fuzzy=fuzzy)
                except (ValueError, TypeError, OverflowError):
                    pass

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
                            # But be more lenient if the original format was using schema tokens (no %)
                            if fmt and '%' in fmt and not _matches_format_exact(value, candidate_fmt):
                                parsed_dt = None
                                continue
                            break
                        except (ValueError, TypeError):
                            continue

                    # If specific formats were provided but none matched, raise error
                    if not parsed_dt and (fmt or formats):
                        if fmt:
                            raise ValueError(f"Value '{value}' does not match required format '{fmt}'")
                        else:
                            raise ValueError(f"Value '{value}' does not match any of the specified formats")

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
                    except (ValueError, TypeError, OverflowError):
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
    'COMMON_DATETIME_FORMATS',
    'SCHEMA_TOKEN_MAP'
]
