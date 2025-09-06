"""Core parsing utilities for date and datetime parsing."""

import datetime
import re
from typing import Any, List, Optional, Union
from dateutil import parser as dateutil_parser

from .constants import COMMON_DATE_FORMATS, COMMON_DATETIME_FORMATS
from .epoch import is_epoch_timestamp, parse_epoch_timestamp, datetime_to_epoch
from .format_utils import normalize_format, matches_format_exact, try_strptime


def parse_date_value(
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
    if is_epoch_timestamp(value):
        try:
            parse_epoch_timestamp(value)
            return True
        except ValueError:
            return False

    # Try specific format if provided (strict matching)
    if fmt:
        normalized_fmt = normalize_format(fmt)
        try:
            datetime.datetime.strptime(value, normalized_fmt)
            # For strict format enforcement, check exact match
            return matches_format_exact(value, normalized_fmt)
        except (ValueError, TypeError):
            return False

    # Try list of formats if provided (strict matching)
    if formats:
        normalized_formats = [normalize_format(f) for f in formats]
        for normalized_fmt in normalized_formats:
            try:
                datetime.datetime.strptime(value, normalized_fmt)
                return True
            except (ValueError, TypeError):
                continue
        # If formats list was provided but none matched, still try fallback parsing
        # (This allows for more flexible parsing when a formats list is provided)

    # Try common date formats
    if try_strptime(value, COMMON_DATE_FORMATS):
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


def coerce_date_value(
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
    if is_epoch_timestamp(value):
        try:
            dt = parse_epoch_timestamp(value)
            return dt.date().isoformat()
        except ValueError:
            pass  # Fall through to other parsing methods

    # Build list of format candidates
    candidates = []

    if fmt:
        normalized_fmt = normalize_format(fmt)
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
        candidates.extend(normalize_format(f) for f in formats)

    # Try candidate formats first with strict matching
    if candidates:
        for candidate_fmt in candidates:
            try:
                parsed_dt = datetime.datetime.strptime(value, candidate_fmt)
                # For strict format enforcement when fmt is specified, check exact match
                if fmt and '%' in fmt:
                    # Original format was strptime - always check exact match
                    if not matches_format_exact(value, candidate_fmt):
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
                        if not matches_format_exact(value, candidate_fmt):
                            continue
                return parsed_dt.date().isoformat()
            except (ValueError, TypeError):
                continue

    # If specific formats were provided but none matched, raise error
    if fmt or formats:
        raise ValueError(f"bad date: {value}")

    # Try common date formats
    parsed_dt = try_strptime(value, COMMON_DATE_FORMATS)
    if parsed_dt:
        return parsed_dt.date().isoformat()

    # Fallback to dateutil parser
    try:
        dt = dateutil_parser.parse(value)
        return dt.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        pass

    raise ValueError(f"bad date: {value}")


def coerce_datetime_value(
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
        if not is_epoch_timestamp(value):
            raise ValueError(f"Invalid epoch timestamp: {value}")
        parsed_dt = parse_epoch_timestamp(value)
    else:
        # Check if it's an epoch timestamp (auto-detect)
        if is_epoch_timestamp(value):
            try:
                parsed_dt = parse_epoch_timestamp(value)
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
                    candidates.append(normalize_format(fmt))

                if formats:
                    candidates.extend(normalize_format(f) for f in formats)

                # Try candidate formats first with exact matching
                if candidates:
                    for candidate_fmt in candidates:
                        try:
                            parsed_dt = datetime.datetime.strptime(value, candidate_fmt)
                            # For strict format enforcement with schema tokens (no %), always check exact match
                            if fmt and '%' not in fmt:
                                # This is a schema token format like "YYYY-MM-DD", enforce exact match
                                if not matches_format_exact(value, candidate_fmt):
                                    parsed_dt = None
                                    continue
                            # For strptime formats with %, also check exact match if it was the original format
                            elif fmt and '%' in fmt and not matches_format_exact(value, candidate_fmt):
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

                # If no specific format was provided, try common formats
                if not parsed_dt and not fmt and not formats:
                    # Try common datetime formats
                    parsed_dt = try_strptime(value, COMMON_DATETIME_FORMATS)

                    # Try common date formats (will give time 00:00:00)
                    if not parsed_dt:
                        parsed_dt = try_strptime(value, COMMON_DATE_FORMATS)

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
        return datetime_to_epoch(parsed_dt, to_epoch)

    return parsed_dt
