from __future__ import annotations
import datetime
from typing import List, Optional, Union
from dateutil import parser
import re

COMMON_DATE_FORMATS = [
    "%Y%m%d",        # 20250827
    "%Y-%m-%d",      # 2025-08-27
    "%m/%d/%Y",      # 08/27/2025
    "%d/%m/%Y",      # 27/08/2025
    "%Y/%m/%d",      # 2025/08/27
    "%d-%b-%Y",      # 27-Aug-2025
    "%b %d, %Y",     # Aug 27, 2025
    "%d %b %Y",      # 27 Aug 2025
    "%Y.%m.%d",      # 2025.08.27
]

COMMON_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",         # 2025-08-27 14:30:00
    "%Y-%m-%dT%H:%M:%S",         # 2025-08-27T14:30:00
    "%Y-%m-%dT%H:%M:%SZ",        # 2025-08-27T14:30:00Z
    "%Y-%m-%dT%H:%M:%S%z",       # 2025-08-27T14:30:00+00:00
    "%Y-%m-%d %H:%M:%S.%f",      # 2025-08-27 14:30:00.123456
    "%Y-%m-%dT%H:%M:%S.%f",      # 2025-08-27T14:30:00.123456
    "%Y-%m-%dT%H:%M:%S.%fZ",     # 2025-08-27T14:30:00.123456Z
    "%m/%d/%Y %H:%M:%S",         # 08/27/2025 14:30:00
    "%d/%m/%Y %H:%M:%S",         # 27/08/2025 14:30:00
    "%Y/%m/%d %H:%M:%S",         # 2025/08/27 14:30:00
    "%d-%b-%Y %H:%M:%S",         # 27-Aug-2025 14:30:00
    "%b %d, %Y %H:%M:%S",        # Aug 27, 2025 14:30:00
    "%d %b %Y %H:%M:%S",         # 27 Aug 2025 14:30:00
]

# Map common schema tokens -> strptime
# Case-insensitive; longer tokens first to avoid partial replacements.
_TOKEN_MAP = [
    (re.compile(r"YYYY", re.IGNORECASE), "%Y"),
    (re.compile(r"MMMM", re.IGNORECASE), "%B"),  # full month name
    (re.compile(r"MMM", re.IGNORECASE), "%b"),   # abbreviated month
    (re.compile(r"MM", re.IGNORECASE), "%m"),
    (re.compile(r"DD", re.IGNORECASE), "%d"),
    (re.compile(r"HH", re.IGNORECASE), "%H"),
    (re.compile(r"mm", re.IGNORECASE), "%M"),
    (re.compile(r"SS", re.IGNORECASE), "%S"),
]

def _normalize_format(fmt: str) -> str:
    """
    Converts schema-style tokens (YYYY, MM, DD, MMM, MMMM, HH, mm, SS) to strptime tokens.

    :param fmt: The format string to normalize.
    :return: Normalized format string compatible with strptime.
    """
    out = fmt
    for pat, repl in _TOKEN_MAP:
        out = pat.sub(repl, out)
    return out

def _is_epoch_timestamp(value: str) -> bool:
    """Check if a string value looks like an epoch timestamp."""
    if not value or not value.isdigit():
        return False

    # Check if it's a reasonable epoch timestamp (between 1970 and 2050)
    try:
        timestamp = int(value)
        # Handle seconds, milliseconds, microseconds, nanoseconds
        if 1000000000 <= timestamp <= 9999999999:  # 10-digit seconds (1970-2286)
            return True
        elif 1000000000000 <= timestamp <= 9999999999999:  # 13-digit milliseconds
            return True
        elif 1000000000000000 <= timestamp <= 9999999999999999:  # 16-digit microseconds
            return True
        elif 1000000000000000000 <= timestamp <= 9999999999999999999:  # 19-digit nanoseconds
            return True
    except ValueError:
        pass
    return False

def _parse_epoch_timestamp(value: str) -> datetime.datetime:
    """Parse an epoch timestamp to a datetime object."""
    timestamp = int(value)

    # Determine the unit based on the number of digits
    if 1000000000 <= timestamp <= 9999999999:  # 10-digit seconds
        return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    elif 1000000000000 <= timestamp <= 9999999999999:  # 13-digit milliseconds
        return datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.timezone.utc)
    elif 1000000000000000 <= timestamp <= 9999999999999999:  # 16-digit microseconds
        return datetime.datetime.fromtimestamp(timestamp / 1000000, tz=datetime.timezone.utc)
    elif 1000000000000000000 <= timestamp <= 9999999999999999999:  # 19-digit nanoseconds
        return datetime.datetime.fromtimestamp(timestamp / 1000000000, tz=datetime.timezone.utc)
    else:
        raise ValueError(f"Invalid epoch timestamp: {value}")

def _datetime_to_epoch(dt: datetime.datetime, unit: str = "seconds") -> Union[int, float]:
    """Convert a datetime object to epoch timestamp in specified unit."""
    timestamp = dt.timestamp()

    if unit == "seconds":
        return int(timestamp)
    elif unit == "milliseconds":
        return int(timestamp * 1000)
    elif unit == "microseconds":
        return int(timestamp * 1000000)
    elif unit == "nanoseconds":
        return int(timestamp * 1000000000)
    else:
        raise ValueError(f"Invalid epoch unit: {unit}")

def _matches_format_exact(value: str, fmt: str) -> bool:
    """
    Parses with strptime, then requires exact match via strftime (strict).

    :param value: The date string to check.
    :param fmt: The format string to use for parsing.
    :return: True if the value matches the format exactly, False otherwise.
    """
    try:
        dt = datetime.datetime.strptime(value, fmt)
        return dt.strftime(fmt) == value
    except Exception:
        return False

# ----------------------------------------------------------------------------
# Public validation helper (bool) – existing API relied upon elsewhere
# ----------------------------------------------------------------------------

def parse_date(value: Optional[str], fmt: str = None, formats: List[str] = None) -> bool:
    """
    Tries to parse a date string using a specific format or a list of common formats.
    If a format is provided (schema tokens or strptime), requires an exact textual match
    (enforces zero-padding and literals). Falls back to dateutil.parser.parse(fuzzy=False)
    if no format matches.

    :param value: The date string to parse.
    :param fmt: Optional format string to use for parsing.
    :param formats: Optional list of format strings to try.
    :return: True if the date string can be parsed, False otherwise.
    """
    if not value or not isinstance(value, str):
        return False

    # Check for epoch timestamps
    if _is_epoch_timestamp(value):
        try:
            _parse_epoch_timestamp(value)
            return True
        except ValueError:
            pass

    if fmt:
        # Accept either native strptime directives or schema tokens.
        norm = _normalize_format(fmt) if "%" not in fmt else fmt
        return _matches_format_exact(value, norm)

    if formats:
        for f in formats:
            norm = _normalize_format(f) if "%" not in f else f
            if _matches_format_exact(value, norm):
                return True
        return False

    for f in COMMON_DATE_FORMATS:
        if _matches_format_exact(value, f):
            return True

    try:
        parser.parse(value, fuzzy=False)
        return True
    except Exception:
        return False

# ----------------------------------------------------------------------------
# New coercion utilities (return parsed values) for reuse (e.g. TypeCoercion)
# ----------------------------------------------------------------------------

def _try_strptime(value: str, fmts: List[str]) -> Optional[datetime.datetime]:
    for f in fmts:
        try:
            return datetime.datetime.strptime(value, f)
        except Exception:
            continue
    return None

def coerce_date(value: str, fmt: str | None = None, formats: List[str] | None = None) -> str:
    """Coerce a date string into canonical ISO (YYYY-MM-DD).

    Attempts, in order:
      * Explicit fmt (schema tokens or strptime) if provided
      * Provided formats list (first matching)
      * COMMON_DATE_FORMATS lookup (strict round-trip)
      * dateutil.parser.parse (fuzzy=False)

    :raises ValueError: if parsing fails or value empty.
    """
    if value is None:
        raise ValueError("empty date")
    token = value.strip()
    if token == "":
        raise ValueError("empty date")

    # Handle epoch timestamps
    if _is_epoch_timestamp(token):
        try:
            dt = _parse_epoch_timestamp(token)
            return dt.date().isoformat()
        except ValueError:
            pass

    candidates: List[str] = []
    if fmt:
        norm = _normalize_format(fmt) if "%" not in fmt else fmt
        candidates.append(norm)
    if formats:
        for f in formats:
            norm = _normalize_format(f) if "%" not in f else f
            candidates.append(norm)

    if candidates:
        dt = _try_strptime(token, candidates)
        if dt:
            return dt.date().isoformat()
        raise ValueError(f"bad date: {value}")

    dt = _try_strptime(token, COMMON_DATE_FORMATS)
    if dt:
        return dt.date().isoformat()

    # Fallback to robust parser
    try:
        parsed = parser.parse(token, fuzzy=False)
        return parsed.date().isoformat()
    except Exception:
        raise ValueError(f"bad date: {value}")

def coerce_datetime(value: str, fmt: str | None = None, formats: List[str] | None = None,
                   allow_fuzzy: bool = False, from_epoch: bool = False,
                   to_epoch: str | None = None) -> Union[datetime.datetime, str, int, float]:
    """Coerce a datetime string to a datetime object or epoch timestamp.

    Args:
        value: The datetime string to parse
        fmt: Optional specific format to enforce
        formats: Optional list of formats to try
        allow_fuzzy: Whether to allow fuzzy parsing with dateutil
        from_epoch: Whether to treat input as epoch timestamp
        to_epoch: If specified, convert result to epoch in this unit (seconds, milliseconds, etc.)

    Returns:
        Parsed datetime object, ISO string, or epoch timestamp based on parameters

    :raises ValueError: on failure/empty.
    """
    if value is None:
        raise ValueError("empty datetime")
    token = value.strip()
    if token == "":
        raise ValueError("empty datetime")

    parsed_dt = None

    # Handle epoch input explicitly
    if from_epoch or _is_epoch_timestamp(token):
        try:
            parsed_dt = _parse_epoch_timestamp(token)
        except ValueError:
            if from_epoch:  # If explicitly expecting epoch, fail
                raise ValueError(f"Invalid epoch timestamp: {value}")

    # Try explicit format first with STRICT matching
    if not parsed_dt and fmt:
        norm = _normalize_format(fmt) if "%" not in fmt else fmt
        if _matches_format_exact(token, norm):
            try:
                parsed_dt = datetime.datetime.strptime(token, norm)
            except ValueError:
                pass
        if not parsed_dt:
            raise ValueError(f"Value '{value}' does not match required format '{fmt}'")

    # Try provided formats with STRICT matching
    if not parsed_dt and formats:
        normalized_formats = [_normalize_format(f) if "%" not in f else f for f in formats]
        for norm_fmt in normalized_formats:
            if _matches_format_exact(token, norm_fmt):
                try:
                    parsed_dt = datetime.datetime.strptime(token, norm_fmt)
                    break
                except ValueError:
                    continue
        if not parsed_dt:
            raise ValueError(f"Value '{value}' does not match any of the specified formats")

    # Try common datetime formats (only if no explicit format was specified)
    if not parsed_dt and not fmt and not formats:
        parsed_dt = _try_strptime(token, COMMON_DATETIME_FORMATS)

    # Try common date formats (only if no explicit format was specified)
    if not parsed_dt and not fmt and not formats:
        parsed_dt = _try_strptime(token, COMMON_DATE_FORMATS)

    # Fallback to dateutil parser (only if no explicit format was specified)
    if not parsed_dt and not fmt and not formats:
        iso_try = token.replace("Z", "+00:00")
        try:
            parsed_dt = parser.parse(iso_try, fuzzy=allow_fuzzy)
        except Exception:
            if not allow_fuzzy:
                raise ValueError(f"bad datetime: {value}")
            # Try one more time with fuzzy=True if allowed
            try:
                parsed_dt = parser.parse(token, fuzzy=True)
            except Exception:
                raise ValueError(f"bad datetime: {value}")

    if not parsed_dt:
        raise ValueError(f"bad datetime: {value}")

    # Convert to epoch if requested
    if to_epoch:
        return _datetime_to_epoch(parsed_dt, to_epoch)

    return parsed_dt

__all__ = [
    "parse_date",
    "coerce_date",
    "coerce_datetime",
    "COMMON_DATE_FORMATS",
    "COMMON_DATETIME_FORMATS",
]
