from __future__ import annotations
import datetime
from typing import List, Optional
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

# Map common schema tokens -> strptime
# Case-insensitive; longer tokens first to avoid partial replacements.
_TOKEN_MAP = [
    (re.compile(r"YYYY", re.IGNORECASE), "%Y"),
    (re.compile(r"MMMM", re.IGNORECASE), "%B"),  # full month name
    (re.compile(r"MMM", re.IGNORECASE), "%b"),   # abbreviated month
    (re.compile(r"MM", re.IGNORECASE), "%m"),
    (re.compile(r"DD", re.IGNORECASE), "%d"),
]

def _normalize_format(fmt: str) -> str:
    """
    Converts schema-style tokens (YYYY, MM, DD, MMM, MMMM) to strptime tokens.

    :param fmt: The format string to normalize.
    :return: Normalized format string compatible with strptime.
    """
    out = fmt
    for pat, repl in _TOKEN_MAP:
        out = pat.sub(repl, out)
    return out

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

def coerce_datetime(value: str) -> datetime.datetime:
    """Coerce a datetime string to a datetime object.

    Accepts common explicit patterns and falls back to dateutil.parser.parse.
    Recognizes trailing 'Z' as UTC (converted to +00:00 aware datetime).
    :raises ValueError: on failure/empty.
    """
    if value is None:
        raise ValueError("empty datetime")
    token = value.strip()
    if token == "":
        raise ValueError("empty datetime")

    iso_try = token.replace("Z", "+00:00")
    try:
        # dateutil handles many variants; ensure we get a datetime (not date)
        dt = parser.parse(iso_try, fuzzy=False)
        return dt
    except Exception:
        pass

    # Explicit fallbacks (strptime) if dateutil failed (rare)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.datetime.strptime(token, fmt)
        except ValueError:
            continue
    raise ValueError(f"bad datetime: {value}")

__all__ = [
    "parse_date",
    "coerce_date",
    "coerce_datetime",
    "COMMON_DATE_FORMATS",
]
