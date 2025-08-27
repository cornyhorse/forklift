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