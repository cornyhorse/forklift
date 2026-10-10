"""Format normalization and validation utilities."""

import datetime
import re
from functools import lru_cache
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Schema token -> strptime conversion
# ---------------------------------------------------------------------------

# Longest alternatives first. Seconds/fraction runs: "SS" is seconds, three or more S (or f)
# characters are a fractional second ("ss.SSS", "HH:mm:ss.ffffff").
_TOKEN_RE = re.compile(
    r"[Ss]{3,9}|f{3,9}"
    r"|Yyyy|YYYY|yyyy|YY|yy|Yy"
    r"|Mmmm|MMMM|mmmm|Mmm|MMM|mmm"
    r"|Mm|MM|mm|M|m"
    r"|Dd|DD|dd|D|d"
    r"|Hh|HH|hh|H|h"
    r"|Ss|SS|ss|S|s"
)
# Letters that may sit between/after tokens inside one run: ISO "T" separator and "Z" suffix
_LITERAL_LETTERS = {"T", "Z"}
_ALPHA_OR_OTHER = re.compile(r"[A-Za-z]+|[^A-Za-z]+")

_YEAR4 = {"YYYY", "yyyy", "Yyyy"}
_YEAR2 = {"YY", "yy", "Yy"}
_MONTH_NAME_FULL = {"MMMM", "mmmm", "Mmmm"}
_MONTH_NAME_ABBR = {"MMM", "mmm", "Mmm"}
_MONTH_OR_MINUTE = {"MM", "mm", "Mm", "M", "m"}
_DAY = {"DD", "dd", "Dd", "D", "d"}
_HOUR = {"HH", "hh", "Hh", "H", "h"}
_SECOND = {"SS", "ss", "Ss", "S", "s"}
_SINGLE_LETTER_TOKENS = {"M", "m", "D", "d", "H", "h", "S", "s"}


def _tokenize_run(run: str) -> Optional[List[Tuple[str, str]]]:
    """Split a run of letters into schema tokens, or None if it is not made of tokens.

    A run such as ``YYYYMMDDHHmmss`` yields six tokens. A literal ``T`` or ``Z`` between or after
    tokens (``DDTHH``, ``ssZ``) is kept as literal text. Words that are not token soup (``de``,
    ``at``) come back as None and stay literal, so ``"DD de MMMM de YYYY"`` keeps its ``de``.
    """
    items: List[Tuple[str, str]] = []
    position = 0
    while position < len(run):
        match = _TOKEN_RE.match(run, position)
        if match:
            items.append(("tok", match.group()))
            position = match.end()
        elif run[position] in _LITERAL_LETTERS and items:
            items.append(("lit", run[position]))
            position += 1
        else:
            return None
    return items


@lru_cache(maxsize=512)
def _convert_tokens(fmt: str) -> Tuple[str, bool]:
    """Convert a schema-token format to strptime. Returns (strptime_format, has_flexible_token).

    ``has_flexible_token`` is True when the format uses single-letter tokens (``M``, ``D``, ``H``,
    ``S``...) which, unlike the double-letter ones, accept values that are not zero padded.
    """
    items: List[Tuple[str, str]] = []
    for part in _ALPHA_OR_OTHER.findall(fmt):
        if part[0].isalpha() and part.isascii():
            tokens = _tokenize_run(part)
            if tokens is not None:
                items.extend(tokens)
                continue
        items.append(("lit", part))

    flexible = any(kind == "tok" and text in _SINGLE_LETTER_TOKENS for kind, text in items)

    out: List[str] = []
    for index, (kind, text) in enumerate(items):
        if kind == "lit":
            out.append(text)
        elif text in _YEAR4:
            out.append("%Y")
        elif text in _YEAR2:
            out.append("%y")
        elif text in _MONTH_NAME_FULL:
            out.append("%B")
        elif text in _MONTH_NAME_ABBR:
            out.append("%b")
        elif text in _MONTH_OR_MINUTE:
            out.append("%M" if _is_minute(items, index) else "%m")
        elif text in _DAY:
            out.append("%d")
        elif text in _HOUR:
            out.append("%H")
        elif text in _SECOND:
            out.append("%S")
        else:  # fractional seconds: SSS / ffffff ...
            out.append("%f")
    return "".join(out), flexible


def _separator_between(items: List[Tuple[str, str]], start: int, stop: int) -> Optional[str]:
    """Text between two token positions if it is only a short non-alphanumeric separator."""
    between = "".join(text for _, text in items[start + 1 : stop])
    if len(between) <= 2 and not any(ch.isalnum() for ch in between):
        return between
    return None


def _is_minute(items: List[Tuple[str, str]], index: int) -> bool:
    """Decide whether the M/MM token at ``index`` means minutes (True) or month (False).

    Minutes follow an hour token (``HH:mm``, ``HHmm``) or precede a seconds token (``mm:ss``);
    everything else, e.g. ``YYYYMMDD``, ``DD MM YYYY``, is a month.
    """
    previous = next((i for i in range(index - 1, -1, -1) if items[i][0] == "tok"), None)
    if previous is not None and items[previous][1] in _HOUR:
        if _separator_between(items, previous, index) is not None:
            return True

    following = next((i for i in range(index + 1, len(items)) if items[i][0] == "tok"), None)
    if following is not None and items[following][1] in _SECOND:
        separator = _separator_between(items, index, following)
        if separator is not None and ":" in separator:
            return True
    return False


def normalize_format(fmt: str) -> str:
    """Normalize schema tokens to strptime format.

    Month vs. minute is decided by context: ``mm``/``MM`` right after an hour token (or before a
    seconds token separated by ``:``) is the minute, otherwise the month. So
    ``YYYY-MM-DD HH:mm:ss.SSS``, ``YYYYMMDDHHmmss`` and ``DD MM YYYY HH:mm:ss`` all convert to
    valid strptime formats. Three or more ``S``/``f`` characters are a fractional second
    (``%f``).

    Args:
        fmt: Format string (either strptime or schema tokens)

    Returns:
        Normalized strptime format string
    """
    # If already contains %, assume it's strptime format
    if "%" in fmt:
        return fmt
    return _convert_tokens(fmt)[0]


def format_accepts_unpadded(fmt: str) -> bool:
    """True if a *schema token* format uses single-letter tokens (``M``, ``D``, ``H``, ...).

    Such formats accept values that are not zero padded (``2025-8-7`` for ``YYYY-M-D``); formats
    with double-letter tokens or strptime ``%`` directives require exact, zero-padded values.
    """
    if "%" in fmt:
        return False
    return _convert_tokens(fmt)[1]


# ---------------------------------------------------------------------------
# Exact matching
# ---------------------------------------------------------------------------

_STRICT_DIRECTIVES = {
    "Y": r"\d{4}",
    "y": r"\d{2}",
    "m": r"\d{2}",
    "d": r"\d{2}",
    "H": r"\d{2}",
    "I": r"\d{2}",
    "M": r"\d{2}",
    "S": r"\d{2}",
    "f": r"\d{1,6}",
    "j": r"\d{3}",
    "p": r"(?:[AaPp][Mm])",
    "z": r"(?:Z|z|[+-]\d{2}(?::?\d{2}(?::?\d{2}(?:\.\d{1,6})?)?)?)",
    "Z": r"[A-Za-z]+",
    "a": r"[^\W\d_]+\.?",
    "A": r"[^\W\d_]+",
    "b": r"[^\W\d_]+\.?",
    "B": r"[^\W\d_]+",
    "%": "%",
}


@lru_cache(maxsize=256)
def _strict_pattern(fmt: str) -> Optional["re.Pattern[str]"]:
    """Regex that accepts exactly the zero-padded renderings of ``fmt``, or None if unknown.

    Used to reject values that strptime parses leniently (``2025-8-7`` for ``%Y-%m-%d``) without
    re-formatting the parsed value, which cannot work for ``%z`` (``Z`` and ``+05:00`` are valid
    input but ``strftime`` writes ``+0000`` / ``+0500``).
    """
    pieces = []
    position = 0
    while position < len(fmt):
        char = fmt[position]
        if char == "%":
            # A trailing "%" gives "", which is unknown like any other unsupported directive
            pattern = _STRICT_DIRECTIVES.get(fmt[position + 1 : position + 2])
            if pattern is None:
                return None
            pieces.append(pattern)
            position += 2
        else:
            pieces.append(re.escape(char))
            position += 1
    # Every piece is a complete, valid regex fragment, so the joined pattern always compiles
    return re.compile("".join(pieces))


def _round_trip_matches(value: str, fmt: str, parsed: datetime.datetime) -> bool:
    """Legacy exactness check for directives without a strict pattern: re-format and compare."""
    try:
        return parsed.strftime(fmt) == value
    except (ValueError, TypeError):
        return False


def matches_format_exact(value: str, fmt: str) -> bool:
    """Check if a value matches a format exactly.

    "Exactly" means strptime accepts the value *and* every field has its canonical width
    (zero padded), so ``2025-8-27`` does not match ``%Y-%m-%d``. Timezone offsets are accepted
    in every form strptime reads (``Z``, ``+0500``, ``+05:00``).

    Args:
        value: String to check
        fmt: Format string (strptime format)

    Returns:
        True if value matches format exactly
    """
    try:
        parsed = datetime.datetime.strptime(value, fmt)
    except (ValueError, TypeError, re.error):
        return False

    pattern = _strict_pattern(fmt)
    if pattern is not None:
        return pattern.fullmatch(value) is not None
    return _round_trip_matches(value, fmt, parsed)


# ---------------------------------------------------------------------------
# Common formats
# ---------------------------------------------------------------------------


@lru_cache(maxsize=16)
def _ordered_formats(formats: Tuple[str, ...], dayfirst: bool) -> Tuple[str, ...]:
    if dayfirst:
        return formats

    def day_before_month(fmt: str) -> bool:
        month, day = fmt.find("%m"), fmt.find("%d")
        return month >= 0 and day >= 0 and day < month

    # Stable sort: month-first renderings (%m/%d/%Y) move ahead of day-first ones (%d/%m/%Y)
    return tuple(sorted(formats, key=day_before_month))


def ordered_formats(formats: List[str], dayfirst: bool = True) -> List[str]:
    """Order a list of common formats for day-first (default) or month-first resolution.

    Only the relative order of the ambiguous formats changes; unambiguous ones (ISO, month names)
    are unaffected because they cannot match the same text.
    """
    return list(_ordered_formats(tuple(formats), dayfirst))


def try_strptime(value: str, formats: List[str]) -> Optional[datetime.datetime]:
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
        except (ValueError, TypeError, re.error):
            # re.error: an invalid user-supplied format must not escape the ValueError handlers
            continue
    return None
