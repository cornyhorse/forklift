"""Small text helpers shared by the CSV processing components."""

from __future__ import annotations

import re

_UTF8_NAMES = {"utf-8", "utf8", "u8"}


def read_encoding(encoding: str) -> str:
    """Return the codec to use when reading text with Python's csv module.

    A plain UTF-8 file may start with a byte order mark, which Python's ``utf-8`` codec keeps
    as a zero-width character glued to the first column name. Reading with ``utf-8-sig``
    drops it and behaves like ``utf-8`` otherwise.

    Args:
        encoding: Configured text encoding

    Returns:
        ``"utf-8-sig"`` for UTF-8, the unchanged encoding for anything else
    """
    if isinstance(encoding, str) and encoding.strip().lower().replace("_", "-") in _UTF8_NAMES:
        return "utf-8-sig"
    return encoding


# "Expected 3 columns, got 4: <raw row text>" -> keep only the part before the row text
_COLUMN_MISMATCH = re.compile(r"(Expected \d+ columns?, got \d+)\s*:.*", re.DOTALL)
# "invalid value 'abc'" -> value is dropped
_INVALID_VALUE = re.compile(r"(invalid value)\s*(['\"]).*", re.DOTALL)
# "Failed to parse string: 'abc' as a scalar ..." / "Failed to parse value: abc"
_FAILED_PARSE = re.compile(r"(Failed to parse (?:string|value))\s*:.*", re.DOTALL)


def sanitize_arrow_error(message: str) -> str:
    """Remove raw cell/row content from an Arrow CSV error message.

    Arrow embeds the offending row or value in its messages. Row content can be PII, so it
    must not end up in logs or in ``ProcessingResults.errors``. The error class text and the
    column location Arrow reports (``In CSV column #N``) are kept.

    Args:
        message: Original exception text

    Returns:
        The message with raw content replaced by ``<redacted>``
    """
    text = str(message)
    text = _COLUMN_MISMATCH.sub(r"\1: <row content redacted>", text)
    text = _INVALID_VALUE.sub(r"\1 <redacted>", text)
    text = _FAILED_PARSE.sub(r"\1: <redacted>", text)
    return text
