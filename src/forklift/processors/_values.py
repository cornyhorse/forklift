"""Shared value helpers for validators: exact decimals and date parsing."""

from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, List, Optional, Union

_ISO_DATE_PREFIX = re.compile(r"\d{4}-\d{2}-\d{2}")


def to_decimal(value: Any) -> Decimal:
    """Exact decimal for a number or numeric string (floats via their shortest ``repr``).

    Raises:
        decimal.InvalidOperation / TypeError / ValueError: If the value is not numeric.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    if isinstance(value, str):
        return Decimal(value.strip())
    raise TypeError(f"unsupported numeric type {type(value).__name__}")


def parse_temporal(
    value: Any, formats: Optional[List[str]] = None, allow_iso: bool = True
) -> Optional[Union[date, datetime]]:
    """``date``/``datetime`` for a date object or a string in ISO format and/or ``formats``.

    Returns ``None`` when the value cannot be interpreted as a date.
    """
    if isinstance(value, (date, datetime)):
        return value
    if not isinstance(value, str):
        return None

    text = value.strip()
    if allow_iso and _ISO_DATE_PREFIX.match(text):
        try:
            return date.fromisoformat(text) if len(text) == 10 else datetime.fromisoformat(text)
        except ValueError:
            pass
    for fmt in formats or ():
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed if any(code in fmt for code in ("%H", "%I", "%M", "%S")) else parsed.date()
    return None


def as_datetime(value: Union[date, datetime]) -> datetime:
    """``datetime`` for a date (midnight) or datetime."""
    return value if isinstance(value, datetime) else datetime.combine(value, time.min)


def compare_temporal(value: Any, bound: Any) -> int:
    """-1/0/1. A date-only bound compares calendar dates; a datetime bound compares instants."""
    if isinstance(value, (date, datetime)):
        if isinstance(bound, datetime):
            value = as_datetime(value)
        else:
            value = value.date() if isinstance(value, datetime) else value
    return (value > bound) - (value < bound)
