"""Expression evaluation functions for calculated columns."""

import math
from datetime import date, datetime
from typing import Any, Callable, Dict, Optional

from .limits import MAX_SEQUENCE_LENGTH, ExpressionError, check_result, checked_mul, checked_pow

MAX_ROUND_DIGITS = 1000

_TRUE_STRINGS = frozenset({"true", "t", "yes", "y", "1", "on"})
_FALSE_STRINGS = frozenset({"false", "f", "no", "n", "0", "off"})


def _to_bool(x: Any) -> Optional[bool]:
    """Convert to bool; strings are parsed (``"false"``/``"0"``/``"no"`` are False)."""
    if x is None:
        return None
    if isinstance(x, str):
        key = x.strip().lower()
        if key == "":
            return None
        if key in _TRUE_STRINGS:
            return True
        if key in _FALSE_STRINGS:
            return False
        raise ValueError("string is not a recognised boolean value")
    return bool(x)


def _substring(x: Any, start: Any, length: Any = None) -> Optional[str]:
    """Python-style (0-based) slice ``x[start:start+length]``; NULL if x or start is NULL."""
    if x is None or start is None:
        return None
    text = str(x)
    if start < 0:
        start = max(len(text) + start, 0)
    if length is None:
        return text[start:]
    if length < 0:
        return ""
    return text[start : start + length]


def _round(x: Any, digits: Any = 0) -> Any:
    """Python's ``round`` (ties to even); |digits| is bounded (``round(5, -10**9)`` hangs)."""
    if x is None:
        return None
    if isinstance(digits, int) and abs(digits) > MAX_ROUND_DIGITS:
        raise ExpressionError(f"round() digits must be within +/-{MAX_ROUND_DIGITS}")
    return round(x, digits)


def _left(x: Any, n: Any) -> Optional[str]:
    if x is None or n is None:
        return None
    return str(x)[: max(n, 0)]


def _right(x: Any, n: Any) -> Optional[str]:
    if x is None or n is None:
        return None
    if n <= 0:
        return ""
    return str(x)[-n:]


def _replace(x: Any, old: Any, new: Any) -> Optional[str]:
    if x is None:
        return None
    text, old, new = str(x), str(old), str(new)
    # Bound the size of the result before building it (replace(x, '', 'abc') multiplies it).
    occurrences = len(text) + 1 if old == "" else text.count(old)
    if len(text) + occurrences * (len(new) - len(old)) > MAX_SEQUENCE_LENGTH + len(text):
        raise ExpressionError(
            f"replace() result would exceed the maximum length of {MAX_SEQUENCE_LENGTH}"
        )
    return text.replace(old, new)


def _concat(*args: Any) -> str:
    return check_result("".join(str(arg) for arg in args if arg is not None), args)


def _sum(*args: Any) -> Any:
    """Sum of the non-NULL arguments; NULL when every argument is NULL (like min/max/avg)."""
    values = [arg for arg in args if arg is not None]
    return sum(values) if values else None


def get_available_functions(clock: Optional[Callable[[], datetime]] = None) -> Dict[str, Callable]:
    """Get all available functions for expression evaluation.

    Args:
        clock: Callable returning the current ``datetime``. ``now()`` and ``today()`` use it, so
            an evaluator can hand every row of a batch the same snapshot. Defaults to
            ``datetime.now``.
    """
    current_time = clock or datetime.now

    return {
        # Arithmetic functions
        "add": lambda a, b: (
            check_result(a + b, (a, b)) if a is not None and b is not None else None
        ),
        "subtract": lambda a, b: a - b if a is not None and b is not None else None,
        "multiply": lambda a, b: checked_mul(a, b) if a is not None and b is not None else None,
        "divide": lambda a, b: a / b if a is not None and b is not None and b != 0 else None,
        "power": lambda a, b: checked_pow(a, b) if a is not None and b is not None else None,
        "mod": lambda a, b: a % b if a is not None and b is not None and b != 0 else None,
        # Mathematical functions
        "abs": lambda x: abs(x) if x is not None else None,
        # round() is Python's: ties go to the even neighbour ("banker's rounding"), so
        # round(0.5) == 0, round(1.5) == 2, round(2.5) == 2.
        "round": _round,
        "floor": lambda x: math.floor(x) if x is not None else None,
        "ceil": lambda x: math.ceil(x) if x is not None else None,
        "sqrt": lambda x: math.sqrt(x) if x is not None and x >= 0 else None,
        "log": lambda x: math.log(x) if x is not None and x > 0 else None,
        "log10": lambda x: math.log10(x) if x is not None and x > 0 else None,
        "sin": lambda x: math.sin(x) if x is not None else None,
        "cos": lambda x: math.cos(x) if x is not None else None,
        "tan": lambda x: math.tan(x) if x is not None else None,
        # String functions
        "concat": _concat,
        "upper": lambda x: str(x).upper() if x is not None else None,
        "lower": lambda x: str(x).lower() if x is not None else None,
        "trim": lambda x: str(x).strip() if x is not None else None,
        "length": lambda x: len(str(x)) if x is not None else None,
        "substring": _substring,
        "replace": _replace,
        "left": _left,
        "right": _right,
        # Conditional functions
        "if_then_else": lambda condition, then_val, else_val: then_val if condition else else_val,
        "coalesce": lambda *args: next((arg for arg in args if arg is not None), None),
        "nullif": lambda x, y: None if x == y else x,
        "isnull": lambda x: x is None,
        "isnotnull": lambda x: x is not None,
        # Type conversion functions
        "to_string": lambda x: str(x) if x is not None else None,
        "to_int": lambda x: int(x) if x is not None else None,
        "to_float": lambda x: float(x) if x is not None else None,
        "to_bool": _to_bool,
        # Date/time functions (now/today read the evaluator's per-run snapshot)
        "now": lambda: current_time(),
        "today": lambda: current_time().date(),
        "year": lambda x: x.year if isinstance(x, (date, datetime)) else None,
        "month": lambda x: x.month if isinstance(x, (date, datetime)) else None,
        "day": lambda x: x.day if isinstance(x, (date, datetime)) else None,
        "weekday": lambda x: x.weekday() if isinstance(x, (date, datetime)) else None,
        # Comparison functions
        "equals": lambda a, b: a == b,
        "not_equals": lambda a, b: a != b,
        "greater_than": lambda a, b: a > b if a is not None and b is not None else False,
        "less_than": lambda a, b: a < b if a is not None and b is not None else False,
        "greater_equal": lambda a, b: a >= b if a is not None and b is not None else False,
        "less_equal": lambda a, b: a <= b if a is not None and b is not None else False,
        # Logical functions
        "and": lambda a, b: a and b,
        "or": lambda a, b: a or b,
        "not": lambda a: not a,
        # Utility functions
        "min": lambda *args: (
            min(arg for arg in args if arg is not None)
            if any(arg is not None for arg in args)
            else None
        ),
        "max": lambda *args: (
            max(arg for arg in args if arg is not None)
            if any(arg is not None for arg in args)
            else None
        ),
        "sum": _sum,
        "avg": lambda *args: (
            sum(arg for arg in args if arg is not None)
            / len([arg for arg in args if arg is not None])
            if any(arg is not None for arg in args)
            else None
        ),
    }


def get_constants() -> Dict[str, Any]:
    """Get common constants for expression evaluation."""
    return {"PI": math.pi, "E": math.e, "TRUE": True, "FALSE": False, "NULL": None}
