"""Expression evaluation functions for calculated columns."""

from typing import Dict, Callable, Any
from datetime import datetime, date
import math


def get_available_functions() -> Dict[str, Callable]:
    """Get all available functions for expression evaluation."""
    return {
        # Arithmetic functions
        'add': lambda a, b: a + b if a is not None and b is not None else None,
        'subtract': lambda a, b: a - b if a is not None and b is not None else None,
        'multiply': lambda a, b: a * b if a is not None and b is not None else None,
        'divide': lambda a, b: a / b if a is not None and b is not None and b != 0 else None,
        'power': lambda a, b: a ** b if a is not None and b is not None else None,
        'mod': lambda a, b: a % b if a is not None and b is not None and b != 0 else None,

        # Mathematical functions
        'abs': lambda x: abs(x) if x is not None else None,
        'round': lambda x, digits=0: round(x, digits) if x is not None else None,
        'floor': lambda x: math.floor(x) if x is not None else None,
        'ceil': lambda x: math.ceil(x) if x is not None else None,
        'sqrt': lambda x: math.sqrt(x) if x is not None and x >= 0 else None,
        'log': lambda x: math.log(x) if x is not None and x > 0 else None,
        'log10': lambda x: math.log10(x) if x is not None and x > 0 else None,
        'sin': lambda x: math.sin(x) if x is not None else None,
        'cos': lambda x: math.cos(x) if x is not None else None,
        'tan': lambda x: math.tan(x) if x is not None else None,

        # String functions
        'concat': lambda *args: ''.join(str(arg) for arg in args if arg is not None),
        'upper': lambda x: str(x).upper() if x is not None else None,
        'lower': lambda x: str(x).lower() if x is not None else None,
        'trim': lambda x: str(x).strip() if x is not None else None,
        'length': lambda x: len(str(x)) if x is not None else None,
        'substring': lambda x, start, length=None: str(x)[start:start+length] if x is not None else None,
        'replace': lambda x, old, new: str(x).replace(old, new) if x is not None else None,
        'left': lambda x, n: str(x)[:n] if x is not None else None,
        'right': lambda x, n: str(x)[-n:] if x is not None else None,

        # Conditional functions
        'if_then_else': lambda condition, then_val, else_val: then_val if condition else else_val,
        'coalesce': lambda *args: next((arg for arg in args if arg is not None), None),
        'nullif': lambda x, y: None if x == y else x,
        'isnull': lambda x: x is None,
        'isnotnull': lambda x: x is not None,

        # Type conversion functions
        'to_string': lambda x: str(x) if x is not None else None,
        'to_int': lambda x: int(x) if x is not None else None,
        'to_float': lambda x: float(x) if x is not None else None,
        'to_bool': lambda x: bool(x) if x is not None else None,

        # Date/time functions
        'now': lambda: datetime.now(),
        'today': lambda: date.today(),
        'year': lambda x: x.year if isinstance(x, (date, datetime)) else None,
        'month': lambda x: x.month if isinstance(x, (date, datetime)) else None,
        'day': lambda x: x.day if isinstance(x, (date, datetime)) else None,
        'weekday': lambda x: x.weekday() if isinstance(x, (date, datetime)) else None,

        # Comparison functions
        'equals': lambda a, b: a == b,
        'not_equals': lambda a, b: a != b,
        'greater_than': lambda a, b: a > b if a is not None and b is not None else False,
        'less_than': lambda a, b: a < b if a is not None and b is not None else False,
        'greater_equal': lambda a, b: a >= b if a is not None and b is not None else False,
        'less_equal': lambda a, b: a <= b if a is not None and b is not None else False,

        # Logical functions
        'and': lambda a, b: a and b,
        'or': lambda a, b: a or b,
        'not': lambda a: not a,

        # Utility functions
        'min': lambda *args: min(arg for arg in args if arg is not None) if any(arg is not None for arg in args) else None,
        'max': lambda *args: max(arg for arg in args if arg is not None) if any(arg is not None for arg in args) else None,
        'sum': lambda *args: sum(arg for arg in args if arg is not None),
        'avg': lambda *args: sum(arg for arg in args if arg is not None) / len([arg for arg in args if arg is not None]) if any(arg is not None for arg in args) else None,
    }


def get_constants() -> Dict[str, Any]:
    """Get common constants for expression evaluation."""
    return {
        'PI': math.pi,
        'E': math.e,
        'TRUE': True,
        'FALSE': False,
        'NULL': None
    }
