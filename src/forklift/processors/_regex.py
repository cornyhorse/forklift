"""Shared, defensive regular-expression handling for the validators.

Patterns come from schema files / configuration and are evaluated against every cell, so they are
treated as untrusted input:

* patterns are compiled once, at configuration time (``re.error`` becomes ``ValueError``),
* they are limited in length,
* patterns with *nested unbounded quantifiers* such as ``(a+)+``, ``(.*)*`` or ``(\\w+)*`` - the
  classic catastrophic-backtracking shape (ReDoS) - are rejected unless the caller passes
  ``allow_unsafe_regex=True``. The check is a conservative heuristic: it can reject harmless
  patterns such as ``(\\d+\\.)+``; opt in for those.

Match semantics (used by every validator in ``forklift.processors``)
--------------------------------------------------------------------
A pattern is an **unanchored search**, exactly like the JSON Schema ``pattern`` keyword: the
value is valid when the pattern matches *anywhere* in it. Anchor the pattern with ``^`` and ``$``
to require the whole value to match. Unlike plain Python, a trailing ``$`` does not accept a value
that ends with a newline (JSON Schema/ECMAScript behaviour): it is compiled as ``\\Z``.
"""

from __future__ import annotations

import functools
import re
from typing import Any, Iterable

try:  # Python 3.11+
    from re import _parser as _sre_parse
except ImportError:  # pragma: no cover - older Pythons
    import sre_parse as _sre_parse  # type: ignore[no-redef]

#: Longest accepted pattern, in characters.
MAX_PATTERN_LENGTH = 2000

_REPEAT_OPS = {"MAX_REPEAT", "MIN_REPEAT"}
_LARGE_REPEAT = 100  # {n,m} with m >= this counts as "unbounded" for the nesting check


class UnsafeRegexError(ValueError):
    """Raised for a pattern that is too long or has a catastrophic-backtracking shape."""


def compile_pattern(pattern: str, allow_unsafe_regex: bool = False) -> "re.Pattern[str]":
    """Compile a validation pattern (cached).

    Args:
        pattern: Regular expression (see the module docstring for the match semantics).
        allow_unsafe_regex: Accept patterns flagged by the nested-quantifier heuristic.

    Returns:
        The compiled pattern; use :func:`pattern_matches` to apply it.

    Raises:
        ValueError: If the pattern is not a string, is invalid, or is too long.
        UnsafeRegexError: (a ``ValueError``) if the pattern looks prone to catastrophic
            backtracking and ``allow_unsafe_regex`` is false.
    """
    if not isinstance(pattern, str):
        raise ValueError(f"Regular expression must be a string, got {type(pattern).__name__}")
    return _compile_cached(pattern, bool(allow_unsafe_regex))


def pattern_matches(compiled: "re.Pattern[str]", value: Any) -> bool:
    """Whether ``compiled`` matches anywhere in ``value`` (JSON Schema ``pattern`` semantics)."""
    return compiled.search(value if isinstance(value, str) else str(value)) is not None


@functools.lru_cache(maxsize=512)
def _compile_cached(pattern: str, allow_unsafe_regex: bool) -> "re.Pattern[str]":
    if len(pattern) > MAX_PATTERN_LENGTH:
        raise ValueError(f"Regular expression is longer than {MAX_PATTERN_LENGTH} characters")

    try:
        parsed = _sre_parse.parse(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regular expression: {exc}") from None
    except (RecursionError, OverflowError):
        raise ValueError("Invalid regular expression: pattern is too deeply nested") from None

    if not allow_unsafe_regex and _has_nested_quantifier(parsed):
        raise UnsafeRegexError(
            "Regular expression has nested unbounded quantifiers (for example '(a+)+') and may "
            "take exponential time on some inputs; simplify it or set allow_unsafe_regex=True"
        )

    try:
        return re.compile(_ecma_end_anchor(pattern))
    except re.error as exc:
        raise ValueError(f"Invalid regular expression: {exc}") from None


def _ecma_end_anchor(pattern: str) -> str:
    """Compile a final ``$`` as ``\\Z`` so that ``'abc\\n'`` does not match ``'^abc$'``."""
    if not pattern.endswith("$") or re.search(r"\(\?[a-zA-Z]*m", pattern):
        return pattern
    backslashes = len(pattern) - 1 - len(pattern[:-1].rstrip("\\"))
    if backslashes % 2 == 1:  # the '$' is escaped
        return pattern
    return pattern[:-1] + r"\Z"


def _has_nested_quantifier(items: Iterable, inside_unbounded: bool = False) -> bool:
    """Whether a variable-length repeat sits inside an unbounded repeat."""
    for op, av in items:
        name = str(op)
        if name in _REPEAT_OPS:
            low, high, body = av
            if inside_unbounded and high > low and high > 1:
                return True
            if _has_nested_quantifier(body, inside_unbounded or high >= _LARGE_REPEAT):
                return True
        elif name == "SUBPATTERN":
            if _has_nested_quantifier(av[-1], inside_unbounded):
                return True
        elif name == "BRANCH":
            if any(_has_nested_quantifier(branch, inside_unbounded) for branch in av[1]):
                return True
        elif name in ("ASSERT", "ASSERT_NOT"):
            if _has_nested_quantifier(av[1], inside_unbounded):
                return True
        elif name == "GROUPREF_EXISTS":
            for branch in av[1:]:
                if branch is not None and _has_nested_quantifier(branch, inside_unbounded):
                    return True
        # possessive repeats and atomic groups (3.11+) cannot backtrack into their body: safe
    return False
