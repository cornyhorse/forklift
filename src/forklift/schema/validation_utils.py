"""Shared building blocks for the schema importers' validation.

The CSV, Excel, SQL and FWF importers validate user-supplied JSON documents, so every helper here
treats its input as untrusted: wrong-typed values produce error messages, never exceptions.
"""

from __future__ import annotations

import codecs
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

JSON_TYPES = frozenset({"string", "integer", "number", "boolean", "array", "object"})

_MAX_UNION_DEPTH = 4


def normalize_encoding(name: Any) -> Optional[str]:
    """Return Python's canonical codec name for ``name`` (``"Latin-1"`` -> ``"iso8859-1"``), or
    None if it is not a text encoding Python knows (binary codecs such as ``hex`` are rejected)."""
    if not isinstance(name, str) or not name.strip():
        return None
    try:
        info = codecs.lookup(name.strip())
    except (LookupError, ValueError):
        return None
    if not getattr(info, "_is_text_encoding", True) or info.name == "undefined":
        return None
    return info.name


def resolve_json_types(
    definition: Dict[str, Any], allowed: Iterable[str] = JSON_TYPES
) -> Tuple[List[str], List[Any], List[str]]:
    """Resolve the JSON Schema type(s) a property/column definition declares.

    Understands a plain ``"type": "string"``, nullable type arrays (``["string", "null"]``) and
    nullable ``anyOf``/``oneOf`` unions (``{"anyOf": [{"type": "string"}, {"type": "null"}]}``).

    Returns:
        ``(types, invalid, problems)``: the distinct non-null base types, the offending ``type``
        values (so each importer can report them in its own message format) and structural
        problems as short phrases.
    """
    types, invalid, problems, _ = _resolve(definition, set(allowed), 0)
    if not types and not invalid and not problems:
        problems.append("must declare at least one non-null type")
    return types, invalid, problems


def _resolve(
    definition: Dict[str, Any], allowed: set, depth: int
) -> Tuple[List[str], List[Any], List[str], bool]:
    types: List[str] = []
    invalid: List[Any] = []
    problems: List[str] = []
    saw_null = False

    def add(found: str) -> None:
        if found not in types:
            types.append(found)

    declared = definition.get("type")
    branch_keys = [key for key in ("anyOf", "oneOf") if key in definition]

    if isinstance(declared, str):
        if declared == "null":
            saw_null = True
        elif declared in allowed:
            add(declared)
        else:
            invalid.append(declared)
    elif isinstance(declared, list):
        if not declared:
            problems.append("type array must not be empty")
        for item in declared:
            if item == "null":
                saw_null = True
            elif isinstance(item, str) and item in allowed:
                add(item)
            else:
                invalid.append(item)
    elif declared is not None or (not branch_keys and depth == 0):
        invalid.append(declared)  # a bare union branch (e.g. just an enum) may omit its type

    for key in branch_keys:
        branches = definition[key]
        if not isinstance(branches, list) or not branches or depth >= _MAX_UNION_DEPTH:
            problems.append(f"{key} must be a non-empty array of schema objects")
            continue
        for index, branch in enumerate(branches):
            if not isinstance(branch, dict):
                problems.append(f"{key}[{index}] must be an object")
                continue
            b_types, b_invalid, b_problems, b_null = _resolve(branch, allowed, depth + 1)
            for found in b_types:
                add(found)
            invalid.extend(b_invalid)
            problems.extend(f"{key}[{index}]: {problem}" for problem in b_problems)
            saw_null = saw_null or b_null
    return types, invalid, problems, saw_null


def validate_required(
    required: Any, available: Iterable[str], label: str = "required"
) -> List[str]:
    """Check a JSON Schema ``required`` list: a list of strings, each one a known property."""
    if required is None:
        return []
    if not isinstance(required, list):
        return [f"'{label}' must be an array of property names"]
    known = set(available)
    errors = []
    for index, item in enumerate(required):
        if not isinstance(item, str):
            errors.append(f"{label}[{index}] must be a string")
        elif item not in known:
            errors.append(f"{label}[{index}] refers to unknown property '{item}'")
    return errors


def required_names(required: Any) -> List[str]:
    """The string entries of a ``required`` value, tolerant of malformed input (``"id"`` is NOT
    treated as the list ``['i', 'd']``; validation reports it)."""
    if isinstance(required, list):
        return [item for item in required if isinstance(item, str)]
    return []


def bounds_inverted(lower: Any, upper: Any) -> bool:
    """True if both bounds are numbers and ``lower > upper`` (unusable range)."""
    return isinstance(lower, (int, float)) and isinstance(upper, (int, float)) and lower > upper


def regex_error(pattern: Any) -> Optional[str]:
    """Return a reason if ``pattern`` is not a compilable regular expression, else None."""
    if not isinstance(pattern, str):
        return "must be a string"
    try:
        re.compile(pattern)
    except re.error:
        return "is not a valid regular expression"
    except RecursionError:  # pathological nesting
        return "is not a valid regular expression"
    return None
