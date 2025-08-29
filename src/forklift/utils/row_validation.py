"""Row validation utilities.

Currently provides a lightweight validator used by the Parquet output stub to
check basic field types against a simplified schema structure (mirrors the
subset of JSON Schema supported in tests).
"""
from __future__ import annotations
from typing import Dict, Any, Callable
from forklift.types import Row
from forklift.utils.date_parser import parse_date

__all__ = ["validate_row_against_schema"]

# ---- per-type validators -------------------------------------------------

def _validate_integer(name: str, value: Any) -> None:
    """Validate an integer field (convertible by ``int`` when non-blank)."""
    if value is None or value == "":
        return
    try:
        int(value)
    except Exception:  # pragma: no cover - narrow failure path
        raise ValueError(f"Field '{name}' expected integer, got '{value}'")

def _validate_date(name: str, value: Any, fmt: str | None) -> None:
    """Validate a date field using :func:`parse_date` respecting optional format."""
    if value is None or value == "":
        return
    if not parse_date(value, fmt):
        raise ValueError(f"Field '{name}' expected date{f' {fmt}' if fmt else ''}, got '{value}'")

def _validate_boolean(name: str, value: Any, field: Dict[str, Any]) -> None:
    """Validate a boolean token against allowed true/false value lists.

    Empty values are considered nullable and skipped.
    """
    if value in (None, ""):
        return
    true_vals = field.get("true", ["Y", "1", "T", "True"])
    false_vals = field.get("false", ["N", "0", "F", "False"])
    if value not in true_vals and value not in false_vals:
        raise ValueError(f"Field '{name}' expected boolean, got '{value}'")

# Mapping of schema type -> validator function signature adapter.
_TYPE_DISPATCH: Dict[str, Callable[[str, Any, Dict[str, Any]], None]] = {
    "integer": lambda n, v, f: _validate_integer(n, v),
    "date": lambda n, v, f: _validate_date(n, v, f.get("format")),
    "boolean": lambda n, v, f: _validate_boolean(n, v, f),
}

def validate_row_against_schema(row: Row, schema: Dict[str, Any] | None) -> None:
    """Validate a single row against a schema (subset of types).

    Supported field specs inside ``schema['fields']``:

    * ``integer`` – value must be convertible via ``int`` when not blank
    * ``date`` – value must parse with :func:`forklift.utils.date_parser.parse_date`
      honoring optional ``format``
    * ``boolean`` – membership in configured ``true`` / ``false`` lists (defaults
      provided if absent)

    Blank (``None`` or "") values are skipped (treated as nullable) – consistent
    with existing PQOutput stub semantics.

    :param row: Row dictionary.
    :param schema: Schema dict containing optional ``fields`` list; if absent or
        missing fields no validation occurs.
    :raises ValueError: On first detected mismatch.
    """
    if not schema or "fields" not in schema:
        return
    for field in schema["fields"]:
        name = field["name"]
        field_type = field.get("type")
        value = row.get(name)
        validator = _TYPE_DISPATCH.get(field_type)
        if validator:
            validator(name, value, field)
