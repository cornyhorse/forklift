# src/forklift/preprocessors/type_coercion.py

from __future__ import annotations
from typing import Any, Dict, List
import re
from datetime import datetime
from .base import Preprocessor

_NUM_CURRENCY = re.compile(r"[,$€]")
_NUM_NEG_PARENS = re.compile(r"^\((.*)\)$")

_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0"}


def _coerce_bool(value: Any) -> bool:
    """Coerce a scalar into a boolean.

    Accepts a broad set of truthy / falsy tokens (case-insensitive).

    :param value: Value to coerce.
    :return: Boolean result.
    :raises ValueError: If token set is unrecognized.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    lowered_token = str(value).strip().lower()
    if lowered_token in _TRUE:
        return True
    if lowered_token in _FALSE:
        return False
    raise ValueError(f"bad boolean: {value!r}")


def _coerce_number(numeric_string: str) -> float:
    """Coerce a formatted numeric string into a float.

    Handles currency symbols (,$,€) and parenthetical negatives ``(1234)``.

    :param numeric_string: Raw numeric string.
    :return: Float value (negative when original in parens).
    :raises ValueError: On empty input or invalid numeric form.
    """
    numeric_string = numeric_string.strip()
    if numeric_string == "":
        raise ValueError("empty number")
    paren_match = _NUM_NEG_PARENS.match(numeric_string)
    is_negative = False
    if paren_match:
        numeric_string = paren_match.group(1)
        is_negative = True
    numeric_string = _NUM_CURRENCY.sub("", numeric_string)
    numeric_value = float(numeric_string)
    return -numeric_value if is_negative else numeric_value


def _coerce_date(date_string: str) -> str:
    """Normalize a date string to ISO (YYYY-MM-DD).

    Tries a small, ordered set of common formats. Raises if none match.

    :param date_string: Raw date string.
    :return: ISO date string.
    :raises ValueError: On empty or unparseable input.
    """
    date_string = date_string.strip()
    if date_string == "":
        raise ValueError("empty date")
    for date_format in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            parsed_date = datetime.strptime(date_string, date_format).date()
            return parsed_date.isoformat()
        except ValueError:
            continue
    raise ValueError(f"bad date: {date_string}")


def _normalize_type(spec: Any) -> str | None:
    """Normalize a user / schema type specification.

    Acceptable inputs: simple strings (``number``, ``integer``, ``float``,
    ``boolean``, ``date``, ``string``) or JSON-Schema-like dicts with ``type``
    and optional ``format`` for date.

    :param spec: Raw spec (str or dict) to normalize.
    :return: Canonical type string or ``None`` if unsupported.
    """
    if spec is None:
        return None
    if isinstance(spec, str):
        normalized_type = spec.lower()
        if normalized_type in {"integer", "float"}:
            return "number"
        if normalized_type in {"number", "boolean", "date", "string"}:
            return normalized_type
        return None
    if isinstance(spec, dict):
        normalized_type = str(spec.get("type", "")).lower()
        if normalized_type in {"integer", "float"}:
            return "number"
        if normalized_type == "number":
            return "number"
        if normalized_type == "boolean":
            return "boolean"
        if normalized_type == "string" and str(spec.get("format", "")).lower() == "date":
            return "date"
        if normalized_type == "string":
            return "string"
    return None


class TypeCoercion(Preprocessor):
    """Minimal type coercion preprocessor.

    Supports number, date, boolean, and string pass-through coercions; unknown
    types are left untouched. Null token replacement is handled per-column.
    """

    def __init__(self, types: Dict[str, Any] | None = None, nulls: Dict[str, List[str]] | None = None) -> None:
        """Build a coercion map and null token sets.

        :param types: Mapping of field name → type spec (string or dict form).
        :param nulls: Mapping of field name → list of tokens considered null.
        """
        self._specs: Dict[str, str] = {}
        for field_name, type_spec in (types or {}).items():
            normalized_type = _normalize_type(type_spec)
            if normalized_type:
                self._specs[field_name] = normalized_type
        self.nulls = {field_name: set(null_tokens) for field_name, null_tokens in (nulls or {}).items()}

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Coerce supported column values according to the spec map.

        :param row: Input row dictionary.
        :return: New row dict with coerced values (original untouched).
        :raises ValueError: If a value fails coercion for its declared type.
        """
        coerced_row: Dict[str, Any] = {}
        for field_name, value in row.items():
            trimmed_value = value.strip() if isinstance(value, str) else value
            if field_name in self.nulls and trimmed_value in self.nulls[field_name]:
                coerced_row[field_name] = None
                continue
            if trimmed_value in ("", None):
                coerced_row[field_name] = None
                continue
            declared_type = self._specs.get(field_name)
            if declared_type == "number":
                coerced_row[field_name] = _coerce_number(trimmed_value) if isinstance(trimmed_value, str) else float(trimmed_value)
            elif declared_type == "date":
                if isinstance(trimmed_value, str):
                    coerced_row[field_name] = _coerce_date(trimmed_value)
                else:
                    raise ValueError("non-string date")
            elif declared_type == "boolean":
                coerced_row[field_name] = _coerce_bool(trimmed_value)
            elif declared_type == "string":
                coerced_row[field_name] = str(trimmed_value)
            else:
                coerced_row[field_name] = value
        return coerced_row

    def process(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Helper used by tests: wraps :meth:`apply` capturing errors.

        :param row: Row to coerce.
        :return: Dict with keys ``row`` (possibly coerced) and ``error`` (exception or ``None``).
        """
        try:
            coerced_row = self.apply(row)
            return {"row": coerced_row, "error": None}
        except Exception as exc:  # pragma: no cover - thin wrapper
            return {"row": row, "error": exc}
