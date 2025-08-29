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


def _coerce_bool(v: Any) -> bool:
    """Coerce a scalar into a boolean.

    Accepts a broad set of truthy / falsy tokens (case-insensitive).

    :param v: Value to coerce.
    :return: Boolean result.
    :raises ValueError: If token set is unrecognized.
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    raise ValueError(f"bad boolean: {v!r}")


def _coerce_number(s: str) -> float:
    """Coerce a formatted numeric string into a float.

    Handles currency symbols (,$,€) and parenthetical negatives ``(1234)``.

    :param s: Raw numeric string.
    :return: Float value (negative when original in parens).
    :raises ValueError: On empty input or invalid numeric form.
    """
    s = s.strip()
    if s == "":
        raise ValueError("empty number")
    m = _NUM_NEG_PARENS.match(s)
    neg = False
    if m:
        s = m.group(1)
        neg = True
    s = _NUM_CURRENCY.sub("", s)
    val = float(s)
    return -val if neg else val


def _coerce_date(s: str) -> str:
    """Normalize a date string to ISO (YYYY-MM-DD).

    Tries a small, ordered set of common formats. Raises if none match.

    :param s: Raw date string.
    :return: ISO date string.
    :raises ValueError: On empty or unparseable input.
    """
    s = s.strip()
    if s == "":
        raise ValueError("empty date")
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt).date()
            return dt.isoformat()
        except ValueError:
            continue
    raise ValueError(f"bad date: {s}")


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
        t = spec.lower()
        if t in {"integer", "float"}:
            return "number"
        if t in {"number", "boolean", "date", "string"}:
            return t
        return None
    if isinstance(spec, dict):
        t = str(spec.get("type", "")).lower()
        if t in {"integer", "float"}:
            return "number"
        if t == "number":
            return "number"
        if t == "boolean":
            return "boolean"
        if t == "string" and str(spec.get("format", "")).lower() == "date":
            return "date"
        if t == "string":
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
        for k, spec in (types or {}).items():
            t = _normalize_type(spec)
            if t:
                self._specs[k] = t
        self.nulls = {k: set(v) for k, v in (nulls or {}).items()}

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Coerce supported column values according to the spec map.

        :param row: Input row dictionary.
        :return: New row dict with coerced values (original untouched).
        :raises ValueError: If a value fails coercion for its declared type.
        """
        out: Dict[str, Any] = {}
        for k, v in row.items():
            raw = v.strip() if isinstance(v, str) else v
            if k in self.nulls and raw in self.nulls[k]:
                out[k] = None
                continue
            if raw in ("", None):
                out[k] = None
                continue
            t = self._specs.get(k)
            if t == "number":
                out[k] = _coerce_number(raw) if isinstance(raw, str) else float(raw)
            elif t == "date":
                if isinstance(raw, str):
                    out[k] = _coerce_date(raw)
                else:
                    raise ValueError("non-string date")
            elif t == "boolean":
                out[k] = _coerce_bool(raw)
            elif t == "string":
                out[k] = str(raw)
            else:
                out[k] = v
        return out

    def process(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Helper used by tests: wraps :meth:`apply` capturing errors.

        :param row: Row to coerce.
        :return: Dict with keys ``row`` (possibly coerced) and ``error`` (exception or ``None``).
        """
        try:
            coerced = self.apply(row)
            return {"row": coerced, "error": None}
        except Exception as e:  # pragma: no cover - thin wrapper
            return {"row": row, "error": e}
