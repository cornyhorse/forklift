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
    """
    Accept either:
      - "number" | "integer" | "float" | "boolean" | "date" | "string"
      - {"type":"number"} / {"type":"string","format":"date"} etc.
    Return canonical: "number" | "boolean" | "date" | "string"
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
    """
    Minimal coercion so tests can assert on kept/rejected counts.
    Handles number, date, boolean, and string (pass-through).
    Accepts simple type strings or JSON-Schema-like dict specs.
    """

    def __init__(self, types: Dict[str, Any] | None = None, nulls: Dict[str, List[str]] | None = None) -> None:
        # Normalize specs up-front
        self._specs: Dict[str, str] = {}
        for k, spec in (types or {}).items():
            t = _normalize_type(spec)
            if t:
                self._specs[k] = t
        self.nulls = {k: set(v) for k, v in (nulls or {}).items()}

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            raw = v.strip() if isinstance(v, str) else v

            # null handling
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
                # unknown/unspecified type: pass through original
                out[k] = v
        return out

    # Tests call process() and index into the dict it returns.
    def process(self, row: Dict[str, Any]) -> Dict[str, Any]:
        try:
            coerced = self.apply(row)
            return {"row": coerced, "error": None}
        except Exception as e:
            return {"row": row, "error": e}