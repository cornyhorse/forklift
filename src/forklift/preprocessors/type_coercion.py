from __future__ import annotations
from typing import Any, Dict, List, Optional
import re
from datetime import datetime
from .base import Preprocessor

_NUM_CURRENCY = re.compile(r"[,$€]")
_NUM_NEG_PARENS = re.compile(r"^\((.*)\)$")


def _coerce_number(s: str) -> float:
    s = s.strip()
    if s == "":
        raise ValueError("empty number")
    # handle (123.45) → -123.45
    m = _NUM_NEG_PARENS.match(s)
    neg = False
    if m:
        s = m.group(1)
        neg = True
    s = _NUM_CURRENCY.sub("", s)  # remove commas/currency symbols
    val = float(s)
    return -val if neg else val


def _coerce_date(s: str) -> str:
    s = s.strip()
    if s == "":
        raise ValueError("empty date")
    # accept ISO or 2024/01/03
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt).date()
            return dt.isoformat()
        except ValueError:
            continue
    raise ValueError(f"bad date: {s}")


class TypeCoercion(Preprocessor):
    """
    Minimal coercion so tests can assert on kept/rejected counts without full schema engine.
    Only handles: number + date columns. Everything else passes through.
    """

    def __init__(self, types: Dict[str, str] | None = None, nulls: Dict[str, List[str]] | None = None) -> None:
        self.types = types or {}  # e.g. {"amount": "number", "order_date": "date"}
        self.nulls = {k: set(v) for k, v in (nulls or {}).items()}

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, str):
                raw = v.strip()
            else:
                raw = v

            # null handling
            if k in self.nulls and raw in self.nulls[k]:
                out[k] = None
                continue
            if raw in ("", None):
                out[k] = None
                continue

            t = self.types.get(k)
            if t in ("number", "integer", "float"):
                if not isinstance(raw, str):
                    out[k] = float(raw)
                else:
                    out[k] = _coerce_number(raw)
            elif t == "date":
                if isinstance(raw, str):
                    out[k] = _coerce_date(raw)
                else:
                    raise ValueError("non-string date")
            else:
                out[k] = v
        return out
