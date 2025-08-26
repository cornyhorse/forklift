from .base import Preprocessor
from decimal import Decimal
from datetime import datetime


class TypeCoercion(Preprocessor):
    def __init__(self, types: dict[str, str], nulls: dict[str, list[str]] | None = None):
        self.types = types
        self.nulls = nulls or {}

    def apply(self, row):
        out = {}
        for k, v in row.items():
            tok = v.strip() if isinstance(v, str) else v
            if tok in (self.nulls.get(k, []) or []):
                out[k] = None
                continue
            t = self.types.get(k, "string")
            if t.startswith("decimal"):
                out[k] = Decimal(tok) if tok not in (None, "") else None
            elif t in ("integer", "int"):
                out[k] = int(tok) if tok not in (None, "") else None
            elif t in ("number", "float"):
                out[k] = float(tok) if tok not in (None, "") else None
            elif t == "date":
                out[k] = datetime.fromisoformat(tok).date() if tok else None
            else:
                out[k] = tok
        return out
