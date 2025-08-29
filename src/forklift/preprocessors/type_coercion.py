# src/forklift/preprocessors/type_coercion.py

from __future__ import annotations
from typing import Any, Dict, List, Set, Tuple
import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import base64
from forklift.utils.date_parser import coerce_date, coerce_datetime

from .base import Preprocessor

_NUM_CURRENCY = re.compile(r"[,$€]")
_NUM_NEG_PARENS = re.compile(r"^\((.*)\)$")
_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]+$")

_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0"}

# ---------------------------------------------------------------------------
# Individual coercion helpers
# ---------------------------------------------------------------------------

def _coerce_bool(value: Any) -> bool:
    """Coerce a scalar into a boolean.

    Accepts a broad set of truthy / falsy tokens (case-insensitive).
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


def _strip_numeric_artifacts(token: str) -> Tuple[str, bool]:
    token = token.strip()
    if token == "":
        raise ValueError("empty number")
    paren_match = _NUM_NEG_PARENS.match(token)
    is_negative = False
    if paren_match:
        token = paren_match.group(1)
        is_negative = True
    token = _NUM_CURRENCY.sub("", token)
    return token, is_negative


def _coerce_number(numeric_string: str) -> float:
    numeric_string, is_negative = _strip_numeric_artifacts(numeric_string)
    numeric_value = float(numeric_string)
    return -numeric_value if is_negative else numeric_value


def _coerce_integer(numeric_string: str) -> int:
    numeric_string, is_negative = _strip_numeric_artifacts(numeric_string)
    # Remove common thousands separators silently
    numeric_string = numeric_string.replace(",", "")
    int_value = int(float(numeric_string))  # tolerate trailing .0
    return -int_value if is_negative else int_value


def _coerce_decimal(numeric_string: str, scale: int | None = None) -> Decimal:
    numeric_string, is_negative = _strip_numeric_artifacts(numeric_string)
    normalized = numeric_string.replace(",", "")
    try:
        dec_value = Decimal(normalized)
    except InvalidOperation:
        raise ValueError(f"bad decimal: {numeric_string}")
    if is_negative:
        dec_value = -dec_value
    if scale is not None:
        quant = Decimal(1).scaleb(-scale)  # 10^-scale
        dec_value = dec_value.quantize(quant, rounding=ROUND_HALF_UP)
    return dec_value


def _coerce_binary(raw: str) -> bytes:
    token = raw.strip()
    if token == "":
        raise ValueError("empty binary")
    if _HEX_RE.match(token):
        token_noprefix = token[2:] if token.lower().startswith("0x") else token
        return bytes.fromhex(token_noprefix)
    try:
        return base64.b64decode(token, validate=True)
    except Exception:
        raise ValueError(f"bad binary: {raw}")

# ---------------------------------------------------------------------------
# Type normalization
# ---------------------------------------------------------------------------
_SUPPORTED_SIMPLE_TYPES: Set[str] = {
    "integer", "number", "float", "double", "boolean", "date", "datetime", "timestamp",
    "string", "decimal", "binary"
}


def _normalize_type(spec: Any) -> Tuple[str | None, Dict[str, Any]]:
    """Normalize a user / schema type specification.

    Returns canonical type name plus supplemental metadata (e.g. precision/scale).
    """
    meta: Dict[str, Any] = {}
    if spec is None:
        return None, meta
    # Simple string spec
    if isinstance(spec, str):
        lowered = spec.lower()
        if lowered in {"float", "double"}:
            return "number", meta
        if lowered in {"timestamp"}:
            return "datetime", meta
        if lowered in _SUPPORTED_SIMPLE_TYPES:
            return lowered, meta
        return None, meta
    # Dict / JSON-Schema-ish
    if isinstance(spec, dict):
        t = str(spec.get("type", "")).lower()
        fmt = str(spec.get("format", "")).lower()
        if t in {"float", "double"}:
            t = "number"
        if t == "string" and fmt == "date":
            return "date", meta
        if t == "string" and fmt in {"datetime", "date-time", "timestamp"}:
            return "datetime", meta
        if t == "integer":
            return "integer", meta
        if t == "number":
            return "number", meta
        if t == "boolean":
            return "boolean", meta
        if t == "decimal":
            scale = spec.get("scale")
            if isinstance(scale, int):
                meta["scale"] = scale
            precision = spec.get("precision")
            if isinstance(precision, int):
                meta["precision"] = precision  # currently informational
            return "decimal", meta
        if t == "binary":
            return "binary", meta
        if t == "string":
            return "string", meta
    return None, meta

# ---------------------------------------------------------------------------
# Preprocessor implementation
# ---------------------------------------------------------------------------

import polars as pl

class TypeCoercion(Preprocessor):
    """Enhanced type coercion preprocessor for Parquet-compatible primitives.

    Supported canonical type names (case-insensitive in schema):
      * integer → Python ``int``
      * number / float / double → Python ``float``
      * decimal → ``Decimal`` (optional ``scale`` quantization)
      * boolean → Python ``bool`` (broad token set)
      * date (string/format=date) → ISO ``YYYY-MM-DD`` string
      * datetime / timestamp (string/format=date-time) → ``datetime`` object
      * string → Python ``str``
      * binary → ``bytes`` (hex: ``0xABCD`` or base64)

    Unknown / unsupported types are left untouched.
    Null token replacement is handled per-column.
    """

    def __init__(self, types: Dict[str, Any] | None = None, nulls: Dict[str, List[str]] | None = None) -> None:
        self._specs: Dict[str, str] = {}
        self._meta: Dict[str, Dict[str, Any]] = {}
        for field_name, type_spec in (types or {}).items():
            normalized_type, meta = _normalize_type(type_spec)
            if normalized_type:
                self._specs[field_name] = normalized_type
                if meta:
                    self._meta[field_name] = meta
        self.nulls = {field_name: set(null_tokens) for field_name, null_tokens in (nulls or {}).items()}

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        raise NotImplementedError("TypeCoercion is DataFrame-only; use process_dataframe().")

    def process(self, row: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        raise NotImplementedError("TypeCoercion is DataFrame-only; use process_dataframe().")

    def process_dataframe(self, df: "pl.DataFrame") -> "pl.DataFrame":  # type: ignore[name-defined]
        """Process a Polars DataFrame row-wise (DataFrame-only API).

        Rows that fail coercion for any declared field are omitted from the
        returned DataFrame and recorded in ``self._df_errors`` as
        ``(original_row_dict, Exception)`` tuples.
        """
        if not self._specs or df.height == 0:
            self._df_errors = []  # type: ignore[attr-defined]
            return df
        kept_rows: List[Dict[str, Any]] = []
        self._df_errors: List[tuple[dict, Exception]] = []  # type: ignore[attr-defined]
        for row in df.to_dicts():
            try:
                out_row: Dict[str, Any] = {}
                for field_name, value in row.items():
                    declared_type = self._specs.get(field_name)
                    # Null / blank handling & null tokens
                    val = value
                    if isinstance(val, str):
                        val_stripped = val.strip()
                    else:
                        val_stripped = val
                    if field_name in self.nulls and val_stripped in self.nulls[field_name]:
                        out_row[field_name] = None
                        continue
                    if val_stripped in (None, ""):
                        out_row[field_name] = None
                        continue
                    if not declared_type:
                        # Pass through untouched
                        out_row[field_name] = value
                        continue
                    # Coercion per type
                    if declared_type == "number":
                        if isinstance(val, str):
                            out_row[field_name] = _coerce_number(val)
                        else:
                            out_row[field_name] = float(val)
                    elif declared_type == "integer":
                        if isinstance(val, str):
                            out_row[field_name] = _coerce_integer(val)
                        else:
                            out_row[field_name] = int(val)
                    elif declared_type == "date":
                        if isinstance(val, str):
                            out_row[field_name] = coerce_date(val)
                        else:
                            raise ValueError("non-string date")
                    elif declared_type == "datetime":
                        if isinstance(val, str):
                            out_row[field_name] = coerce_datetime(val)
                        elif isinstance(val, datetime):
                            out_row[field_name] = val
                        else:
                            raise ValueError("unsupported datetime value")
                    elif declared_type == "boolean":
                        out_row[field_name] = _coerce_bool(val)
                    elif declared_type == "string":
                        out_row[field_name] = str(val)
                    elif declared_type == "decimal":
                        if isinstance(val, (int, float, Decimal)):
                            dec_val = Decimal(str(val))
                        elif isinstance(val, str):
                            dec_val = _coerce_decimal(val, self._meta.get(field_name, {}).get("scale"))
                        else:
                            raise ValueError("unsupported decimal value")
                        scale_meta = self._meta.get(field_name, {}).get("scale")
                        if scale_meta is not None and isinstance(val, (int, float, Decimal)):
                            quant = Decimal(1).scaleb(-scale_meta)
                            dec_val = dec_val.quantize(quant, rounding=ROUND_HALF_UP)
                        out_row[field_name] = dec_val
                    elif declared_type == "binary":
                        if isinstance(val, bytes):
                            out_row[field_name] = val
                        elif isinstance(val, str):
                            out_row[field_name] = _coerce_binary(val)
                        else:
                            raise ValueError("unsupported binary value")
                    else:
                        out_row[field_name] = value
                kept_rows.append(out_row)
            except Exception as exc:
                # retain original row for diagnostics
                self._df_errors.append((row, exc))
        return pl.DataFrame(kept_rows) if kept_rows else pl.DataFrame([])
