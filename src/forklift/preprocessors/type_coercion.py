from __future__ import annotations
from typing import Any, Dict, List, Set, Tuple
import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import base64
from forklift.utils.date_parser import coerce_date, coerce_datetime

# --- Schema token → strptime normalization (local) -------------------------
# Supports tokens like YYYY, MM, DD, MMM, MMMM, HH, mm, ss, SSS/ffffff, Z/XXX
_TOKEN_SUBS = [
    (re.compile(r"MMMM", re.IGNORECASE), "%B"),
    (re.compile(r"MMM", re.IGNORECASE), "%b"),
    (re.compile(r"YYYY", re.IGNORECASE), "%Y"),
    (re.compile(r"HH", re.IGNORECASE), "%H"),
    (re.compile(r"mm"), "%M"),         # minutes (lowercase 'mm')
    (re.compile(r"SSSSSS", re.IGNORECASE), "%.f"),  # microseconds (6)
    (re.compile(r"ffffff", re.IGNORECASE), "%.f"),
    (re.compile(r"SSS", re.IGNORECASE), "%.f"),     # milliseconds → fractional seconds
    (re.compile(r"ss", re.IGNORECASE), "%S"),
    (re.compile(r"MM"), "%m"),         # month (uppercase 'MM') after minutes
    (re.compile(r"DD", re.IGNORECASE), "%d"),
    (re.compile(r"XXX|ZZZ|Z", re.IGNORECASE), "%z"),
]

def _normalize_schema_format(fmt: str) -> str:
    out = fmt
    for pat, repl in _TOKEN_SUBS:
        out = pat.sub(repl, out)
    return out

# Extract possible user-provided date/datetime formats from a schema-ish dict
# Accept a variety of keys (strings or lists) for flexibility.
_FORMAT_KEYS = ("x-format", "x_formats", "x-formats", "pattern", "patterns", "date_format", "date_formats", "formats")

def _extract_user_formats(spec: Dict[str, Any]) -> List[str]:
    fmts: List[str] = []
    for k in _FORMAT_KEYS:
        if k in spec:
            v = spec[k]
            if isinstance(v, str) and v.strip():
                fmts.append(v.strip())
            elif isinstance(v, (list, tuple)):
                fmts.extend([str(x).strip() for x in v if str(x).strip()])
    return fmts

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


def _coerce_binary(raw: str | bytes | bytearray) -> bytes:
    # Accept already-bytes
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    token = str(raw).strip()
    if token == "":
        raise ValueError("empty binary")
    if _HEX_RE.match(token):
        token_noprefix = token[2:] if token.lower().startswith("0x") else token
        return bytes.fromhex(token_noprefix)
    try:
        return base64.b64decode(token, validate=True)
    except Exception:
        raise ValueError(f"bad binary: {raw}")


def _coerce_binary_opt(raw: Any) -> bytes | None:
    try:
        return _coerce_binary(raw)
    except Exception:
        return None

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
            meta["user_formats"] = _extract_user_formats(spec)
            return "date", meta
        if t == "string" and fmt in {"datetime", "date-time", "timestamp"}:
            meta["user_formats"] = _extract_user_formats(spec)
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

    def __init__(self, types: Dict[str, Any] | None = None, nulls: Dict[str, List[str]] | None = None, booleans: Dict[str, set[str]] | None = None) -> None:
        self._specs: Dict[str, str] = {}
        self._meta: Dict[str, Dict[str, Any]] = {}
        # Dynamic boolean token sets (lowercased)
        self._true_tokens = set(_TRUE)
        self._false_tokens = set(_FALSE)
        if booleans:
            self._true_tokens |= {str(v).lower() for v in booleans.get("true", set())}
            self._false_tokens |= {str(v).lower() for v in booleans.get("false", set())}
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
        """Process a Polars DataFrame using vectorized expressions without adding temp error columns.

        - Coerce declared fields with vectorized casts / parses.
        - Build invalid masks per field (in-memory), **without** materializing them as columns.
        - Return only good rows; rejected rows are recorded in `self._df_errors` as (row_dict, ValueError).
        - Undeclared fields pass through unchanged.
        """
        if not self._specs or df.height == 0:
            self._df_errors = []  # type: ignore[attr-defined]
            return df

        cast_exprs: list[pl.Expr] = []
        invalid_exprs: list[tuple[str, pl.Expr]] = []  # (field_name, invalid_mask_expr)

        # Helpers -----------------------------------------------------------
        def _nullify(field: str) -> pl.Expr:
            col = pl.col(field)
            null_tokens = self.nulls.get(field)
            cond = col.is_null() | col.cast(pl.Utf8).str.strip_chars().eq("")
            if null_tokens:
                cond = cond | col.cast(pl.Utf8).is_in(list(null_tokens))
            return pl.when(cond).then(pl.lit(None)).otherwise(col)

        def _norm_numeric_tokens(e: pl.Expr) -> pl.Expr:
            # Strip currency and commas; convert parentheses to leading minus
            e = e.cast(pl.Utf8).str.strip_chars()
            e = e.str.replace_all(r"^\((.*)\)$", r"-\\1")
            e = e.str.replace_all(r"[,$€]", "")
            return e

        # Build per-field cast expressions and invalid masks ----------------
        for field in self._specs.keys():
            if field not in df.columns:
                continue

            declared = self._specs[field]
            src = _nullify(field)
            base_nonblank = src.is_not_null()

            if declared == "integer":
                norm = _norm_numeric_tokens(src)
                casted = norm.cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias(field)
                invalid = base_nonblank & casted.is_null()
                cast_exprs.append(casted)
                invalid_exprs.append((field, invalid))

            elif declared == "number":
                norm = _norm_numeric_tokens(src)
                casted = norm.cast(pl.Float64, strict=False).alias(field)
                invalid = base_nonblank & casted.is_null()
                cast_exprs.append(casted)
                invalid_exprs.append((field, invalid))

            elif declared == "decimal":
                meta = self._meta.get(field, {})
                scale = int(meta.get("scale", 0)) if meta.get("scale") is not None else None
                norm = _norm_numeric_tokens(src)
                dtype = pl.Decimal(38, scale if scale is not None else 9)
                casted = norm.cast(dtype, strict=False).alias(field)
                invalid = base_nonblank & casted.is_null()
                cast_exprs.append(casted)
                invalid_exprs.append((field, invalid))

            elif declared == "boolean":
                lowered = src.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
                true_vals = list(self._true_tokens)
                false_vals = list(self._false_tokens)
                mapped = (
                    pl.when(lowered.is_in(true_vals)).then(pl.lit(True))
                    .when(lowered.is_in(false_vals)).then(pl.lit(False))
                    .otherwise(None)
                ).alias(field)
                invalid = base_nonblank & (~lowered.is_in(true_vals + false_vals))
                cast_exprs.append(mapped)
                invalid_exprs.append((field, invalid))

            elif declared == "date":
                meta = self._meta.get(field, {})
                user_fmts = [
                    _normalize_schema_format(s) if "%" not in s else s
                    for s in meta.get("user_formats", [])
                ]
                candidates = user_fmts or [
                    "%Y-%m-%d",
                    "%Y/%m/%d",
                    "%d/%m/%Y",
                    "%Y.%m.%d",
                    "%Y%m%d",
                    "%d-%b-%Y",
                    "%b %d, %Y",
                ]
                base_parsed = pl.coalesce([
                    src.cast(pl.Utf8).str.strptime(pl.Date, format=f, strict=False)
                    for f in candidates
                ])
                # Final value: use Python fallback only where base is null & source nonblank
                parsed = (
                    pl.when(base_nonblank & base_parsed.is_null())
                    .then(
                        src.cast(pl.Utf8)
                           .map_elements(lambda s: coerce_date(s) if s is not None and str(s).strip() != "" else None,
                                         return_dtype=pl.Utf8, strict=False)
                           .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                    )
                    .otherwise(base_parsed)
                    .alias(field)
                )
                invalid = base_nonblank & parsed.is_null()
                cast_exprs.append(parsed)
                invalid_exprs.append((field, invalid))

            elif declared == "datetime" or declared == "timestamp":
                meta = self._meta.get(field, {})
                user_fmts = [
                    _normalize_schema_format(s) if "%" not in s else s
                    for s in meta.get("user_formats", [])
                ]
                candidates = user_fmts or [
                    "%Y-%m-%dT%H:%M:%S%.f%z",
                    "%Y-%m-%d %H:%M:%S%.f",
                    "%Y/%m/%d %H:%M:%S",
                ]
                base_parsed_dt = pl.coalesce([
                    src.cast(pl.Utf8).str.strptime(pl.Datetime, format=f, strict=False)
                    for f in candidates
                ])
                parsed_dt = (
                    pl.when(base_nonblank & base_parsed_dt.is_null())
                    .then(
                        src.cast(pl.Utf8)
                           .map_elements(
                               lambda s: coerce_datetime(s) if s is not None and str(s).strip() != "" else None,
                               return_dtype=pl.Datetime,
                               strict=False,
                           )
                    )
                    .otherwise(base_parsed_dt)
                    .alias(field)
                )
                invalid = base_nonblank & parsed_dt.is_null()
                cast_exprs.append(parsed_dt)
                invalid_exprs.append((field, invalid))

            elif declared == "string":
                cast_exprs.append(src.cast(pl.Utf8).alias(field))
                # strings are never invalid by casting

            elif declared == "binary":
                # Note: Python fallback; acceptable since binary columns are rare.
                casted = pl.col(field).map_elements(_coerce_binary_opt, return_dtype=pl.Binary).alias(field)
                invalid = base_nonblank & casted.is_null()
                cast_exprs.append(casted)
                invalid_exprs.append((field, invalid))

            else:
                cast_exprs.append(src.alias(field))

        # Apply casts once to get the typed DataFrame
        typed = df.with_columns(cast_exprs)

        if not invalid_exprs:
            self._df_errors = []  # type: ignore[attr-defined]
            return typed

        # Compute a combined bad-mask without materializing helper columns
        bad_mask_expr = pl.any_horizontal([expr for _, expr in invalid_exprs])
        good = typed.filter(~bad_mask_expr)
        bad = typed.filter(bad_mask_expr)

        # Build legacy error list from boolean selections (no extra columns kept)
        error_rows: list[tuple[dict, Exception]] = []
        if bad.height:
            # Evaluate per-field booleans for the bad slice only
            bad_err_df = bad.select([expr.alias(name) for name, expr in invalid_exprs])
            flags = bad_err_df.to_numpy()
            # Original (pre-cast) columns for context
            bad_orig = bad.select(df.columns).to_dicts()
            for row_dict, flag_row in zip(bad_orig, flags):
                failing = [name for (name, _), flag in zip(invalid_exprs, flag_row) if bool(flag)]
                error_rows.append((row_dict, ValueError(f"type coercion failed: {', '.join(failing)}")))

        self._df_errors = error_rows  # type: ignore[attr-defined]
        return good
