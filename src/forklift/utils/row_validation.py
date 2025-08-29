"""Row validation utilities.

Currently provides a lightweight validator used by the Parquet output stub to
check basic field types against a simplified schema structure (mirrors the
subset of JSON Schema supported in tests).
"""
from __future__ import annotations
from typing import Dict, Any, Callable, List
from forklift.types import Row
from forklift.utils.date_parser import parse_date, coerce_datetime
from decimal import Decimal, InvalidOperation
import base64
import re
import polars as pl

__all__ = ["validate_row_against_schema", "validate_dataframe_against_schema"]

ISO_DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
ISO_DT_RE = r"^\d{4}-\d{2}-\d{2}T.+$"
INT_RE = r"^[+-]?\d+$"
NUMBER_RE = r"^[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?$"
HEX_RE = r"^(?:0x)?[0-9a-fA-F]+$"
B64_RE = r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"

# Precompiled patterns for binary validator (after definitions)
_HEX_RE = re.compile(HEX_RE)
_B64_RE = re.compile(B64_RE)
# ---- per-type validators -------------------------------------------------

def _validate_integer(name: str, value: Any) -> None:
    if value is None or value == "":
        return
    try:
        int(value)
    except Exception:
        raise ValueError(f"Field '{name}' expected integer, got '{value}'")

def _validate_number(name: str, value: Any) -> None:
    if value in (None, ""):
        return
    try:
        float(value)
    except Exception:
        raise ValueError(f"Field '{name}' expected number, got '{value}'")

def _validate_decimal(name: str, value: Any, field: Dict[str, Any]) -> None:
    if value in (None, ""):
        return
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Field '{name}' expected decimal, got '{value}'")
    scale = field.get("scale")
    if isinstance(scale, int):
        # Verify scale by quantizing string digits after '.'
        parts = str(d).split('.')
        actual_scale = len(parts[1]) if len(parts) == 2 else 0
        if actual_scale > scale:
            # Allow truncation/round behavior upstream; here just validate max scale
            raise ValueError(f"Field '{name}' expected decimal scale <= {scale}, got scale {actual_scale}")

def _validate_date(name: str, value: Any, fmt: str | None) -> None:
    if value is None or value == "":
        return
    # JSON Schema uses format: 'date' as a semantic indicator, not a custom literal pattern
    effective_fmt = None if fmt in (None, "date") else fmt
    if not parse_date(value, effective_fmt):
        raise ValueError(f"Field '{name}' expected date{f' {fmt}' if effective_fmt else ''}, got '{value}'")

def _validate_datetime(name: str, value: Any) -> None:
    if value in (None, ""):
        return
    try:
        coerce_datetime(str(value))
    except Exception:
        raise ValueError(f"Field '{name}' expected datetime, got '{value}'")

def _validate_boolean(name: str, value: Any, field: Dict[str, Any]) -> None:
    if value in (None, ""):
        return
    # Allow custom true/false lists if provided (extend with case variants)
    true_vals = field.get("true", ["Y", "1", "T", "True", True, "YES", "yes", "true"])
    false_vals = field.get("false", ["N", "0", "F", "False", False, "NO", "no", "false"])
    # Normalize to string (except keep booleans) and compare case-insensitively for strings
    def _norm_set(vals):
        out = set()
        for v in vals:
            if isinstance(v, bool):
                out.add(v)
            else:
                out.add(str(v))
                out.add(str(v).lower())
                out.add(str(v).upper())
        return out
    allowed = _norm_set(true_vals) | _norm_set(false_vals)
    if value not in allowed and str(value) not in allowed and str(value).lower() not in allowed:
        raise ValueError(f"Field '{name}' expected boolean, got '{value}'")

def _validate_binary(name: str, value: Any) -> None:
    if value in (None, ""):
        return
    if isinstance(value, (bytes, bytearray)):
        return
    token = str(value)
    # Hex
    if _HEX_RE.match(token):
        try:
            token_noprefix = token[2:] if token.lower().startswith('0x') else token
            bytes.fromhex(token_noprefix)
            return
        except Exception:
            pass
    # Base64
    try:
        base64.b64decode(token, validate=True)
        return
    except Exception:
        raise ValueError(f"Field '{name}' expected binary (hex/base64), got '{value}'")

# Mapping of schema type -> validator adapter
_TYPE_DISPATCH: Dict[str, Callable[[str, Any, Dict[str, Any]], None]] = {
    "integer": lambda n, v, f: _validate_integer(n, v),
    "number": lambda n, v, f: _validate_number(n, v),
    "decimal": lambda n, v, f: _validate_decimal(n, v, f),
    "date": lambda n, v, f: _validate_date(n, v, f.get("format")),
    "datetime": lambda n, v, f: _validate_datetime(n, v),
    "boolean": lambda n, v, f: _validate_boolean(n, v, f),
    "binary": lambda n, v, f: _validate_binary(n, v),
}

def _coalesce_fields(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "fields" in schema:
        return schema["fields"]
    props = schema.get("properties") or {}
    out = []
    for name, spec in props.items():
        # Map JSON Schema style to internal simple types
        ftype = spec.get("type")
        fmt = spec.get("format")
        internal_type = None
        if ftype == "integer":
            internal_type = "integer"
        elif ftype == "number":
            internal_type = "number"
        elif ftype == "string" and fmt == "date":
            internal_type = "date"
        elif ftype == "string" and fmt in ("date-time", "datetime", "timestamp"):
            internal_type = "datetime"
        elif ftype == "boolean":
            internal_type = "boolean"
        elif ftype == "decimal":
            internal_type = "decimal"
        elif ftype == "binary":
            internal_type = "binary"
        # Custom extensions (decimal declared as object) already covered.
        spec_out = {"name": name, "type": internal_type}
        if "scale" in spec:
            spec_out["scale"] = spec["scale"]
        if "format" in spec:
            spec_out["format"] = spec["format"]
        out.append(spec_out)
    return out

def validate_row_against_schema(row: Row, schema: Dict[str, Any] | None) -> None:
    if not schema:
        return
    field_specs = _coalesce_fields(schema)
    for field in field_specs:
        name = field["name"]
        field_type = field.get("type")
        if not field_type:
            continue
        value = row.get(name)
        validator = _TYPE_DISPATCH.get(field_type)
        if validator:
            validator(name, value, field)

def validate_dataframe_against_schema(df: pl.DataFrame, schema: Dict[str, Any] | None):
    """Vectorized validation of an entire DataFrame against a simplified schema.

    Returns
    -------
    (valid_mask, error_rows)
        valid_mask: pl.Series[bool] same row order as input df, True where all field validations passed.
        error_rows: list[(row_index, message)] capturing each field failure.
    """
    if not schema or df.height == 0:
        return pl.Series([True] * df.height), []

    field_specs = _coalesce_fields(schema)
    # Early short‑circuit: keep only fields that both have a type and exist in the DataFrame
    active_fields = [f for f in field_specs if f.get("type") and f["name"] in df.columns]
    if not active_fields:
        return pl.Series([True] * df.height), []

    # Add a row index once (avoid repeated with_row_count inside loop)
    df_idx = df.with_row_index("__row_idx__")

    validity_exprs = []  # list[(field_name, bool_expr)]
    for field in active_fields:
        name = field["name"]
        ftype = field["type"]
        col_utf8 = pl.col(name).cast(pl.Utf8).str.strip_chars()
        is_blank = col_utf8.is_null() | (col_utf8 == "")
        expr = pl.lit(True)  # default permissive
        if ftype == "integer":
            expr = is_blank | col_utf8.str.contains(INT_RE)
        elif ftype == "number":
            expr = is_blank | col_utf8.str.contains(NUMBER_RE)
        elif ftype == "decimal":
            numeric_ok = is_blank | col_utf8.str.contains(NUMBER_RE)
            scale = field.get("scale")
            if isinstance(scale, int):
                # Extract digits after decimal point; missing -> 0
                digits_after_decimal = col_utf8.str.extract(r"\.(\d+)").str.len_chars().fill_null(0)
                scale_ok = digits_after_decimal <= scale
                expr = numeric_ok & scale_ok
            else:
                expr = numeric_ok
        elif ftype == "date":
            # Lightweight ISO date pattern (full parsing done in row validator when used individually)
            expr = is_blank | col_utf8.str.contains(ISO_DATE_RE)
            # If a custom format specified, we still rely on downstream parse_date for single row checks; keep regex quick.
        elif ftype == "datetime":
            expr = is_blank | col_utf8.str.contains(ISO_DT_RE)
        elif ftype == "boolean":
            true_vals = field.get("true", ["Y", "1", "T", "True", True])
            false_vals = field.get("false", ["N", "0", "F", "False", False])
            # Normalize plus ensure lowercase variants are accepted for backwards compatibility
            base_allowed = set()
            for v in (true_vals + false_vals):
                base_allowed.add(str(v))
                if isinstance(v, str):
                    base_allowed.add(v.lower())
                    base_allowed.add(v.upper())
            allowed = sorted(base_allowed)
            expr = is_blank | col_utf8.is_in(allowed)
        elif ftype == "binary":
            # Accept hex or base64 encodings; quick regex screen only (deep validation done in row-wise path if needed)
            expr = is_blank | col_utf8.str.contains(HEX_RE) | col_utf8.str.contains(B64_RE)
        else:
            # Unknown types treated as pass-through (could choose to mark invalid instead)
            expr = pl.lit(True)
        validity_exprs.append((name, expr))

    # Materialize all validity columns in one pass (select keeps original order because no reordering operations used)
    valid_df = df_idx.select([
        pl.col("__row_idx__"),
        *[e.alias(f"__valid_{fname}") for fname, e in validity_exprs]
    ])

    # Compute overall row validity via horizontal AND across all validity columns
    validity_col_names = [f"__valid_{fname}" for fname, _ in validity_exprs]
    overall_expr = pl.lit(True)
    for cname in validity_col_names:
        overall_expr = overall_expr & pl.col(cname)
    overall_valid_expr = overall_expr.alias("__all_valid__")
    all_valid_series = valid_df.select(overall_valid_expr).get_column("__all_valid__")

    # Build error rows list without iterating per original row (iterate per failing field only)
    error_rows: List[tuple[int, str]] = []
    for fname, _expr in validity_exprs:
        col_name = f"__valid_{fname}"
        # Collect indices where the field failed
        failing_idx = valid_df.filter(~pl.col(col_name)).get_column("__row_idx__").to_list()
        if failing_idx:
            # Determine field type for message consistency
            ftype = next((f["type"] for f in active_fields if f["name"] == fname), None)
            msg = f"Field '{fname}' failed {ftype} validation" if ftype else f"Field '{fname}' failed validation"
            error_rows.extend((idx, msg) for idx in failing_idx)

    return all_valid_series, error_rows
