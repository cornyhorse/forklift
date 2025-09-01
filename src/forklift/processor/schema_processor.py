from __future__ import annotations
from typing import Dict, Any, List, Tuple
import json
from pathlib import Path
import polars as pl

_TYPE_MAP = {
    "integer": pl.Int64,
    "number": pl.Float64,
    "boolean": pl.Boolean,
    "string": pl.Utf8,
    # Future: date, date-time specialized parsing
}

# Accepted boolean textual values
_TRUE_SET = {"true", "t", "1", "yes", "y"}
_FALSE_SET = {"false", "f", "0", "no", "n"}


def _coerce_boolean_value(v):  # pragma: no cover - simple pure helper exercised indirectly
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    try:
        s = str(v).strip().lower()
    except Exception:
        return None
    if s == "":
        return None
    if s in _TRUE_SET:
        return True
    if s in _FALSE_SET:
        return False
    return None  # unrecognized -> failure marker


def apply_schema(df: pl.DataFrame, schema: Dict[str, Any], badrows_path: str) -> pl.DataFrame:
    """Apply JSON Schema primitive type coercions to a Polars DataFrame.

    Rows whose values cannot be coerced to the declared JSON Schema primitive
    types are written (appended) to ``badrows_path`` as JSON lines. Each JSON
    object contains:
      * row_index : original integer row number in the incoming frame
      * errors    : mapping of column -> original value (stringified) that failed
      * original  : mapping of original column values for that row
      * row       : mapping of (partially) coerced values after casting

    Successful rows are returned in a new DataFrame with coerced dtypes.
    """
    properties = (schema or {}).get("properties", {})
    if not isinstance(properties, dict) or df.height == 0:
        return df  # Nothing to do

    target_columns: List[Tuple[str, Any]] = []  # (name, pl.DataType)
    for col, spec in properties.items():
        if col not in df.columns:
            continue
        if not isinstance(spec, dict):
            continue
        js_type = spec.get("type")
        pl_type = _TYPE_MAP.get(js_type)
        if pl_type is None:
            continue
        target_columns.append((col, pl_type))

    if not target_columns:
        return df

    # Preserve originals for error detection
    orig_prefix = "__orig__"
    work_df = df.clone()
    for col, _ in target_columns:
        work_df = work_df.with_columns(pl.col(col).alias(f"{orig_prefix}{col}"))

    # Perform casts (non-strict so failures -> null). Custom handling for boolean
    for col, pl_type in target_columns:
        if pl_type == pl.Boolean:
            # Map textual representations to booleans; unknown -> None
            work_df = work_df.with_columns(
                pl.col(col)
                .map_elements(_coerce_boolean_value, return_dtype=pl.Boolean)
                .alias(col)
            )
            continue
        try:
            work_df = work_df.with_columns(pl.col(col).cast(pl_type, strict=False))
        except Exception:  # pragma: no cover - defensive fallback
            pass

    # Detect per-row errors
    error_rows: List[dict] = []
    keep_mask = pl.Series([True] * work_df.height)
    col_type_lookup = dict(target_columns)
    for idx in range(work_df.height):
        row_errors: Dict[str, Any] = {}
        for col, _pltype in target_columns:
            orig_val = work_df[f"{orig_prefix}{col}"][idx]
            cast_val = work_df[col][idx]
            if orig_val is None or (isinstance(orig_val, str) and orig_val == ""):
                continue  # ignore empty inputs
            if cast_val is None:
                row_errors[col] = orig_val
        if row_errors:
            keep_mask[idx] = False
            original_map = {
                c: work_df[f"{orig_prefix}{c}"][idx] if c in col_type_lookup else work_df[c][idx]
                for c in df.columns
            }
            cast_map = {c: work_df[c][idx] for c in df.columns}
            error_rows.append({
                "row_index": idx,
                "errors": row_errors,
                "original": original_map,
                "row": cast_map,
            })

    if error_rows:
        path = Path(badrows_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fp:
            for er in error_rows:
                fp.write(json.dumps(er, ensure_ascii=False) + "\n")

    drop_cols = [f"{orig_prefix}{col}" for col, _ in target_columns]
    cleaned = work_df.filter(keep_mask).drop(drop_cols)
    return cleaned

__all__ = ["apply_schema"]
