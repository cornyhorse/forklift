from __future__ import annotations
from typing import Any, Dict, Optional, List, Tuple
import io
import polars as pl

__all__ = ["parse_chunk_text"]

def parse_chunk_text(
    text: str,
    *,
    schema_mode: str,
    dtype_map: Optional[Dict[str, pl.datatypes.DataType]],
    header_columns: Optional[List[str]],
    has_header: bool,
    header_found: bool,
    base_options: Dict[str, Any],
) -> Tuple[pl.DataFrame, Optional[Dict[str, pl.datatypes.DataType]]]:
    """Parse a chunk of CSV text, performing optional infer-mode dtype stabilization.

    Returns the DataFrame and (possibly updated) dtype_map for infer stabilization.
    Casting uses strict=False so that incompatible later chunk values degrade gracefully.
    """
    sio = io.StringIO(text)
    if header_columns is None:
        parse_has_header = has_header and header_found
        parse_new_columns = None
    else:
        parse_has_header = False
        parse_new_columns = header_columns
    df = pl.read_csv(
        sio,
        has_header=parse_has_header,
        new_columns=parse_new_columns,
        infer_schema=(schema_mode == "infer" and dtype_map is None),
        **base_options,
    )
    if dtype_map is None and schema_mode == "infer":
        dtype_map = {c: dt for c, dt in zip(df.columns, df.dtypes)}
    elif dtype_map is not None and schema_mode == "infer":
        casts = []
        for c, dt in dtype_map.items():
            if c in df.columns and df[c].dtype != dt:
                try:
                    casts.append(pl.col(c).cast(dt, strict=False))
                except Exception:  # pragma: no cover - defensive
                    pass
        if casts:
            df = df.with_columns(casts)
    return df, dtype_map

