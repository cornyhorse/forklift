"""Rows for results: Parquet files read in bounded pieces, cells turned into JSON values.

Only the tools that are meant to show rows (``preview``, ``get_bad_rows``, ``explain_failure``)
use this, and always within limits on rows, columns, bytes and cell length that the results state.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ..engine.processors.extensions import REASON_COLUMN

#: Longest text of one cell in a result; longer text is cut (and counted in truncated_cells)
MAX_CELL_CHARS = 2000
#: Most items of a list or entries of a map inside one cell
MAX_CELL_ITEMS = 100
#: Rows of a Parquet file read per step
_BATCH_ROWS = 1024


def cell(value: Any) -> Tuple[Any, bool]:
    """``(JSON value, was_cut)`` for one cell of a Parquet file or a preview."""
    if value is None or isinstance(value, (bool, int)):
        return value, False
    if isinstance(value, float):
        return (value, False) if math.isfinite(value) else (str(value), False)
    if isinstance(value, str):
        return _text(value)
    if isinstance(value, (bytes, bytearray)):
        return _text("base64:" + base64.b64encode(bytes(value)).decode("ascii"))
    if isinstance(value, decimal.Decimal):
        return str(value), False
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat(), False
    if isinstance(value, dict):
        items = list(value.items())
        converted = {}
        cut = len(items) > MAX_CELL_ITEMS
        for key, item in items[:MAX_CELL_ITEMS]:
            converted[str(key)], item_cut = cell(item)
            cut = cut or item_cut
        return converted, cut
    if isinstance(value, (list, tuple)):
        converted_items = []
        cut = len(value) > MAX_CELL_ITEMS
        for item in value[:MAX_CELL_ITEMS]:
            converted_item, item_cut = cell(item)
            converted_items.append(converted_item)
            cut = cut or item_cut
        return converted_items, cut
    return _text(str(value))  # timedelta and anything else Arrow returns


def _text(value: str) -> Tuple[str, bool]:
    if len(value) > MAX_CELL_CHARS:
        return value[:MAX_CELL_CHARS], True
    return value, False


def compact_size(value: Any) -> int:
    """Bytes of ``value`` as compact JSON (what a result's byte limits count)."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


class RowCollector:
    """Rows for a result, until a row or byte limit is reached.

    ``limit`` says which one stopped it (``"rows"`` or ``"bytes"``; None while neither did).
    """

    def __init__(self, max_rows: int, max_bytes: int):
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.rows: List[List[Any]] = []
        self.truncated_cells = 0
        self.limit: Optional[str] = None
        self._size = 0

    def add(self, values: Sequence[Any]) -> bool:
        """Add a row; False when the result is full (that row is not added)."""
        if len(self.rows) >= self.max_rows:
            self.limit = "rows"
            return False
        row = []
        for value in values:
            converted, cut = cell(value)
            row.append(converted)
            self.truncated_cells += cut
        size = compact_size(row) + 1
        if self._size + size > self.max_bytes:
            self.limit = "bytes"
            return False
        self._size += size
        self.rows.append(row)
        return True

    def view(self) -> Dict[str, Any]:
        return {
            "rows": self.rows,
            "row_count": len(self.rows),
            "truncated": self.limit is not None,
            "limit": self.limit,
            "truncated_cells": self.truncated_cells,
        }


def parquet_columns(path: Path) -> List[Dict[str, str]]:
    """``[{"name", "type"}]`` of a Parquet file (Arrow type names)."""
    import pyarrow.parquet as pq

    schema = pq.read_schema(str(path))
    return [{"name": field.name, "type": str(field.type)} for field in schema]


def parquet_row_count(path: Path) -> int:
    import pyarrow.parquet as pq

    return pq.read_metadata(str(path)).num_rows


def iter_parquet_rows(path: Path, columns: Sequence[str], offset: int = 0) -> Iterator[List[Any]]:
    """The rows of ``columns`` from row ``offset`` on; whole row groups before it are skipped."""
    import pyarrow.parquet as pq

    handle = pq.ParquetFile(str(path))
    first_group, skip = 0, offset
    while first_group < handle.num_row_groups:
        rows = handle.metadata.row_group(first_group).num_rows
        if skip < rows:
            break
        skip -= rows
        first_group += 1
    groups = list(range(first_group, handle.num_row_groups))
    if not groups:
        return
    batches = handle.iter_batches(
        batch_size=_BATCH_ROWS, row_groups=groups, columns=list(columns), use_threads=False
    )
    for batch in batches:
        if skip >= batch.num_rows:
            skip -= batch.num_rows
            continue
        records = batch.slice(skip).to_pylist()
        skip = 0
        for record in records:
            yield [record[name] for name in columns]


def rejection_reasons(path: Path, max_rows: int, top: int) -> Dict[str, Any]:
    """How often each reason appears in the ``_rejection_reason`` column of a bad_rows file.

    A row can have several reasons ("A; B"); each is counted. At most ``max_rows`` rows are
    read; the ``top`` most frequent reasons are returned.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    handle = pq.ParquetFile(str(path))
    if REASON_COLUMN not in handle.schema_arrow.names:
        return {"column": None, "reasons": [], "scanned_rows": 0, "distinct": 0}
    counts: Counter = Counter()
    scanned = 0
    for batch in handle.iter_batches(batch_size=65536, columns=[REASON_COLUMN], use_threads=False):
        batch = batch.slice(0, max_rows - scanned)
        scanned += batch.num_rows
        for entry in pc.value_counts(batch.column(0)).to_pylist():
            for reason in (entry["values"] or "REJECTED").split("; "):
                counts[reason] += entry["counts"]
        if scanned >= max_rows:
            break
    reasons = [
        dict(split_finding(reason), reason=reason, rows=rows)
        for reason, rows in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:top]
    ]
    return {
        "column": REASON_COLUMN,
        "reasons": reasons,
        "scanned_rows": scanned,
        "distinct": len(counts),
    }


def split_finding(key: str) -> Dict[str, Optional[str]]:
    """``"CODE:column"`` (as in validation_summary and _rejection_reason) as rule and column."""
    rule, separator, column = key.partition(":")
    return {"rule": rule, "column": column if separator else None}
