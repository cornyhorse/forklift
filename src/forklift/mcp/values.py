"""Bounded pieces of results: cells as JSON values, names, rows and Parquet files read in steps.

Rows appear only where a tool is meant to show them (``preview``, ``validate_schema``'s
``rejected_rows``, ``get_bad_rows``, ``explain_failure``), always within limits on rows, columns,
bytes and cell length that the results state. Long text is cut in Arrow before it becomes a Python
object, and Parquet files are read a few rows at a time, so memory stays bounded by what a result
can show, also for a file whose cells are huge.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from ..engine.processors.extensions import REASON_COLUMN

#: Longest text of one cell in a result; longer text is cut (and counted in truncated_cells)
MAX_CELL_CHARS = 2000
#: Most items of a list or entries of a map inside one cell
MAX_CELL_ITEMS = 100
#: Longest column name, title, type or other name in a result; longer ones are cut
MAX_NAME_CHARS = 256
#: Most rows of a Parquet file read at a time
MAX_BATCH_ROWS = 64


def cell(value: Any) -> Tuple[Any, bool]:
    """``(JSON value, was_cut)`` for one cell of a Parquet file or a preview."""
    if value is None or isinstance(value, (bool, int)):
        return value, False
    if isinstance(value, float):
        return (value, False) if math.isfinite(value) else (str(value), False)
    if isinstance(value, str):
        return _text(value, MAX_CELL_CHARS)
    if isinstance(value, (bytes, bytearray)):
        return _text("base64:" + base64.b64encode(bytes(value)).decode("ascii"), MAX_CELL_CHARS)
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
    return _text(str(value), MAX_CELL_CHARS)  # timedelta and anything else Arrow returns


def _text(value: str, limit: int) -> Tuple[str, bool]:
    if len(value) > limit:
        return value[:limit], True
    return value, False


def name(value: Any) -> Tuple[str, bool]:
    """``(text, was_cut)`` for a name in a result (column, title, type, extension)."""
    return _text(value if isinstance(value, str) else str(value), MAX_NAME_CHARS)


def compact_size(value: Any) -> int:
    """Bytes of ``value`` as compact JSON (what a result's byte limits count)."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


class ByteBudget:
    """Bytes (compact JSON) left for the parts of a result that share one limit."""

    def __init__(self, limit: int):
        self.limit = limit
        self.used = 0

    def take(self, value: Any) -> bool:
        """Count ``value`` against the budget; False (and nothing counted) when it does not fit."""
        size = compact_size(value) + 1
        if self.used + size > self.limit:
            return False
        self.used += size
        return True


def bounded_names(
    values: Iterable[Any], max_items: int, budget: Optional[ByteBudget]
) -> Tuple[List[str], int, int]:
    """``(names shown, how many there are, how many shown were cut)`` of a list of names.

    Names are cut to :data:`MAX_NAME_CHARS`; at most ``max_items`` are shown, and only while they
    fit ``budget`` (if one is given). The count covers the whole list.
    """
    shown: List[str] = []
    total = cut_names = 0
    full = False
    for value in values:
        total += 1
        if full or len(shown) >= max_items:
            continue
        text, cut = name(value)
        if budget is not None and not budget.take(text):
            full = True
            continue
        shown.append(text)
        cut_names += cut
    return shown, total, cut_names


class RowCollector:
    """Rows for a result, until a row or byte limit is reached.

    ``limit`` says which one stopped it (``"rows"`` or ``"bytes"``; None while neither did). The
    first row is always taken: if it alone is larger than ``max_bytes``, its text cells are cut
    shorter until it fits (counted in ``truncated_cells``, and ``limit`` is ``"bytes"``), so a
    page always shows at least one row and paging always moves on.
    """

    def __init__(self, max_rows: int, max_bytes: int):
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.rows: List[List[Any]] = []
        self.truncated_cells = 0
        self.limit: Optional[str] = None
        self._size = 0

    def add(self, values: Sequence[Any]) -> bool:
        """Add a row; False when the result is full (then the row was not added, unless it is
        the first one, which is added cut to fit)."""
        if len(self.rows) >= self.max_rows:
            self.limit = "rows"
            return False
        converted = [cell(value) for value in values]
        row = [value for value, _ in converted]
        cut = sum(was_cut for _, was_cut in converted)
        size = compact_size(row) + 1
        if self._size + size > self.max_bytes:
            self.limit = "bytes"
            if self.rows:
                return False
            row, cut = self._shrink(row, converted)
            self._add(row, cut, compact_size(row) + 1)
            return False
        self._add(row, cut, size)
        return True

    def _add(self, row: List[Any], cut: int, size: int) -> None:
        self.rows.append(row)
        self.truncated_cells += cut
        self._size += size

    def _shrink(self, row: List[Any], converted: List[Tuple[Any, bool]]) -> Tuple[List[Any], int]:
        """The row with its text cells cut to 1/2, 1/4, ... of the cell limit until it fits."""
        limit = MAX_CELL_CHARS
        while True:
            limit //= 2
            shrunk = [
                (_text(value, limit) if isinstance(value, str) else (value, False))
                for value in row
            ]
            values = [value for value, _ in shrunk]
            if limit == 0 or compact_size(values) + 1 <= self.max_bytes:
                cut = sum(a or b for (_, a), (_, b) in zip(converted, shrunk))
                return values, cut

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


def _cut_in_arrow(array: Any) -> Any:
    """Text and binary cells cut before they become Python objects (one character / byte more
    than a result shows, so :func:`cell` still sees and counts the cut)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    kind = array.type
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return pc.utf8_slice_codeunits(array, 0, MAX_CELL_CHARS + 1)
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind):
        return pc.binary_slice(array, 0, MAX_CELL_CHARS)
    return array


def iter_parquet_rows(
    path: Path, columns: Sequence[str], offset: int = 0, batch_rows: int = MAX_BATCH_ROWS
) -> Iterator[List[Any]]:
    """The rows of ``columns`` from row ``offset`` on, ``batch_rows`` (at most 64) at a time.

    Whole row groups before ``offset`` are skipped without reading them; text and binary cells
    are cut in Arrow (see :func:`_cut_in_arrow`).
    """
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
        batch_size=max(1, min(batch_rows, MAX_BATCH_ROWS)),
        row_groups=groups,
        columns=list(columns),
        use_threads=False,
    )
    for batch in batches:
        if skip >= batch.num_rows:
            skip -= batch.num_rows
            continue
        batch = batch.slice(skip)
        skip = 0
        cells = [_cut_in_arrow(batch.column(column)).to_pylist() for column in columns]
        for row in zip(*cells):
            yield list(row)


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
    for batch in handle.iter_batches(batch_size=4096, columns=[REASON_COLUMN], use_threads=False):
        batch = batch.slice(0, max_rows - scanned)
        scanned += batch.num_rows
        # The engine cuts reasons to 200 characters; a file from elsewhere may not have
        reasons = pc.utf8_slice_codeunits(batch.column(0), 0, MAX_NAME_CHARS)
        for entry in pc.value_counts(reasons).to_pylist():
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
