"""Bounded pieces of results: cells, rows, texts, lists and schema documents.

The same rules as the local server's (``forklift.mcp``): cells longer than 2000 characters are
cut, at most 100 items of a list or map inside a cell, rows and documents are bounded in bytes of
compact JSON, and every result says which caps applied.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

#: Longest text of one cell in a result; longer text is cut (and counted in truncated_cells)
MAX_CELL_CHARS = 2000
#: Most items of a list or entries of a map inside one cell
MAX_CELL_ITEMS = 100
#: Longest free text from the gateway in a result (descriptions, notes, messages)
MAX_TEXT_CHARS = 1000
#: Entries of a column list (schema summaries, headers)
MAX_LISTED_COLUMNS = 500
#: Warnings and artifacts in a result
MAX_LIST_ITEMS = 50
#: Entries of validation_summary in a result (the most frequent)
MAX_FINDINGS = 100

_CONSTRAINT_KEYWORDS = frozenset(
    {"minimum", "maximum", "minLength", "maxLength", "pattern", "enum", "x-unique"}
)


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


def text(value: Any, limit: int = MAX_TEXT_CHARS) -> Any:
    """A text from the gateway, cut at ``limit`` characters (other values as they are)."""
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    return value


def bounded(value: Any) -> Any:
    """A JSON value (a connection's config, a dataset's options) with long texts and lists cut."""
    converted, _ = cell(value)
    return converted


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


def split_finding(key: str) -> Dict[str, Optional[str]]:
    """``"CODE:column"`` (as in validation_summary and _rejection_reason) as rule and column."""
    rule, separator, column = key.partition(":")
    return {"rule": rule, "column": column if separator else None}


def top(summary: Any, cap: int) -> List[Tuple[str, int]]:
    """The ``cap`` most frequent entries of a ``{key: count}`` map."""
    if not isinstance(summary, dict):
        return []
    counts = [(str(key), count) for key, count in summary.items() if isinstance(count, int)]
    return sorted(counts, key=lambda item: (-item[1], item[0]))[:cap]


def bounded_findings(summary: Any) -> Dict[str, Any]:
    return {
        "validation_summary": dict(top(summary, MAX_FINDINGS)),
        "validation_summary_total": len(summary) if isinstance(summary, dict) else 0,
    }


def bounded_warnings(warnings: Any) -> Dict[str, Any]:
    items = [text(item) for item in warnings] if isinstance(warnings, list) else []
    return {"warnings": items[:MAX_LIST_ITEMS], "warnings_total": len(items)}


def bounded_list(values: Any, cap: int = MAX_LISTED_COLUMNS) -> Any:
    return values[:cap] if isinstance(values, list) else values


def summarize_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Columns, types, required columns and extensions of a schema (names only, bounded)."""
    properties = schema.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    required_list = schema.get("required")
    required = [name for name in required_list or [] if isinstance(name, str)]
    columns = []
    for name, spec in list(properties.items())[:MAX_LISTED_COLUMNS]:
        spec = spec if isinstance(spec, dict) else {}
        entry: Dict[str, Any] = {
            "name": name,
            "type": spec.get("type"),
            "required": name in required,
        }
        if isinstance(spec.get("format"), str):
            entry["format"] = spec["format"]
        extensions = sorted(key for key in spec if key.startswith("x-"))
        if extensions:
            entry["extensions"] = extensions
        constraints = sorted(key for key in spec if key in _CONSTRAINT_KEYWORDS)
        if constraints:
            entry["constraints"] = constraints
        columns.append(entry)
    return {
        "title": schema.get("title") if isinstance(schema.get("title"), str) else None,
        "column_count": len(properties),
        "columns": columns,
        "columns_truncated": len(properties) > MAX_LISTED_COLUMNS,
        "required": required[:MAX_LISTED_COLUMNS],
        "extensions": sorted(key for key in schema if key.startswith("x-")),
    }


def bounded_document(document: Dict[str, Any], max_bytes: int) -> Dict[str, Any]:
    """The document itself when its compact JSON fits ``max_bytes``; else the start of it."""
    size = compact_size(document)
    if size <= max_bytes:
        return {"document": document, "document_bytes": size, "truncated": False}
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return {
        "document": None,
        "document_text": encoded[:max_bytes].decode("utf-8", errors="ignore"),
        "document_bytes": size,
        "truncated": True,
    }
