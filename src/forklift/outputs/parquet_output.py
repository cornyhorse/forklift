from __future__ import annotations

"""Parquet output writer.

Creates one Parquet file per logical table (or ``data.parquet`` when no
``_table`` key is present) using :mod:`pyarrow`. Also records counts and a
quarantine JSONL for rejected rows.

Artifacts written under ``dest``:

* ``<table>.parquet`` – one per logical table (or ``data.parquet``)
* ``_quarantine.jsonl`` – one JSON object per rejected row (may be empty)
* ``_manifest.json`` – summary counters: ``read``, ``kept``, ``rejected``

Requirements:
  * ``pyarrow`` must be installed (import is unconditional).
"""

import json
from pathlib import Path
from typing import Dict, List

from ..outputs.base import BaseOutput
from ..types import Row, RowResult
from ..utils.row_validation import validate_row_against_schema
import pyarrow as pa
import pyarrow.parquet as pq


class PQOutput(BaseOutput):
    """Parquet output writer with basic per-table file emission.

    Buffers rows in-memory keyed by logical table name until :meth:`close`,
    then writes each buffer to ``<table>.parquet`` (or ``data.parquet`` when
    the ``_table`` key is absent).

    Attributes (set after :meth:`open`):
      * ``output_dir`` (Path): Destination directory.
      * ``quarantine_file_path`` (Path): Path to quarantine JSONL file.
      * ``quarantine_handle`` (IO): Open handle for quarantine writes.
      * ``counters`` (dict): Read/kept/rejected integer counters.
      * ``row_buffers`` (dict[str, list[Row]]): Buffered kept rows per table.

    :param dest: Output directory path (created if missing).
    :param schema: Optional schema dict containing ``fields`` collection.
    :param kwargs: Additional keyword arguments (``compression``, ``combine`` future use).
    """

    def __init__(self, dest: str, schema: dict | None = None, **kwargs):
        super().__init__(dest, schema, **kwargs)
        self.schema = schema
        self.compression = kwargs.get("compression", "snappy")
        self.combine_files = kwargs.get("combine", False)
        self.row_buffers: Dict[str, List[Row]] = {}

    def open(self) -> None:
        """Initialize output directory and open quarantine file.

        Side effects:
          * Ensures ``dest`` directory exists.
          * Opens (truncates) ``_quarantine.jsonl`` for writing.
          * Initializes internal counters and buffers.

        :returns: ``None``
        :raises OSError: If the destination directory cannot be created.
        """
        self.output_dir = Path(self.dest)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_file_path = self.output_dir / "_quarantine.jsonl"
        self.quarantine_handle = self.quarantine_file_path.open("w", encoding="utf-8")
        self.counters = {"read": 0, "kept": 0, "rejected": 0}

    def validate_row(self, row: Row) -> None:
        """Validate a row against the schema (if provided) using utility helper.

        Delegates to :func:`forklift.utils.row_validation.validate_row_against_schema`.

        :param row: Row dictionary to validate.
        :returns: ``None``
        :raises ValueError: On type mismatch for any configured field.
        """
        validate_row_against_schema(row, self.schema)

    def write(self, row: Row) -> None:
        """Validate, count, and buffer an accepted row.

        Rows tagged with ``__forklift_skip__`` are *counted* (``read``) but not
        validated nor kept. Successful validation increments ``kept``; failures
        are delegated to :meth:`quarantine`.

        :param row: Row dictionary (possibly mutated by preprocessors upstream).
        :returns: ``None``
        """
        # Skip rows explicitly marked by preprocessors
        if row.get("__forklift_skip__"):
            self.counters["read"] += 1
            return
        self.counters["read"] += 1
        try:
            self.validate_row(row)
            self.counters["kept"] += 1
            # Buffer row for parquet emission later
            table_name = row.get("_table") or "data"
            # Optionally drop internal keys before writing
            clean_row = {k: v for k, v in row.items() if not k.startswith("__forklift_")}
            self.row_buffers.setdefault(table_name, []).append(clean_row)
        except Exception as e:
            self.quarantine(RowResult(row=row, error=e))

    def quarantine(self, rr: RowResult) -> None:
        """Record a rejected row (serialize to quarantine) and update counters.

        :param rr: RowResult containing the original row and validation (or other) error.
        :returns: ``None``
        """
        self.counters["read"] += 1
        self.counters["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self.quarantine_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Return a filesystem-safe table name for parquet emission.

        Uses only the basename component (drops any directory parts) so an
        absolute source path cannot escape the destination directory.
        """
        # Take basename then replace remaining path separators defensively.
        base = Path(name).name
        return base.replace("/", "_").replace("\\", "_")

    def _flush_parquet(self) -> None:
        """Write buffered rows to Parquet files (one per table).

        No-op if there are no buffered rows.

        :returns: ``None``
        """
        if not self.row_buffers:
            return
        for table_name, rows in self.row_buffers.items():
            if not rows:
                continue
            # Infer schema; allow mixed types (pyarrow will best-effort cast)
            batch_table = pa.Table.from_pylist(rows)
            safe_table_name = self._sanitize_table_name(table_name)
            out_path = self.output_dir / f"{safe_table_name}.parquet"
            pq.write_table(batch_table, out_path, compression=self.compression)

    def close(self) -> None:
        """Flush buffered rows, close quarantine file, and write manifest.

        Always writes ``_manifest.json`` even if no rows were processed.

        :returns: ``None``
        """
        try:
            self._flush_parquet()
            if hasattr(self, "quarantine_handle") and not self.quarantine_handle.closed:
                self.quarantine_handle.close()
        finally:
            manifest = self.output_dir / "_manifest.json"
            manifest.write_text(json.dumps(self.counters, indent=2), encoding="utf-8")
