from __future__ import annotations

"""Parquet output writer.

Creates one Parquet file per logical table (or ``data.parquet`` when no
``_table`` key is present). Default mode is vectorized via ``polars`` for
fast columnar construction. Optional ``mode='chunked'`` streams row batches
using ``pyarrow`` append writes (useful for very large datasets that would
otherwise not fit in memory).

Artifacts written under ``dest``:

* ``<table>.parquet`` – one per logical table (or ``data.parquet``)
* ``_quarantine.jsonl`` – one JSON object per rejected row (may be empty)
* ``_manifest.json`` – summary counters: ``read``, ``kept``, ``rejected``

Requirements:
  * ``pyarrow`` (always required; chunked mode uses ParquetWriter)
  * ``polars`` for vectorized mode (default)
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Literal

from ..outputs.base import BaseOutput
from ..types import Row, RowResult
from ..utils.row_validation import validate_row_against_schema
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl


class PQOutput(BaseOutput):
    """Parquet output writer with per-table file emission.

    Modes:
      * ``vectorized`` (default): buffer rows in-memory then build a Polars
        DataFrame per table and write with ``DataFrame.write_parquet``.
      * ``chunked``: flush row buffers to Parquet incrementally using a
        ``pyarrow.parquet.ParquetWriter`` once ``chunk_size`` is reached.

    :param dest: Output directory path (created if missing).
    :param schema: Optional schema dict containing ``fields`` collection (for validation only).
    :param mode: ``vectorized`` or ``chunked``.
    :param chunk_size: Row count threshold for flushing in ``chunked`` mode.
    :param compression: Parquet compression codec (default ``snappy``).
    """

    def __init__(
        self,
        dest: str,
        schema: dict | None = None,
        *,
        mode: str = "vectorized",
        chunk_size: int = 50_000,
        compression: str = "snappy",
        **kwargs: Any,
    ):
        super().__init__(dest, schema, **kwargs)
        self.schema = schema
        allowed_comp: set[str] = {"snappy", "gzip", "brotli", "zstd", "lz4", "uncompressed"}
        if compression not in allowed_comp:
            raise ValueError(f"Unsupported compression '{compression}'. Allowed: {sorted(allowed_comp)}")
        self.compression: Literal["snappy", "gzip", "brotli", "zstd", "lz4", "uncompressed"] = compression  # type: ignore[assignment]
        self.mode = mode.lower()
        if self.mode not in {"vectorized", "chunked"}:
            raise ValueError("mode must be 'vectorized' or 'chunked'")
        # vectorized mode always valid now (polars assumed present)
        self.chunk_size = chunk_size
        self.row_buffers: Dict[str, List[Row]] = {}
        # For chunked mode: maintain ParquetWriter per table
        self._writers: Dict[str, pq.ParquetWriter] = {}

    def open(self) -> None:
        self.output_dir = Path(self.dest)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_file_path = self.output_dir / "_quarantine.jsonl"
        self.quarantine_handle = self.quarantine_file_path.open("w", encoding="utf-8")
        self.counters = {"read": 0, "kept": 0, "rejected": 0}
        # (Removed legacy integer/boolean coercion collection to preserve declared schema types.)

    # ---------------- Validation -----------------
    def validate_row(self, row: Row) -> None:
        validate_row_against_schema(row, self.schema)

    # ---------------- Public write API -----------
    def write(self, row: Row) -> None:
        self.counters["read"] += 1
        if row.get("__forklift_skip__"):
            return
        try:
            self.validate_row(row)
        except Exception as e:
            # Single failure: increment rejected only.
            self.quarantine(RowResult(row=row, error=e))
            return
        self.counters["kept"] += 1
        table_name = row.get("_table") or "data"
        clean_row = {k: v for k, v in row.items() if not k.startswith("__forklift_")}
        buf = self.row_buffers.setdefault(table_name, [])
        buf.append(clean_row)
        if self.mode == "chunked" and len(buf) >= self.chunk_size:
            self._flush_table_chunk(table_name)

    def quarantine(self, rr: RowResult) -> None:
        self.counters["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self.quarantine_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # ---------------- Internal helpers ------------
    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        base = Path(name).name
        return base.replace("/", "_").replace("\\", "_")

    def _flush_table_chunk(self, table_name: str) -> None:
        """Flush current buffer for a single table in chunked mode.

        Creates writer lazily on first flush using inferred schema from first chunk.
        """
        rows = self.row_buffers.get(table_name)
        if not rows:
            return
        table_pa = pa.Table.from_pylist(rows)  # type: ignore[arg-type]
        safe_name = self._sanitize_table_name(table_name)
        out_path = self.output_dir / f"{safe_name}.parquet"
        writer = self._writers.get(table_name)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table_pa.schema, compression=self.compression)
            self._writers[table_name] = writer
        writer.write_table(table_pa)
        # Clear buffer
        rows.clear()

    def _flush_all_chunked(self) -> None:
        for table_name in list(self.row_buffers.keys()):
            self._flush_table_chunk(table_name)
        # Close writers
        for writer in self._writers.values():
            try:
                writer.close()
            except Exception:  # pragma: no cover
                pass
        self._writers.clear()

    def _flush_vectorized(self) -> None:
        if not self.row_buffers:
            return
        for table_name, rows in self.row_buffers.items():
            if not rows:
                continue
            safe_table_name = self._sanitize_table_name(table_name)
            out_path = self.output_dir / f"{safe_table_name}.parquet"
            # Use pyarrow for schema consistency (handles Decimal, bytes) even in vectorized mode
            table_pa = pa.Table.from_pylist(rows)  # type: ignore[arg-type]
            pq.write_table(table_pa, out_path, compression=self.compression)
            rows.clear()

    def _flush_parquet(self) -> None:
        if self.mode == "chunked":
            self._flush_all_chunked()
        else:
            self._flush_vectorized()

    # ---------------- Lifecycle -------------------
    def close(self) -> None:
        try:
            self._flush_parquet()
            if hasattr(self, "quarantine_handle") and not self.quarantine_handle.closed:
                self.quarantine_handle.close()
        finally:
            manifest = self.output_dir / "_manifest.json"
            manifest.write_text(json.dumps(self.counters, indent=2), encoding="utf-8")
