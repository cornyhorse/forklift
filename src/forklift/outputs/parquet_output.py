from __future__ import annotations

"""Parquet (test stub) output writer.

This module provides :class:`PQOutput`, a minimal implementation of the
``BaseOutput`` interface used by the test-suite. It intentionally does *not*
produce real Parquet files yet; instead it validates rows (lightweight type
checks), tracks counts, and records rejected rows in a quarantine JSONL file.

Artifacts written under the destination directory (``dest``):

* ``_quarantine.jsonl`` – one JSON object per rejected row (may be empty)
* ``_manifest.json`` – summary counters: ``read``, ``kept``, ``rejected``

Future enhancement could swap the in-memory no-op write for an actual Parquet
export using ``pyarrow`` or an equivalent backend without changing the public
Engine contract.
"""

import json
from pathlib import Path

from ..outputs.base import BaseOutput
from ..types import Row, RowResult
from ..utils.row_validation import validate_row_against_schema


class PQOutput(BaseOutput):
    """Test-oriented minimal writer (no real Parquet emission yet).

    Responsibilities:

    * Validate incoming rows against an optional schema (subset: integer, date, boolean)
    * Maintain counts (``read``, ``kept``, ``rejected``)
    * Append rejected rows (with error messages) to ``_quarantine.jsonl``
    * Persist counts to ``_manifest.json`` on close

    :param dest: Output directory path (created if missing).
    :param schema: Optional schema dict containing ``fields`` collection.
    :param kwargs: Additional (unused) keyword arguments for future expansion.
    """

    def __init__(self, dest: str, schema: dict | None = None, **kwargs):
        super().__init__(dest, schema, **kwargs)
        self.schema = schema

    def open(self) -> None:
        """Initialize output directory and open quarantine file.

        Side effects:
          * Ensures ``dest`` directory exists.
          * Opens (truncates) ``_quarantine.jsonl`` for writing.
          * Initializes internal counters.

        :raises OSError: If the destination directory cannot be created.
        """
        self.output_dir = Path(self.dest)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_file_path = self.output_dir / "_quarantine.jsonl"
        self.quarantine_handle = self.quarantine_file_path.open("w", encoding="utf-8")
        self.counters = {"read": 0, "kept": 0, "rejected": 0}

    def validate_row(self, row: Row) -> None:
        """Validate a row against the schema (if provided) using utility helper.

        Delegates to :func:`forklift.utils.row_validation.validate_row_against_schema`
        which implements the lightweight subset of field type checks (integer,
        date, boolean) used in tests.

        :param row: Row dictionary to validate.
        :raises ValueError: On type mismatch for any configured field.
        """
        validate_row_against_schema(row, self.schema)

    def write(self, row: Row) -> None:
        """Validate and account for an accepted row.

        Rows tagged with ``__forklift_skip__`` are *counted* (``read``) but not
        validated nor kept. Successful validation increments ``kept``; failures
        are delegated to :meth:`quarantine`.

        :param row: Row dictionary (possibly mutated by preprocessors upstream).
        """
        # Skip rows explicitly marked by preprocessors
        if row.get("__forklift_skip__"):
            self.counters["read"] += 1
            return
        self.counters["read"] += 1
        try:
            self.validate_row(row)
            self.counters["kept"] += 1
            # Intentionally no parquet writing yet (unit-test stub)
        except Exception as e:
            self.quarantine(RowResult(row=row, error=e))

    def quarantine(self, rr: RowResult) -> None:
        """Record a rejected row and increment counters.

        :param rr: RowResult containing the original row and validation (or other) error.
        :raises ValueError: Never raised; errors are serialized to JSONL.
        """
        self.counters["read"] += 1
        self.counters["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self.quarantine_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def close(self) -> None:
        """Flush quarantine log (if open) and emit manifest.

        Always writes ``_manifest.json`` even if no rows were processed, so
        callers / tests can rely on the file's presence.
        """
        try:
            if hasattr(self, "quarantine_handle") and not self.quarantine_handle.closed:
                self.quarantine_handle.close()
        finally:
            manifest = self.output_dir / "_manifest.json"
            manifest.write_text(json.dumps(self.counters, indent=2), encoding="utf-8")
