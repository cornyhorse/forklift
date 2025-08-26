from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..outputs.base import BaseOutput
from ..types import Row, RowResult


class PQOutput(BaseOutput):
    """Test-oriented minimal writer.

    - Does not actually write Parquet yet.
    - Tracks counts and writes a manifest and a quarantine JSONL so tests can assert.
    - Quarantine file path: <dest>/_quarantine.jsonl
    """

    def open(self) -> None:
        self._dest = Path(self.dest)
        self._dest.mkdir(parents=True, exist_ok=True)

        # Flat quarantine file to satisfy tests (no subdir)
        self._qpath = self._dest / "_quarantine.jsonl"
        self._qfp = self._qpath.open("w", encoding="utf-8")

        # Basic counters for manifest
        self._counts = {"read": 0, "kept": 0, "rejected": 0}

    def write(self, row: Row) -> None:
        # Skip rows explicitly marked by preprocessors
        if row.get("__forklift_skip__"):
            self._counts["read"] += 1
            return
        self._counts["read"] += 1
        self._counts["kept"] += 1
        # Intentionally no parquet writing yet (unit-test stub)

    def quarantine(self, rr: RowResult) -> None:
        self._counts["read"] += 1
        self._counts["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self._qfp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def close(self) -> None:
        try:
            # Ensure quarantine file is closed/flushed
            if hasattr(self, "_qfp") and not self._qfp.closed:
                self._qfp.close()
        finally:
            # Always write a small manifest for assertions
            manifest = self._dest / "_manifest.json"
            manifest.write_text(json.dumps(self._counts, indent=2), encoding="utf-8")