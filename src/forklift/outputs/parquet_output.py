from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from .base import BaseOutput
from ..types import Row


class PQOutput(BaseOutput):
    """Test-oriented minimal writer.
    - Does not actually write Parquet yet.
    - Tracks counts and writes a manifest and a quarantine JSONL so tests can assert.
    """

    def open(self) -> None:
        self._dest = Path(self.dest)
        self._dest.mkdir(parents=True, exist_ok=True)
        self._qdir = self._dest / "_quarantine"
        self._qdir.mkdir(parents=True, exist_ok=True)
        self._qfile = self._qdir / "rows.jsonl"
        self._qfp = self._qfile.open("w", encoding="utf-8")
        self._counts = {"read": 0, "kept": 0, "rejected": 0}

    def write(self, row: Row) -> None:
        if row.get("__forklift_skip__"):
            self._counts["read"] += 1
            return
        self._counts["read"] += 1
        self._counts["kept"] += 1

    def quarantine(self, rr) -> None:
        self._counts["read"] += 1
        self._counts["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self._qfp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def close(self) -> None:
        try:
            self._qfp.close()
        finally:
            manifest = self._dest / "_manifest.json"
            manifest.write_text(json.dumps(self._counts, indent=2), encoding="utf-8")
