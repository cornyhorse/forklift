from __future__ import annotations

import json
from pathlib import Path

from ..outputs.base import BaseOutput
from ..types import Row, RowResult


class PQOutput(BaseOutput):
    """Test-oriented minimal writer.

    - Does not actually write Parquet yet.
    - Tracks counts and writes a manifest and a quarantine JSONL so tests can assert.
    - Quarantine file path: ``<dest>/_quarantine.jsonl``
    """

    def __init__(self, dest: str, schema: dict | None = None, **kwargs):
        super().__init__(dest, schema, **kwargs)
        self.schema = schema

    def open(self) -> None:
        """Initialize output directory and open quarantine file."""
        self._dest = Path(self.dest)
        self._dest.mkdir(parents=True, exist_ok=True)
        self._qpath = self._dest / "_quarantine.jsonl"
        self._qfp = self._qpath.open("w", encoding="utf-8")
        self._counts = {"read": 0, "kept": 0, "rejected": 0}

    def validate_row(self, row: Row) -> None:
        """Validate a row against the schema (if provided).

        Performs lightweight type checks for field specifications inside the
        schema's ``fields`` list. Supported types:

        * ``integer`` – value convertible via ``int``.
        * ``date`` – value convertible using :func:`forklift.utils.date_parser.parse_date`.
        * ``boolean`` – membership in configured ``true`` / ``false`` token lists.

        :param row: Row dictionary to validate.
        :raises ValueError: On type mismatch for any configured field.
        """
        if not self.schema or "fields" not in self.schema:
            return
        from forklift.utils.date_parser import parse_date
        for field in self.schema["fields"]:
            name = field["name"]
            field_type = field.get("type")
            fmt = field.get("format")
            value = row.get(name)
            if field_type == "integer":
                if value is not None and value != "":
                    try:
                        int(value)
                    except Exception:
                        raise ValueError(f"Field '{name}' expected integer, got '{value}'")
            elif field_type == "date":
                if value is not None and value != "":
                    if not parse_date(value, fmt):
                        raise ValueError(f"Field '{name}' expected date{f' {fmt}' if fmt else ''}, got '{value}'")
            elif field_type == "boolean":
                true_vals = field.get("true", ["Y", "1", "T", "True"])
                false_vals = field.get("false", ["N", "0", "F", "False"])
                if value not in true_vals and value not in false_vals:
                    raise ValueError(f"Field '{name}' expected boolean, got '{value}'")

    def write(self, row: Row) -> None:
        """Process an accepted row: validate and update counters.

        Skips rows explicitly marked with ``__forklift_skip__``.

        :param row: Row dictionary.
        """
        # Skip rows explicitly marked by preprocessors
        if row.get("__forklift_skip__"):
            self._counts["read"] += 1
            return
        self._counts["read"] += 1
        try:
            self.validate_row(row)
            self._counts["kept"] += 1
            # Intentionally no parquet writing yet (unit-test stub)
        except Exception as e:
            self.quarantine(RowResult(row=row, error=e))

    def quarantine(self, rr: RowResult) -> None:
        """Record a rejected row to the quarantine JSONL and count it.

        :param rr: RowResult containing original row + error.
        """
        self._counts["read"] += 1
        self._counts["rejected"] += 1
        payload = {"row": rr.row, "error": str(rr.error)}
        self._qfp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def close(self) -> None:
        """Close quarantine file and emit a manifest JSON file."""
        try:
            # Ensure quarantine file is closed/flushed
            if hasattr(self, "_qfp") and not self._qfp.closed:
                self._qfp.close()
        finally:
            # Always write a small manifest for assertions
            manifest = self._dest / "_manifest.json"
            manifest.write_text(json.dumps(self._counts, indent=2), encoding="utf-8")