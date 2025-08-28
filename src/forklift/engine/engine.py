from __future__ import annotations
from typing import Iterable, Any, Dict, Tuple

from .registry import get_input_cls, get_output_cls, get_preprocessors

try:
    from ..types import Row, RowResult
except Exception:
    from dataclasses import dataclass

    Row = Dict[str, Any]  # type: ignore


    @dataclass
    class RowResult:
        row: Row
        error: Exception | None


class DuplicateRow(Exception):
    pass


class Engine:
    def __init__(
            self,
            input_kind: str,
            output_kind: str,
            schema: Dict[str, Any] | None = None,
            preprocessors: list[str] | None = None,
            header_mode: str = "auto",  # "present", "absent", "auto"
            **input_opts: Any,
    ) -> None:
        """
        Engine for data import/export.

        Args:
            input_kind: Type of input (e.g., "csv").
            output_kind: Type of output (e.g., "parquet").
            schema: Optional schema for validation and type/null extraction.
            preprocessors: List of preprocessors to apply.
            header_mode: Explicit header handling mode ("present", "absent", "auto").
                - "present": File is expected to have a header row.
                - "absent": File does not have a header row; use header_override for field names.
                - "auto": Try to detect header row.
            **input_opts: Additional options for input class.
        """
        self.schema = schema or {}
        self.input_opts = input_opts
        self.input_opts["header_mode"] = header_mode  # enforce explicit header handling
        self.output_opts: Dict[str, Any] = {}
        self.Input = get_input_cls(input_kind)
        self.Output = get_output_cls(output_kind)

        # Pass 'include' patterns to SQLInput if input_kind is 'sql'
        if input_kind == "sql":
            include_patterns = self.schema.get("include", ["*.*"])
            self.input_opts["include"] = include_patterns

        # pass schema so type_coercion can extract minimal types/nulls
        self.preprocessors = get_preprocessors(preprocessors or [], schema=self.schema)

        # very light in-engine validation
        self.required = list(self.schema.get("required", []))

        # dedupe keys from x-csv.dedupe.keys (if present)
        xcsv = (self.schema.get("x-csv") or {})
        self.dedupe_keys: Tuple[str, ...] = tuple((xcsv.get("dedupe") or {}).get("keys", []) or ())

        self.validator = None  # placeholder for future JSON Schema validator

        xcsv = (self.schema.get("x-csv") or {})
        self.allow_required_nulls = bool((xcsv.get("nulls") or {}))

    def _required_ok(self, row: Row) -> bool:
        # Presence-only check: the column must exist in the header.
        # NULLs are allowed when x-csv.nulls is configured; otherwise, they are NOT allowed.
        if not self.required:
            return True
        for k in self.required:
            if k not in row:  # header didn’t include it; don’t fail here
                continue
            v = row.get(k, None)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                if self.allow_required_nulls:
                    continue
                return False
        return True

    def _process(self, rows: Iterable[Row]) -> Iterable[RowResult]:
        seen_keys = set()
        for row in rows:
            try:
                # preprocessors (coercion etc.)
                for p in self.preprocessors:
                    row = p.apply(row)

                # required fields
                if not self._required_ok(row):
                    raise ValueError("missing required field")

                # dedupe-by-key if configured
                if self.dedupe_keys:
                    key = tuple(row.get(k) for k in self.dedupe_keys)
                    if key in seen_keys:
                        row["__forklift_skip__"] = True  # mark as read-only skip
                        yield RowResult(row=row, error=None)
                        continue
                    seen_keys.add(key)
                # row is good
                yield RowResult(row=row, error=None)
            except Exception as e:
                yield RowResult(row=row, error=e)

    def run(self, source: str, dest: str) -> None:
        # header override for headerless TSV from schema.x-csv.header.mode == "provided"
        header_override = None
        xcsv = (self.schema or {}).get("x-csv") or {}
        header = xcsv.get("header") or {}
        if header.get("mode") == "provided":
            header_override = header.get("columns")

        inp = self.Input(source, header_override=header_override, **self.input_opts)
        out = self.Output(dest, schema=self.schema, **self.output_opts)

        out.open()
        try:
            for table in inp.get_tables():
                table_name = table["name"]
                for rr in self._process((dict(row, _table=table_name) for row in table["rows"])):
                    if rr.error is None:
                        out.write(rr.row)
                    else:
                        out.quarantine(rr)
        finally:
            out.close()
