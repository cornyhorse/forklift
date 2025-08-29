from __future__ import annotations
from typing import Iterable, Any, Dict, Tuple

from .registry import get_input_cls, get_output_cls, get_preprocessors
from ..utils.sql_include import derive_sql_include_patterns
from ..types import Row, RowResult


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
        """Initialize the data processing engine.

        Wires together the selected input and output plugins, derives include
        patterns (for SQL variants), loads preprocessors, and prepares light
        validation metadata derived from the provided schema.

        :param input_kind: Registered input kind (e.g. ``"csv"``, ``"excel"``, ``"sql"``).
        :param output_kind: Registered output kind (e.g. ``"parquet"``).
        :param schema: Optional schema dict containing ``fields`` and extension
            blocks like ``x-csv`` or ``x-sql``.
        :param preprocessors: Ordered list of preprocessor names to apply.
        :param header_mode: Header handling mode for CSV-like inputs (``"present"``,
            ``"absent"``, or ``"auto"``).
        :param input_opts: Additional keyword options forwarded to the input class.
        """
        self.schema = schema or {}
        self.input_opts = input_opts
        self.input_opts["header_mode"] = header_mode
        self.output_opts: Dict[str, Any] = {}
        self.Input = get_input_cls(input_kind)
        self.Output = get_output_cls(output_kind)

        # Derive include patterns for SQL families via utility helper.
        if input_kind in ("sql", "sql_backup"):
            self.input_opts["include"] = derive_sql_include_patterns(self.schema)

        self.preprocessors = get_preprocessors(preprocessors or [], schema=self.schema)
        self.required = list(self.schema.get("required", []))
        xcsv = (self.schema.get("x-csv") or {})
        self.dedupe_keys: Tuple[str, ...] = tuple((xcsv.get("dedupe") or {}).get("keys", []) or ())
        self.validator = None  # placeholder
        self.allow_required_nulls = bool((xcsv.get("nulls") or {}))

    def _required_ok(self, row: Row) -> bool:
        """Check whether required columns are satisfied.

        A required column passes if it is absent from the row header (header-level
        omission tolerated) or present with a non-empty, non-null value; unless
        the ``x-csv.nulls`` extension allows nulls.

        :param row: Row under evaluation.
        :return: ``True`` if row satisfies required constraints, else ``False``.
        """
        if not self.required:
            return True
        for k in self.required:
            if k not in row:
                continue
            v = row.get(k)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                if self.allow_required_nulls:
                    continue
                return False
        return True

    def _process(self, rows: Iterable[Row]) -> Iterable[RowResult]:
        """Apply preprocessors, required-field checks, and optional dedupe.

        :param rows: Iterable of raw row dicts.
        :yield: ``RowResult`` containing either a clean row or an error.
        """
        seen_keys = set()
        for row in rows:
            try:
                for p in self.preprocessors:
                    row = p.apply(row)
                if not self._required_ok(row):
                    raise ValueError("missing required field")
                if self.dedupe_keys:
                    key = tuple(row.get(k) for k in self.dedupe_keys)
                    if key in seen_keys:
                        row["__forklift_skip__"] = True
                        yield RowResult(row=row, error=None)
                        continue
                    seen_keys.add(key)
                yield RowResult(row=row, error=None)
            except Exception as e:  # pragma: no cover - defensive
                yield RowResult(row=row, error=e)

    def run(self, source: str, dest: str) -> None:
        """Execute ingest → preprocess → output pipeline.

        Reads tables from the input plugin, augments each row with ``_table`` and
        streams them through processing; accepted rows are written, failures are
        quarantined.

        :param source: Input location (filepath, connection string, etc.).
        :param dest: Output destination path.
        """
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
