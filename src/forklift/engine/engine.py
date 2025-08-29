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
        # Plugin class references (kept short historically). Provide verbose aliases.
        self.Input = get_input_cls(input_kind)
        self.Output = get_output_cls(output_kind)
        self.input_plugin_class = self.Input
        self.output_plugin_class = self.Output

        # Derive include patterns for SQL families via utility helper.
        if input_kind in ("sql", "sql_backup"):
            self.input_opts["include"] = derive_sql_include_patterns(self.schema)

        self.preprocessors = get_preprocessors(preprocessors or [], schema=self.schema)
        # Required fields collection (retain original attribute for backward compatibility)
        self.required = list(self.schema.get("required", []))
        self.required_field_names = self.required  # alias
        xcsv_extension_block = (self.schema.get("x-csv") or {})
        dedupe_config = (xcsv_extension_block.get("dedupe") or {})
        self.dedupe_keys: Tuple[str, ...] = tuple(dedupe_config.get("keys", []) or ())
        self.deduplication_key_fields = self.dedupe_keys  # alias
        self.validator = None  # placeholder for potential future validator object
        self.allow_required_nulls = bool((xcsv_extension_block.get("nulls") or {}))

    def _required_ok(self, row: Row) -> bool:
        """Check whether required columns are satisfied.

        A required column passes if it is absent from the row header (header-level
        omission tolerated) or present with a non-empty, non-null value; unless
        the ``x-csv.nulls`` extension allows nulls.

        :param row: Row under evaluation.
        :return: ``True`` if row satisfies required constraints, else ``False``.
        """
        if not self.required_field_names:
            return True
        for required_field_name in self.required_field_names:
            if required_field_name not in row:
                # Header missing that column altogether — treated as pass.
                continue
            required_field_value = row.get(required_field_name)
            if required_field_value is None or (isinstance(required_field_value, str) and required_field_value.strip() == ""):
                if self.allow_required_nulls:
                    continue
                return False
        return True

    def _process(self, input_rows: Iterable[Row]) -> Iterable[RowResult]:
        """Apply preprocessors, required-field checks, and optional dedupe.

        :param input_rows: Iterable of raw row dicts.
        :yield: ``RowResult`` containing either a clean row or an error.
        """
        seen_deduplication_key_tuples = set()
        for current_row in input_rows:
            try:
                for preprocessor in self.preprocessors:
                    current_row = preprocessor.apply(current_row)
                if not self._required_ok(current_row):
                    raise ValueError("missing required field")
                if self.deduplication_key_fields:
                    deduplication_key_tuple = tuple(current_row.get(key) for key in self.deduplication_key_fields)
                    if deduplication_key_tuple in seen_deduplication_key_tuples:
                        current_row["__forklift_skip__"] = True
                        yield RowResult(row=current_row, error=None)
                        continue
                    seen_deduplication_key_tuples.add(deduplication_key_tuple)
                yield RowResult(row=current_row, error=None)
            except Exception as exc:  # pragma: no cover - defensive
                yield RowResult(row=current_row, error=exc)

    def run(self, source: str, dest: str) -> None:
        """Execute ingest → preprocess → output pipeline.

        Reads tables from the input plugin, augments each row with ``_table`` and
        streams them through processing; accepted rows are written, failures are
        quarantined.

        :param source: Input location (filepath, connection string, etc.).
        :param dest: Output destination path.
        """
        header_override = None
        x_csv_config = (self.schema or {}).get("x-csv") or {}
        header_config = x_csv_config.get("header") or {}
        if header_config.get("mode") == "provided":
            header_override = header_config.get("columns")

        input_plugin = self.Input(source, header_override=header_override, **self.input_opts)
        output_plugin = self.Output(dest, schema=self.schema, **self.output_opts)

        output_plugin.open()
        try:
            for table_descriptor in input_plugin.get_tables():
                table_name = table_descriptor["name"]
                for row_result in self._process(
                        (dict(row, _table=table_name) for row in table_descriptor["rows"])):
                    if row_result.error is None:
                        output_plugin.write(row_result.row)
                    else:
                        output_plugin.quarantine(row_result)
        finally:
            output_plugin.close()
