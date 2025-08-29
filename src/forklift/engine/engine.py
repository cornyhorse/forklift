from __future__ import annotations
from typing import Iterable, Any, Dict, Tuple, List

from .registry import get_input_cls, get_output_cls, get_preprocessors
from ..utils.sql_include import derive_sql_include_patterns
from ..types import Row, RowResult

import polars as pl  # mandatory now


class Engine:
    def __init__(
            self,
            input_kind: str,
            output_kind: str,
            schema: Dict[str, Any] | None = None,
            preprocessors: list[str] | None = None,
            header_mode: str = "auto",  # "present", "absent", "auto"
            processing_chunk_size: int = 50_000,
            output_mode: str = "vectorized",
            output_chunk_size: int = 50_000,
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
        # configure output opts for parquet (and ignore silently for others)
        self.output_opts: Dict[str, Any] = {}
        if output_kind == "parquet":
            self.output_opts["mode"] = output_mode
            self.output_opts["chunk_size"] = output_chunk_size

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
        self.processing_chunk_size = processing_chunk_size

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

    def _apply_preprocessors_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply preprocessors in order on a Polars DataFrame.

        Row-level preprocessors (without process_dataframe) are applied by iterating rows
        then re-materializing a DataFrame to keep pipeline generic, though current design
        expects TypeCoercion only (DataFrame path).
        """
        self._row_level_errors = {}
        self._vectorized_errors = []  # list[(row_dict, exc)] from vectorized preprocessors
        for pre in self.preprocessors:
            if hasattr(pre, "process_dataframe"):
                df = pre.process_dataframe(df)  # type: ignore[attr-defined]
                if hasattr(pre, "_df_errors"):
                    self._vectorized_errors.extend(getattr(pre, "_df_errors"))  # type: ignore[arg-type]
            else:
                new_rows: List[Dict[str, Any]] = []
                for idx, row in enumerate(df.to_dicts()):
                    try:
                        new_rows.append(pre.apply(row))  # type: ignore[attr-defined]
                    except Exception as exc:
                        new_rows.append(row)
                        self._row_level_errors[idx] = exc
                df = pl.DataFrame(new_rows)
        return df

    def _process_dataframe_rows(self, df: pl.DataFrame, table_name: str, seen_keys: set) -> Iterable[RowResult]:
        # First emit vectorized errors (dropped rows)
        if hasattr(self, "_vectorized_errors"):
            for orig_row, exc in getattr(self, "_vectorized_errors"):
                # ensure _table for quarantine context
                r = dict(orig_row)
                r["_table"] = table_name
                yield RowResult(row=r, error=exc)
        for idx, row in enumerate(df.to_dicts()):
            row["_table"] = table_name
            # If a row-level preprocessor error recorded, emit quarantine directly
            if hasattr(self, "_row_level_errors") and idx in self._row_level_errors:
                yield RowResult(row=row, error=self._row_level_errors[idx])
                continue
            try:
                if not self._required_ok(row):
                    raise ValueError("missing required field")
                if self.deduplication_key_fields:
                    key_tuple = tuple(row.get(k) for k in self.deduplication_key_fields)
                    if key_tuple in seen_keys:
                        row["__forklift_skip__"] = True
                        yield RowResult(row=row, error=None)
                        continue
                    seen_keys.add(key_tuple)
                yield RowResult(row=row, error=None)
            except Exception as exc:
                yield RowResult(row=row, error=exc)

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
                buffer: List[Dict[str, Any]] = []
                seen_keys: set = set()
                for row in table_descriptor["rows"]:
                    buffer.append(dict(row))
                    if len(buffer) >= self.processing_chunk_size:
                        df = pl.DataFrame(buffer)
                        df = self._apply_preprocessors_dataframe(df)
                        for rr in self._process_dataframe_rows(df, table_name, seen_keys):
                            if rr.error is None:
                                output_plugin.write(rr.row)
                            else:
                                output_plugin.quarantine(rr)
                        buffer.clear()
                if buffer:
                    df = pl.DataFrame(buffer)
                    df = self._apply_preprocessors_dataframe(df)
                    for rr in self._process_dataframe_rows(df, table_name, seen_keys):
                        if rr.error is None:
                            output_plugin.write(rr.row)
                        else:
                            output_plugin.quarantine(rr)
        finally:
            output_plugin.close()
