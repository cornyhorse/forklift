"""CSV input adapter.

Streaming wrapper around :func:`polars.read_csv` with placeholders / early
implementations for:
- schema handling modes (accept / infer / enforce)
- header detection strategies (enforce mode): header, firstcol, regex
- processing modes: atomic (full load) and chunk (streaming, single pass)
- delimiter alias passthrough (``delimiter`` -> ``separator``)
- forklift schema integration (CsvSchemaImporter) with conflict detection

Chunk implementation (streaming)
--------------------------------
Reads the file once linearly, slicing it into logical chunks of at most
``chunk_size`` data rows. Each chunk is parsed independently by Polars using
an in-memory buffer (``io.StringIO``). This avoids O(n^2) rescans present in
the earlier naïve implementation but still copies text into memory per chunk.
Future optimization could use a lower-level streaming parser.

Header detection (enforce mode)
-------------------------------
If ``schema_mode='enforce'`` and ``header_comment_detection_mode`` is:
  * header  : first non-empty line is header
  * firstcol: same as header for now (placeholder for future refinement)
  * regex   : use option ``header_regex`` (compiled) to match first column; the
               first line whose first column matches is the header. Lines prior
               to the detected header become *bad rows*.
  * off     : treated as 'header' internally.

Bad rows
--------
If ``collect_bad_rows=True`` is passed, lines classified as bad (currently only
pre-header lines in enforce mode w/ regex / firstcol) are stored in
``CsvReader.last_bad_rows`` as raw text lines (without trailing newlines).

Limitations
-----------
* Dtype stabilization in infer mode: first chunk infers schema; subsequent
  chunks are cast to those dtypes where possible.
* Header detection logic is intentionally minimal and will evolve.
"""
from __future__ import annotations
from typing import Dict, Any, Literal, List, Optional
import polars as pl
import io, re

from ..schema.csv_schema_importer import CsvSchemaImporter
from .csv_header import stability_scan_skip_rows, atomic_regex_skip_rows
from .csv_chunk import parse_chunk_text

SchemaMode = Literal["accept", "infer", "enforce"]
HeaderDetectionMode = Literal["header", "firstcol", "regex", "off"]
ProcessingMode = Literal["atomic", "chunk"]


class CsvReader:
    """CSV reader with streaming chunk support and simple header detection."""

    def __init__(self, default_has_header: bool = True, default_encoding: str = "utf-8-sig") -> None:
        self.default_has_header = default_has_header
        self.default_encoding = default_encoding
        self.last_bad_rows: List[str] = []
        self.last_forklift_schema: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    def get_bad_rows(self) -> List[str]:  # pragma: no cover - trivial accessor
        return list(self.last_bad_rows)

    # ------------------------------------------------------------------
    def read(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
        """Public entrypoint – orchestrates read flow (atomic or chunk).

        Behavior preserved from original monolithic implementation; logic is now
        decomposed into focused helpers for schema integration, option prep,
        mode validation, optional pre-scans, and final dispatch.
        """
        # 1. Forklift schema integration & merge
        importer, options = self._integrate_forklift_schema(options)

        # 2. Provided header columns (from schema importer) retained separately
        provided_header_cols = options.pop("_provided_header_columns", None)

        # 3. Normalize delimiter alias & establish encoding early
        separator = self._apply_delimiter_alias(options)
        encoding = options.get("encoding", self.default_encoding)

        # 4. Stability scan (special schema extension) may set skip_rows
        self._maybe_apply_stability_scan(importer, source_path, encoding, options)

        # 5. Extract core modes & parameters
        (processing_mode,
         chunk_size,
         schema_mode,
         header_mode,
         collect_bad,
         has_header,
         encoding,
         header_regex) = self._extract_core_parameters(options, encoding)

        # 6. Validate mode combinations
        self._validate_mode_combinations(schema_mode, header_mode)

        # 7. Atomic regex header prescan (sets skip_rows) if needed
        self._maybe_atomic_regex_prescan(
            processing_mode=processing_mode,
            schema_mode=schema_mode,
            header_mode=header_mode,
            header_regex=header_regex,
            has_header=has_header,
            source_path=source_path,
            encoding=encoding,
            separator=separator,
            options=options,
        )

        # 8. Dispatch by processing mode
        if processing_mode == "atomic":
            df = self._read_atomic(
                source_path=source_path,
                options=options,
                schema_mode=schema_mode,
                has_header=has_header,
                header_mode=header_mode,
            )
        else:
            # Prepare base_options for streaming (avoid duplicate has_header)
            base_options = dict(options)
            base_options.pop("has_header", None)
            df = self._read_chunked_stream(
                source_path=source_path,
                base_options=base_options,
                schema_mode=schema_mode,
                has_header=has_header,
                header_mode=header_mode,
                chunk_size=chunk_size,
                separator=separator,
                collect_bad_rows=collect_bad,
                header_regex=header_regex,
                encoding=encoding,
            )

        # 9. Apply provided header column rename/drop if present
        if provided_header_cols:
            df = self._apply_provided_header(df, provided_header_cols)
        return df

    # ------------------------------------------------------------------
    # Helper decomposition of original read logic
    def _integrate_forklift_schema(self, options: Dict[str, Any]):
        """Handle forklift_schema option merge & conflict detection.

        Returns (importer_or_None, merged_options)
        """
        forklift_schema_obj = options.pop("forklift_schema", None)
        if forklift_schema_obj is None:
            self.last_forklift_schema = None
            return None, options
        importer = CsvSchemaImporter(forklift_schema_obj)
        self.last_forklift_schema = importer.as_dict()
        derived_schema_options: Dict[str, Any] = importer.derive_reader_options()
        # Conflict detection with user explicit structural params
        conflict_keys = {k for k in options.keys() if k in {"columns", "new_columns", "dtypes", "schema"}}
        if conflict_keys:
            raise ValueError(
                "Cannot supply both forklift_schema and explicit column structure arguments: "
                + ", ".join(sorted(conflict_keys))
            )
        # Merge: importer derived first, then user overrides (user precedence)
        merged: Dict[str, Any] = {**derived_schema_options, **options}
        return importer, merged

    def _apply_delimiter_alias(self, options: Dict[str, Any]) -> str:
        delimiter = options.pop("delimiter", None)
        if delimiter and "separator" not in options:
            options["separator"] = delimiter
        return options.get("separator", ",")

    def _maybe_apply_stability_scan(self, importer, source_path: str, encoding: str, options: Dict[str, Any]) -> None:
        if importer is None:
            return
        header_cfg = importer.csv_ext.get("header") if isinstance(importer.csv_ext, dict) else None
        if (
            isinstance(header_cfg, dict)
            and header_cfg.get("mode") == "stability_scan"
            and "skip_rows" not in options
        ):
            options["skip_rows"] = stability_scan_skip_rows(
                source_path, encoding=encoding, keywords=header_cfg.get("keywords")
            ) or options.get("skip_rows", 0)

    def _extract_core_parameters(self, options: Dict[str, Any], encoding: str):
        has_header = options.pop("has_header", self.default_has_header)
        options["has_header"] = has_header
        # Normalize encoding handling (pop to ensure explicit entry preserved)
        encoding = options.pop("encoding", encoding)
        options["encoding"] = encoding

        processing_mode: ProcessingMode = options.pop("processing_mode", "atomic")  # type: ignore[assignment]
        chunk_size = int(options.pop("chunk_size", 50_000))
        if processing_mode == "chunk" and chunk_size <= 0:
            raise ValueError("chunk_size must be > 0 for processing_mode='chunk'")
        if processing_mode not in ("atomic", "chunk"):
            raise ValueError("processing_mode must be 'atomic' or 'chunk'")

        schema_mode: SchemaMode = options.pop("schema_mode", "accept")  # type: ignore[assignment]
        header_mode: HeaderDetectionMode = options.pop("header_comment_detection_mode", "off")  # type: ignore[assignment]
        collect_bad = bool(options.pop("collect_bad_rows", False))
        header_regex_pattern: Optional[str] = options.pop("header_regex", None)
        header_regex = (
            re.compile(header_regex_pattern)
            if header_regex_pattern and header_mode == "regex"
            else None
        )
        return (
            processing_mode,
            chunk_size,
            schema_mode,
            header_mode,
            collect_bad,
            has_header,
            encoding,
            header_regex,
        )

    def _validate_mode_combinations(self, schema_mode: SchemaMode, header_mode: HeaderDetectionMode) -> None:
        if schema_mode in ("accept", "infer") and header_mode != "off":
            raise ValueError("header_comment_detection_mode only supported with schema_mode='enforce'")

    def _maybe_atomic_regex_prescan(
        self,
        *,
        processing_mode: ProcessingMode,
        schema_mode: SchemaMode,
        header_mode: HeaderDetectionMode,
        header_regex,
        has_header: bool,
        source_path: str,
        encoding: str,
        separator: str,
        options: Dict[str, Any],
    ) -> None:
        if (
            processing_mode == "atomic"
            and schema_mode == "enforce"
            and header_mode == "regex"
            and header_regex is not None
            and has_header
            and "skip_rows" not in options
        ):
            skip_found = atomic_regex_skip_rows(
                source_path, encoding=encoding, regex=header_regex, separator=separator
            )
            if skip_found:
                options["skip_rows"] = skip_found

    def _read_atomic(
        self,
        *,
        source_path: str,
        options: Dict[str, Any],
        schema_mode: SchemaMode,
        has_header: bool,
        header_mode: HeaderDetectionMode,
    ) -> pl.DataFrame:
        if schema_mode == "accept":
            df = self._schema_accept(source_path, options)
        elif schema_mode == "infer":
            df = self._schema_infer(source_path, options)
        elif schema_mode == "enforce":
            df = self._schema_enforce(
                source_path, options, has_header=has_header, header_mode=header_mode
            )
        else:  # Defensive (already validated elsewhere)
            raise ValueError(
                f"Unknown schema_mode '{schema_mode}'. Expected one of: accept, infer, enforce"
            )
        return df

    # ------------------------------------------------------------------
    def _read_chunked_stream(
        self,
        *,
        source_path: str,
        base_options: Dict[str, Any],
        schema_mode: SchemaMode,
        has_header: bool,
        header_mode: HeaderDetectionMode,
        chunk_size: int,
        separator: str,
        collect_bad_rows: bool,
        header_regex,
        encoding: str,
    ) -> pl.DataFrame:
        self.last_bad_rows.clear()
        enforce = schema_mode == "enforce"
        if enforce:
            if not has_header and header_mode != "off":
                raise ValueError("Cannot perform header detection when has_header is False")
            if header_mode == "off":
                header_mode = "header"
            if header_mode not in ("header", "firstcol", "regex"):
                raise ValueError(
                    f"Unknown header detection mode '{header_mode}'. Expected one of: header, firstcol, regex, off"
                )

        dataframes: List[pl.DataFrame] = []
        dtype_map = None  # For infer stabilization
        header_columns: Optional[List[str]] = None
        header_found = False

        # Read file streaming
        with open(source_path, "r", encoding=encoding, newline="") as f:
            buffer_lines: List[str] = []
            data_rows_in_buffer = 0

            def flush_chunk():
                nonlocal buffer_lines, data_rows_in_buffer, dtype_map, header_columns
                if data_rows_in_buffer == 0:
                    buffer_lines = []
                    return
                text = "".join(buffer_lines)
                df_chunk, dtype_map = parse_chunk_text(
                    text,
                    schema_mode=schema_mode,
                    dtype_map=dtype_map,
                    header_columns=header_columns,
                    has_header=has_header,
                    header_found=header_found,
                    base_options=base_options,
                )
                dataframes.append(df_chunk)
                buffer_lines = []
                data_rows_in_buffer = 0

            while True:
                line = f.readline()
                if not line:  # EOF
                    flush_chunk()
                    break
                if not header_found:
                    # Header detection phase (only first segment until header discovered)
                    candidate = line.rstrip("\n")
                    if enforce and header_mode == "regex" and header_regex is not None:
                        first_field = candidate.split(separator, 1)[0]
                        if header_regex.search(first_field):
                            header_columns = candidate.split(separator)
                            header_found = True
                            continue  # header consumed, do not treat as data row
                        else:
                            if collect_bad_rows:
                                self.last_bad_rows.append(candidate)
                            continue  # skip accumulating until header found
                    else:
                        # header, firstcol, or off->header (treated same currently)
                        if has_header:
                            header_columns = candidate.split(separator)
                            header_found = True
                            continue
                        else:
                            # No header expected; mark as data row
                            header_found = True  # Avoid repeating branch
                            # fall through to treat line as data
                # Data accumulation
                buffer_lines.append(line)
                data_rows_in_buffer += 1
                if data_rows_in_buffer >= chunk_size:
                    flush_chunk()
        if not dataframes:
            # Empty file / no data rows => create empty frame
            if header_columns:
                return pl.DataFrame({col: [] for col in header_columns})
            return pl.DataFrame()
        if len(dataframes) == 1:
            return dataframes[0]
        return pl.concat(dataframes, how="vertical_relaxed")

    # ------------------------------------------------------------------
    def _schema_accept(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
        # Production path: no debug prints
        options.pop('infer_schema', None)
        return pl.read_csv(source_path, **options)

    def _schema_infer(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover
        return pl.read_csv(source_path, infer_schema=True, **options)

    def _schema_enforce(self, source_path: str, options: Dict[str, Any], *, has_header: bool, header_mode: HeaderDetectionMode) -> pl.DataFrame:
        if not has_header and header_mode != "off":
            raise ValueError("Cannot perform header detection when has_header is False")
        if header_mode == "off":
            header_mode = "header"
        if header_mode not in ("header", "firstcol", "regex"):
            raise ValueError(
                f"Unknown header detection mode '{header_mode}'. Expected one of: header, firstcol, regex, off"
            )
        return pl.read_csv(source_path, infer_schema=False, **options)

    def _apply_provided_header(self, df: pl.DataFrame, provided_cols: List[str]) -> pl.DataFrame:
        # Rename first n columns and drop extras beyond provided count
        if len(df.columns) < len(provided_cols):
            raise ValueError("Parsed columns fewer than provided header columns")
        rename_map = {df.columns[i]: provided_cols[i] for i in range(len(provided_cols))}
        df = df.rename(rename_map)
        if len(df.columns) > len(provided_cols):
            extras = df.columns[len(provided_cols):]
            df = df.drop(extras)
        return df

    # Deprecated placeholder retained
    def _detect_header_comments(self, mode: HeaderDetectionMode, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover
        return None

# Singleton instance used by engine dynamic registration
_csv_reader = CsvReader()

def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    return _csv_reader.read(source_path, options)

__all__ = ["read", "CsvReader"]
