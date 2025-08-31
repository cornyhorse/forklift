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
        # Extract forklift schema if provided
        forklift_schema_obj = options.pop("forklift_schema", None)
        derived_schema_options: Dict[str, Any] = {}
        importer = None
        if forklift_schema_obj is not None:
            importer = CsvSchemaImporter(forklift_schema_obj)
            self.last_forklift_schema = importer.as_dict()
            derived_schema_options = importer.derive_reader_options()
            # Conflict detection: user-specified structural params
            conflict_keys = {k for k in options.keys() if k in {"columns", "new_columns", "dtypes", "schema"}}
            if conflict_keys:
                raise ValueError(
                    "Cannot supply both forklift_schema and explicit column structure arguments: "
                    + ", ".join(sorted(conflict_keys))
                )
            # Merge: importer derived first, then user overrides (user precedence)
            merged: Dict[str, Any] = {**derived_schema_options, **options}
            options = merged
        else:
            self.last_forklift_schema = None

        # delimiter alias -> separator (non-destructive if separator already set)
        delimiter = options.pop("delimiter", None)
        if delimiter and "separator" not in options:
            options["separator"] = delimiter
        separator = options.get("separator", ",")

        # Establish encoding early (may be needed for header scan)
        encoding = options.get("encoding", self.default_encoding)

        # Header stability scan (schema extension header.mode == 'stability_scan')
        if importer is not None:
            header_cfg = importer.csv_ext.get("header") if isinstance(importer.csv_ext, dict) else None
            if isinstance(header_cfg, dict) and header_cfg.get("mode") == "stability_scan" and "skip_rows" not in options:
                keywords = header_cfg.get("keywords") or []
                lowered_keywords = [k.lower() for k in keywords if isinstance(k, str)]
                skip_rows_calc = 0
                try:
                    with open(source_path, "r", encoding=encoding) as fscan:
                        while True:
                            pos = fscan.tell()
                            line = fscan.readline()
                            if not line:
                                break
                            stripped = line.strip("\n")
                            if stripped.startswith("#"):
                                skip_rows_calc += 1
                                continue
                            # Determine if this looks like header: either contains all keywords or first non-comment line if no keywords
                            header_line_lower = stripped.lower()
                            if not lowered_keywords or all(kw in header_line_lower for kw in lowered_keywords):
                                # Found header line; do not skip it (polars will parse it as header)
                                break
                            else:
                                # Not a header yet; treat as comment/skip (defensive)
                                skip_rows_calc += 1
                    if skip_rows_calc > 0:
                        options["skip_rows"] = skip_rows_calc
                except OSError:
                    pass  # If file can't be read, fall back silently

        has_header = options.pop("has_header", self.default_has_header)
        options["has_header"] = has_header
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
        header_regex = re.compile(header_regex_pattern) if header_regex_pattern and header_mode == "regex" else None

        if schema_mode in ("accept", "infer") and header_mode != "off":
            raise ValueError("header_comment_detection_mode only supported with schema_mode='enforce'")

        if processing_mode == "atomic":
            if schema_mode == "accept":
                return self._schema_accept(source_path, options)
            if schema_mode == "infer":
                return self._schema_infer(source_path, options)
            if schema_mode == "enforce":
                return self._schema_enforce(source_path, options, has_header=has_header, header_mode=header_mode)
            raise ValueError(f"Unknown schema_mode '{schema_mode}'. Expected one of: accept, infer, enforce")

        # Streaming chunk mode
        return self._read_chunked_stream(
            source_path=source_path,
            base_options=options,
            schema_mode=schema_mode,
            has_header=has_header,
            header_mode=header_mode,
            chunk_size=chunk_size,
            separator=separator,
            collect_bad_rows=collect_bad,
            header_regex=header_regex,
            encoding=encoding,
        )

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

            def flush_chunk(final=False):
                nonlocal buffer_lines, data_rows_in_buffer, dtype_map, header_columns
                if data_rows_in_buffer == 0:
                    buffer_lines = []
                    return
                text = "".join(buffer_lines)
                sio = io.StringIO(text)
                # Determine parsing flags
                if header_columns is None:
                    # First chunk with header line included if has_header
                    parse_has_header = has_header and header_found
                    parse_new_columns = None
                else:
                    parse_has_header = False
                    parse_new_columns = header_columns
                df = pl.read_csv(
                    sio,
                    has_header=parse_has_header,
                    new_columns=parse_new_columns,
                    infer_schema=(schema_mode == "infer" and dtype_map is None),
                    **base_options,
                )
                if dtype_map is None and schema_mode == "infer":
                    dtype_map = {c: dt for c, dt in zip(df.columns, df.dtypes)}
                elif dtype_map is not None and schema_mode == "infer":
                    # Cast columns to stabilized dtypes
                    casts = []
                    for c, dt in dtype_map.items():
                        if c in df.columns and df[c].dtype != dt:
                            try:
                                casts.append(pl.col(c).cast(dt))
                            except Exception:
                                pass
                    if casts:
                        df = df.with_columns(casts)
                dataframes.append(df)
                buffer_lines = []
                data_rows_in_buffer = 0

            line_number = 0
            while True:
                line = f.readline()
                if not line:  # EOF
                    flush_chunk(final=True)
                    break
                line_number += 1
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

    # Deprecated placeholder retained
    def _detect_header_comments(self, mode: HeaderDetectionMode, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover
        return None

# Singleton instance used by engine dynamic registration
_csv_reader = CsvReader()

def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    return _csv_reader.read(source_path, options)

__all__ = ["read", "CsvReader"]
