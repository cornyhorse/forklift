"""The tools of the local MCP server, as plain methods (the MCP glue is in ``server.py``).

Every tool runs the engine through :func:`forklift.jobs.run_job`, the job contract the service's
workers use, so a result means the same here as from the remote server:

- ``preview``, ``validate_schema`` and ``generate_schema`` copy the beginning of the input (a CSV
  file's first records, an Excel workbook whole) into a private temporary directory and run the
  job there (:mod:`.staging`). Nothing is written inside the roots, so they also work with
  ``--read-only``.
- ``run_job`` runs the import in place (input and output inside one root) and writes the result,
  ``forklift-job.json``, next to the outputs, where ``get_job``, ``explain_failure`` and
  ``get_bad_rows`` read it later. With ``overwrite`` it replaces only an earlier run's own
  files, and only once the engine accepted the new job (:mod:`.previous`).

Results are JSON objects with bounded sizes; the bounds that applied are in their ``caps``. A
refused call raises :class:`~forklift.mcp.workspace.ToolInputError`. A job that ran and failed
is not an error of the tool: its result says ``status: failed`` with the engine's error code.
"""

from __future__ import annotations

import codecs
import contextlib
import datetime
import json
import os
import secrets
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..engine.processors.extensions import REASON_COLUMN
from ..engine.processors.text_utils import sanitize_arrow_error
from ..jobs import ContractError, JobResult, JobSpec, run_job
from ..jobs.spec import InputOptions
from . import explain, previous
from .docs import URI_PREFIX, schema_pages
from .staging import Staged, scratch_directory, stage
from .values import (
    MAX_CELL_CHARS,
    MAX_NAME_CHARS,
    ByteBudget,
    RowCollector,
    bounded_names,
    compact_size,
    iter_parquet_rows,
    name,
    parquet_columns,
    parquet_row_count,
    rejection_reasons,
    split_finding,
)
from .workspace import ToolInputError, Workspace

#: The file run_job writes next to the outputs of a run
JOB_FILE = "forklift-job.json"
JOB_FILE_FORMAT = "forklift-mcp-job/1"

FORMATS = ("csv", "excel")
_EXTENSIONS = {
    ".csv": "csv",
    ".tsv": "csv",
    ".tab": "csv",
    ".txt": "csv",
    ".xlsx": "excel",
    ".xlsm": "excel",
    ".xls": "excel",
}
_TAB_SEPARATED = (".tsv", ".tab")

MIN_BYTES = 1024
DEFAULT_DOCUMENT_BYTES = 65536
MAX_DOCUMENT_BYTES = 1048576
DEFAULT_ROWS_BYTES = 65536
MAX_ROWS_BYTES = 1048576
DEFAULT_PREVIEW_ROWS = 20
MAX_PREVIEW_ROWS = 500
DEFAULT_SAMPLE_ROWS = 1000
MAX_SAMPLE_ROWS = 100_000
DEFAULT_BAD_ROWS = 20
MAX_BAD_ROWS = 200
DEFAULT_EXPLAIN_ROWS = 10
MAX_EXPLAIN_ROWS = 50
#: Columns of one row view (previews, bad rows)
MAX_COLUMNS = 100
#: Entries of a column list (schema summaries, headers)
MAX_LISTED_COLUMNS = 500
#: Bytes all the column lists of a validate_schema result share
LISTS_MAX_BYTES = 65536
#: Warnings, artifacts, extensions and other short lists in a result
MAX_LIST_ITEMS = 50
#: Longest warning in a result
MAX_MESSAGE_CHARS = 2000
#: Entries of validation_summary in a result (the most frequent)
MAX_FINDINGS = 100
#: Rejection reasons in a result (the most frequent), and bad rows read to count them
MAX_REASONS = 20
MAX_REASON_SCAN_ROWS = 5_000_000
#: Largest schema or job file read
MAX_SCHEMA_FILE_BYTES = 8 * 1024 * 1024
MAX_JOB_FILE_BYTES = 16 * 1024 * 1024
#: preview.json the engine may write for a raw preview (the result is cut to max_bytes after the
#: columns are cut to MAX_COLUMNS, so a wide file still shows rows)
ENGINE_PREVIEW_BYTES = 16 * 1024 * 1024
#: Rejected rows of a validate_schema sample shown in its result
VALIDATE_REJECTED_ROWS = 10
#: Rows of the header search the engine does by default (input_options.header_search_rows)
_HEADER_SEARCH_ROWS = 10
_VALIDATE_NOTE = (
    "validate_schema checks a temporary copy of the input's first rows and keeps no files: the "
    "first rows the sample rejected are in rejected_rows (run_job keeps a bad_rows.parquet); "
    "preview with the schema shows the typed rows and why rows are rejected"
)
#: What messages say instead of the paths of the files of a temporary copy
_SAMPLE_BAD_ROWS = "rejected_rows of this result (the first of them; run_job keeps the file)"
_PREVIEW_BAD_ROWS = (
    "sample.rejection_reasons of this result (the rows a preview rejects are not kept)"
)
_PREVIEW_DATA = "the rows of this result (a preview's data file is not kept)"
_THRESHOLD_NOTE = (
    "x-validation's bad-rows threshold is not applied to a preview (a share of rejected rows is "
    "judged on a whole input): rejected rows are counted in sample.rejected_rows instead"
)
_WORKBOOK_NOTE = (
    "A typed preview of a workbook imports the private copy of every sheet the schema selects; "
    "this schema's x-excel does not say which rows, so all of them are read"
)


class Toolbox:
    """The tools, bound to a :class:`Workspace` (the roots and ``--read-only``)."""

    def __init__(self, workspace: Workspace):
        self.workspace = workspace

    # ================================================================== schemas

    def get_schema(self, path: str, max_bytes: int = DEFAULT_DOCUMENT_BYTES) -> dict[str, Any]:
        """A schema file: a summary of its columns and extensions, and the document."""
        _check_range("max_bytes", max_bytes, MIN_BYTES, MAX_DOCUMENT_BYTES)
        real = self.workspace.input_file(path, "path")
        document = self._read_json(real, "path", path, MAX_SCHEMA_FILE_BYTES)
        if not isinstance(document, dict):
            raise ToolInputError(f"path {path!r} is not a schema: its JSON is not an object")
        return {
            "path": self.workspace.display(real),
            "bytes": real.stat().st_size,
            "summary": summarize_schema(document, max_bytes),
            **bounded_document(document, max_bytes),
            "caps": _schema_caps(max_bytes),
        }

    def generate_schema(
        self,
        input_path: str,
        format: Optional[str] = None,
        input_options: Optional[Dict[str, Any]] = None,
        sample_rows: int = DEFAULT_SAMPLE_ROWS,
        infer_primary_key: bool = False,
        output_path: Optional[str] = None,
        overwrite: bool = False,
        dry_run: bool = False,
        max_bytes: int = DEFAULT_DOCUMENT_BYTES,
    ) -> dict[str, Any]:
        """Infer a schema from the first rows of a local file; optionally write it."""
        _check_range("sample_rows", sample_rows, 1, MAX_SAMPLE_ROWS)
        _check_range("max_bytes", max_bytes, MIN_BYTES, MAX_DOCUMENT_BYTES)
        source, fmt, options = self._input(input_path, format, input_options)
        target = None
        if output_path is not None:
            target = self.workspace.output_file(output_path, "output_path", overwrite)
            if not dry_run:
                self.workspace.require_writable("generate_schema with output_path")
        job_options = {"sample_rows": sample_rows, "infer_primary_key": infer_primary_key}
        with scratch_directory() as scratch:
            # The generator reads the first row as the header, then sample_rows rows
            staged = self._stage(source, fmt, scratch, options, sample_rows + 2)
            result = self._run_interactive(
                "generate_schema", staged, scratch, options, options_=job_options
            )
            schema = _read_artifact(scratch, result, "schema")
        view: Dict[str, Any] = {
            "status": result.status,
            "error": staged.error(result),
            "input": staged.view,
            "rows_analyzed": result.counts.get("total_rows"),
            **_bounded_warnings(staged.clean_list(result.warnings)),
        }
        if schema is not None:
            view.update(
                summary=summarize_schema(schema, max_bytes), **bounded_document(schema, max_bytes)
            )
        written = False
        if target is not None and schema is not None and not dry_run:
            try:
                _write_text_atomically(target, json.dumps(schema, indent=2, ensure_ascii=False))
            except OSError as error:
                raise ToolInputError(
                    f"output_path {output_path!r} cannot be written: {error.strerror or error}"
                ) from None
            written = True
        view.update(
            output_path=self.workspace.display(target) if target is not None else None,
            written=written,
            dry_run=dry_run,
            caps=dict(
                _schema_caps(max_bytes),
                sample_rows=sample_rows,
                max_sample_rows=MAX_SAMPLE_ROWS,
                warnings=MAX_LIST_ITEMS,
            ),
        )
        return view

    def validate_schema(
        self,
        input_path: str,
        schema: Optional[Dict[str, Any]] = None,
        schema_path: Optional[str] = None,
        format: Optional[str] = None,
        input_options: Optional[Dict[str, Any]] = None,
        sample_rows: int = DEFAULT_SAMPLE_ROWS,
        apply_schema_extensions: bool = True,
    ) -> dict[str, Any]:
        """Check a schema against a CSV file's header and first rows, as the import would."""
        _check_range("sample_rows", sample_rows, 1, MAX_SAMPLE_ROWS)
        document, schema_source = self._schema(schema, schema_path, required=True)
        source, fmt, options = self._input(input_path, format, input_options)
        if fmt != "csv":
            raise ToolInputError(
                "validate_schema checks a schema against a CSV input; for an Excel workbook use "
                "preview with the schema (typed rows of the first rows) or run_job"
            )
        job_options = {
            "sample_rows": sample_rows,
            "apply_schema_extensions": apply_schema_extensions,
        }
        records = (options.header_search_rows or _HEADER_SEARCH_ROWS) + sample_rows + 1
        with scratch_directory() as scratch:
            staged = self._stage(source, fmt, scratch, options, records)
            if staged.records == 0:
                raise ToolInputError(
                    f"input_path {staged.view['path']!r} has no records (it is empty or only "
                    "blank lines): there is no header row to check the schema against"
                )
            result = self._run_interactive(
                "validate_schema", staged, scratch, options, schema=document, options_=job_options
            )
            report = _read_artifact(scratch, result, "report") or {}
            rejected = self._sample_rejections(scratch, result, staged)
        lists: Dict[str, Any] = {}
        budget = ByteBudget(LISTS_MAX_BYTES)
        for key in (
            "columns",
            "schema_columns",
            "required_columns",
            "columns_not_in_schema",
            "schema_columns_not_in_input",
        ):
            values = report.get(key)
            if isinstance(values, list):
                shown, total, _ = bounded_names(values, MAX_LISTED_COLUMNS, budget)
                lists.update({key: shown, f"{key}_total": total})
            else:
                lists.update({key: None, f"{key}_total": None})
        return {
            "valid": bool(report.get("valid")) and result.succeeded,
            "status": result.status,
            "error": staged.error(result),
            "input": staged.view,
            "schema": schema_source,
            **lists,
            "sample_rows": report.get("sample_rows"),
            "counts": dict(result.counts),
            "schema_extensions": list(result.schema_extensions),
            **_bounded_findings(result.validation_summary),
            **_bounded_warnings(staged.clean_list(result.warnings)),
            "rejected_rows": rejected,
            "notes": [] if result.succeeded else [_VALIDATE_NOTE],
            "caps": {
                "sample_rows": sample_rows,
                "max_sample_rows": MAX_SAMPLE_ROWS,
                "listed_columns": MAX_LISTED_COLUMNS,
                "lists_max_bytes": LISTS_MAX_BYTES,
                "max_name_chars": MAX_NAME_CHARS,
                "findings": MAX_FINDINGS,
                "warnings": MAX_LIST_ITEMS,
                "rejected_rows": VALIDATE_REJECTED_ROWS,
                "max_bytes": DEFAULT_ROWS_BYTES,
                "columns": MAX_COLUMNS,
                "reasons": MAX_REASONS,
                "max_cell_chars": MAX_CELL_CHARS,
            },
        }

    def _sample_rejections(
        self, scratch: Path, result: JobResult, staged: Staged
    ) -> Optional[Dict[str, Any]]:
        """The first rows a validate_schema sample rejected, read before the copy is removed.

        The engine keeps them (in the job's directory, here the private copy) when the sample
        fails its bad-rows threshold; messages then point at this result instead of that file.
        """
        staged.name_artifacts(result, bad_rows=_SAMPLE_BAD_ROWS)
        bad = _artifact_path(scratch, result, "bad_rows")
        if bad is None:
            return None
        view = self._rows_view(bad, None, 0, VALIDATE_REJECTED_ROWS, DEFAULT_ROWS_BYTES)
        view.pop("next_offset")
        reasons = rejection_reasons(bad, MAX_REASON_SCAN_ROWS, MAX_REASONS)
        return dict(view, reasons=reasons["reasons"])

    # ================================================================== preview

    def preview(
        self,
        input_path: str,
        rows: int = DEFAULT_PREVIEW_ROWS,
        schema: Optional[Dict[str, Any]] = None,
        schema_path: Optional[str] = None,
        format: Optional[str] = None,
        input_options: Optional[Dict[str, Any]] = None,
        max_bytes: int = DEFAULT_ROWS_BYTES,
    ) -> dict[str, Any]:
        """The first rows: as text (raw), or typed by a schema as the import would write them."""
        _check_range("rows", rows, 1, MAX_PREVIEW_ROWS)
        _check_range("max_bytes", max_bytes, MIN_BYTES, MAX_ROWS_BYTES)
        document, schema_source = self._schema(schema, schema_path, required=False)
        source, fmt, options = self._input(input_path, format, input_options)
        records = (options.header_search_rows or _HEADER_SEARCH_ROWS) + rows + 1
        caps = {
            "rows": rows,
            "max_rows": MAX_PREVIEW_ROWS,
            "max_bytes": max_bytes,
            "max_cell_chars": MAX_CELL_CHARS,
            "columns": MAX_COLUMNS,
            "max_name_chars": MAX_NAME_CHARS,
            "warnings": MAX_LIST_ITEMS,
        }
        with scratch_directory() as scratch:
            staged = self._stage(source, fmt, scratch, options, records)
            if document is None:
                return self._raw_preview(staged, scratch, options, rows, max_bytes, caps)
            return self._typed_preview(
                staged, scratch, options, document, schema_source, rows, max_bytes, caps
            )

    def _raw_preview(
        self,
        staged: Staged,
        scratch: Path,
        options: InputOptions,
        rows: int,
        max_bytes: int,
        caps: Dict[str, Any],
    ) -> Dict[str, Any]:
        job_options = {"preview_rows": rows, "preview_max_bytes": ENGINE_PREVIEW_BYTES}
        result = self._run_interactive("preview", staged, scratch, options, options_=job_options)
        staged.name_artifacts(result)
        preview = _read_artifact(scratch, result, "preview") or {}
        columns, columns_total, names_cut = bounded_names(
            preview.get("columns", []), MAX_COLUMNS, None
        )
        collector = RowCollector(rows, max_bytes)
        for row in preview.get("rows", []):
            if not collector.add(row[: len(columns)]):
                break
        view = collector.view()
        limit = collector.limit or preview.get("limit")
        view.update(
            truncated=limit is not None,
            limit=limit,
            truncated_cells=view["truncated_cells"] + preview.get("truncated_cells", 0),
        )
        result_view = {
            "mode": "raw",
            "status": result.status,
            "error": staged.error(result),
            "input": staged.view,
            "columns": columns,
            "columns_total": columns_total,
            "columns_truncated": len(columns) < columns_total,
            "truncated_names": names_cut,
            **view,
            **_bounded_warnings(staged.clean_list(result.warnings)),
            "caps": caps,
        }
        if "sheet" in preview:
            result_view["sheet"] = name(preview["sheet"])[0]
        return result_view

    def _typed_preview(
        self,
        staged: Staged,
        scratch: Path,
        options: InputOptions,
        document: Dict[str, Any],
        schema_source: Optional[str],
        rows: int,
        max_bytes: int,
        caps: Dict[str, Any],
    ) -> Dict[str, Any]:
        notes = []
        document, relaxed = _without_threshold(document)
        if relaxed:
            notes.append(_THRESHOLD_NOTE)
        if staged.fmt == "excel":
            document, bounded = _first_rows_of_sheets(document, rows)
            if not bounded:
                notes.append(_WORKBOOK_NOTE)
        result = self._run_interactive("run", staged, scratch, options, schema=document)
        staged.name_artifacts(result, bad_rows=_PREVIEW_BAD_ROWS, data=_PREVIEW_DATA)
        data = _artifact_path(scratch, result, "data")
        collector = RowCollector(rows, max_bytes)
        columns: List[Dict[str, str]] = []
        columns_total = names_cut = 0
        if data is not None:
            available = parquet_columns(data)
            columns_total = len(available)
            for column in available[:MAX_COLUMNS]:
                column_name, cut_name = name(column["name"])
                column_type, cut_type = name(column["type"])
                columns.append({"name": column_name, "type": column_type})
                names_cut += cut_name + cut_type
            selected = [column["name"] for column in available[:MAX_COLUMNS]]
            for row in iter_parquet_rows(data, selected, 0, rows + 1):
                if not collector.add(row):
                    break
        bad = _artifact_path(scratch, result, "bad_rows")
        reasons = rejection_reasons(bad, MAX_REASON_SCAN_ROWS, MAX_REASONS) if bad else None
        caps.update(reasons=MAX_REASONS)
        return {
            "mode": "typed",
            "status": result.status,
            "error": staged.error(result),
            "input": staged.view,
            "schema": schema_source,
            "columns": columns,
            "columns_total": columns_total,
            "columns_truncated": len(columns) < columns_total,
            "truncated_names": names_cut,
            **collector.view(),
            "sample": {
                "rows_read": result.counts.get("total_rows", 0),
                "rejected_rows": result.counts.get("invalid_rows", 0),
                "rejection_reasons": reasons["reasons"] if reasons else [],
            },
            "schema_extensions": list(result.schema_extensions),
            **_bounded_findings(result.validation_summary),
            **_bounded_warnings(staged.clean_list(result.warnings)),
            "notes": notes,
            "caps": caps,
        }

    # ================================================================== runs

    def run_job(
        self,
        input_path: str,
        output_dir: str,
        schema: Optional[Dict[str, Any]] = None,
        schema_path: Optional[str] = None,
        format: Optional[str] = None,
        input_options: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        limits: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        overwrite: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Apply a schema: import a local file into Parquet files in an output directory."""
        document, schema_source = self._schema(schema, schema_path, required=False)
        source, fmt, input_opts = self._input(input_path, format, input_options)
        target = self.workspace.output_dir(output_dir, "output_dir", overwrite)
        base = _job_base(self.workspace, source, target)
        spec = _build_spec(
            kind="run",
            job_id=job_id or _new_job_id("run"),
            fmt=fmt,
            input_path=source.relative_to(base).as_posix(),
            input_options=input_opts.to_dict(),
            schema=document,
            output_path=target.relative_to(base).as_posix() + "/",
            options=options,
            limits=limits,
        )
        _check_encoding(input_opts)
        earlier = self._previous_run(target, source) if overwrite else None
        plan = {
            "job_id": spec.job_id,
            "input": {"path": self.workspace.display(source), "format": fmt},
            "output_dir": self.workspace.display(target),
            "result_file": self.workspace.display(target / JOB_FILE),
            "schema": schema_source,
            "overwrite": overwrite,
            "previous_run": self._previous_view(earlier),
        }
        if dry_run:
            return {
                "dry_run": True,
                "would_run": plan,
                "writable": not self.workspace.read_only,
                "spec": _spec_summary(spec),
            }
        self.workspace.require_writable("run_job")
        with self.workspace.claim(target, "output_dir"):
            # Checked again now that this call holds the directory
            self.workspace.output_dir(output_dir, "output_dir", overwrite)
            earlier = self._previous_run(target, source) if overwrite else None
            size = self.workspace.check_regular_file(source, "input_path", input_path)
            held = previous.Holding(earlier) if earlier is not None else None
            try:
                result = run_job(spec, base_dir=base)
                if result.error is not None and result.error.code == "SPEC_INVALID":
                    raise ToolInputError(f"The job was refused: {result.error.message}")
            except BaseException:
                if held is not None:
                    held.restore()
                raise
            if held is not None:
                held.discard()
            job = {
                "format": JOB_FILE_FORMAT,
                "created_at": _now(),
                "input": {"path": str(source), "format": fmt, "bytes": size},
                "schema": schema_source,
                "result": self._stored_result(result, base, target, source),
            }
            stored_error = None
            try:
                _write_text_atomically(
                    target / JOB_FILE, json.dumps(job, indent=2, ensure_ascii=False)
                )
            except OSError as error:
                # The run happened: say so, and why get_job will not find it
                stored_error = f"{type(error).__name__}: {error.strerror}"
        view = self._job_view(job, target, JobResult.from_dict(job["result"]))
        if stored_error is not None:
            view["result_file"] = None
            view["result_file_error"] = (
                f"The result could not be written to {self.workspace.display(target / JOB_FILE)}"
                f" ({stored_error}); get_job and explain_failure cannot read this run"
            )
        if earlier is not None:
            view["previous_run"] = self._previous_view(earlier)
        view["next"] = _next_step(result)
        return view

    def _previous_run(self, target: Path, source: Path) -> previous.PreviousRun:
        def read_job(path: Path) -> Any:
            shown = self.workspace.display(path)
            return self._read_json(path, "the earlier job file", shown, MAX_JOB_FILE_BYTES)

        return previous.plan(target, JOB_FILE, source, read_job)

    def _previous_view(self, earlier: Optional[previous.PreviousRun]) -> Optional[Dict[str, Any]]:
        if earlier is None:
            return None
        return earlier.view(self.workspace.display, MAX_LIST_ITEMS)

    def _stored_result(
        self, result: JobResult, base: Path, target: Path, source: Path
    ) -> Dict[str, Any]:
        """The result as forklift-job.json keeps it: artifact paths relative to the output
        directory, and the paths in messages as results show them (relative to the first root)."""
        document = result.to_dict()
        named = [(source.relative_to(base).as_posix(), source)]
        for artifact in document["artifacts"]:
            real = Path(os.path.realpath(base / artifact["path"]))
            named.append((artifact["path"], real))
            artifact["path"] = real.relative_to(target).as_posix()

        def shown(text: str) -> str:
            for relative, real in sorted(named, key=lambda item: -len(item[0])):
                text = text.replace(relative, self.workspace.display(real))
            return text

        if document["error"] is not None:
            document["error"]["message"] = shown(document["error"]["message"])
        document["warnings"] = [shown(warning) for warning in document["warnings"]]
        return document

    def get_job(self, output_dir: str) -> dict[str, Any]:
        """A past run's result, read from the forklift-job.json in its output directory."""
        job, directory, result = self._load_job(output_dir)
        return self._job_view(job, directory, result)

    def explain_failure(
        self,
        output_dir: str,
        rows: int = DEFAULT_EXPLAIN_ROWS,
        max_bytes: int = DEFAULT_ROWS_BYTES,
    ) -> dict[str, Any]:
        """Why a run failed (or rejected rows): the error, findings by rule and rejected rows."""
        _check_range("rows", rows, 1, MAX_EXPLAIN_ROWS)
        _check_range("max_bytes", max_bytes, MIN_BYTES, MAX_ROWS_BYTES)
        job, directory, result = self._load_job(output_dir)
        pages = {page.name for page in schema_pages()}
        bad = _bad_rows_file(directory, result)
        rejected = None
        docs: List[str] = []
        if bad is not None:
            with _parquet_errors(self.workspace.display(bad)):
                reasons = rejection_reasons(bad, MAX_REASON_SCAN_ROWS, MAX_REASONS)
                sample = self._rows_view(bad, None, 0, rows, max_bytes)
            for reason in reasons["reasons"]:
                meaning = explain.REASON_MEANINGS.get(reason["rule"])
                if meaning:
                    reason["meaning"] = meaning
                docs.extend(explain.RULE_PAGES.get(reason["rule"], []))
            rejected = {
                "path": self.workspace.display(bad),
                "total_rows": sample["total_rows"],
                "reasons": reasons["reasons"],
                "reasons_scanned_rows": reasons["scanned_rows"],
                "distinct_reasons": reasons["distinct"],
                "first_rows": {
                    key: sample[key]
                    for key in (
                        "columns",
                        "columns_total",
                        "rows",
                        "row_count",
                        "truncated",
                        "limit",
                        "truncated_cells",
                    )
                },
            }
        findings = [
            dict(split_finding(key), key=key, count=count)
            for key, count in _bounded_findings(result.validation_summary)[
                "validation_summary"
            ].items()
        ]
        for finding in findings:
            docs.extend(explain.RULE_PAGES.get(finding["rule"], []))
        error = None
        if result.error is not None:
            meaning, steps, error_pages = explain.ERROR_HELP[result.error.code]
            error = {**result.error.to_dict(), "meaning": meaning, "next_steps": steps}
            docs = list(error_pages) + docs
        return {
            "job_id": result.job_id,
            "status": result.status,
            "output_dir": self.workspace.display(directory),
            "summary": _failure_summary(result, rejected),
            "error": error,
            "counts": dict(result.counts),
            "findings": findings,
            "findings_total": len(result.validation_summary),
            "rejected": rejected,
            **_bounded_warnings(result.warnings),
            "docs": [URI_PREFIX + page for page in dict.fromkeys(docs) if page in pages],
            "caps": {
                "rows": rows,
                "max_rows": MAX_EXPLAIN_ROWS,
                "max_bytes": max_bytes,
                "columns": MAX_COLUMNS,
                "max_name_chars": MAX_NAME_CHARS,
                "reasons": MAX_REASONS,
                "reason_scan_rows": MAX_REASON_SCAN_ROWS,
                "findings": MAX_FINDINGS,
                "warnings": MAX_LIST_ITEMS,
                "max_cell_chars": MAX_CELL_CHARS,
            },
        }

    def get_bad_rows(
        self,
        output_dir: str,
        offset: int = 0,
        limit: int = DEFAULT_BAD_ROWS,
        columns: Optional[List[str]] = None,
        max_bytes: int = DEFAULT_ROWS_BYTES,
    ) -> dict[str, Any]:
        """Rows a run rejected (bad_rows.parquet), from ``offset`` on, within bounds."""
        _check_range("offset", offset, 0, None)
        _check_range("limit", limit, 1, MAX_BAD_ROWS)
        _check_range("max_bytes", max_bytes, MIN_BYTES, MAX_ROWS_BYTES)
        _job, directory, result = self._load_job(output_dir)
        caps = {
            "limit": limit,
            "max_limit": MAX_BAD_ROWS,
            "max_bytes": max_bytes,
            "columns": MAX_COLUMNS,
            "max_name_chars": MAX_NAME_CHARS,
            "max_cell_chars": MAX_CELL_CHARS,
        }
        bad = _bad_rows_file(directory, result)
        if bad is None:
            return {
                "job_id": result.job_id,
                "status": result.status,
                "path": None,
                "total_rows": 0,
                "offset": offset,
                "columns": [],
                "columns_total": 0,
                "columns_truncated": False,
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "limit": None,
                "truncated_cells": 0,
                "next_offset": None,
                "message": (
                    "This run kept no bad_rows.parquet: it rejected no rows, or it stopped "
                    "before keeping them (see counts and error in get_job)"
                ),
                "caps": caps,
            }
        with _parquet_errors(self.workspace.display(bad)):
            view = self._rows_view(bad, columns, offset, limit, max_bytes)
        return {
            "job_id": result.job_id,
            "status": result.status,
            "path": self.workspace.display(bad),
            **view,
            "caps": caps,
        }

    # ================================================================== helpers

    def _rows_view(
        self,
        path: Path,
        columns: Optional[List[str]],
        offset: int,
        limit: int,
        max_bytes: int,
    ) -> Dict[str, Any]:
        available = [column["name"] for column in parquet_columns(path)]
        if columns is not None:
            unknown = [column for column in columns if column not in available]
            if unknown:
                shown = ", ".join(repr(name(column)[0][:64]) for column in unknown[:10])
                listed = ", ".join(repr(name(column)[0][:64]) for column in available[:50])
                more = f", ... ({len(available)} columns)" if len(available) > 50 else ""
                raise ToolInputError(
                    f"columns {shown} are not in the bad rows (its columns: {listed}{more})"
                )
            if len(columns) > MAX_COLUMNS:
                raise ToolInputError(f"columns: at most {MAX_COLUMNS} columns can be selected")
            selected = list(dict.fromkeys(columns))
        else:
            selected = available[:MAX_COLUMNS]
            if REASON_COLUMN in available and REASON_COLUMN not in selected:
                selected[-1] = REASON_COLUMN
        total = parquet_row_count(path)
        collector = RowCollector(limit, max_bytes)
        for row in iter_parquet_rows(path, selected, offset, limit + 1):
            if not collector.add(row):
                break
        view = collector.view()
        shown_end = offset + view["row_count"]
        shown_names, _, names_cut = bounded_names(selected, MAX_COLUMNS, None)
        return {
            "total_rows": total,
            "offset": offset,
            "columns": shown_names,
            "columns_total": len(available),
            "columns_truncated": len(selected) < len(available),
            "truncated_names": names_cut,
            **view,
            "next_offset": shown_end if shown_end < total else None,
        }

    def _read_json(self, real: Path, what: str, shown: str, cap: int) -> Any:
        size = self.workspace.check_regular_file(real, what, shown)
        if size > cap:
            raise ToolInputError(f"{what} {shown!r} is {size} bytes, more than the {cap} read")
        try:
            with open(real, encoding="utf-8-sig") as handle:
                return json.load(handle)
        except UnicodeDecodeError:
            raise ToolInputError(f"{what} {shown!r} is not UTF-8 text") from None
        except ValueError as error:
            # json's message gives the position only, never the text
            raise ToolInputError(f"{what} {shown!r} is not valid JSON: {error}") from None
        except RecursionError:
            raise ToolInputError(f"{what} {shown!r} is nested too deeply to read") from None
        except OSError as error:
            raise ToolInputError(f"{what} {shown!r} cannot be read: {error.strerror}") from None

    def _schema(
        self, schema: Optional[Dict[str, Any]], schema_path: Optional[str], required: bool
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """The schema document and how results name it (a path, or ``"inline"``)."""
        if schema is not None and schema_path is not None:
            raise ToolInputError(
                "give the schema inline (schema) or as a file (schema_path), not both"
            )
        if schema_path is not None:
            real = self.workspace.input_file(schema_path, "schema_path")
            document = self._read_json(real, "schema_path", schema_path, MAX_SCHEMA_FILE_BYTES)
            if not isinstance(document, dict):
                raise ToolInputError(
                    f"schema_path {schema_path!r} is not a schema: its JSON is not an object"
                )
            return document, self.workspace.display(real)
        if schema is not None:
            return schema, "inline"
        if required:
            raise ToolInputError("a schema is needed: give schema (inline) or schema_path")
        return None, None

    def _input(
        self, input_path: str, fmt: Optional[str], input_options: Optional[Dict[str, Any]]
    ) -> Tuple[Path, str, InputOptions]:
        """The input file, its format and its options (checked against the job contract)."""
        source = self.workspace.input_file(input_path, "input_path")
        suffix = source.suffix.lower()
        if fmt is None:
            fmt = _EXTENSIONS.get(suffix)
            if fmt is None:
                raise ToolInputError(
                    f"the format of {input_path!r} cannot be told from its extension: pass "
                    "format 'csv' or 'excel'"
                )
        elif fmt not in FORMATS:
            raise ToolInputError(
                f"format {fmt!r} is not read by the local server; it reads 'csv' and 'excel' "
                "files ('fwf' is not implemented by the engine, and 'sql' inputs need a "
                "connection: use forklift run-job or the remote server)"
            )
        values = dict(input_options or {})
        if fmt == "csv" and suffix in _TAB_SEPARATED and "delimiter" not in values:
            values["delimiter"] = "\t"
        try:
            options = InputOptions.from_dict(values)
        except ContractError as error:
            problems = "\n- ".join(
                f"input_options.{path}: {message}" for path, message in error.problems
            )
            raise ToolInputError(f"input_options is invalid:\n- {problems}") from None
        return source, fmt, options

    def _stage(
        self, source: Path, fmt: str, scratch: Path, options: InputOptions, records: int
    ) -> Staged:
        """Copy the input (or its first ``records`` CSV records) into the scratch directory."""
        return stage(source, self.workspace.display(source), fmt, scratch, options, records)

    def _run_interactive(
        self,
        kind: str,
        staged: Staged,
        scratch: Path,
        input_options: InputOptions,
        schema: Optional[Dict[str, Any]] = None,
        options_: Optional[Dict[str, Any]] = None,
    ) -> JobResult:
        spec = _build_spec(
            kind=kind,
            job_id=_new_job_id(kind),
            fmt=staged.fmt,
            input_path=staged.path,
            input_options=input_options.to_dict(),
            schema=schema,
            output_path="out/",
            options=options_,
            limits=None,
        )
        result = run_job(spec, base_dir=scratch)
        if result.error is not None and result.error.code == "SPEC_INVALID":
            raise ToolInputError(f"The job was refused: {staged.clean(result.error.message)}")
        return result

    def _load_job(self, output_dir: str) -> Tuple[Dict[str, Any], Path, JobResult]:
        real = self.workspace.resolve(output_dir, "output_dir")
        if real.is_dir():
            directory, job_file = real, real / JOB_FILE
        else:
            directory, job_file = real.parent, real
        if not os.path.lexists(job_file) and job_file.name == JOB_FILE:
            raise ToolInputError(
                f"{self.workspace.display(directory)!r} has no {JOB_FILE}: run_job writes one "
                "next to the outputs of every run it starts (runs started another way are not "
                "known to this server)"
            )
        shown = self.workspace.display(job_file)
        # A job file that is a link must lead to a file inside the roots too
        job = self._read_json(
            self.workspace.resolve(str(job_file), "output_dir"),
            "output_dir",
            shown,
            MAX_JOB_FILE_BYTES,
        )
        if not isinstance(job, dict) or job.get("format") != JOB_FILE_FORMAT:
            raise ToolInputError(f"{shown!r} is not a job file written by forklift-mcp")
        try:
            result = JobResult.from_dict(job.get("result"))
        except ContractError as error:
            raise ToolInputError(f"{shown!r} does not hold a valid job result: {error}") from None
        for artifact in result.artifacts:
            path = Path(os.path.realpath(directory / artifact.path))
            if directory != path and directory not in path.parents:
                raise ToolInputError(
                    f"{shown!r} names a file outside its directory ({artifact.path!r}); it was "
                    "not written by forklift-mcp or has been edited"
                )
        return job, directory, result

    def _job_view(self, job: Dict[str, Any], directory: Path, result: JobResult) -> Dict[str, Any]:
        source = job.get("input") or {}
        artifacts = []
        for artifact in result.artifacts[:MAX_LIST_ITEMS]:
            path = directory / artifact.path
            artifacts.append(
                {
                    "kind": artifact.kind,
                    "path": self.workspace.display(path),
                    "rows": artifact.rows,
                    "bytes": artifact.bytes,
                    "exists": path.is_file(),
                }
            )
        input_path = source.get("path")
        return {
            "job_id": result.job_id,
            "status": result.status,
            "error": result.error.to_dict() if result.error is not None else None,
            "output_dir": self.workspace.display(directory),
            "result_file": self.workspace.display(directory / JOB_FILE),
            "input": {
                "path": self.workspace.display(Path(input_path)) if input_path else None,
                "format": source.get("format"),
                "bytes": source.get("bytes"),
            },
            "schema": job.get("schema"),
            "created_at": job.get("created_at"),
            "counts": dict(result.counts),
            "schema_extensions": list(result.schema_extensions),
            **_bounded_findings(result.validation_summary),
            **_bounded_warnings(result.warnings),
            "artifacts": artifacts,
            "artifacts_total": len(result.artifacts),
            "caps": {
                "findings": MAX_FINDINGS,
                "warnings": MAX_LIST_ITEMS,
                "artifacts": MAX_LIST_ITEMS,
            },
        }


# ---------------------------------------------------------------------- module helpers


def summarize_schema(schema: Dict[str, Any], max_bytes: int) -> Dict[str, Any]:
    """Columns, types, required columns and extensions of a schema: names only, cut to
    :data:`MAX_NAME_CHARS`, and within ``max_bytes`` (compact JSON) for the lists."""
    budget = ByteBudget(max_bytes)
    properties = schema.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    required_list = schema.get("required")
    required_names = [n for n in required_list or [] if isinstance(n, str)]
    required_set = set(required_names)
    title, names_cut = (
        name(schema["title"]) if isinstance(schema.get("title"), str) else (None, False)
    )
    extensions, extensions_total, cut = bounded_names(
        sorted(key for key in schema if key.startswith("x-")), MAX_LIST_ITEMS, budget
    )
    names_cut += cut
    required, required_total, cut = bounded_names(required_names, MAX_LISTED_COLUMNS, budget)
    names_cut += cut
    columns = []
    for column_name, spec in properties.items():
        if len(columns) >= MAX_LISTED_COLUMNS:
            break
        entry, cut = _column_entry(column_name, spec, required_set)
        if not budget.take(entry):
            break
        columns.append(entry)
        names_cut += cut
    return {
        "title": title,
        "column_count": len(properties),
        "columns": columns,
        "columns_truncated": len(columns) < len(properties),
        "required": required,
        "required_total": required_total,
        "extensions": extensions,
        "extensions_total": extensions_total,
        "truncated_names": names_cut,
    }


_CONSTRAINT_KEYWORDS = frozenset(
    {"minimum", "maximum", "minLength", "maxLength", "pattern", "enum", "x-unique"}
)


def _column_entry(column_name: str, spec: Any, required: set) -> Tuple[Dict[str, Any], int]:
    """The summary of one column, and how many of its names were cut."""
    spec = spec if isinstance(spec, dict) else {}
    shown, cut = name(column_name)
    entry: Dict[str, Any] = {"name": shown, "required": column_name in required}
    kind = spec.get("type")
    if isinstance(kind, list):
        types = [name(value) for value in kind[:10] if isinstance(value, str)]
        entry["type"] = [value for value, _ in types]
        cut += sum(was_cut for _, was_cut in types)
    elif isinstance(kind, str):
        entry["type"], type_cut = name(kind)
        cut += type_cut
    else:
        entry["type"] = None
    if isinstance(spec.get("format"), str):
        entry["format"], format_cut = name(spec["format"])
        cut += format_cut
    extensions = [name(key) for key in sorted(key for key in spec if key.startswith("x-"))[:20]]
    if extensions:
        entry["extensions"] = [value for value, _ in extensions]
        cut += sum(was_cut for _, was_cut in extensions)
    constraints = sorted(key for key in spec if key in _CONSTRAINT_KEYWORDS)
    if constraints:
        entry["constraints"] = constraints
    return entry, cut


def _schema_caps(max_bytes: int) -> Dict[str, Any]:
    return {
        "max_bytes": max_bytes,
        "max_bytes_applies_to": "the summary's lists and the document, each",
        "listed_columns": MAX_LISTED_COLUMNS,
        "extensions": MAX_LIST_ITEMS,
        "max_name_chars": MAX_NAME_CHARS,
    }


def bounded_document(document: Dict[str, Any], max_bytes: int) -> Dict[str, Any]:
    """The document itself when its compact JSON fits ``max_bytes``; else the start of it."""
    size = compact_size(document)
    if size <= max_bytes:
        return {"document": document, "document_bytes": size, "truncated": False}
    text = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
    cut = text.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
    return {
        "document": None,
        "document_text": cut,
        "document_bytes": size,
        "truncated": True,
    }


def _without_threshold(document: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """The schema with x-validation's bad-rows threshold off (``badRowsHandling`` missing means
    the engine's default, 10%); a ``badRowsHandling`` that is not an object stays, so the engine
    still reports it."""
    validation = document.get("x-validation")
    if not isinstance(validation, dict):
        return document, False
    handling = validation.get("badRowsHandling")
    if handling is not None and not isinstance(handling, dict):
        return document, False
    handling = dict(handling or {}, failOnExceedThreshold=False)
    return dict(document, **{"x-validation": dict(validation, badRowsHandling=handling)}), True


def _first_rows_of_sheets(document: Dict[str, Any], rows: int) -> Tuple[Dict[str, Any], bool]:
    """The schema with every sheet of x-excel read up to the rows a typed preview shows (and
    one more); False when its x-excel has no sheet settings to limit (the engine then reports
    the schema, or reads every row)."""
    excel = document.get("x-excel")
    if not isinstance(excel, dict):
        return document, False
    if isinstance(excel.get("sheets"), list):
        sheets = [_first_rows(sheet, rows) for sheet in excel["sheets"]]
        excel = dict(excel, sheets=sheets)
    elif "sheet" in excel:
        select = _sheet_select(excel["sheet"])
        if select is None:
            return document, False
        # The single-sheet form, written as a list of one sheet (as the engine reads it)
        sheet = {"select": select}
        sheet.update({k: excel[k] for k in ("header", "columns", "dataStartRow") if k in excel})
        excel = {key: value for key, value in excel.items() if key != "sheet"}
        excel["sheets"] = [_first_rows(sheet, rows)]
    else:
        return document, False
    return dict(document, **{"x-excel": excel}), True


def _sheet_select(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value:
        return {"name": value}
    if isinstance(value, int):
        return {"index": value}
    return value if isinstance(value, dict) else None


def _first_rows(sheet: Any, rows: int) -> Any:
    """A sheet definition with ``dataEndRow`` at the first data row + ``rows`` (sheet rows are
    1-based); left as it is when its settings are not valid, for the engine to report."""
    if not isinstance(sheet, dict):
        return sheet
    header = sheet.get("header")
    if _is_int(header):
        header = {"row": header}
    header = {} if header is None else header
    if not isinstance(header, dict):
        return sheet
    offset = header.get("row", 0) if header.get("row") is not None else 0
    start = sheet.get("dataStartRow")
    end = sheet.get("dataEndRow")
    if not _is_int(offset) or offset < 0 or not (start is None or _is_int(start)):
        return sheet
    if end is not None and not _is_int(end):
        return sheet
    if header.get("mode") == "absent":
        first = start or 1
    else:
        first = max(start or 0, offset + 2)
    last = first + rows
    return dict(sheet, dataEndRow=min(last, end) if end is not None else last)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_range(name: str, value: int, low: int, high: Optional[int]) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ToolInputError(f"{name} must be an integer")
    if value < low or (high is not None and value > high):
        bounds = f"from {low} to {high}" if high is not None else f"at least {low}"
        raise ToolInputError(f"{name} must be {bounds} (got {value})")


def _check_encoding(options: InputOptions) -> None:
    """The engine refuses an unknown encoding only once the job runs: say so before."""
    if options.encoding is not None:
        try:
            codecs.lookup(options.encoding)
        except LookupError:
            raise ToolInputError(
                f"input_options.encoding: unknown encoding {options.encoding!r}"
            ) from None


def _top(summary: Dict[str, int], cap: int) -> List[Tuple[str, int]]:
    return sorted(summary.items(), key=lambda item: (-item[1], item[0]))[:cap]


def _bounded_findings(summary: Dict[str, int]) -> Dict[str, Any]:
    shown: Dict[str, int] = {}
    for key, count in _top(summary, MAX_FINDINGS):
        # A finding is "CODE:column"; the column part is a name like any other
        rule, separator, column = key.partition(":")
        shown_key = rule + separator + name(column)[0] if separator else name(key)[0]
        shown[shown_key] = shown.get(shown_key, 0) + count
    return {"validation_summary": shown, "validation_summary_total": len(summary)}


def _bounded_warnings(warnings: List[str]) -> Dict[str, Any]:
    shown = [warning[:MAX_MESSAGE_CHARS] for warning in warnings[:MAX_LIST_ITEMS]]
    return {"warnings": shown, "warnings_total": len(warnings)}


def _new_job_id(kind: str) -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"mcp-{kind.replace('_', '-')}-{stamp}-{secrets.token_hex(3)}"


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _build_spec(
    *,
    kind: str,
    job_id: str,
    fmt: str,
    input_path: str,
    input_options: Dict[str, Any],
    schema: Optional[Dict[str, Any]],
    output_path: str,
    options: Optional[Dict[str, Any]],
    limits: Optional[Dict[str, Any]],
) -> JobSpec:
    document: Dict[str, Any] = {
        "spec_version": 1,
        "job_id": job_id,
        "kind": kind,
        "input": {
            "format": fmt,
            "location": {"type": "file", "path": input_path},
            "options": input_options,
        },
        "output": {"location": {"type": "file", "path": output_path}},
        "options": dict(options or {}),
        "limits": dict(limits or {}),
    }
    if schema is not None:
        document["schema"] = schema
    try:
        return JobSpec.from_dict(document)
    except ContractError as error:
        raise ToolInputError(
            f"The arguments do not make a valid job: {error}\n(input_options are the job's "
            "input.options, options its options and limits its limits in the job contract)"
        ) from None


def _spec_summary(spec: JobSpec) -> Dict[str, Any]:
    """The spec of a dry run, without the schema document (results name it instead)."""
    document = spec.to_dict()
    document.pop("schema", None)
    return document


def _job_base(workspace: Workspace, source: Path, target: Path) -> Path:
    """The job's base directory: the deepest directory that holds both the input and the output
    directory (the engine puts its private ``.forklift-job-*`` directory there while it runs).

    Both must be inside one root, so the base is inside that root too.
    """
    for root in workspace.roots:
        if all(path == root or root in path.parents for path in (source, target)):
            return Path(os.path.commonpath([str(source.parent), str(target)]))
    raise ToolInputError(
        "input_path and output_dir must be inside the same --root directory (the import "
        f"reads and writes relative to one); roots: {workspace.describe_roots()}"
    )


def _write_text_atomically(target: Path, text: str) -> None:
    """Write ``text`` so that a reader never sees half a file.

    A new file gets the permissions files get here (0666 less the umask); a replaced one keeps
    its own.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{secrets.token_hex(6)}"
    handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(text + "\n")
        with contextlib.suppress(FileNotFoundError):
            os.chmod(temporary, os.stat(target).st_mode & 0o7777)
        os.replace(temporary, target)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


@contextlib.contextmanager
def _parquet_errors(shown: str) -> Iterator[None]:
    """A Parquet file that cannot be read is a refusal that says so."""
    import pyarrow as pa

    try:
        yield
    except (OSError, pa.ArrowException) as error:
        message = sanitize_arrow_error(str(error))
        raise ToolInputError(f"{shown} cannot be read as Parquet: {message}") from None


def _artifact_path(scratch: Path, result: JobResult, kind: str) -> Optional[Path]:
    """The first artifact of ``kind`` that exists."""
    for artifact in result.artifacts:
        path = scratch / artifact.path
        if artifact.kind == kind and path.is_file():
            return path
    return None


def _read_artifact(scratch: Path, result: JobResult, kind: str) -> Optional[Dict[str, Any]]:
    path = _artifact_path(scratch, result, kind)
    if path is None:
        return None
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _bad_rows_file(directory: Path, result: JobResult) -> Optional[Path]:
    for artifact in result.artifacts:
        path = directory / artifact.path
        if artifact.kind == "bad_rows" and path.is_file():
            return path
    return None


def _next_step(result: JobResult) -> str:
    if not result.succeeded:
        return "explain_failure explains the error and shows the first rejected rows"
    if result.counts.get("invalid_rows"):
        return "get_bad_rows shows the rejected rows"
    return "get_job shows this result again; the data is in the data artifacts"


def _failure_summary(result: JobResult, rejected: Optional[Dict[str, Any]]) -> str:
    counts = result.counts
    read = counts.get("total_rows")
    rows = f" after reading {read} rows" if read is not None else ""
    if result.status == "cancelled":
        text = f"The run was cancelled{rows}."
    elif result.error is not None:
        text = f"The run failed with {result.error.code}{rows}."
    else:
        text = (
            f"The run succeeded: {counts.get('valid_rows', 0)} of {read or 0} rows were "
            f"written and {counts.get('invalid_rows', 0)} rejected."
        )
    if rejected is not None and rejected["reasons"]:
        top = ", ".join(f"{r['reason']} ({r['rows']})" for r in rejected["reasons"][:3])
        kept = rejected["total_rows"]
        rows = f"{kept} rejected row is" if kept == 1 else f"{kept} rejected rows are"
        text += f" {rows} kept; most common reasons: {top}."
    return text
