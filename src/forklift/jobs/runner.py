"""``run_job``: run a :class:`~forklift.jobs.spec.JobSpec` and describe it as a ``JobResult``.

This is the code path the worker's engine process runs (``forklift run-job``) and that library
users call directly. A job never raises for its own failures: whatever goes wrong becomes a
``failed`` (or ``cancelled``) result with a stable error code and the engine's message.
"""

from __future__ import annotations

import codecs
import csv
import hashlib
import io
import json
import logging
import os
import shutil
import tempfile
import time
import urllib.parse
from dataclasses import fields
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

from ..engine.config import ImportConfig, ProcessingResults
from ..engine.exceptions import (
    CANCELLED,
    ENCODING_ERROR,
    INPUT_UNREADABLE,
    TARGET_WRITE_FAILED,
    ImportInterrupted,
    LimitExceededError,
    with_error_code,
)
from ..engine.input_source import CountingReader, InputSource
from ..engine.processors.text_utils import read_encoding
from ..engine.progress import CancelCallback, ImportHooks, ProgressCallback
from ._model import ContractError
from .errors import classify_error
from .http_input import PresignedUrlSource
from .result import Artifact, JobError, JobResult
from .spec import (
    DEFAULT_ARTIFACTS_PATH,
    FileLocation,
    InputOptions,
    JobOptions,
    JobSpec,
    PresignedUrlLocation,
    S3Location,
    SqlLocation,
    SqlTableLocation,
)

logger = logging.getLogger(__name__)

#: Monotonic clock for ``limits.max_seconds`` (a module attribute so tests can replace it)
clock: Callable[[], float] = time.monotonic

#: Longest cell text in a preview; longer cells are cut and counted in ``truncated_cells``
PREVIEW_MAX_CELL_CHARS = 2000

_CSV_OPTIONS = (
    "encoding",
    "delimiter",
    "quote_char",
    "escape_char",
    "header_mode",
    "header_search_rows",
    "skip_blank_lines",
    "comment_rows",
    "excess_column_mode",
)
_EXCEL_OPTIONS = ("sheet", "values_only", "engine", "date_system")
_SQL_OPTIONS = (
    "query_timeout",
    "connection_timeout",
    "schema_name",
    "null_values",
    "enable_streaming",
)
# What generate_schema can honour: the schema generator reads the first row as the header
_GENERATE_OPTIONS = {"csv": ("encoding", "delimiter"), "excel": ("sheet",)}
# JobOptions that only one kind of job reads
_KIND_OPTIONS = {
    "preview_rows": ("preview",),
    "preview_max_bytes": ("preview",),
    "sample_rows": ("validate_schema", "generate_schema"),
    "infer_primary_key": ("generate_schema",),
    "apply_schema_extensions": ("run", "validate_schema"),
}

SpecLike = Union[JobSpec, Mapping[str, Any]]


def run_job(
    spec: SpecLike,
    *,
    base_dir: Union[str, Path],
    allowed_url_hosts: Iterable[str] = (),
    progress: Optional[ProgressCallback] = None,
    cancel: Optional[CancelCallback] = None,
    s3_client: Any = None,
) -> JobResult:
    """Run a job and return its result; job failures are results, not exceptions.

    Args:
        spec: A :class:`JobSpec`, or a dictionary in the contract's JSON form (checked first: an
            invalid one gives a ``failed`` result with code ``SPEC_INVALID``)
        base_dir: Directory every ``file`` location is relative to (the worker's scratch
            directory). Nothing outside it is read or written, except ``s3`` locations and
            temporary files the engine puts in the system temporary directory.
        allowed_url_hosts: Hosts (or ``host:port``) a ``presigned_url`` input may point at; empty:
            presigned URLs are refused
        progress: Called at every batch boundary with ``{"rows_read", "rows_rejected",
            "bytes_read"}`` (plus ``"rows_written"`` while a ``sql_table`` output is loaded)
        cancel: Asked after every batch; True stops the job (``status: cancelled``)
        s3_client: ``forklift.io.S3StreamingClient`` for ``s3`` locations (default: boto3's
            credential chain)

    Returns:
        The :class:`JobResult`

    Raises:
        ValueError: ``base_dir`` is not a directory, or ``allowed_url_hosts`` is a string
        TypeError: ``progress`` or ``cancel`` is not callable
    """
    base = Path(os.path.realpath(os.fspath(base_dir)))
    if not base.is_dir():
        raise ValueError(f"base_dir is not a directory: {base_dir}")
    if isinstance(allowed_url_hosts, str):
        raise ValueError("allowed_url_hosts must be a list of host names, not a string")
    ImportHooks(progress, cancel)  # checks that both are callable
    if not isinstance(spec, JobSpec):
        try:
            spec = JobSpec.from_dict(spec)
        except ContractError as error:
            return _invalid_spec_result(spec, error)
    return _Job(spec, base, list(allowed_url_hosts), progress, cancel, s3_client).run()


def _invalid_spec_result(document: Any, error: ContractError) -> JobResult:
    job_id = document.get("job_id") if isinstance(document, Mapping) else None
    logger.error("Job spec is invalid: %s", error)
    return JobResult(
        job_id=job_id if isinstance(job_id, str) and job_id else None,
        status="failed",
        error=JobError(code="SPEC_INVALID", message=str(error), retryable=False),
    )


def _spec_error(path: str, message: str) -> ContractError:
    return ContractError("job spec", [(path, message)])


class _Job:
    """One run of one spec: collects counts, warnings and artifacts for the result."""

    def __init__(
        self,
        spec: JobSpec,
        base: Path,
        allowed_url_hosts: List[str],
        progress: Optional[ProgressCallback],
        cancel: Optional[CancelCallback],
        s3_client: Any,
    ):
        self.spec = spec
        self.base = base
        self.allowed_url_hosts = allowed_url_hosts
        self.progress = progress
        self.cancel = cancel
        self.s3_client = s3_client
        self.started = clock()
        self.counts: Dict[str, int] = {}
        self.warnings: List[str] = []
        self.schema_extensions: List[str] = []
        self.validation_summary: Dict[str, int] = {}
        self.artifacts: List[Artifact] = []
        self.last_event = {"rows_read": 0, "rows_rejected": 0, "bytes_read": 0}
        self._scratch: Optional[Path] = None
        self._cancel_requested = False
        self._limit_error: Optional[LimitExceededError] = None

    # ------------------------------------------------------------------ outcome

    def run(self) -> JobResult:
        spec = self.spec
        logger.info(
            "Job %s: %s of a %s input (%s)",
            spec.job_id,
            spec.kind,
            spec.input.format,
            spec.input.location.location_type,
        )
        try:
            self._check_options()
            {
                "run": self._run,
                "preview": self._preview,
                "validate_schema": self._validate_schema,
                "generate_schema": self._generate_schema,
            }[spec.kind]()
        except Exception as error:
            return self._failed(error)
        finally:
            self._remove_scratch()
        logger.info("Job %s succeeded", spec.job_id)
        return self._result("succeeded")

    def _failed(self, error: BaseException) -> JobResult:
        if self._limit_error is not None and not isinstance(error, ImportInterrupted):
            # A component that wraps the errors of its callbacks (the table writer) hid it
            error = self._limit_error
        code, message, retryable = classify_error(
            error, secrets=self._secrets(), base_dir=self.base
        )
        if self._cancel_requested:
            # Whatever the component that noticed it raised, the job was cancelled
            code, retryable = CANCELLED, False
        kept = getattr(error, "bad_rows_file", None)
        if kept and not any(a.kind == "bad_rows" for a in self.artifacts):
            self.artifacts.append(self._artifact("bad_rows", kept))
        if not self.counts and self.last_event["rows_read"]:
            self.counts = {
                "total_rows": self.last_event["rows_read"],
                "invalid_rows": self.last_event["rows_rejected"],
            }
        status = "cancelled" if code == CANCELLED else "failed"
        logger.warning("Job %s %s: %s: %s", self.spec.job_id, status, code, message)
        return self._result(status, JobError(code=code, message=message, retryable=retryable))

    def _result(self, status: str, error: Optional[JobError] = None) -> JobResult:
        return JobResult(
            job_id=self.spec.job_id,
            status=status,
            counts=dict(self.counts),
            schema_extensions=list(self.schema_extensions),
            validation_summary=dict(self.validation_summary),
            warnings=list(self.warnings),
            artifacts=list(self.artifacts),
            error=error,
        )

    def _secrets(self) -> List[str]:
        """Values that must never appear in a message."""
        found = []
        for location in (self.spec.input.location, getattr(self.spec.output, "location", None)):
            if isinstance(location, (SqlLocation, SqlTableLocation)):
                found.append(location.connection_string)
            elif isinstance(location, PresignedUrlLocation):
                found.extend([location.url, urllib.parse.urlsplit(location.url).query])
        return found

    # ------------------------------------------------------------------ progress, limits

    def _on_progress(self, event: Dict[str, int]) -> None:
        self.last_event.update(event)
        limits = self.spec.limits
        if limits.max_rows is not None and event.get("rows_read", 0) > limits.max_rows:
            self._limit(
                f"The input has more than {limits.max_rows} rows (limits.max_rows); the job "
                f"stopped after {event['rows_read']}"
            )
        if (
            limits.max_input_bytes is not None
            and event.get("bytes_read", 0) > limits.max_input_bytes
        ):
            self._limit(
                f"The job read more than {limits.max_input_bytes} bytes of input "
                "(limits.max_input_bytes)"
            )
        self._check_time()
        if self.progress is not None:
            self.progress(dict(event))

    def _check_time(self) -> None:
        max_seconds = self.spec.limits.max_seconds
        if max_seconds is not None and clock() - self.started > max_seconds:
            self._limit(f"The job ran longer than {max_seconds:g} seconds (limits.max_seconds)")

    def _limit(self, message: str) -> None:
        self._limit_error = LimitExceededError(message)
        raise self._limit_error

    def _check_input_size(self, size: Optional[int]) -> None:
        limit = self.spec.limits.max_input_bytes
        if size is not None and limit is not None and size > limit:
            self._limit(
                f"The input is {size} bytes, more than limits.max_input_bytes ({limit}); "
                "nothing was read"
            )

    def _cancel(self) -> bool:
        if not self._cancel_requested and self.cancel is not None and self.cancel():
            self._cancel_requested = True
        return self._cancel_requested

    # ------------------------------------------------------------------ options

    def _check_options(self) -> None:
        """Refuse options that cannot work; warn about the ones this job ignores."""
        spec, fmt = self.spec, self.spec.input.format
        options = spec.input.options
        if options.encoding is not None:
            try:
                codecs.lookup(options.encoding)
            except LookupError:
                raise _spec_error(
                    "input.options.encoding", f"unknown encoding {options.encoding!r}"
                ) from None
        for item in fields(InputOptions):
            value = getattr(options, item.name)
            if value is None:
                continue
            if fmt not in item.metadata["formats"]:
                formats = " or ".join(repr(f) for f in item.metadata["formats"])
                self.warnings.append(
                    f"input.options.{item.name} only applies to format {formats} and is ignored"
                )
            elif spec.kind == "generate_schema" and item.name not in _GENERATE_OPTIONS.get(
                fmt, ()
            ):
                self.warnings.append(
                    f"input.options.{item.name} is not used by generate_schema (the schema "
                    "generator reads the first row as the header) and is ignored"
                )
        defaults = JobOptions()
        for name, kinds in _KIND_OPTIONS.items():
            if getattr(spec.options, name) != getattr(defaults, name) and spec.kind not in kinds:
                self.warnings.append(
                    f"options.{name} only applies to kind {' or '.join(map(repr, kinds))} and "
                    "is ignored"
                )
        if (
            spec.output is not None
            and spec.output.compression != "snappy"
            and (spec.kind != "run" or fmt != "csv")
        ):
            self.warnings.append(
                "output.compression only applies to CSV runs and is ignored (snappy is used)"
            )

    # ------------------------------------------------------------------ locations

    def _inside(self, location: FileLocation, field: str) -> Path:
        """The absolute path of a ``file`` location; it must stay inside the base directory."""
        target = Path(os.path.realpath(self.base / location.path))
        if target != self.base and self.base not in target.parents:
            raise _spec_error(
                f"{field}.path",
                f"{location.path!r} leads outside the base directory (through a symbolic link)",
            )
        return target

    def _relative(self, path: Union[str, Path]) -> str:
        """``path`` (inside the base directory) relative to it, '/' separated."""
        return Path(os.path.realpath(str(path))).relative_to(self.base).as_posix()

    def _scratch_dir(self) -> Path:
        """A private directory inside the base directory, removed when the job ends."""
        if self._scratch is None:
            self._scratch = Path(tempfile.mkdtemp(prefix=".forklift-job-", dir=str(self.base)))
        return self._scratch

    def _remove_scratch(self) -> None:
        if self._scratch is not None:
            shutil.rmtree(self._scratch, ignore_errors=True)
            self._scratch = None

    def _input(self) -> Dict[str, Any]:
        """How the engine reaches the input: ``path`` (what it is called) and ``source``."""
        location = self.spec.input.location
        if isinstance(location, FileLocation):
            path = self._inside(location, "input.location")
            if not path.is_file():
                raise with_error_code(
                    FileNotFoundError(
                        f"Input file not found: {location.path} (relative to the base directory)"
                    ),
                    INPUT_UNREADABLE,
                )
            return {"path": str(path), "source": None, "size": path.stat().st_size}
        if isinstance(location, S3Location):
            return {"path": location.uri, "source": None, "size": None}
        if isinstance(location, PresignedUrlLocation):
            source = PresignedUrlSource(
                location.url,
                allowed_hosts=self.allowed_url_hosts,
                size=location.size,
                etag=location.etag,
            )
            return {"path": source.name, "source": source, "size": location.size}
        return {"path": None, "source": None, "size": None}  # sql: no path

    def _s3_size(self, uri: str) -> Optional[int]:
        """Size of an S3 input, asked for only when a size limit needs it."""
        if self.spec.limits.max_input_bytes is None:
            return None
        from ..io import UnifiedIOHandler

        return UnifiedIOHandler(self.s3_client).get_size(uri)

    def _artifact_dir(self) -> Path:
        """Directory for the artifacts of kinds other than run (default ``out/``)."""
        output = self.spec.output
        location = output.location if output is not None else FileLocation(DEFAULT_ARTIFACTS_PATH)
        directory = self._inside(location, "output.location")
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _run_output(self) -> str:
        """Where the import writes: a local directory or an s3:// prefix."""
        output = self.spec.output
        location = output.location
        if isinstance(location, S3Location):
            return location.uri
        if isinstance(location, SqlTableLocation):
            artifacts = output.artifacts or FileLocation(DEFAULT_ARTIFACTS_PATH)
            directory = self._inside(artifacts, "output.artifacts")
        else:
            directory = self._inside(location, "output.location")
        directory.mkdir(parents=True, exist_ok=True)
        return str(directory)

    def _schema_file(self) -> Optional[str]:
        """The inline schema, written into the job's private directory (never a caller path)."""
        if self.spec.schema is None:
            return None
        path = self._scratch_dir() / "schema.json"
        path.write_text(json.dumps(self.spec.schema, ensure_ascii=False), encoding="utf-8")
        return str(path)

    # ------------------------------------------------------------------ artifacts

    def _artifact(self, kind: str, path: Union[str, Path], rows: Optional[int] = None) -> Artifact:
        text = str(path)
        if text.startswith("s3://"):
            return Artifact(kind=kind, path=text, rows=rows)
        digest = hashlib.sha256()
        with open(text, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if kind in ("data", "bad_rows"):
            import pyarrow.parquet as pq

            rows = pq.read_metadata(text).num_rows
        return Artifact(
            kind=kind,
            path=self._relative(text),
            rows=rows,
            bytes=os.path.getsize(text),
            sha256=digest.hexdigest(),
        )

    def _write_artifact(
        self, kind: str, name: str, payload: Any, rows: Optional[int] = None, text: str = ""
    ) -> None:
        """Write a JSON artifact into the artifact directory and record it."""
        target = self._artifact_dir() / name
        if not text:
            text = json.dumps(payload, indent=2, ensure_ascii=False, default=str, allow_nan=False)
        target.write_text(text + "\n", encoding="utf-8")
        self.artifacts.append(self._artifact(kind, target, rows))

    def _record_import(self, results: ProcessingResults, output: str) -> None:
        """Counts, findings and files of a finished import."""
        self.counts.update(
            total_rows=results.total_rows,
            valid_rows=results.valid_rows,
            invalid_rows=results.invalid_rows,
            truncated_rows=results.truncated_rows,
        )
        self.schema_extensions = list(results.schema_extensions)
        self.validation_summary = dict(results.validation_summary)
        self.warnings.extend(results.warnings)
        self.warnings.extend(f"Not written: {message}" for message in results.errors)
        s3 = output.startswith("s3://")
        # Local Parquet files are counted from their footers. On S3 only a CSV import knows its
        # counts per file (Excel and SQL write a file per sheet or table)
        counted = s3 and self.spec.input.format == "csv"
        for path in results.output_files:
            bad = path == results.bad_rows_file
            rows = (results.invalid_rows if bad else results.valid_rows) if counted else None
            self.artifacts.append(self._artifact("bad_rows" if bad else "data", path, rows))
        if not s3:
            collected = Path(output) / "output_data_metadata.json"
            if collected.is_file():
                self.artifacts.append(self._artifact("metadata", collected))
        for kind, path in (
            ("metadata", results.metadata_file),
            ("manifest", results.manifest_file),
        ):
            if path:
                self.artifacts.append(self._artifact(kind, path))
        if not s3 and self.spec.input.format == "sql":
            metadata = Path(output) / "metadata.json"
            if metadata.is_file():
                self.artifacts.append(self._artifact("metadata", metadata))

    # ------------------------------------------------------------------ run

    def _run(self) -> None:
        spec = self.spec
        fmt = spec.input.format
        if fmt == "fwf":
            raise self._fwf_unsupported()
        source = self._input()
        size = source["size"]
        if size is None and isinstance(spec.input.location, S3Location):
            size = self._s3_size(source["path"])
        self._check_input_size(size)
        output = self._run_output()
        schema_file = self._schema_file()
        hooks = {"progress": self._on_progress, "cancel": self._cancel}
        if fmt == "csv":
            from ..engine.forklift_core import ForkliftCore

            config = self._csv_config(source["path"], output, schema_file)
            results = ForkliftCore(config, input_source=source["source"], **hooks).process_csv()
        elif fmt == "excel":
            from ..engine.forklift_core import import_excel

            options = self._options(_EXCEL_OPTIONS)
            if self.s3_client is not None:
                options["s3_client"] = self.s3_client
            results = import_excel(source["path"], output, schema_file, **options, **hooks)
        else:  # sql
            from ..engine.forklift_core import import_sql

            options = self._options(_SQL_OPTIONS)
            if self.s3_client is not None:
                options["s3_client"] = self.s3_client
            results = import_sql(
                spec.input.location.connection_string,
                output,
                schema_file,
                batch_size=spec.options.batch_size,
                **options,
                **hooks,
            )
        self._record_import(results, output)
        if isinstance(spec.output.location, SqlTableLocation):
            self._write_table(spec.output.location)

    def _fwf_unsupported(self) -> ContractError:
        return _spec_error(
            "input.format",
            "'fwf': fixed-width import is not implemented in this version of the engine "
            "(forklift.import_fwf raises NotImplementedError); convert the file to CSV first",
        )

    def _options(self, names: Iterable[str]) -> Dict[str, Any]:
        options = self.spec.input.options
        return {
            name: getattr(options, name) for name in names if getattr(options, name) is not None
        }

    def _csv_config(
        self, input_path: str, output: str, schema_file: Optional[str]
    ) -> ImportConfig:
        spec = self.spec
        options = self._options(_CSV_OPTIONS)
        if spec.input.options.footer_detection is not None:
            options["footer_detection"] = spec.input.options.footer_detection.to_dict()
        return ImportConfig(
            input_path=input_path,
            output_path=output,
            schema_file=schema_file,
            batch_size=spec.options.batch_size,
            apply_schema_extensions=spec.options.apply_schema_extensions,
            include_value_statistics=spec.options.include_value_statistics,
            compression=spec.output.compression if spec.output is not None else "snappy",
            s3_client=self.s3_client,
            **options,
        )

    def _write_table(self, location: SqlTableLocation) -> None:
        """Load the validated rows into the target table (the Parquet files stay artifacts)."""
        data = [a for a in self.artifacts if a.kind == "data"]
        if not data:
            self.counts["rows_written"] = 0
            self.warnings.append(
                f"Nothing was loaded into {location.table!r}: the import produced no data file"
            )
            return
        if len(data) > 1:
            raise with_error_code(
                ValueError(
                    f"A sql_table output loads one data file, but the import produced "
                    f"{len(data)} ({', '.join(a.path for a in data)}); select one sheet "
                    "(input.options.sheet) or one x-sql table"
                ),
                TARGET_WRITE_FAILED,
            )
        try:
            from ..outputs.sql import write_table
        except ImportError as error:
            raise with_error_code(
                RuntimeError(f"sql_table outputs need forklift.outputs.sql ({error})"),
                TARGET_WRITE_FAILED,
            ) from None

        def table_progress(event: Any) -> None:
            combined = dict(self.last_event)
            if isinstance(event, Mapping) and isinstance(event.get("rows_written"), int):
                combined["rows_written"] = event["rows_written"]
            self._check_time()
            if self.progress is not None:
                self.progress(combined)

        logger.info(
            "Job %s: loading %s into table %s", self.spec.job_id, data[0].path, location.table
        )
        try:
            written = write_table(
                str(self.base / data[0].path),
                location.connection_string,
                location.table,
                schema_name=location.schema_name,
                mode=location.mode,
                key_columns=location.key_columns,
                batch_size=self.spec.options.batch_size,
                staging=location.staging,
                job_id=self.spec.job_id,
                progress=table_progress,
                cancel=self._cancel,
            )
        except ImportInterrupted:
            raise
        except Exception as error:
            raise with_error_code(error, TARGET_WRITE_FAILED)
        self.counts["rows_written"] = int(written.rows_written)
        self.warnings.extend(getattr(written, "warnings", None) or [])

    # ------------------------------------------------------------------ preview

    def _preview(self) -> None:
        fmt = self.spec.input.format
        if fmt == "fwf":
            raise self._fwf_unsupported()
        if fmt == "sql":
            raise _spec_error("input.format", "kind 'preview' reads csv and excel inputs, not sql")
        source = self._input()
        builder = _PreviewBuilder(
            self.spec.options.preview_rows, self.spec.options.preview_max_bytes
        )
        if fmt == "csv":
            self._preview_csv(source, builder)
        else:
            self._preview_excel(self._local_copy(source), builder)
        self.counts["total_rows"] = len(builder.rows)
        self._write_artifact("preview", "preview.json", None, len(builder.rows), builder.text())

    def _detector(self, source: Dict[str, Any]):
        from ..engine.processors.header_detector import HeaderDetector
        from ..io import UnifiedIOHandler

        config = self._csv_config(source["path"], str(self.base), None)
        return config, HeaderDetector(
            config, UnifiedIOHandler(self.s3_client), input_source=source["source"]
        )

    def _header(self, config: ImportConfig, detector: Any, path: str):
        """``(header_row_index, column_names)`` as the import would see them."""
        names = None
        properties = (self.spec.schema or {}).get("properties")
        if config.header_mode.value == "absent" and isinstance(properties, dict) and properties:
            names = list(properties)
        try:
            return detector.detect_header_row(path, names)
        except UnicodeDecodeError as error:
            raise _encoding_error(error, config.encoding) from None

    def _preview_csv(self, source: Dict[str, Any], builder: "_PreviewBuilder") -> None:
        config, detector = self._detector(source)
        header_index, columns = self._header(config, detector, source["path"])
        builder.columns = list(columns)
        try:
            for index, row in enumerate(detector.rows(source["path"])):
                if index <= header_index or not row:
                    continue
                if detector.should_stop_for_footer(row) or not builder.add(row):
                    break
        except UnicodeDecodeError as error:
            raise _encoding_error(error, config.encoding) from None

    def _preview_excel(self, path: str, builder: "_PreviewBuilder") -> None:
        from ..inputs.config import ExcelInputConfig, ExcelSheetConfig
        from ..inputs.excel import ExcelInputHandler

        options = self.spec.input.options
        config = ExcelInputConfig(
            sheets=[],
            values_only=True if options.values_only is None else options.values_only,
            date_system=options.date_system or "1900",
            engine=options.engine,
        )
        handler = ExcelInputHandler(config)
        handler.open_workbook(Path(path))
        try:
            sheet = options.sheet
            if (
                isinstance(sheet, str)
                and sheet.isdigit()
                and sheet not in handler.get_sheet_names()
            ):
                sheet = int(sheet)  # as for a run: a sheet name wins, otherwise an index
            select = {"name": sheet} if isinstance(sheet, str) else {"index": sheet or 0}
            # One row more than shown, so the preview knows whether there are more
            wanted = ExcelSheetConfig(select=select, data_end_row=builder.max_rows + 2)
            ((name, sheet_config),) = handler.select_sheets([wanted])
            table = handler.read_sheet_data(name, sheet_config)
        finally:
            handler.close_workbook()
        builder.columns = list(table.column_names)
        builder.sheet = name
        for record in table.to_pylist():
            if not builder.add([_cell_text(value) for value in record.values()]):
                break

    def _local_copy(self, source: Dict[str, Any]) -> str:
        """A local path for a reader that needs one (Excel): s3 inputs are copied to scratch."""
        if not str(source["path"]).startswith("s3://"):
            return source["path"]
        from ..io import S3Path, UnifiedIOHandler

        self._check_input_size(self._s3_size(source["path"]))
        target = self._scratch_dir() / S3Path(source["path"]).name
        UnifiedIOHandler(self.s3_client).copy_file(source["path"], str(target))
        return str(target)

    # ------------------------------------------------------------------ validate_schema

    def _validate_schema(self) -> None:
        from ..engine.forklift_core import ForkliftCore

        fmt = self.spec.input.format
        if fmt != "csv":
            raise _spec_error(
                "input.format",
                f"kind 'validate_schema' checks a schema against a CSV input; {fmt!r} is not "
                "supported (run the job on a small input to check an Excel or SQL schema)",
            )
        report: Dict[str, Any] = {"valid": False, "columns": None}
        error: Optional[BaseException] = None
        try:
            source = self._input()
            sample, header_index, columns = self._csv_sample(source, self.spec.options.sample_rows)
            report["columns"] = columns
            config = self._csv_config(
                str(sample), str(self._scratch_dir() / "validate-out"), self._schema_file()
            )
            results = ForkliftCore(
                config, progress=self._on_progress, cancel=self._cancel
            ).process_csv()
            self._record_counts(results)
            report["valid"] = True
        except Exception as caught:
            error = caught
        report.update(self._schema_comparison(report.get("columns")))
        report.update(
            sample_rows=self.counts.get("total_rows", self.last_event["rows_read"]),
            counts=dict(self.counts),
            schema_extensions=list(self.schema_extensions),
            validation_summary=dict(self.validation_summary),
            warnings=list(self.warnings),
            error=None,
        )
        if error is not None:
            code, message, _ = classify_error(error, secrets=self._secrets(), base_dir=self.base)
            report["error"] = {"code": code, "message": message}
        self._write_artifact("report", "report.json", report)
        if error is not None:
            raise error

    def _record_counts(self, results: ProcessingResults) -> None:
        self.counts.update(
            total_rows=results.total_rows,
            valid_rows=results.valid_rows,
            invalid_rows=results.invalid_rows,
            truncated_rows=results.truncated_rows,
        )
        self.schema_extensions = list(results.schema_extensions)
        self.validation_summary = dict(results.validation_summary)
        self.warnings.extend(results.warnings)

    def _schema_comparison(self, columns: Optional[List[str]]) -> Dict[str, Any]:
        """Names only: what the schema declares against what the input has."""
        schema = self.spec.schema or {}
        properties = schema.get("properties")
        declared = list(properties) if isinstance(properties, dict) else []
        required = [n for n in schema.get("required", []) if isinstance(n, str)]
        found = columns or []
        return {
            "schema_columns": declared,
            "required_columns": required,
            "columns_not_in_schema": [c for c in found if c not in declared],
            "schema_columns_not_in_input": (
                [c for c in declared if c not in found] if columns is not None else None
            ),
        }

    def _csv_sample(
        self, source: Dict[str, Any], data_rows: int, header_index: Optional[int] = None
    ):
        """Copy the rows up to the header and the next ``data_rows`` rows into scratch.

        The rows are copied as the input has them (same text, same line breaks), so the engine
        sees exactly what it would see in the input. Returns ``(path, header_index, columns)``.
        """
        config, detector = self._detector(source)
        columns = None
        if header_index is None:
            header_index, columns = self._header(config, detector, source["path"])
        name = Path(urllib.parse.urlsplit(str(source["path"])).path).name or "input.csv"
        target = self._scratch_dir() / "sample" / name
        target.parent.mkdir(exist_ok=True)
        limit = self.spec.limits.max_input_bytes
        raw = self._open_input(source)
        counter = CountingReader(raw)
        text = io.TextIOWrapper(
            io.BufferedReader(counter), encoding=read_encoding(config.encoding), newline=""
        )
        pending: List[str] = []

        def lines():
            for line in text:
                pending.append(line)
                if limit is not None and counter.count > limit:
                    self._limit(
                        f"The job read more than {limit} bytes of input (limits.max_input_bytes)"
                    )
                yield line

        try:
            with text, open(target, "w", encoding=config.encoding, newline="") as sink:
                reader = csv.reader(
                    lines(),
                    delimiter=config.delimiter,
                    quotechar=config.quote_char,
                    escapechar=config.escape_char,
                )
                copied = 0
                for index, record in enumerate(reader):
                    sink.write("".join(pending))
                    pending.clear()
                    if index > header_index and record:
                        copied += 1
                        if copied >= data_rows:
                            break
        except UnicodeDecodeError as error:
            raise _encoding_error(error, config.encoding) from None
        return target, header_index, columns

    def _open_input(self, source: Dict[str, Any]):
        """A binary stream of the whole input, from its first byte."""
        if isinstance(source["source"], InputSource):
            return source["source"].open()
        if str(source["path"]).startswith("s3://"):
            from ..io import UnifiedIOHandler

            return UnifiedIOHandler(self.s3_client).open_for_read(
                source["path"], mode="rb", seekable=False
            )
        return open(source["path"], "rb")

    # ------------------------------------------------------------------ generate_schema

    def _generate_schema(self) -> None:
        from ..schema.schema_generator import (
            FileType,
            OutputTarget,
            SchemaGenerationConfig,
            SchemaGenerator,
        )

        spec = self.spec
        fmt = spec.input.format
        if fmt == "fwf":
            raise self._fwf_unsupported()
        if fmt == "sql":
            raise _spec_error(
                "input.format", "kind 'generate_schema' reads csv and excel inputs, not sql"
            )
        options = spec.input.options
        source = self._input()
        if fmt == "csv":
            path = source["path"]
            if source["source"] is not None or str(path).startswith("s3://"):
                # Not a local file: the generator reads a copy of the first rows
                path = str(self._csv_sample(source, spec.options.sample_rows, header_index=0)[0])
            settings = {
                "file_type": FileType.CSV,
                "delimiter": options.delimiter or ",",
                "encoding": options.encoding or "utf-8",
            }
        else:
            path = self._local_copy(source)
            settings = {"file_type": FileType.EXCEL, "sheet_name": options.sheet}
        generator = SchemaGenerator(
            SchemaGenerationConfig(
                input_path=path,
                nrows=spec.options.sample_rows,
                output_target=OutputTarget.STDOUT,
                include_value_statistics=spec.options.include_value_statistics,
                infer_primary_key_from_metadata=spec.options.infer_primary_key,
                **settings,
            )
        )
        try:
            schema = generator.generate_schema()
        except ValueError as error:
            # The generator's messages say what is wrong; it has no error codes of its own
            undecodable = isinstance(error, UnicodeError) or (
                "invalid text for the configured encoding" in str(error)
            )
            raise with_error_code(error, ENCODING_ERROR if undecodable else INPUT_UNREADABLE)
        analysed = schema.get("x-generation", {}).get("rows_analyzed")
        if isinstance(analysed, int):
            self.counts["total_rows"] = analysed
        self._write_artifact("schema", "schema.json", schema)


def _encoding_error(error: UnicodeDecodeError, encoding: str) -> ValueError:
    return with_error_code(
        ValueError(
            f"Input contains bytes that are not valid for encoding '{encoding}' (byte offset "
            f"{error.start}). Set the encoding the file was written with (for example "
            "'latin-1' or 'cp1252')."
        ),
        ENCODING_ERROR,
    )


def _cell_text(value: Any) -> Optional[str]:
    """A cell of a preview as text (dates in ISO form, whole floats without '.0')."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        return str(int(value))
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


class _PreviewBuilder:
    """Collects preview rows until the row or byte limit is reached."""

    def __init__(self, max_rows: int, max_bytes: int):
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.columns: List[str] = []
        self.sheet: Optional[str] = None
        self.rows: List[List[Optional[str]]] = []
        self.truncated_cells = 0
        self.limit: Optional[str] = None
        self._size = 0

    def add(self, row: List[Optional[str]]) -> bool:
        """Add a row; False when the preview is full (that row is not added)."""
        if len(self.rows) >= self.max_rows:
            self.limit = "rows"
            return False
        cut = [c is not None and len(c) > PREVIEW_MAX_CELL_CHARS for c in row]
        cells = [c[:PREVIEW_MAX_CELL_CHARS] if long else c for c, long in zip(row, cut)]
        size = len(_compact(cells).encode("utf-8")) + 1  # and a comma
        if self._size + size > self.max_bytes - self._overhead():
            self.limit = "bytes"
            return False
        self._size += size
        self.rows.append(cells)
        self.truncated_cells += sum(cut)
        return True

    def _overhead(self) -> int:
        """Bytes of preview.json without its rows (with room for the final counts)."""
        return len(_compact(self.payload(rows=[])).encode("utf-8")) + 64

    def payload(self, rows: Optional[List[Any]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "columns": self.columns,
            "rows": self.rows if rows is None else rows,
            "row_count": len(self.rows),
            "truncated": self.limit is not None,
            "limit": self.limit,
            "truncated_cells": self.truncated_cells,
        }
        if self.sheet is not None:
            payload["sheet"] = self.sheet
        return payload

    def text(self) -> str:
        """preview.json: compact, so that its size is what the byte limit allowed for."""
        return _compact(self.payload())


def _compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
