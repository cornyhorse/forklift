"""CSV-specific data processor implementation."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, List, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc

from ...io import S3Path, UnifiedIOHandler, create_parquet_writer, is_s3_path
from ...metadata import MetadataWriteError, OutputMetadataCollector
from ...processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
)
from ..config import HeaderMode, ImportConfig, ProcessingResults
from .base_processor import BaseProcessor
from .batch_processor import BatchProcessor
from .extensions import (
    REASON_COLUMN,
    ExtensionPipeline,
    build_extension_pipeline,
    strip_hidden_columns,
)
from .header_detector import HeaderDetector
from .schema_processor import SchemaProcessor
from .text_utils import sanitize_arrow_error
from .type_conversion import raw_rows

logger = logging.getLogger(__name__)


class _ParquetOutputs:
    """Owns the data and bad-rows writers so each one is either finished or discarded.

    Writers are created lazily from the schema of the first batch. ``close()`` finishes them;
    ``abort()`` discards whatever is still open (partial local file removed, S3 writer asked to
    drop its temporary file without uploading) and is safe to call at any time.
    """

    def __init__(
        self,
        good_file: str,
        bad_file: str,
        io_handler: UnifiedIOHandler,
        use_s3_output: bool,
        compression: str,
        reason_column: bool = False,
    ):
        self.good_file = good_file
        self.bad_file = bad_file
        self._io_handler = io_handler
        self._use_s3_output = use_s3_output
        self._compression = compression
        self._reason_column = reason_column

        self.good_writer = None
        self.bad_writer = None
        self.good_schema: Optional[pa.Schema] = None
        self.bad_schema: Optional[pa.Schema] = None
        self.good_written = False
        self.bad_written = False

    def _create_writer(self, path: str, schema: pa.Schema):
        return create_parquet_writer(
            path,
            schema,
            s3_client=self._io_handler.s3_client if self._use_s3_output else None,
            compression=self._compression,
        )

    def ensure_good_writer(self, schema: pa.Schema) -> None:
        """Create the data writer (an empty data file still carries the schema)."""
        if self.good_writer is None:
            self.good_writer = self._create_writer(self.good_file, schema)
            self.good_schema = schema

    def write_good(self, batch: pa.RecordBatch) -> None:
        """Append rows to the data file."""
        self.ensure_good_writer(batch.schema)
        if len(batch) > 0:
            self.good_writer.write_table(pa.Table.from_batches([batch]))

    def write_bad(
        self, batch: pa.RecordBatch, reason: Union[str, Sequence[str], None] = None
    ) -> None:
        """Append rejected rows to the bad rows file.

        Rejected rows are stored as strings, as the input file had them: a value that failed type
        conversion cannot live in a typed column, and one stable schema lets rows from every
        batch share the file.
        When the file has a reason column (the schema asks for validation or constraints),
        ``reason`` is one text for all rows or one text per row.
        """
        if len(batch) == 0:
            return
        batch = raw_rows(batch)
        if self._reason_column:
            batch = self._with_reason(batch, reason)
        if self.bad_writer is None:
            self.bad_writer = self._create_writer(self.bad_file, batch.schema)
            self.bad_schema = batch.schema
        elif not batch.schema.equals(self.bad_schema):
            batch = self._align(batch, self.bad_schema)
        self.bad_writer.write_table(pa.Table.from_batches([batch]))

    @staticmethod
    def _with_reason(
        batch: pa.RecordBatch, reason: Union[str, Sequence[str], None]
    ) -> pa.RecordBatch:
        """Append the reason column (the last column of the bad rows file)."""
        if isinstance(reason, str) or reason is None:
            values = pa.array([reason or "rejected"] * len(batch), type=pa.string())
        else:
            values = pa.array(list(reason), type=pa.string())
        return pa.RecordBatch.from_arrays(
            list(batch.columns) + [values],
            schema=pa.schema(list(batch.schema) + [pa.field(REASON_COLUMN, pa.string())]),
        )

    @staticmethod
    def _align(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
        """Arrange ``batch`` by column name in the order of ``schema`` (missing -> null)."""
        arrays = []
        for field in schema:
            position = batch.schema.get_field_index(field.name)
            if position >= 0:
                arrays.append(batch.column(position))
            else:
                arrays.append(pa.nulls(len(batch), type=field.type))
        return pa.RecordBatch.from_arrays(arrays, names=schema.names)

    def close(self) -> None:
        """Finish both files; if one fails to close the other is discarded."""
        if self.good_writer is not None:
            writer, self.good_writer = self.good_writer, None
            try:
                writer.close()
            except BaseException:
                self._abort_writer(writer, self.good_file)
                self.abort()
                raise
            self.good_written = True

        if self.bad_writer is not None:
            writer, self.bad_writer = self.bad_writer, None
            try:
                writer.close()
            except BaseException:
                self._abort_writer(writer, self.bad_file)
                raise
            self.bad_written = True

    def keep_bad_rows(self) -> Optional[str]:
        """Finish the bad-rows file and discard the data file (for a run that stops on purpose).

        A run that is aborted because too many rows were rejected keeps what explains it.

        Returns:
            The path of the finished bad-rows file, or None if there is none
        """
        if self.good_writer is not None:
            writer, self.good_writer = self.good_writer, None
            self._abort_writer(writer, self.good_file)
        if self.bad_writer is None:
            return None
        writer, self.bad_writer = self.bad_writer, None
        try:
            writer.close()
        except BaseException:
            self._abort_writer(writer, self.bad_file)
            raise
        self.bad_written = True
        return self.bad_file

    def abort(self) -> None:
        """Discard every writer that is still open (idempotent)."""
        if self.good_writer is not None:
            writer, self.good_writer = self.good_writer, None
            self._abort_writer(writer, self.good_file)
        if self.bad_writer is not None:
            writer, self.bad_writer = self.bad_writer, None
            self._abort_writer(writer, self.bad_file)

    @staticmethod
    def _abort_writer(writer, path: str) -> None:
        """Drop a partial output without publishing it."""
        abort = getattr(writer, "abort", None)
        if callable(abort):
            try:
                abort()
            except Exception:
                logger.warning("Could not abort partial output %s", path, exc_info=True)
            if not is_s3_path(path):
                try:
                    Path(path).unlink()
                except OSError:
                    pass
            return

        # Writers without abort(): an S3 writer keeps its data in a temporary file that close()
        # would upload, so release the temporary file directly instead of closing
        temp_path = getattr(writer, "_temp_path", None)
        if temp_path is not None:
            try:
                inner = getattr(writer, "_writer", None)
                if inner is not None:
                    inner.close()
            except Exception:
                pass
            try:
                Path(temp_path).unlink()
            except OSError:
                pass
            return

        try:
            writer.close()
        except Exception:
            pass
        if not is_s3_path(path):
            try:
                Path(path).unlink()
            except OSError:
                pass


def _json_safe(value: Any) -> Any:
    """Make a value serialisable with ``allow_nan=False`` (NaN/inf become null)."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return [_json_safe(v) for v in sorted(value, key=str)]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


class CSVProcessor(BaseProcessor):
    """Handles CSV-specific data processing operations."""

    def __init__(self):
        """Initialize the CSV processor."""
        self.io_handler = None
        self.schema_processor = None
        self.header_detector = None
        self.batch_processor = None

    def process(self, config: ImportConfig) -> ProcessingResults:
        """Process CSV file with streaming and validation.

        Main processing method that orchestrates the entire CSV import workflow
        including header detection, streaming processing, validation, and output generation.
        Now supports S3 streaming for both input and output.

        Args:
            config: ImportConfig instance with processing configuration

        Returns:
            ProcessingResults object containing processing statistics and output paths

        Raises:
            Exception: Any failure is recorded in ``results.errors`` and re-raised. A failed run
                leaves no partial ``data.parquet`` / ``bad_rows.parquet`` behind (local files
                are removed, S3 uploads are not completed), and outputs of earlier runs in the
                destination are removed when processing starts. One exception: a
                ``BadRowsThresholdExceededError`` (``x-validation`` rejected more rows than
                ``maxBadRowsPercent`` allows) discards ``data.parquet`` but keeps a finished
                ``bad_rows.parquet`` and names it (``error.bad_rows_file``).
        """
        start_time = time.time()
        results = ProcessingResults()
        outputs: Optional[_ParquetOutputs] = None

        try:
            # Initialize components
            self.io_handler = UnifiedIOHandler()
            self.schema_processor = SchemaProcessor(config, self.io_handler)
            self.header_detector = HeaderDetector(config, self.io_handler)

            # Load schema if provided
            schema = self.schema_processor.load_schema()
            converter = self.schema_processor.build_converter()

            # Detect header - now works with S3 inputs
            header_row_index, column_names = self._detect_header_row(config)

            # Required columns are looked up by name, so they have to exist
            required_columns = self._required_columns(config)
            self._check_required_columns_present(required_columns, column_names)

            # Schema extensions (x-transformations, x-columnMapping, x-calculatedColumns,
            # x-validation, x-primaryKey, x-rowHash, ...). Misconfiguration fails here, before
            # any output is written.
            pipeline = self._build_extension_pipeline(
                config, column_names, results, mark_nulls=converter.mark_nulls
            )

            # Prepare output paths - support both local and S3 outputs
            good_file, bad_file, use_s3_output = self._prepare_output_paths(config)

            # A re-run must not expose outputs of an earlier run
            self._remove_stale_outputs([good_file, bad_file], use_s3_output)

            # Initialize parquet writers using unified I/O
            outputs = _ParquetOutputs(
                good_file,
                bad_file,
                self.io_handler,
                use_s3_output,
                config.compression,
                reason_column=bool(pipeline and pipeline.rejects_rows),
            )

            # Initialize output metadata collector if enabled
            output_metadata_collector = self._initialize_metadata_collector(config)

            def write_rejected(rejected_batch: pa.RecordBatch) -> None:
                """Rows the reader could not turn into valid typed rows."""
                outputs.write_bad(
                    rejected_batch, self.batch_processor.last_reject_reason or "rejected_by_reader"
                )
                results.invalid_rows += len(rejected_batch)
                results.total_rows += len(rejected_batch)

            self.batch_processor = BatchProcessor(
                config,
                self.io_handler,
                converter=converter,
                reject_handler=write_rejected,
                pre_convert=pipeline.pre_convert if pipeline and pipeline.has_pre_stage else None,
            )

            # Process batches using extracted batch processor (no columns: nothing to read)
            batches = (
                self.batch_processor.create_s3_batch_reader(
                    config.input_path,
                    column_names,
                    header_row_index,
                    self.header_detector.should_stop_for_footer,
                )
                if column_names
                else ()
            )
            for batch in batches:
                # Validate and split batch
                valid_batch, invalid_batch = self._validate_batch(
                    batch, schema, config, required_columns
                )

                # Schema extensions run on the rows that passed the required check
                rejected_by_extensions = None
                extension_reasons: List[str] = []
                if pipeline is not None and pipeline.is_active:
                    stage = pipeline.post_convert(valid_batch)
                    valid_batch = stage.kept
                    rejected_by_extensions = stage.rejected
                    extension_reasons = stage.reasons
                else:
                    valid_batch = strip_hidden_columns(valid_batch)

                # Initialize writers on first batch (to get schema)
                outputs.ensure_good_writer(valid_batch.schema)

                # Write batches and collect metadata from FINAL OUTPUT DATA
                if len(valid_batch) > 0:
                    outputs.write_good(valid_batch)
                    # Collect metadata from the final transformed valid data
                    if output_metadata_collector:
                        output_metadata_collector.add_batch(valid_batch)
                    results.valid_rows += len(valid_batch)

                if len(invalid_batch) > 0:
                    outputs.write_bad(invalid_batch, "required_value_missing")
                    results.invalid_rows += len(invalid_batch)

                if rejected_by_extensions is not None and len(rejected_by_extensions) > 0:
                    outputs.write_bad(rejected_by_extensions, extension_reasons)
                    results.invalid_rows += len(rejected_by_extensions)

                results.total_rows += len(batch)

            # A header without rows (or only rejected rows) still yields an empty data file
            # that carries the schema
            if outputs.good_writer is None and column_names:
                empty_schema = self.batch_processor.established_schema or converter.empty_schema(
                    column_names
                )
                if pipeline is not None and pipeline.is_active:
                    empty_schema = pipeline.output_schema(empty_schema)
                outputs.ensure_good_writer(empty_schema)

            if pipeline is not None:
                # Constraints that judge the whole input (errorMode: fail_complete) report now
                pipeline.finalize()
                results.validation_summary = dict(pipeline.summary)

            results.truncated_rows = self.batch_processor.truncated_rows
            if results.truncated_rows:
                logger.warning(
                    "%d row(s) had more fields than the header and were truncated "
                    "(excess_column_mode=TRUNCATE)",
                    results.truncated_rows,
                )

            # Close writers
            outputs.close()
            self._record_output_files(outputs, results)

            # Create manifest and metadata (support S3 outputs)
            self._create_output_files(config, results, output_metadata_collector, outputs)

            results.execution_time = time.time() - start_time

        except BadRowsThresholdExceededError as e:
            # Too many rows were rejected: the data file is discarded, the rejected rows are kept
            # because they are what the user needs to see why
            error = self._explain_kept_bad_rows(e, outputs)
            results.errors.append(self._error_text(error))
            results.execution_time = time.time() - start_time
            raise error from None
        except Exception as e:
            error = self._friendly_error(e, config)
            results.errors.append(self._error_text(error))
            results.execution_time = time.time() - start_time
            if error is e:
                raise
            raise error from None
        finally:
            # Nothing is left open on success; after a failure this discards partial outputs
            if outputs is not None:
                outputs.abort()

        return results

    def _build_extension_pipeline(
        self,
        config: ImportConfig,
        column_names: Sequence[str],
        results: ProcessingResults,
        mark_nulls: Optional[Callable[[pa.RecordBatch], pa.RecordBatch]] = None,
    ) -> Optional[ExtensionPipeline]:
        """Create the schema extension pipeline (None when nothing is to be applied)."""
        schema_dict = self.schema_processor.schema_dict
        if not config.apply_schema_extensions or not schema_dict or not column_names:
            return None
        pipeline = build_extension_pipeline(
            schema_dict,
            column_names,
            source_uri=str(config.input_path),
            mark_nulls=mark_nulls,
        )
        if pipeline is not None:
            results.warnings.extend(pipeline.warnings)
            results.schema_extensions = list(pipeline.applied)
        return pipeline

    @staticmethod
    def _explain_kept_bad_rows(
        error: BadRowsThresholdExceededError, outputs: Optional[_ParquetOutputs]
    ) -> BadRowsThresholdExceededError:
        """Keep the rejected rows of an import that stopped on its threshold, say where."""
        try:
            kept = outputs.keep_bad_rows() if outputs is not None else None
        except Exception:
            logger.warning("Could not keep the rejected rows", exc_info=True)
            kept = None
        if kept is None:
            return error
        which = (
            "All rows rejected by the checks"
            if error.whole_input_checked
            else "The rows rejected before the import stopped"
        )
        explained = BadRowsThresholdExceededError(
            f"{error} {which} are in {kept}"
            + (" (the _rejection_reason column says why)." if outputs._reason_column else ".")
        )
        explained.whole_input_checked = error.whole_input_checked
        explained.bad_rows_file = kept
        return explained

    @staticmethod
    def _friendly_error(error: Exception, config: ImportConfig) -> Exception:
        """Undecodable input gets one clear message (Python's own names the byte, not the fix)."""
        if isinstance(error, UnicodeDecodeError):
            return ValueError(
                f"Input contains bytes that are not valid for encoding '{config.encoding}' "
                f"(byte offset {error.start}). Set the encoding the file was written with "
                "(for example 'latin-1' or 'cp1252')."
            )
        return error

    @staticmethod
    def _error_text(error: Exception) -> str:
        """Message for ``results.errors``; Arrow messages carry raw row content, so drop it."""
        if isinstance(error, pa.ArrowException):
            return f"{type(error).__name__}: {sanitize_arrow_error(str(error))}"
        return str(error)

    def _detect_header_row(self, config: ImportConfig):
        """Detect header row location and extract column names."""
        schema_columns = None
        if config.header_mode == HeaderMode.ABSENT:
            # No header in the file: the schema names the columns
            schema_columns = self.schema_processor.get_column_names_from_schema()

        return self.header_detector.detect_header_row(config.input_path, schema_columns)

    def _required_columns(self, config: ImportConfig) -> List[str]:
        """Columns the schema marks as required (none when validation is off)."""
        if not config.validate_schema or not self.schema_processor.schema_dict:
            return []
        return self.schema_processor.get_required_columns()

    @staticmethod
    def _check_required_columns_present(
        required_columns: Sequence[str], column_names: Sequence[str]
    ) -> None:
        """Fail early, with the column names, if a required column is not in the input."""
        if not column_names:
            return  # nothing to read
        missing = [name for name in required_columns if name not in column_names]
        if missing:
            raise ValueError(
                "Required column(s) missing from the input header: "
                + ", ".join(repr(name) for name in missing)
            )

    def _prepare_output_paths(self, config: ImportConfig):
        """Prepare output file paths for both local and S3 outputs."""
        if is_s3_path(config.output_path):
            # S3 output path
            output_s3_path = S3Path(str(config.output_path))
            good_file = str(output_s3_path.join("data.parquet"))
            bad_file = str(output_s3_path.join("bad_rows.parquet"))
            use_s3_output = True
        else:
            # Local output path
            output_dir = Path(config.output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            good_file = str(output_dir / "data.parquet")
            bad_file = str(output_dir / "bad_rows.parquet")
            use_s3_output = False

        return good_file, bad_file, use_s3_output

    def _remove_stale_outputs(self, files: Sequence[str], use_s3_output: bool) -> None:
        """Delete the parquet files an earlier run may have left (only the names written here)."""
        for path in files:
            try:
                if use_s3_output:
                    client = self.io_handler.s3_client
                    delete = getattr(client, "delete", None)
                    if callable(delete):
                        delete(path)
                    else:
                        target = S3Path(path)
                        client._s3_client.delete_object(Bucket=target.bucket, Key=target.key)
                else:
                    Path(path).unlink(missing_ok=True)
            except Exception:
                logger.warning("Could not remove stale output %s", path, exc_info=True)

    def _initialize_metadata_collector(self, config: ImportConfig):
        """Initialize output metadata collector if enabled."""
        if not config.create_metadata:
            return None

        # Read metadata configuration from schema if available
        metadata_config = {}
        if self.schema_processor.schema:
            metadata_config = self.schema_processor.get_metadata_config()

        return OutputMetadataCollector(
            enabled=metadata_config.get("enabled", True),
            enum_threshold=metadata_config.get("enum_detection", {}).get(
                "uniqueness_threshold", 0.1
            ),
            uniqueness_threshold=0.95,  # Default threshold for too unique columns
            top_n_values=metadata_config.get("statistics", {})
            .get("categorical", {})
            .get("top_n_values", 10),
            quantiles=metadata_config.get("statistics", {})
            .get("numeric", {})
            .get("quantiles", [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]),
            include_value_statistics=config.include_value_statistics,
        )

    def _validate_batch(
        self,
        batch: pa.RecordBatch,
        schema: Optional[pa.Schema],
        config: ImportConfig,
        required_columns: Optional[Sequence[str]] = None,
    ):
        """Validate batch and separate good/bad rows.

        Required columns are looked up by name. A row is invalid when a required column is null,
        or (string columns) empty.

        Args:
            batch: Batch to check
            schema: Schema loaded from the schema file, if any
            config: Import configuration
            required_columns: Required column names; derived from the schema when omitted

        Raises:
            ValueError: If a required column is not part of the batch
        """
        if not config.validate_schema or not schema:
            # No validation, return all as good
            empty_batch = batch.slice(0, 0)  # Empty batch with same schema
            return batch, empty_batch

        if required_columns is None:
            required_columns = [field.name for field in schema if not field.nullable]

        valid_mask = None
        for name in required_columns:
            position = batch.schema.get_field_index(name)
            if position < 0:
                raise ValueError(f"Required column '{name}' is missing from the input")

            column = batch.column(position)
            column_ok = pc.is_valid(column)
            if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                # An empty string is a missing value for a required text column
                is_empty = pc.fill_null(pc.equal(column, ""), False)
                column_ok = pc.and_(column_ok, pc.invert(is_empty))
            valid_mask = column_ok if valid_mask is None else pc.and_(valid_mask, column_ok)

        if valid_mask is None or valid_mask.false_count == 0:
            return batch, batch.slice(0, 0)

        # Split into valid and invalid batches
        return batch.filter(valid_mask), batch.filter(pc.invert(valid_mask))

    def _record_output_files(self, outputs: _ParquetOutputs, results: ProcessingResults) -> None:
        """List the finished files in the results."""
        if outputs.good_written:
            results.output_files.append(outputs.good_file)

        if outputs.bad_written:
            results.output_files.append(outputs.bad_file)
            results.bad_rows_file = outputs.bad_file

    def _create_output_files(
        self,
        config: ImportConfig,
        results: ProcessingResults,
        output_metadata_collector,
        outputs: _ParquetOutputs,
    ):
        """Create manifest and metadata files."""
        # Create manifest and metadata (support S3 outputs)
        if config.create_manifest:
            results.manifest_file = self._create_s3_manifest(
                config.output_path, results.output_files
            )

        if config.create_metadata:
            # Generate and save output metadata if we collected it
            if output_metadata_collector and output_metadata_collector.total_rows > 0:
                # Provenance recorded in the metadata file. Base names only: absolute paths
                # would leak local directory names into a file that is often shared.
                source_info = {
                    "input_path": os.path.basename(str(config.input_path)),
                    "processing_type": "csv_processing",
                    "schema_file": (
                        os.path.basename(str(config.schema_file)) if config.schema_file else None
                    ),
                    "total_batches_processed": "streaming",
                    "final_output_files": [os.path.basename(str(f)) for f in results.output_files],
                }

                # Save output metadata to a separate file (local directory or S3 prefix). The
                # data files are already written at this point, so a failure here is recorded
                # in results.errors and logged rather than discarding a finished run.
                try:
                    output_metadata_path = output_metadata_collector.save_metadata(
                        str(config.output_path),
                        "output_data_metadata.json",
                        schema=outputs.good_schema,
                        source_info=source_info,
                    )
                except MetadataWriteError as e:
                    logger.error("Output metadata was not written: %s", e)
                    results.errors.append(str(e))
                    output_metadata_path = None

                if output_metadata_path:
                    logger.info("Output data metadata saved to: %s", output_metadata_path)

            # Still create the traditional processing metadata
            results.metadata_file = self._create_s3_metadata(config.output_path, results)

    @staticmethod
    def _join_output(output_path: Union[str, Path], name: str) -> str:
        """Path of a file inside the output location (local directory or S3 prefix)."""
        if is_s3_path(output_path):
            return str(S3Path(str(output_path)).join(name))
        return os.path.join(str(output_path), name)

    def _write_json(self, path: str, payload: Any) -> str:
        """Serialise first (so a failure never leaves a half-written file), then write."""
        text = json.dumps(_json_safe(payload), indent=2, allow_nan=False, ensure_ascii=False)
        with self.io_handler.open_for_write(path, encoding="utf-8") as f:
            f.write(text)
        return path

    def _create_s3_manifest(self, output_path: Union[str, Path], files: list) -> str:
        """Create manifest file supporting S3 output locations."""
        manifest = {
            "format_version": "1.0",
            "files": [
                {
                    "file_path": S3Path(f).name if is_s3_path(f) else os.path.basename(str(f)),
                    "file_size": self.io_handler.get_size(f) if self.io_handler.exists(f) else 0,
                }
                for f in files
            ],
            "created_at": datetime.now().isoformat(),
        }

        return self._write_json(self._join_output(output_path, "manifest.json"), manifest)

    def _create_s3_metadata(
        self, output_path: Union[str, Path], results: ProcessingResults
    ) -> str:
        """Create metadata file supporting S3 output locations."""
        metadata = {
            "processing_summary": {
                "total_rows": results.total_rows,
                "valid_rows": results.valid_rows,
                "invalid_rows": results.invalid_rows,
                "truncated_rows": results.truncated_rows,
                "execution_time_seconds": results.execution_time,
            },
            "input_config": {
                "input_path": str(self.schema_processor.config.input_path),
                "schema_file": (
                    str(self.schema_processor.config.schema_file)
                    if self.schema_processor.config.schema_file
                    else None
                ),
                "header_mode": (
                    self.schema_processor.config.header_mode.value
                    if hasattr(self.schema_processor.config.header_mode, "value")
                    else str(self.schema_processor.config.header_mode)
                ),
                "batch_size": self.schema_processor.config.batch_size,
            },
            "output_files": results.output_files,
            "bad_rows_file": results.bad_rows_file,
            "schema_extensions": results.schema_extensions,
            "validation_summary": results.validation_summary,
            "warnings": results.warnings,
            "created_at": datetime.now().isoformat(),
        }

        return self._write_json(self._join_output(output_path, "metadata.json"), metadata)
