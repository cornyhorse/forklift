"""Batch processing logic for CSV data."""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional, Union

import pyarrow as pa
import pyarrow.csv as pv_csv

from ...io import S3Path, UnifiedIOHandler, is_s3_path
from ..config import ExcessColumnMode, ImportConfig
from .text_utils import read_encoding, sanitize_arrow_error
from .type_conversion import ColumnConverter

logger = logging.getLogger(__name__)

# Receives an all-string batch of rows that must go to bad_rows
RejectHandler = Callable[[pa.RecordBatch], None]
# Receives every raw batch before the schema types are applied and returns it, possibly changed
PreConvertHook = Callable[[pa.RecordBatch], pa.RecordBatch]

#: Values of ``BatchProcessor.last_reject_reason``
REJECT_TYPE_CONVERSION = "type_conversion_failed"
REJECT_EXCESS_COLUMNS = "too_many_fields"


class BatchProcessor:
    """Handles batch processing operations for CSV data.

    Both readers (the local Arrow stream and the S3/fallback row reader) hand batches through
    the same :class:`ColumnConverter`, so the schema's types are applied identically. Rows that
    cannot be used are not yielded: rows with an unconvertible value or (excess_column_mode
    REJECT) too many fields go to ``reject_handler`` as all-string batches.

    Attributes:
        truncated_rows: Rows that had more fields than the header and were cut (TRUNCATE)
        rejected_rows: Rows rejected for excess fields (REJECT) or an unconvertible value
        established_schema: Schema of the first batch produced; later batches are cast to it
        last_reject_reason: Why the batch most recently handed to ``reject_handler`` was rejected
            (``REJECT_TYPE_CONVERSION`` or ``REJECT_EXCESS_COLUMNS``)
    """

    def __init__(
        self,
        config: ImportConfig,
        io_handler: UnifiedIOHandler,
        converter: Optional[ColumnConverter] = None,
        reject_handler: Optional[RejectHandler] = None,
        pre_convert: Optional[PreConvertHook] = None,
    ):
        """Initialize batch processor with configuration.

        Args:
            config: Import configuration with processing settings
            io_handler: Unified I/O handler for file operations
            converter: Applies schema types and null markers (default: no schema)
            reject_handler: Called with an all-string batch for every group of rejected rows
            pre_convert: Called with each raw batch before the schema types are applied (the
                schema extensions use it to clean text columns); must keep the row count
        """
        self.config = config
        self.io_handler = io_handler
        self.converter = converter if converter is not None else ColumnConverter()
        self.reject_handler = reject_handler
        self.pre_convert = pre_convert
        self.last_reject_reason: Optional[str] = None
        self.truncated_rows = 0
        self.rejected_rows = 0
        self.established_schema: Optional[pa.Schema] = None
        self._batches_yielded = 0

    def _reset_state(self) -> None:
        """Forget what a previous read established (one processor may read several files)."""
        self.truncated_rows = 0
        self.rejected_rows = 0
        self.established_schema = None
        self._batches_yielded = 0

    def create_batch_reader(
        self, file_path: Path, column_names: List[str], header_row_index: int, footer_detector_func
    ) -> Iterator[pa.RecordBatch]:
        """Create a streaming batch reader for the CSV file.

        Sets up PyArrow CSV streaming reader with appropriate configuration
        and handles footer detection by creating filtered temporary files.

        Args:
            file_path: Path to the input CSV file
            column_names: List of column names for the data
            header_row_index: Index of the header row
            footer_detector_func: Function to detect footer rows

        Yields:
            PyArrow RecordBatch objects containing data from the CSV

        Raises:
            ArrowInvalid: If CSV parsing fails due to format issues. The message never
                contains row content.
        """
        # Check if file is empty before processing
        if file_path.stat().st_size == 0:
            return  # Return empty generator for empty files

        self._reset_state()

        # Skip to data start (after header/comments)
        skip_rows = 0
        if header_row_index is not None and header_row_index >= 0:
            skip_rows = header_row_index + 1

        # For footer detection, we need to create a filtered temporary file that already
        # has the header/comment rows removed
        use_filtered_file = bool(self.config.footer_detection)

        # Build every option first: nothing may fail once the temporary copy exists
        parse_options = pv_csv.ParseOptions(
            delimiter=self.config.delimiter,
            quote_char=self.config.quote_char,
            escape_char=self.config.escape_char or False,
            ignore_empty_lines=True,
        )

        read_options = pv_csv.ReadOptions(
            encoding=self.config.encoding,
            skip_rows=0 if use_filtered_file else skip_rows,
            column_names=column_names,
        )

        # Schema columns are read as raw strings (the converter types them per batch);
        # check_utf8 makes invalid UTF-8 an error instead of silently reaching the Parquet file
        convert_kwargs = {"check_utf8": True}
        arrow_column_types = self.converter.arrow_column_types(column_names)
        if arrow_column_types:
            convert_kwargs["column_types"] = arrow_column_types
        arrow_null_values = self.converter.arrow_null_values()
        if arrow_null_values is not None:
            convert_kwargs["null_values"] = arrow_null_values
        convert_options = pv_csv.ConvertOptions(**convert_kwargs)

        actual_file_path = file_path
        try:
            if use_filtered_file:
                actual_file_path = self._create_filtered_file(
                    file_path, skip_rows, footer_detector_func
                )
                skip_rows = 0  # Already handled in filtered file

            read_errors: List[pa.ArrowInvalid] = []
            rows_read = 0

            for arrow_batch in self._read_arrow_batches(
                actual_file_path, parse_options, read_options, convert_options, read_errors
            ):
                rows_read += len(arrow_batch)
                self._check_no_binary_columns(arrow_batch)
                for chunk in self._split_batch(arrow_batch):
                    finalized = self._finalize_batch(chunk)
                    if finalized is not None:
                        self._batches_yielded += 1
                        yield finalized

            if read_errors:
                error = read_errors[0]
                message = str(error)
                if "Empty CSV file" in message:
                    # Handle empty CSV files gracefully
                    return
                is_mismatch = "Expected" in message and "columns, got" in message
                # A later block holding a value that does not fit the type Arrow inferred from
                # the first one (encoding problems are reported as errors below)
                is_conversion = "CSV conversion error" in message and "utf8" not in message.lower()
                if is_mismatch or is_conversion:
                    if is_mismatch:
                        # Before falling back to column mismatch handler, check if this might
                        # be due to data corruption (null bytes, encoding issues, etc.)
                        error_line = self._extract_error_line_from_exception(message)
                        if error_line and self._contains_problematic_content(error_line):
                            # This appears to be data corruption, not a legitimate mismatch
                            raise pa.ArrowInvalid(sanitize_arrow_error(message)) from None

                    # Handle the rest with the row reader. The rows Arrow already delivered are
                    # skipped so nothing is written twice, and its string batches are cast to
                    # the schema established so far (values that do not fit are rejected).
                    yield from self._handle_column_mismatch_reader(
                        actual_file_path, skip_rows, column_names, rows_read
                    )
                else:
                    raise self._describe_arrow_error(error) from None
        finally:
            # Clean up temporary filtered file if created
            if use_filtered_file and actual_file_path != file_path:
                try:
                    Path(actual_file_path).unlink()
                except OSError:
                    pass

    def _read_arrow_batches(
        self,
        file_path: Path,
        parse_options: pv_csv.ParseOptions,
        read_options: pv_csv.ReadOptions,
        convert_options: pv_csv.ConvertOptions,
        errors: List[pa.ArrowInvalid],
    ) -> Iterator[pa.RecordBatch]:
        """Yield raw batches from Arrow's streaming reader; a parse error ends the stream.

        The error is appended to ``errors`` (instead of raised) so that problems in the code
        consuming the batches are never mistaken for problems in the file.
        """
        try:
            with open(file_path, "rb") as f:
                csv_reader = pv_csv.open_csv(
                    f,
                    parse_options=parse_options,
                    read_options=read_options,
                    convert_options=convert_options,
                )

                # Read in batches
                while True:
                    try:
                        batch = csv_reader.read_next_batch()
                    except StopIteration:
                        break
                    if batch is None:
                        break
                    yield batch
        except pa.ArrowInvalid as exc:
            errors.append(exc)

    def _describe_arrow_error(self, error: pa.ArrowInvalid) -> pa.ArrowInvalid:
        """Arrow error without row content, with a hint for encoding problems."""
        message = str(error)
        if "UTF8" in message or "utf8" in message:
            return pa.ArrowInvalid(
                f"Input contains bytes that are not valid for encoding "
                f"'{self.config.encoding}' ({sanitize_arrow_error(message)}). "
                "Set the encoding the file was written with (for example 'latin-1' or 'cp1252')."
            )
        return pa.ArrowInvalid(sanitize_arrow_error(message))

    def _check_no_binary_columns(self, batch: pa.RecordBatch) -> None:
        """Arrow infers ``binary`` for a column holding invalid UTF-8: treat it as an error."""
        if self.established_schema is not None:
            return  # types are fixed after the first block
        for field in batch.schema:
            if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
                raise pa.ArrowInvalid(
                    f"Column '{field.name}' contains bytes that are not valid for encoding "
                    f"'{self.config.encoding}'. Set the encoding the file was written with "
                    "(for example 'latin-1' or 'cp1252')."
                )

    def _split_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """Cut a batch into pieces of at most ``batch_size`` rows."""
        size = self._row_batch_size()
        if len(batch) <= size:
            yield batch
            return
        for offset in range(0, len(batch), size):
            yield batch.slice(offset, size)

    def _row_batch_size(self) -> int:
        """Configured rows per batch (falls back to the default for unusable values)."""
        size = getattr(self.config, "batch_size", None)
        if isinstance(size, int) and not isinstance(size, bool) and size > 0:
            return size
        return 10000

    def _finalize_batch(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        """Apply schema types/null markers and route unconvertible rows to the reject handler.

        Returns:
            The converted batch, or None if no row of it is left
        """
        if not isinstance(batch, pa.RecordBatch):
            return batch  # not a real batch (callers that stub the row converter)

        if self.pre_convert is not None:
            batch = self.pre_convert(batch)

        converter = self.converter
        if not converter.column_types and not converter.null_policy.configured:
            # No schema to apply; the first batch only fixes the schema later ones must match
            if self.established_schema is None:
                self.established_schema = batch.schema
            if batch.schema.equals(self.established_schema):
                return batch if len(batch) > 0 else None

        converted, rejected = converter.convert(batch, self.established_schema)
        if self.established_schema is None:
            self.established_schema = converted.schema
        if rejected is not None:
            self.rejected_rows += rejected.num_rows
            if self.reject_handler is not None:
                self.last_reject_reason = REJECT_TYPE_CONVERSION
                self.reject_handler(rejected)
        return converted if len(converted) > 0 else None

    def create_s3_batch_reader(
        self,
        input_path: Union[str, Path],
        column_names: List[str],
        header_row_index: int,
        footer_detector_func,
    ) -> Iterator[pa.RecordBatch]:
        """Create a streaming batch reader that works with both local files and S3.

        Args:
            input_path: Path to input file (local or S3 URI)
            column_names: List of column names for the data
            header_row_index: Index of the header row
            footer_detector_func: Function to detect footer rows

        Yields:
            PyArrow RecordBatch objects containing data from the CSV
        """
        if is_s3_path(input_path):
            # S3 input - use fallback to row-by-row processing since PyArrow CSV
            # doesn't directly stream from S3
            yield from self._create_s3_csv_batches(
                input_path, column_names, header_row_index, footer_detector_func
            )
        else:
            # Local file - use existing PyArrow streaming
            yield from self.create_batch_reader(
                Path(input_path), column_names, header_row_index, footer_detector_func
            )

    def _create_s3_csv_batches(
        self,
        s3_path: Union[str, S3Path],
        column_names: List[str],
        header_row_index: int,
        footer_detector_func,
    ) -> Iterator[pa.RecordBatch]:
        """Create batches from S3 CSV by processing rows and converting to RecordBatch.

        Args:
            s3_path: S3 path to CSV file
            column_names: List of column names for the data
            header_row_index: Index of the header row
            footer_detector_func: Function to detect footer rows

        Yields:
            PyArrow RecordBatch objects
        """
        if not column_names:
            return  # Return empty generator for empty column names

        self._reset_state()

        # Skip header rows if needed
        rows_to_skip = 0
        if header_row_index is not None and header_row_index >= 0:
            rows_to_skip = header_row_index + 1

        records = self.io_handler.csv_reader(
            s3_path,
            delimiter=self.config.delimiter,
            quotechar=self.config.quote_char,
            encoding=read_encoding(self.config.encoding),
            escapechar=self.config.escape_char,
        )
        yield from self._batches_from_records(
            records, column_names, rows_to_skip, footer_detector_func
        )

    def _handle_column_mismatch_reader(
        self,
        file_path: Path,
        skip_rows: int,
        column_names: List[str],
        skip_data_rows: int = 0,
    ) -> Iterator[pa.RecordBatch]:
        """Handle column mismatch by processing rows with different column counts.

        When some rows have more or fewer columns than expected, this method
        processes them according to the excess_column_mode configuration.

        Args:
            file_path: Path to the CSV file
            skip_rows: Number of rows to skip
            column_names: List of column names
            skip_data_rows: Data rows to skip after ``skip_rows`` because the Arrow reader
                already delivered them

        Yields:
            PyArrow RecordBatch objects
        """
        if not column_names:
            return  # Return empty generator for empty column names

        with open(file_path, "r", encoding=read_encoding(self.config.encoding), newline="") as f:
            reader = csv.reader(
                f,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char,
                escapechar=self.config.escape_char,
            )
            yield from self._batches_from_records(
                reader, column_names, skip_rows, None, skip_data_rows
            )

    def _batches_from_records(
        self,
        records: Iterable[List[str]],
        column_names: List[str],
        skip_records: int,
        footer_detector_func,
        skip_data_rows: int = 0,
    ) -> Iterator[pa.RecordBatch]:
        """Turn parsed CSV records into batches (S3 reader and column-mismatch fallback).

        Args:
            records: Parsed records; a blank line is an empty list
            column_names: Column names (extended in place for PASSTHROUGH)
            skip_records: Leading records to drop (header and comment rows)
            footer_detector_func: Optional callable; the first record it accepts ends the data
            skip_data_rows: Leading non-blank data records to drop (already delivered)

        Yields:
            PyArrow RecordBatch objects

        Raises:
            ValueError: In PASSTHROUGH mode when a row is wider than the columns that are
                already being written
        """
        expected_columns = len(column_names)
        mode = self.config.excess_column_mode
        batch_size = self._row_batch_size()

        rows_buffer: List[List[str]] = []
        rejected_buffer: List[List[str]] = []
        data_row = 0

        for record_no, row in enumerate(records, start=1):
            # Skip header rows
            if record_no <= skip_records:
                continue

            # Stop if footer detected
            if footer_detector_func is not None and footer_detector_func(row):
                break

            # csv.reader yields [] for a blank line; Arrow's ignore_empty_lines drops them
            if not row:
                continue

            data_row += 1
            if data_row <= skip_data_rows:
                continue

            # Handle column count mismatches
            if len(row) > expected_columns:
                if mode == ExcessColumnMode.REJECT:
                    rejected_buffer.append(row)
                    if len(rejected_buffer) >= batch_size:
                        self._flush_rejected(rejected_buffer, column_names)
                    continue
                elif mode == ExcessColumnMode.PASSTHROUGH:
                    if self._batches_yielded > 0 or self.established_schema is not None:
                        raise ValueError(
                            f"Data row {data_row} has {len(row)} fields but the output was "
                            f"already started with {expected_columns} columns; "
                            "excess_column_mode='passthrough' cannot add columns once data "
                            "has been written. Put the widest row first, or use 'truncate' "
                            "or 'reject'."
                        )
                    # Keep all columns - generate default names for the extra columns
                    for i in range(len(column_names), len(row)):
                        column_names.append(f"col_{i+1}")
                    expected_columns = len(column_names)
                    # Don't truncate the row, keep all data
                else:  # TRUNCATE mode (default)
                    row = row[:expected_columns]
                    self.truncated_rows += 1
            elif len(row) < expected_columns:
                # Pad with empty strings
                row = row + [""] * (expected_columns - len(row))

            rows_buffer.append(row)

            # Yield batch when buffer is full
            if len(rows_buffer) >= batch_size:
                batch = self._finalize_batch(
                    self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)
                )
                rows_buffer = []
                if batch is not None:
                    self._batches_yielded += 1
                    yield batch

        # Rows rejected for excess fields are written even if no valid row remains
        self._flush_rejected(rejected_buffer, column_names)

        # Yield any remaining rows in buffer
        if rows_buffer:
            batch = self._finalize_batch(
                self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)
            )
            if batch is not None:
                self._batches_yielded += 1
                yield batch

    def _flush_rejected(self, rows: List[List[str]], column_names: List[str]) -> None:
        """Hand rows rejected for excess fields to the reject handler and clear the buffer."""
        if not rows:
            return
        self.rejected_rows += len(rows)
        if self.reject_handler is not None:
            # Stored in the shape of the header: fields beyond it are not kept
            self.last_reject_reason = REJECT_EXCESS_COLUMNS
            self.reject_handler(self._convert_rows_to_batch(rows, len(column_names), column_names))
        rows.clear()

    def _convert_rows_to_batch(
        self, rows: List[List[str]], num_columns: int, column_names: List[str]
    ) -> pa.RecordBatch:
        """Convert a list of rows to a PyArrow RecordBatch.

        Args:
            rows: List of rows, each row is a list of string values
            num_columns: Expected number of columns in each row
            column_names: List of column names

        Returns:
            PyArrow RecordBatch object containing the data
        """
        if not rows:
            # Return empty batch with proper schema
            schema = pa.schema([pa.field(name, pa.string()) for name in column_names])
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=pa.string()) for _ in column_names], schema=schema
            )

        # Convert rows to column arrays
        columns = []
        for col_idx in range(num_columns):
            column_data = [row[col_idx] if col_idx < len(row) else "" for row in rows]
            columns.append(pa.array(column_data, type=pa.string()))

        # Create schema with proper column names
        schema = pa.schema([pa.field(name, pa.string()) for name in column_names])

        return pa.RecordBatch.from_arrays(columns, schema=schema)

    def _create_filtered_file(self, file_path: Path, skip_rows: int, footer_detector_func) -> Path:
        """Create a temporary file with footer content removed.

        When footer detection is enabled, this creates a cleaned version of
        the input file with footer content removed to prevent PyArrow parsing errors.
        The temporary file holds plaintext data: it is deleted again if copying fails, and the
        caller deletes it when the read is done.

        Args:
            file_path: Path to the original input file
            skip_rows: Number of rows to skip from the beginning
            footer_detector_func: Function to detect footer rows

        Returns:
            Path to the temporary filtered file
        """
        # Everything that could fail is prepared before the temporary file exists
        read_codec = read_encoding(self.config.encoding)
        dialect = {
            "delimiter": self.config.delimiter,
            "quotechar": self.config.quote_char,
            "escapechar": self.config.escape_char,
        }

        # Create temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv", text=True)

        try:
            try:
                with open(file_path, "r", encoding=read_codec, newline="") as input_file:
                    with open(
                        temp_fd,
                        "w",
                        encoding=self.config.encoding,
                        newline="",
                        closefd=False,
                    ) as output_file:
                        reader = csv.reader(input_file, **dialect)
                        writer = csv.writer(output_file, **dialect)

                        # Skip the specified number of rows
                        for _ in range(skip_rows):
                            try:
                                next(reader)
                            except StopIteration:
                                break

                        # Copy data rows until footer is detected
                        for row in reader:
                            if footer_detector_func(row):
                                break
                            writer.writerow(row)
            finally:
                os.close(temp_fd)
        except BaseException:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise

        return Path(temp_path)

    def _extract_error_line_from_exception(self, error_message: str) -> str:
        """Extract the problematic line content from PyArrow exception message.

        Args:
            error_message: The exception message from PyArrow

        Returns:
            The line content that caused the error, or empty string if not found
        """
        # PyArrow error messages often include the problematic line content
        # Format: "CSV parse error: Expected X columns, got Y: actual_line_content"
        if ": " in error_message:
            parts = error_message.split(": ")
            if len(parts) >= 3:
                # The last part after the last colon should be the line content
                return parts[-1].strip()
        return ""

    def _contains_problematic_content(self, line_content: str) -> bool:
        """Check if line content contains problematic characters that indicate corruption.

        Args:
            line_content: The line content to check

        Returns:
            True if the content appears to be corrupted, False otherwise
        """
        if not line_content:
            return False

        # Check for null bytes and other control characters that shouldn't be in CSV
        problematic_chars = {
            "\x00",
            "\x01",
            "\x02",
            "\x03",
            "\x04",
            "\x05",
            "\x06",
            "\x07",
            "\x08",
            "\x0b",
            "\x0c",
            "\x0e",
            "\x0f",
            "\x10",
            "\x11",
            "\x12",
            "\x13",
            "\x14",
            "\x15",
            "\x16",
            "\x17",
            "\x18",
            "\x19",
            "\x1a",
            "\x1b",
            "\x1c",
            "\x1d",
            "\x1e",
            "\x1f",
        }

        # Check if any problematic characters are present
        for char in line_content:
            if char in problematic_chars:
                return True

        # Check for other signs of corruption like invalid UTF-8 sequences
        # that might have been converted to replacement characters
        if "�" in line_content:  # Unicode replacement character
            return True

        return False
