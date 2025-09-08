"""Batch processing logic for CSV data."""

from __future__ import annotations
import csv
import tempfile
from pathlib import Path
from typing import Iterator, List, Tuple, Union

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.compute as pc

from ..config import ImportConfig, ExcessColumnMode
from ...io import UnifiedIOHandler, is_s3_path, S3Path


class BatchProcessor:
    """Handles batch processing operations for CSV data."""

    def __init__(self, config: ImportConfig, io_handler: UnifiedIOHandler):
        """Initialize batch processor with configuration.

        Args:
            config: Import configuration with processing settings
            io_handler: Unified I/O handler for file operations
        """
        self.config = config
        self.io_handler = io_handler

    def create_batch_reader(self, file_path: Path, column_names: List[str],
                           header_row_index: int, footer_detector_func) -> Iterator[pa.RecordBatch]:
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
            ArrowInvalid: If CSV parsing fails due to format issues
        """
        # Check if file is empty before processing
        if file_path.stat().st_size == 0:
            return  # Return empty generator for empty files

        # Skip to data start (after header/comments)
        skip_rows = 0
        if header_row_index is not None and header_row_index >= 0:
            skip_rows = header_row_index + 1

        # For footer detection, we need to create a filtered temporary file
        if self.config.footer_detection:
            filtered_file = self._create_filtered_file(file_path, skip_rows, footer_detector_func)
            actual_file_path = filtered_file
            skip_rows = 0  # Already handled in filtered file
        else:
            actual_file_path = file_path

        # Configure CSV read options
        parse_options = pv_csv.ParseOptions(
            delimiter=self.config.delimiter,
            quote_char=self.config.quote_char,
            escape_char=self.config.escape_char,
            ignore_empty_lines=True,
        )

        read_options = pv_csv.ReadOptions(
            encoding=self.config.encoding,
            skip_rows=skip_rows,
            column_names=column_names,
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=False,  # We'll handle encoding issues
        )

        # Create streaming reader
        try:
            with open(actual_file_path, 'rb') as f:
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
                        if batch is None:
                            break
                        # Only yield non-empty batches
                        if len(batch) > 0:
                            yield batch
                    except StopIteration:
                        break

        except pa.ArrowInvalid as e:
            if "Empty CSV file" in str(e):
                # Handle empty CSV files gracefully
                return
            elif "Expected" in str(e) and "columns, got" in str(e):
                # Before falling back to column mismatch handler, check if this might be
                # due to data corruption (null bytes, encoding issues, etc.)
                error_line = self._extract_error_line_from_exception(str(e))
                if error_line and self._contains_problematic_content(error_line):
                    # This appears to be data corruption, not a legitimate column mismatch
                    raise

                # Handle legitimate column count mismatches with new row handling
                yield from self._handle_column_mismatch_reader(actual_file_path, skip_rows, column_names)
            else:
                raise
        finally:
            # Clean up temporary filtered file if created
            if self.config.footer_detection and actual_file_path != file_path:
                try:
                    Path(actual_file_path).unlink()
                except:
                    pass

    def create_s3_batch_reader(self, input_path: Union[str, Path], column_names: List[str],
                              header_row_index: int, footer_detector_func) -> Iterator[pa.RecordBatch]:
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
            yield from self._create_s3_csv_batches(input_path, column_names, header_row_index, footer_detector_func)
        else:
            # Local file - use existing PyArrow streaming
            yield from self.create_batch_reader(Path(input_path), column_names, header_row_index, footer_detector_func)

    def _create_s3_csv_batches(self, s3_path: Union[str, S3Path], column_names: List[str],
                              header_row_index: int, footer_detector_func) -> Iterator[pa.RecordBatch]:
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

        rows_buffer = []
        batch_size = self.config.batch_size
        expected_columns = len(column_names)

        # Skip header rows if needed
        rows_to_skip = 0
        if header_row_index is not None and header_row_index >= 0:
            rows_to_skip = header_row_index + 1

        row_count = 0
        for row in self.io_handler.csv_reader(
            s3_path,
            delimiter=self.config.delimiter,
            quotechar=self.config.quote_char,
            encoding=self.config.encoding
        ):
            # Skip header rows
            if row_count < rows_to_skip:
                row_count += 1
                continue

            # Stop if footer detected
            if footer_detector_func(row):
                break

            # Handle column count mismatches
            if len(row) > expected_columns:
                if self.config.excess_column_mode == ExcessColumnMode.REJECT:
                    continue  # Skip this row
                else:  # TRUNCATE mode
                    row = row[:expected_columns]
            elif len(row) < expected_columns:
                # Pad with empty strings
                row = row + [''] * (expected_columns - len(row))

            rows_buffer.append(row)
            row_count += 1

            # Yield batch when buffer is full
            if len(rows_buffer) >= batch_size:
                yield self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)
                rows_buffer = []

        # Yield any remaining rows in buffer
        if rows_buffer:
            yield self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)

    def _handle_column_mismatch_reader(self, file_path: Path, skip_rows: int,
                                     column_names: List[str]) -> Iterator[pa.RecordBatch]:
        """Handle column mismatch by processing rows with different column counts.

        When some rows have more or fewer columns than expected, this method
        processes them according to the excess_column_mode configuration.

        Args:
            file_path: Path to the CSV file
            skip_rows: Number of rows to skip
            column_names: List of column names

        Yields:
            PyArrow RecordBatch objects
        """
        if not column_names:
            return  # Return empty generator for empty column names

        expected_columns = len(column_names)
        rows_buffer = []
        rejected_rows = []
        batch_size = self.config.batch_size

        with open(file_path, 'r', encoding=self.config.encoding) as f:
            reader = csv.reader(f, delimiter=self.config.delimiter, quotechar=self.config.quote_char)

            # Skip the specified number of rows
            for _ in range(skip_rows):
                try:
                    next(reader)
                except StopIteration:
                    break

            for row in reader:
                # Handle excess columns according to configuration
                if len(row) > expected_columns:
                    if self.config.excess_column_mode == ExcessColumnMode.REJECT:
                        # Reject the entire row if it has excess columns
                        rejected_rows.append(row)
                        continue
                    else:  # TRUNCATE mode (default)
                        # Remove excess columns and keep the row
                        row = row[:expected_columns]
                elif len(row) < expected_columns:
                    # Pad with empty strings for missing columns
                    row = row + [''] * (expected_columns - len(row))

                rows_buffer.append(row)

                # Yield batch when buffer is full
                if len(rows_buffer) >= batch_size:
                    yield self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)
                    rows_buffer = []

            # Yield any remaining rows in buffer
            if rows_buffer:
                yield self._convert_rows_to_batch(rows_buffer, expected_columns, column_names)

            # Note: rejected_rows could be logged or handled separately in future versions

    def _convert_rows_to_batch(self, rows: List[List[str]], num_columns: int,
                              column_names: List[str]) -> pa.RecordBatch:
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
                [pa.array([], type=pa.string()) for _ in column_names],
                schema=schema
            )

        # Convert rows to column arrays
        columns = []
        for col_idx in range(num_columns):
            column_data = [row[col_idx] if col_idx < len(row) else '' for row in rows]
            columns.append(pa.array(column_data, type=pa.string()))

        # Create schema with proper column names
        schema = pa.schema([pa.field(name, pa.string()) for name in column_names])

        return pa.RecordBatch.from_arrays(columns, schema=schema)

    def _create_filtered_file(self, file_path: Path, skip_rows: int,
                             footer_detector_func) -> Path:
        """Create a temporary file with footer content removed.

        When footer detection is enabled, this creates a cleaned version of
        the input file with footer content removed to prevent PyArrow parsing errors.

        Args:
            file_path: Path to the original input file
            skip_rows: Number of rows to skip from the beginning
            footer_detector_func: Function to detect footer rows

        Returns:
            Path to the temporary filtered file
        """
        # Create temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)

        try:
            with open(file_path, 'r', encoding=self.config.encoding) as input_file:
                with open(temp_fd, 'w', encoding=self.config.encoding, closefd=False) as output_file:
                    reader = csv.reader(input_file, delimiter=self.config.delimiter)
                    writer = csv.writer(output_file, delimiter=self.config.delimiter)

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
            import os
            os.close(temp_fd)

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
        problematic_chars = {'\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07',
                           '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12',
                           '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a',
                           '\x1b', '\x1c', '\x1d', '\x1e', '\x1f'}

        # Check if any problematic characters are present
        for char in line_content:
            if char in problematic_chars:
                return True

        # Check for other signs of corruption like invalid UTF-8 sequences
        # that might have been converted to replacement characters
        if '\ufffd' in line_content:  # Unicode replacement character
            return True

        return False
