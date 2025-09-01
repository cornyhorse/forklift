"""Core Forklift engine for streaming data import with PyArrow.

This module provides the core functionality for importing CSV files with PyArrow
streaming capabilities, including header detection, footer detection, validation,
and output generation. Now supports S3 streaming for both input and output.
"""

from __future__ import annotations
import csv
import re
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Iterator, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Import S3 streaming capabilities
from ..io import UnifiedIOHandler, S3StreamingClient, is_s3_path, S3Path, create_parquet_writer


class HeaderMode(Enum):
    """Header detection modes for CSV processing.

    Attributes:
        PRESENT: File has header row that should be used
        ABSENT: No header row, use schema or generate default names
        AUTO: Auto-detect header location by analyzing content
    """
    PRESENT = "present"  # File has header row
    ABSENT = "absent"   # No header, use schema or default names
    AUTO = "auto"       # Auto-detect header location


class ExcessColumnMode(Enum):
    """Modes for handling excess columns beyond expected schema.

    Attributes:
        TRUNCATE: Remove excess columns and keep the row (default)
        REJECT: Reject the entire row if it has excess columns
    """
    TRUNCATE = "truncate"  # Remove excess data, keep row
    REJECT = "reject"      # Reject entire row with excess data


@dataclass
class ImportConfig:
    """Configuration for data import operations.

    Args:
        input_path: Path to input file to process
        output_path: Directory where output files will be created
        schema_file: Optional path to JSON schema file for validation
        batch_size: Number of rows to process in each batch (default: 10000)
        encoding: Text encoding of the input file (default: utf-8)
        header_mode: How to handle header detection (default: PRESENT)
        header_search_rows: Maximum rows to search for header (default: 10)
        skip_blank_lines: Whether to skip blank lines during processing
        comment_rows: List of regex patterns for comment row detection
        footer_detection: Configuration for footer detection and stopping
        delimiter: Field delimiter character (default: comma)
        quote_char: Quote character for fields (default: double quote)
        escape_char: Escape character for special characters
        validate_schema: Whether to perform schema validation
        max_validation_errors: Maximum validation errors before stopping
        create_manifest: Whether to create manifest file
        create_metadata: Whether to create metadata file
        compression: Compression type for output files (default: snappy)
        excess_column_mode: How to handle rows with excess columns (default: TRUNCATE)
    """
    input_path: Union[str, Path]
    output_path: Union[str, Path]
    schema_file: Optional[Union[str, Path]] = None
    batch_size: int = 10000
    encoding: str = "utf-8"
    header_mode: HeaderMode = HeaderMode.PRESENT
    header_search_rows: int = 10
    skip_blank_lines: bool = True
    comment_rows: Optional[List[str]] = None  # Patterns to skip as comments
    footer_detection: Optional[Dict[str, Any]] = None

    # CSV specific
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: Optional[str] = None

    # Row handling options
    excess_column_mode: ExcessColumnMode = ExcessColumnMode.TRUNCATE

    # Validation options
    validate_schema: bool = True
    max_validation_errors: int = 1000

    # Output options
    create_manifest: bool = True
    create_metadata: bool = True
    compression: str = "snappy"


@dataclass
class ProcessingResults:
    """Results from data processing operation.

    Attributes:
        total_rows: Total number of rows processed
        valid_rows: Number of rows that passed validation
        invalid_rows: Number of rows that failed validation
        output_files: List of paths to generated output files
        manifest_file: Path to generated manifest file (if created)
        metadata_file: Path to generated metadata file (if created)
        execution_time: Total processing time in seconds
        errors: List of error messages encountered during processing
    """
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    output_files: List[str] = field(default_factory=list)
    manifest_file: Optional[str] = None
    metadata_file: Optional[str] = None
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)


class ForkliftCore:
    """Core engine for streaming data import with PyArrow.

    This class provides the main functionality for importing CSV files using
    PyArrow's streaming capabilities. It supports header detection, footer
    detection, schema validation, and various output formats.

    Args:
        config: ImportConfig instance with processing configuration

    Attributes:
        config: The configuration object for this processing session
        schema: PyArrow schema loaded from schema file (if provided)
        header_row_index: Index of the header row in the file
        column_names: List of column names extracted from header
    """

    def __init__(self, config: ImportConfig):
        """Initialize the ForkliftCore engine.

        Args:
            config: Configuration object containing processing parameters
        """
        self.config = config
        self.schema: Optional[pa.Schema] = None
        self.header_row_index: Optional[int] = None
        self.column_names: Optional[List[str]] = None

        # Initialize unified I/O handler for S3 and local file support
        self.io_handler = UnifiedIOHandler()

        # Convert string header_mode to enum if needed
        if isinstance(self.config.header_mode, str):
            self.config.header_mode = HeaderMode(self.config.header_mode)

    def _load_schema(self) -> Optional[pa.Schema]:
        """Load and parse schema from file.

        Returns:
            PyArrow schema object if schema file provided, None otherwise

        Raises:
            FileNotFoundError: If schema file path is provided but file doesn't exist
        """
        if not self.config.schema_file:
            return None

        # Use unified I/O handler to support S3 schema files
        if not self.io_handler.exists(self.config.schema_file):
            raise FileNotFoundError(f"Schema file not found: {self.config.schema_file}")

        with self.io_handler.open_for_read(self.config.schema_file, encoding='utf-8') as f:
            schema_dict = json.load(f)

        # Convert JSON schema to PyArrow schema
        return self._json_schema_to_pyarrow(schema_dict)

    def _json_schema_to_pyarrow(self, schema_dict: Dict[str, Any]) -> pa.Schema:
        """Convert JSON schema to PyArrow schema.

        Args:
            schema_dict: Dictionary containing JSON schema definition

        Returns:
            PyArrow schema object with fields and types
        """
        properties = schema_dict.get("properties", {})
        fields = []

        for field_name, field_def in properties.items():
            field_type = self._json_type_to_pyarrow(field_def)
            nullable = field_name not in schema_dict.get("required", [])
            fields.append(pa.field(field_name, field_type, nullable=nullable))

        return pa.schema(fields)

    def _json_type_to_pyarrow(self, field_def: Dict[str, Any]) -> pa.DataType:
        """Convert JSON schema field definition to PyArrow data type.

        Args:
            field_def: Dictionary containing field type definition

        Returns:
            PyArrow data type corresponding to the JSON schema type
        """
        json_type = field_def.get("type", "string")
        format_hint = field_def.get("format", "")

        type_mapping = {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
            "null": pa.null(),
        }

        if json_type == "string" and format_hint == "date":
            return pa.date32()
        elif json_type == "string" and format_hint == "date-time":
            return pa.timestamp("us")

        return type_mapping.get(json_type, pa.string())

    def _detect_header_row(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Detect header row location and extract column names.

        Uses the configured header mode to determine how to find and extract
        column names from the input file (local or S3).

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If header detection fails and no fallback is available
        """
        if self.config.header_mode == HeaderMode.ABSENT:
            # No header, use schema or generate names
            if self.schema:
                return -1, [field.name for field in self.schema]
            else:
                # Generate default names - we'll determine count from first data row
                return -1, []

        elif self.config.header_mode == HeaderMode.PRESENT:
            # Header is expected at first non-comment row
            header_idx, columns = self._find_first_data_row(input_path)
            return header_idx, columns

        else:  # AUTO mode
            return self._auto_detect_header(input_path)

    def _find_first_data_row(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Find the first non-comment row and extract columns.

        Searches through the file to find the first row that is not a comment
        or blank line, treating it as the header row. Works with local files and S3.

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (row_index, column_names). Returns (-1, []) for empty files.
        """
        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self.io_handler.csv_reader(
            input_path,
            delimiter=self.config.delimiter,
            encoding=self.config.encoding
        )):
            if idx >= self.config.header_search_rows:
                break

            # Skip completely empty rows
            if not row:
                continue

            # Check for comment rows (lines starting with #)
            if row and row[0].strip().startswith('#'):
                continue

            if self._is_comment_row(row):
                continue

            if self.config.skip_blank_lines and not any(cell.strip() for cell in row):
                continue

            return idx, [col.strip() for col in row]

        # Handle empty files gracefully
        return -1, []

    def _auto_detect_header(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Auto-detect header row by looking for text patterns.

        Analyzes the first several rows to identify which one looks most like
        a header based on the ratio of text to numeric content. Works with local files and S3.

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If no suitable header row can be detected
        """
        rows = []

        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self.io_handler.csv_reader(
            input_path,
            delimiter=self.config.delimiter,
            encoding=self.config.encoding
        )):
            if idx >= self.config.header_search_rows:
                break

            if self._is_comment_row(row):
                continue

            rows.append((idx, row))

        # Look for a row that looks like headers (mostly text, few numbers)
        for idx, row in rows:
            if self._looks_like_header(row):
                return idx, [col.strip() for col in row]

        # Default to first row
        if rows:
            return rows[0][0], [col.strip() for col in rows[0][1]]

        raise ValueError("Could not detect header row")

    def _looks_like_header(self, row: List[str]) -> bool:
        """Determine if a row looks like a header row.

        Analyzes the content of a row to determine if it appears to be a header
        based on the ratio of text content to numeric content.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if row appears to be a header, False otherwise
        """
        if not row:
            return False

        text_count = 0
        number_count = 0

        for cell in row:
            cell = cell.strip()
            if not cell:
                continue

            try:
                float(cell)
                number_count += 1
            except ValueError:
                text_count += 1

        # Header likely if mostly text
        return text_count > number_count

    def _is_comment_row(self, row: List[str]) -> bool:
        """Check if row should be treated as a comment.

        Tests the first cell of the row against configured comment patterns
        to determine if the entire row should be skipped.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if row matches a comment pattern, False otherwise
        """
        if not self.config.comment_rows or not row:
            return False

        first_cell = row[0].strip() if row else ""

        for comment_pattern in self.config.comment_rows:
            if re.match(comment_pattern, first_cell):
                return True

        return False

    def _should_stop_for_footer(self, row: List[str]) -> bool:
        """Check if we should stop processing due to footer detection.

        Tests the row against configured footer detection rules to determine
        if processing should stop before this row.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if footer detected and processing should stop, False otherwise
        """
        if not self.config.footer_detection:
            return False

        detection = self.config.footer_detection

        # Check for blank row stopping
        if detection.get("stop_on_blank", False):
            # Handle completely empty rows or rows with only empty strings
            if not row or not any(cell.strip() for cell in row):
                return True

        # Check for pattern in specific column
        if "column_index" in detection and "patterns" in detection:
            col_idx = detection["column_index"]
            if 0 <= col_idx < len(row):
                cell_value = row[col_idx].strip()
                for pattern in detection["patterns"]:
                    if re.match(pattern, cell_value):
                        return True

        return False

    def _create_batch_reader(self, file_path: Path) -> Iterator[pa.RecordBatch]:
        """Create a streaming batch reader for the CSV file.

        Sets up PyArrow CSV streaming reader with appropriate configuration
        and handles footer detection by creating filtered temporary files.

        Args:
            file_path: Path to the input CSV file

        Yields:
            PyArrow RecordBatch objects containing data from the CSV

        Raises:
            ArrowInvalid: If CSV parsing fails due to format issues
        """
        # Check if file is empty before processing
        if file_path.stat().st_size == 0:
            return iter([])  # Return empty iterator for empty files

        # Skip to data start (after header/comments)
        skip_rows = 0
        if self.header_row_index is not None and self.header_row_index >= 0:
            skip_rows = self.header_row_index + 1

        # For footer detection, we need to create a filtered temporary file
        if self.config.footer_detection:
            filtered_file = self._create_filtered_file(file_path, skip_rows)
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
            column_names=self.column_names,
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=False,  # We'll handle encoding issues
        )

        # Create streaming reader
        try:
            with open(actual_file_path, 'rb') as f:
                try:
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
                            if batch is None or len(batch) == 0:
                                break
                            yield batch
                        except StopIteration:
                            break
                except pa.ArrowInvalid as e:
                    if "Empty CSV file" in str(e):
                        # Handle empty CSV files gracefully
                        return iter([])
                    elif "Expected" in str(e) and "columns, got" in str(e):
                        # Handle column count mismatches with new row handling
                        yield from self._handle_column_mismatch_reader(actual_file_path, skip_rows)
                    else:
                        raise
        finally:
            # Clean up temporary filtered file if created
            if self.config.footer_detection and actual_file_path != file_path:
                try:
                    Path(actual_file_path).unlink()
                except:
                    pass

    def _handle_column_mismatch_reader(self, file_path: Path, skip_rows: int) -> Iterator[pa.RecordBatch]:
        """Handle column mismatch by processing rows with different column counts.

        When some rows have more or fewer columns than expected, this method
        processes them according to the excess_column_mode configuration.

        Args:
            file_path: Path to the CSV file
            skip_rows: Number of rows to skip

        Yields:
            PyArrow RecordBatch objects
        """
        if not self.column_names:
            return iter([])

        expected_columns = len(self.column_names)
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
                # Stop if footer detected
                if self._should_stop_for_footer(row):
                    break

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
                    yield self._convert_rows_to_batch(rows_buffer, expected_columns)
                    rows_buffer = []

            # Yield any remaining rows in buffer
            if rows_buffer:
                yield self._convert_rows_to_batch(rows_buffer, expected_columns)

            # Note: rejected_rows could be logged or handled separately in future versions

    def _convert_rows_to_batch(self, rows: List[List[str]], num_columns: int) -> pa.RecordBatch:
        """Convert a list of rows to a PyArrow RecordBatch.

        Args:
            rows: List of rows, each row is a list of string values
            num_columns: Expected number of columns in each row

        Returns:
            PyArrow RecordBatch object containing the data
        """
        if not rows:
            # Return empty batch with proper schema
            schema = pa.schema([pa.field(name, pa.string()) for name in self.column_names])
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=pa.string()) for _ in self.column_names],
                schema=schema
            )

        # Convert rows to column arrays
        columns = []
        for col_idx in range(num_columns):
            column_data = [row[col_idx] if col_idx < len(row) else '' for row in rows]
            columns.append(pa.array(column_data, type=pa.string()))

        # Create schema with proper column names
        schema = pa.schema([pa.field(name, pa.string()) for name in self.column_names])

        return pa.RecordBatch.from_arrays(columns, schema=schema)

    def _create_filtered_file(self, file_path: Path, skip_rows: int) -> Path:
        """Create a temporary file with footer content removed.

        When footer detection is enabled, this creates a cleaned version of
        the input file with footer content removed to prevent PyArrow parsing errors.

        Args:
            file_path: Path to the original input file
            skip_rows: Number of rows to skip from the beginning

        Returns:
            Path to the temporary filtered file
        """
        import tempfile

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
                        if self._should_stop_for_footer(row):
                            break
                        writer.writerow(row)
        finally:
            import os
            os.close(temp_fd)

        return Path(temp_path)

    def _validate_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        """Validate batch and separate good/bad rows.

        Applies schema validation rules to separate valid rows from invalid ones.
        Currently focuses on null validation for required fields.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (valid_batch, invalid_batch)
        """
        if not self.config.validate_schema or not self.schema:
            # No validation, return all as good
            empty_batch = batch.slice(0, 0)  # Empty batch with same schema
            return batch, empty_batch

        # For now, let's simplify validation - just check for required fields
        # In a more complete implementation, we'd do full type checking
        num_rows = len(batch)
        valid_mask = pa.array([True] * num_rows)

        for i, field in enumerate(self.schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)

            # Null validation for required fields
            if not field.nullable:
                null_mask = pc.is_valid(column)
                valid_mask = pc.and_(valid_mask, null_mask)

        # Split into valid and invalid batches
        valid_indices = pc.filter(
            pa.array(range(num_rows)),
            valid_mask
        )
        invalid_indices = pc.filter(
            pa.array(range(num_rows)),
            pc.invert(valid_mask)
        )

        if len(valid_indices) > 0:
            valid_batch = pc.take(batch, valid_indices)
        else:
            valid_batch = batch.slice(0, 0)  # Empty batch

        if len(invalid_indices) > 0:
            invalid_batch = pc.take(batch, invalid_indices)
        else:
            invalid_batch = batch.slice(0, 0)  # Empty batch

        return valid_batch, invalid_batch

    def _write_batch_to_parquet(self, batch: pa.RecordBatch, writer: pq.ParquetWriter):
        """Write a batch to parquet file.

        Converts the RecordBatch to a Table and writes it to the parquet file
        using the provided writer.

        Args:
            batch: PyArrow RecordBatch to write
            writer: ParquetWriter instance for output file
        """
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def _create_manifest(self, output_dir: Path, files: List[str]) -> str:
        """Create a manifest file listing output files.

        Generates a JSON manifest file compatible with data catalog systems
        like Databricks and Iceberg, containing file metadata.

        Args:
            output_dir: Directory where manifest file will be created
            files: List of output file paths to include in manifest

        Returns:
            Path to the created manifest file
        """
        from datetime import datetime

        manifest_path = output_dir / "manifest.json"

        manifest = {
            "format_version": "1.0",
            "files": [
                {
                    "file_path": str(Path(f).name),
                    "file_size": Path(f).stat().st_size if Path(f).exists() else 0,
                }
                for f in files
            ],
            "created_at": datetime.now().isoformat(),
        }

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        return str(manifest_path)

    def _create_metadata(self, output_dir: Path, results: ProcessingResults) -> str:
        """Create metadata file with processing statistics.

        Generates a JSON metadata file containing processing summary,
        configuration details, and execution statistics.

        Args:
            output_dir: Directory where metadata file will be created
            results: ProcessingResults object with statistics

        Returns:
            Path to the created metadata file
        """
        from datetime import datetime

        metadata_path = output_dir / "metadata.json"

        # Handle header_mode value properly (could be enum or string)
        header_mode_value = self.config.header_mode
        if hasattr(header_mode_value, 'value'):
            header_mode_value = header_mode_value.value

        metadata = {
            "processing_summary": {
                "total_rows": results.total_rows,
                "valid_rows": results.valid_rows,
                "invalid_rows": results.invalid_rows,
                "execution_time_seconds": results.execution_time,
            },
            "input_config": {
                "input_path": str(self.config.input_path),
                "schema_file": str(self.config.schema_file) if self.config.schema_file else None,
                "header_mode": header_mode_value,
                "batch_size": self.config.batch_size,
            },
            "output_files": results.output_files,
            "created_at": datetime.now().isoformat(),
        }

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        return str(metadata_path)

    def process_csv(self) -> ProcessingResults:
        """Process CSV file with streaming and validation.

        Main processing method that orchestrates the entire CSV import workflow
        including header detection, streaming processing, validation, and output generation.
        Now supports S3 streaming for both input and output.

        Returns:
            ProcessingResults object containing processing statistics and output paths

        Raises:
            Exception: Various exceptions may be raised during processing,
                      all are captured in the results.errors list
        """
        import time
        start_time = time.time()

        results = ProcessingResults()

        try:
            # Load schema if provided
            self.schema = self._load_schema()

            # Detect header - now works with S3 inputs
            self.header_row_index, self.column_names = self._detect_header_row(self.config.input_path)

            # Prepare output paths - support both local and S3 outputs
            if is_s3_path(self.config.output_path):
                # S3 output path
                output_s3_path = S3Path(str(self.config.output_path))
                good_file = str(output_s3_path.join("data.parquet"))
                bad_file = str(output_s3_path.join("bad_rows.parquet"))

                # For S3 outputs, we'll use temporary local storage during processing
                # and upload at the end for optimal performance
                use_s3_output = True
            else:
                # Local output path
                output_dir = Path(self.config.output_path)
                output_dir.mkdir(parents=True, exist_ok=True)
                good_file = str(output_dir / "data.parquet")
                bad_file = str(output_dir / "bad_rows.parquet")
                use_s3_output = False

            # Initialize parquet writers using unified I/O
            good_writer = None
            bad_writer = None

            # Process batches using S3-aware batch reader
            for batch in self._create_s3_batch_reader(self.config.input_path):
                # Validate and split batch
                valid_batch, invalid_batch = self._validate_batch(batch)

                # Initialize writers on first batch (to get schema)
                if good_writer is None:
                    good_writer = create_parquet_writer(
                        good_file,
                        valid_batch.schema,
                        s3_client=self.io_handler.s3_client if use_s3_output else None,
                        compression=self.config.compression
                    )
                if bad_writer is None and len(invalid_batch) > 0:
                    bad_writer = create_parquet_writer(
                        bad_file,
                        invalid_batch.schema,
                        s3_client=self.io_handler.s3_client if use_s3_output else None,
                        compression=self.config.compression
                    )

                # Write batches
                if len(valid_batch) > 0:
                    self._write_batch_to_parquet(valid_batch, good_writer)
                    results.valid_rows += len(valid_batch)

                if len(invalid_batch) > 0:
                    if bad_writer is None:
                        bad_writer = create_parquet_writer(
                            bad_file,
                            invalid_batch.schema,
                            s3_client=self.io_handler.s3_client if use_s3_output else None,
                            compression=self.config.compression
                        )
                    self._write_batch_to_parquet(invalid_batch, bad_writer)
                    results.invalid_rows += len(invalid_batch)

                results.total_rows += len(batch)

            # Close writers
            if good_writer:
                good_writer.close()
                results.output_files.append(good_file)

            if bad_writer:
                bad_writer.close()
                results.output_files.append(bad_file)

            # Create manifest and metadata (support S3 outputs)
            if self.config.create_manifest:
                results.manifest_file = self._create_s3_manifest(self.config.output_path, results.output_files)

            if self.config.create_metadata:
                results.metadata_file = self._create_s3_metadata(self.config.output_path, results)

            results.execution_time = time.time() - start_time

        except Exception as e:
            results.errors.append(str(e))
            results.execution_time = time.time() - start_time
            raise

        return results

    def _create_s3_batch_reader(self, input_path: Union[str, Path]) -> Iterator[pa.RecordBatch]:
        """Create a streaming batch reader that works with both local files and S3.

        Args:
            input_path: Path to input file (local or S3 URI)

        Yields:
            PyArrow RecordBatch objects containing data from the CSV
        """
        if is_s3_path(input_path):
            # S3 input - use fallback to row-by-row processing since PyArrow CSV
            # doesn't directly stream from S3
            yield from self._create_s3_csv_batches(input_path)
        else:
            # Local file - use existing PyArrow streaming
            yield from self._create_batch_reader(Path(input_path))

    def _create_s3_csv_batches(self, s3_path: Union[str, S3Path]) -> Iterator[pa.RecordBatch]:
        """Create batches from S3 CSV by processing rows and converting to RecordBatch.

        Args:
            s3_path: S3 path to CSV file

        Yields:
            PyArrow RecordBatch objects
        """
        if not self.column_names:
            return iter([])

        rows_buffer = []
        batch_size = self.config.batch_size
        expected_columns = len(self.column_names)

        # Skip header rows if needed
        rows_to_skip = 0
        if self.header_row_index is not None and self.header_row_index >= 0:
            rows_to_skip = self.header_row_index + 1

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
            if self._should_stop_for_footer(row):
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
                yield self._convert_rows_to_batch(rows_buffer, expected_columns)
                rows_buffer = []

        # Yield any remaining rows in buffer
        if rows_buffer:
            yield self._convert_rows_to_batch(rows_buffer, expected_columns)

    def _create_s3_manifest(self, output_path: Union[str, Path], files: List[str]) -> str:
        """Create manifest file supporting S3 output locations.

        Args:
            output_path: Output directory (local or S3)
            files: List of output file paths

        Returns:
            Path to created manifest file
        """
        from datetime import datetime

        manifest = {
            "format_version": "1.0",
            "files": [
                {
                    "file_path": str(Path(f).name) if not is_s3_path(f) else S3Path(f).name,
                    "file_size": self.io_handler.get_size(f) if self.io_handler.exists(f) else 0,
                }
                for f in files
            ],
            "created_at": datetime.now().isoformat(),
        }

        if is_s3_path(output_path):
            # S3 output
            manifest_path = S3Path(str(output_path)).join("manifest.json")
            with self.io_handler.open_for_write(manifest_path, encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
            return str(manifest_path)
        else:
            # Local output
            output_dir = Path(output_path)
            manifest_path = output_dir / "manifest.json"
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
            return str(manifest_path)

    def _create_s3_metadata(self, output_path: Union[str, Path], results: ProcessingResults) -> str:
        """Create metadata file supporting S3 output locations.

        Args:
            output_path: Output directory (local or S3)
            results: ProcessingResults with statistics

        Returns:
            Path to created metadata file
        """
        from datetime import datetime

        # Handle header_mode value properly
        header_mode_value = self.config.header_mode
        if hasattr(header_mode_value, 'value'):
            header_mode_value = header_mode_value.value

        metadata = {
            "processing_summary": {
                "total_rows": results.total_rows,
                "valid_rows": results.valid_rows,
                "invalid_rows": results.invalid_rows,
                "execution_time_seconds": results.execution_time,
            },
            "input_config": {
                "input_path": str(self.config.input_path),
                "schema_file": str(self.config.schema_file) if self.config.schema_file else None,
                "header_mode": header_mode_value,
                "batch_size": self.config.batch_size,
            },
            "output_files": results.output_files,
            "created_at": datetime.now().isoformat(),
        }

        if is_s3_path(output_path):
            # S3 output
            metadata_path = S3Path(str(output_path)).join("metadata.json")
            with self.io_handler.open_for_write(metadata_path, encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
        else:
            # Local output
            output_dir = Path(output_path)
            metadata_path = output_dir / "metadata.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
