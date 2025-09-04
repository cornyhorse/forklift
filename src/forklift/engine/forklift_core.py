"""Core Forklift engine for streaming data import with PyArrow.

This module provides the core functionality for importing CSV files with PyArrow
streaming capabilities, including header detection, footer detection, validation,
and output generation. Now supports S3 streaming for both input and output.
"""

from __future__ import annotations
import csv
import re
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Iterator, Tuple
from dataclasses import dataclass, field
from enum import Enum

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Import S3 streaming capabilities
from ..io import UnifiedIOHandler, is_s3_path, S3Path, create_parquet_writer
from ..metadata import OutputMetadataCollector


class ProcessingError(Exception):
    """Raised when data processing fails."""
    pass


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


    def _process_batch_with_transformations(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply transformations to a batch including row hash generation.

        Args:
            batch: Input PyArrow RecordBatch

        Returns:
            Transformed PyArrow RecordBatch with row hash if enabled
        """
        processed_batch = batch

        # Apply row hash if enabled in schema
        if hasattr(self, 'schema_dict') and self.schema_dict:
            row_hash_config = self.schema_dict.get('x-rowHash')
            if row_hash_config:
                row_hash_processor = create_row_hash_processor_from_schema(row_hash_config)
                if row_hash_processor:
                    processed_batch, validation_results = row_hash_processor.process_batch(processed_batch)
                    # Log any validation issues from row hash processing
                    for result in validation_results:
                        if not result.is_valid:
                            print(f"Row hash processing warning: {result.error_message}")

        return processed_batch

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

            # Initialize output metadata collector if enabled
            output_metadata_collector = None
            if self.config.create_metadata:
                # Read metadata configuration from schema if available
                metadata_config = {}
                if self.schema and hasattr(self, 'schema_dict'):
                    metadata_config = self.schema_dict.get('x-metadata-generation', {})

                output_metadata_collector = OutputMetadataCollector(
                    enabled=metadata_config.get('enabled', True),
                    enum_threshold=metadata_config.get('enum_detection', {}).get('uniqueness_threshold', 0.1),
                    uniqueness_threshold=0.95,  # Default threshold for too unique columns
                    top_n_values=metadata_config.get('statistics', {}).get('categorical', {}).get('top_n_values', 10),
                    quantiles=metadata_config.get('statistics', {}).get('numeric', {}).get('quantiles', [0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
                )

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

                # Write batches and collect metadata from FINAL OUTPUT DATA
                if len(valid_batch) > 0:
                    self._write_batch_to_parquet(valid_batch, good_writer)
                    # Collect metadata from the final transformed valid data
                    if output_metadata_collector:
                        output_metadata_collector.add_batch(valid_batch)
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
                # Generate and save output metadata if we collected it
                if output_metadata_collector and output_metadata_collector.total_rows > 0:
                    # Get the schema from the good writer if available
                    output_schema = good_writer.schema if good_writer else None

                    # Generate source info for metadata
                    source_info = {
                        "input_path": str(self.config.input_path),
                        "processing_type": "csv_processing",
                        "schema_file": str(self.config.schema_file) if self.config.schema_file else None,
                        "total_batches_processed": "streaming",
                        "final_output_files": results.output_files
                    }

                    # Generate comprehensive metadata about the final output data
                    output_metadata = output_metadata_collector.generate_metadata(output_schema, source_info)

                    # Save output metadata to separate file
                    output_metadata_path = output_metadata_collector.save_metadata(
                        self.config.output_path,
                        "output_data_metadata.json"
                    )

                    if output_metadata_path:
                        print(f"Output data metadata saved to: {output_metadata_path}")
                        # Optionally add to results for tracking
                        if hasattr(results, 'output_metadata_file'):
                            results.output_metadata_file = output_metadata_path

                # Still create the traditional processing metadata
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
            with self.io_handler.open_for_write(str(manifest_path), encoding='utf-8') as f:
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
            with self.io_handler.open_for_write(str(metadata_path), encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
        else:
            # Local output
            output_dir = Path(output_path)
            metadata_path = output_dir / "metadata.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)


# Public API functions
def import_csv(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import CSV file with streaming and validation.

    High-level API function for importing CSV files using PyArrow streaming.
    Supports header detection, footer detection, schema validation, and various
    output formats including parquet files and metadata. Now supports S3 streaming
    for both input and output.

    Args:
        input_path: Path to input CSV file to process (local or S3 URI)
        output_path: Directory where output files will be created (local or S3 URI)
        schema_file: Optional path to JSON schema file for validation (local or S3 URI)
        **kwargs: Additional configuration options passed to ImportConfig

    Returns:
        ProcessingResults object containing statistics and output file paths

    Examples:
        Basic CSV import::

            results = import_csv("data.csv", "output/")

        With schema validation::

            results = import_csv(
                input_path="data.csv",
                output_path="output/",
                schema_file="schema.json"
            )

        S3 to S3 processing::

            results = import_csv(
                input_path="s3://bucket/data.csv",
                output_path="s3://bucket/output/",
                schema_file="s3://bucket/schema.json"
            )

        With footer detection::

            results = import_csv(
                input_path="data.csv",
                output_path="output/",
                footer_detection={"stop_on_blank": True}
            )
    """
    config = ImportConfig(
        input_path=input_path,
        output_path=output_path,
        schema_file=schema_file,
        **kwargs
    )

    engine = ForkliftCore(config)
    return engine.process_csv()


def import_fwf(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import Fixed Width File (placeholder for future implementation).

    Args:
        input_path: Path to input FWF file (local or S3 URI)
        output_path: Directory for output files (local or S3 URI)
        schema_file: Optional JSON schema file (local or S3 URI)
        **kwargs: Additional configuration options

    Returns:
        ProcessingResults object

    Raises:
        NotImplementedError: This function is not yet implemented
    """
    raise NotImplementedError("FWF import not yet implemented")


def import_excel(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import Excel file with multi-sheet support.

    Processes Excel files (.xlsx and .xls) with support for multiple sheets,
    custom column mappings, header detection, and data range specification.
    Uses an efficient approach of opening the file once and streaming sheets
    from the already opened workbook.

    Args:
        input_path: Path to input Excel file (local or S3 URI)
        output_path: Directory for output files (local or S3 URI)
        schema_file: Optional JSON schema file (local or S3 URI)
        **kwargs: Additional configuration options including:
            - sheet: Specific sheet name/index to process (overrides schema)
            - values_only: Read only cell values, ignoring formulas (default: True)
            - engine: Excel engine to use ('openpyxl' or 'xlrd', auto-detected)
            - date_system: Excel date system ('1900' or '1904', default: '1900')

    Returns:
        ProcessingResults object containing processing statistics and metadata

    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If Excel file format is unsupported or configuration is invalid
        ImportError: If required Excel engine libraries are not installed
        ProcessingError: If data processing fails
    """
    from ..inputs.excel import ExcelInputHandler
    from ..inputs.config import ExcelInputConfig, ExcelSheetConfig
    from ..schema.excel_schema_importer import ExcelSchemaImporter
    import tempfile
    import logging
    import shutil

    logger = logging.getLogger(__name__)
    start_time = time.time()

    try:
        # Convert paths to Path objects
        input_path = Path(input_path) if isinstance(input_path, str) else input_path
        output_path = Path(output_path) if isinstance(output_path, str) else output_path

        # For now, support local files only - S3 support can be added later
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)

        # Load and validate schema if provided
        excel_config = None
        if schema_file:
            schema_path = Path(schema_file) if isinstance(schema_file, str) else schema_file

            # Parse schema
            try:
                schema_importer = ExcelSchemaImporter(schema_path, validate=True)
                excel_config = _create_excel_config_from_schema(schema_importer)
                logger.info(f"Loaded Excel schema from {schema_file}")
            except Exception as e:
                logger.error(f"Failed to load Excel schema: {e}")
                raise ProcessingError(f"Schema validation failed: {e}") from e

        # Create default config if no schema provided
        if excel_config is None:
            excel_config = _create_default_excel_config(input_path, **kwargs)

        # Override config with kwargs
        if 'values_only' in kwargs:
            excel_config.values_only = kwargs['values_only']
        if 'engine' in kwargs:
            excel_config.engine = kwargs['engine']
        if 'date_system' in kwargs:
            excel_config.date_system = kwargs['date_system']

        # Initialize Excel input handler
        excel_handler = ExcelInputHandler(excel_config)

        # Get file information for logging
        file_info = excel_handler.get_sheet_info(input_path)
        logger.info(f"Processing Excel file with {file_info['sheet_count']} sheets using {file_info['engine']} engine")

        # Process sheets and collect results
        results = ProcessingResults()
        processed_sheets = 0
        total_rows = 0

        for sheet_name, arrow_table in excel_handler.process_sheets(input_path):
            logger.info(f"Processing sheet '{sheet_name}' with {arrow_table.num_rows} rows")

            # Generate output filename for this sheet
            safe_sheet_name = _sanitize_filename(sheet_name)
            output_filename = f"{input_path.stem}_{safe_sheet_name}.parquet"
            sheet_output_path = output_path / output_filename

            # Write sheet data to Parquet directly using PyArrow
            pq.write_table(arrow_table, sheet_output_path)
            logger.info(f"Wrote sheet '{sheet_name}' to {sheet_output_path}")

            # Update results
            processed_sheets += 1
            total_rows += arrow_table.num_rows
            results.output_files.append(str(sheet_output_path))

        # Finalize results
        processing_time = time.time() - start_time
        results.total_rows = total_rows
        results.valid_rows = total_rows  # All rows are considered valid for Excel
        results.invalid_rows = 0
        results.execution_time = processing_time

        logger.info(
            f"Excel import completed successfully: {processed_sheets} sheets, "
            f"{total_rows} total rows in {processing_time:.2f}s"
        )

        return results

    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Excel import failed after {processing_time:.2f}s: {e}")

        # Return error results
        results = ProcessingResults()
        results.execution_time = processing_time
        results.errors.append(str(e))
        raise


def import_sql(
    connection_string: str,
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import data from SQL database with ODBC connectivity.

    Processes data from SQL databases (SQLite, PostgreSQL, MySQL, Oracle, SQL Server, etc.)
    using ODBC connections. Supports explicit table specification through schema files
    with one-to-one schema/table mapping for predictable configuration.

    Args:
        connection_string: ODBC connection string for database
        output_path: Directory for output files (local or S3 URI)
        schema_file: Required JSON schema file specifying tables to import (local or S3 URI)
        **kwargs: Additional configuration options including:
            - batch_size: Number of rows to fetch per batch (default: 10000)
            - query_timeout: Query timeout in seconds (default: 300)
            - connection_timeout: Connection timeout in seconds (default: 30)
            - use_quoted_identifiers: Whether to quote table/column names (default: False)
            - schema_name: Default schema name if not specified in table configs
            - enable_streaming: Whether to use streaming cursor (default: True)
            - null_values: Values to treat as NULL/None

    Returns:
        ProcessingResults object containing processing statistics and metadata

    Raises:
        ImportError: If pyodbc is not installed
        ConnectionError: If database connection fails
        ProcessingError: If data processing fails or no schema file provided
        ValueError: If schema file doesn't specify any tables

    Examples:
        Basic SQLite import with schema::

            results = import_sql(
                connection_string="Driver={SQLite3 ODBC Driver};Database=test.db",
                output_path="output/",
                schema_file="sql_schema.json"
            )

        PostgreSQL with custom configuration::

            results = import_sql(
                connection_string="Driver={PostgreSQL ODBC Driver};Server=localhost;Database=mydb;Uid=user;Pwd=pass",
                output_path="output/",
                schema_file="pg_schema.json",
                batch_size=5000,
                use_quoted_identifiers=True
            )

    Schema file example::

        {
          "x-sql": {
            "tables": [
              {
                "select": {
                  "schema": "public",
                  "name": "users"
                },
                "outputName": "users_data"
              },
              {
                "select": {
                  "schema": "sales",
                  "name": "orders"
                }
              }
            ]
          }
        }
    """
    from ..inputs.sql import SqlInputHandler
    from ..inputs.config import SqlInputConfig
    from ..schema.sql_schema_importer import SqlSchemaImporter
    from datetime import datetime
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()

    try:
        # Convert paths to Path objects
        output_path = Path(output_path) if isinstance(output_path, str) else output_path

        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)

        # Schema file is now required for explicit table specification
        if not schema_file:
            raise ProcessingError("Schema file is required for SQL import to specify which tables to process")

        # Load and validate schema
        schema_path = Path(schema_file) if isinstance(schema_file, str) else schema_file

        try:
            schema_importer = SqlSchemaImporter(schema_path, validate=True)
            logger.info(f"Loaded SQL schema from {schema_file}")
        except Exception as e:
            logger.error(f"Failed to load SQL schema: {e}")
            raise ProcessingError(f"Schema validation failed: {e}") from e

        # Get explicit table list from schema
        tables_to_process = schema_importer.get_table_list()
        if not tables_to_process:
            raise ValueError("Schema file must specify at least one table to process")

        # Create SQL config
        config_kwargs = {
            'connection_string': connection_string,
            'batch_size': kwargs.get('batch_size', 10000),
            'query_timeout': kwargs.get('query_timeout', 300),
            'connection_timeout': kwargs.get('connection_timeout', 30),
            'use_quoted_identifiers': kwargs.get('use_quoted_identifiers', False),
            'schema_name': kwargs.get('schema_name'),
            'enable_streaming': kwargs.get('enable_streaming', True),
            'null_values': kwargs.get('null_values'),
        }

        # Remove None values
        config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}
        sql_config = SqlInputConfig(**config_kwargs)

        # Initialize SQL input handler
        sql_handler = SqlInputHandler(sql_config)
        sql_handler.set_schema_importer(schema_importer)

        # Connect to database and process tables
        with sql_handler:
            logger.info(f"Found {len(tables_to_process)} tables to process from schema")

            # Process each table
            total_rows = 0
            valid_rows = 0
            invalid_rows = 0
            processed_tables = 0
            output_files = []

            for schema_name, table_name, output_name in tables_to_process:
                try:
                    logger.info(f"Processing table: {schema_name}.{table_name}")

                    # Generate output filename
                    if output_name:
                        table_output_name = output_name
                    elif schema_name and schema_name != 'default':
                        table_output_name = f"{schema_name}_{table_name}"
                    else:
                        table_output_name = table_name

                    output_file = output_path / f"{table_output_name}.parquet"

                    # Get table schema
                    table_schema = sql_handler.get_table_schema(schema_name, table_name)

                    # Create Parquet writer
                    writer = create_parquet_writer(output_file, table_schema)

                    # Process data in batches
                    table_rows = 0
                    for batch in sql_handler.read_table_data(schema_name, table_name):
                        writer.write_batch(batch)
                        table_rows += batch.num_rows
                        total_rows += batch.num_rows
                        valid_rows += batch.num_rows

                    # Close writer
                    writer.close()

                    if table_rows > 0:
                        output_files.append(str(output_file))
                        logger.info(f"Completed {schema_name}.{table_name}: {table_rows} rows -> {output_file}")
                    else:
                        logger.warning(f"Table {schema_name}.{table_name} contained no data")

                    processed_tables += 1

                except Exception as e:
                    logger.error(f"Failed to process table {schema_name}.{table_name}: {e}")
                    invalid_rows += 1

        # Create results
        processing_time = time.time() - start_time
        results = ProcessingResults(
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            execution_time=processing_time,
            output_files=output_files
        )

        # Create metadata file
        metadata = {
            "processing_summary": {
                "total_tables_processed": processed_tables,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "invalid_rows": invalid_rows,
                "execution_time_seconds": processing_time,
                "processed_at": datetime.now().isoformat()
            },
            "input_config": {
                "connection_string": connection_string,
                "tables_processed": [(schema, table, output) for schema, table, output in tables_to_process],
                "batch_size": sql_config.batch_size,
                "query_timeout": sql_config.query_timeout,
            },
            "output_files": output_files
        }

        metadata_file = output_path / "metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(
            f"SQL import completed successfully: {processed_tables} tables, "
            f"{total_rows} total rows in {processing_time:.2f}s"
        )

        return results

    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"SQL import failed after {processing_time:.2f}s: {e}")

        # Return error results
        results = ProcessingResults()
        results.execution_time = processing_time
        results.errors.append(str(e))
        raise


def _create_excel_config_from_schema(schema_importer: 'ExcelSchemaImporter') -> 'ExcelInputConfig':
    """Create ExcelInputConfig from schema importer."""
    from ..inputs.config import ExcelInputConfig, ExcelSheetConfig

    # Convert schema sheets to config objects
    sheet_configs = []
    for sheet_def in schema_importer.sheets:
        sheet_config = ExcelSheetConfig(
            select=sheet_def.get('select', {}),
            columns=sheet_def.get('columns'),
            header=sheet_def.get('header'),
            data_start_row=sheet_def.get('dataStartRow'),
            data_end_row=sheet_def.get('dataEndRow'),
            skip_blank_rows=sheet_def.get('skipBlankRows', True),
            name_override=sheet_def.get('nameOverride')
        )
        sheet_configs.append(sheet_config)

    return ExcelInputConfig(
        sheets=sheet_configs,
        values_only=schema_importer.values_only,
        date_system=schema_importer.date_system,
        nulls=schema_importer.nulls
    )


def _create_default_excel_config(file_path: Path, **kwargs) -> 'ExcelInputConfig':
    """Create default ExcelInputConfig when no schema is provided."""
    from ..inputs.config import ExcelInputConfig, ExcelSheetConfig
    from ..inputs.excel import ExcelInputHandler

    # Create a temporary handler to get sheet info
    temp_config = ExcelInputConfig(sheets=[])
    temp_handler = ExcelInputHandler(temp_config)

    try:
        file_info = temp_handler.get_sheet_info(file_path)
        sheet_names = file_info['sheet_names']

        # Create configs for all sheets or specific sheet
        sheet_configs = []
        if 'sheet' in kwargs:
            # Process specific sheet
            sheet_spec = kwargs['sheet']
            if isinstance(sheet_spec, str):
                # Sheet name
                if sheet_spec in sheet_names:
                    sheet_config = ExcelSheetConfig(select={'name': sheet_spec})
                    sheet_configs.append(sheet_config)
                else:
                    raise ValueError(f"Sheet '{sheet_spec}' not found in workbook")
            elif isinstance(sheet_spec, int):
                # Sheet index
                if 0 <= sheet_spec < len(sheet_names):
                    sheet_config = ExcelSheetConfig(select={'index': sheet_spec})
                    sheet_configs.append(sheet_config)
                else:
                    raise ValueError(f"Sheet index {sheet_spec} out of range")
        else:
            # Process all sheets
            for i, sheet_name in enumerate(sheet_names):
                sheet_config = ExcelSheetConfig(select={'name': sheet_name})
                sheet_configs.append(sheet_config)

        return ExcelInputConfig(
            sheets=sheet_configs,
            values_only=kwargs.get('values_only', True),
            date_system=kwargs.get('date_system', '1900'),
            engine=kwargs.get('engine')
        )

    finally:
        temp_handler.close_workbook()


def _sanitize_filename(filename: str) -> str:
    """Sanitize sheet name for use as filename."""
    import re
    # Replace invalid filename characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing whitespace and dots
    sanitized = sanitized.strip(' .')
    # Ensure not empty
    return sanitized or 'sheet'
