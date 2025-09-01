"""Core Forklift engine for streaming data import with PyArrow."""

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
import pandas as pd


class HeaderMode(Enum):
    """Header detection modes for CSV processing."""
    PRESENT = "present"  # File has header row
    ABSENT = "absent"   # No header, use schema or default names
    AUTO = "auto"       # Auto-detect header location


@dataclass
class ImportConfig:
    """Configuration for data import operations."""
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

    # Validation options
    validate_schema: bool = True
    max_validation_errors: int = 1000

    # Output options
    create_manifest: bool = True
    create_metadata: bool = True
    compression: str = "snappy"


@dataclass
class ProcessingResults:
    """Results from data processing operation."""
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    output_files: List[str] = field(default_factory=list)
    manifest_file: Optional[str] = None
    metadata_file: Optional[str] = None
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)


class ForkliftCore:
    """Core engine for streaming data import with PyArrow."""

    def __init__(self, config: ImportConfig):
        self.config = config
        self.schema: Optional[pa.Schema] = None
        self.header_row_index: Optional[int] = None
        self.column_names: Optional[List[str]] = None

    def _load_schema(self) -> Optional[pa.Schema]:
        """Load and parse schema from file."""
        if not self.config.schema_file:
            return None

        schema_path = Path(self.config.schema_file)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_dict = json.load(f)

        # Convert JSON schema to PyArrow schema
        return self._json_schema_to_pyarrow(schema_dict)

    def _json_schema_to_pyarrow(self, schema_dict: Dict[str, Any]) -> pa.Schema:
        """Convert JSON schema to PyArrow schema."""
        properties = schema_dict.get("properties", {})
        fields = []

        for field_name, field_def in properties.items():
            field_type = self._json_type_to_pyarrow(field_def)
            nullable = field_name not in schema_dict.get("required", [])
            fields.append(pa.field(field_name, field_type, nullable=nullable))

        return pa.schema(fields)

    def _json_type_to_pyarrow(self, field_def: Dict[str, Any]) -> pa.DataType:
        """Convert JSON schema field definition to PyArrow data type."""
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

    def _detect_header_row(self, file_path: Path) -> Tuple[int, List[str]]:
        """Detect header row location and extract column names."""
        if self.config.header_mode == HeaderMode.ABSENT:
            # No header, use schema or generate names
            if self.schema:
                return -1, [field.name for field in self.schema]
            else:
                # Generate default names - we'll determine count from first data row
                return -1, []

        elif self.config.header_mode == HeaderMode.PRESENT:
            # Header is expected at first non-comment row
            header_idx, columns = self._find_first_data_row(file_path)
            return header_idx, columns

        else:  # AUTO mode
            return self._auto_detect_header(file_path)

    def _find_first_data_row(self, file_path: Path) -> Tuple[int, List[str]]:
        """Find the first non-comment row and extract columns."""
        with open(file_path, 'r', encoding=self.config.encoding) as f:
            reader = csv.reader(f, delimiter=self.config.delimiter)

            for idx, row in enumerate(reader):
                if idx >= self.config.header_search_rows:
                    break

                if self._is_comment_row(row):
                    continue

                if self.config.skip_blank_lines and not any(cell.strip() for cell in row):
                    continue

                return idx, [col.strip() for col in row]

        raise ValueError("No valid header row found")

    def _auto_detect_header(self, file_path: Path) -> Tuple[int, List[str]]:
        """Auto-detect header row by looking for text patterns."""
        with open(file_path, 'r', encoding=self.config.encoding) as f:
            reader = csv.reader(f, delimiter=self.config.delimiter)
            rows = []

            for idx, row in enumerate(reader):
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
        """Determine if a row looks like a header row."""
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
        """Check if row should be treated as a comment."""
        if not self.config.comment_rows or not row:
            return False

        first_cell = row[0].strip() if row else ""

        for comment_pattern in self.config.comment_rows:
            if re.match(comment_pattern, first_cell):
                return True

        return False

    def _should_stop_for_footer(self, row: List[str]) -> bool:
        """Check if we should stop processing due to footer detection."""
        if not self.config.footer_detection or not row:
            return False

        detection = self.config.footer_detection

        # Check for blank row stopping
        if detection.get("stop_on_blank", False):
            if not any(cell.strip() for cell in row):
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
        """Create a streaming batch reader for the CSV file."""
        # Skip to data start (after header/comments)
        skip_rows = 0
        if self.header_row_index is not None and self.header_row_index >= 0:
            skip_rows = self.header_row_index + 1

        # Configure CSV read options
        parse_options = pv_csv.ParseOptions(
            delimiter=self.config.delimiter,
            quote_char=self.config.quote_char,
            escape_char=self.config.escape_char,
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
        with open(file_path, 'rb') as f:
            csv_reader = pv_csv.open_csv(
                f,
                parse_options=parse_options,
                read_options=read_options,
                convert_options=convert_options,
            )

            # Read in batches
            batch_size = self.config.batch_size
            while True:
                try:
                    batch = csv_reader.read_next_batch()
                    if batch is None or len(batch) == 0:
                        break
                    yield batch
                except StopIteration:
                    break

    def _validate_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        """Validate batch and separate good/bad rows."""
        if not self.config.validate_schema or not self.schema:
            # No validation, return all as good
            empty_batch = batch.slice(0, 0)  # Empty batch with same schema
            return batch, empty_batch

        # Apply schema validation
        valid_mask = pa.compute.true()  # Start with all true

        for i, field in enumerate(self.schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)

            # Type validation
            if not pa.types.is_compatible(column.type, field.type):
                # Try to cast
                try:
                    casted = pc.cast(column, field.type)
                    batch = batch.set_column(i, field.name, casted)
                except pa.ArrowInvalid:
                    # Mark rows with invalid data
                    valid_mask = pc.and_(valid_mask, pc.is_null(column))

            # Null validation for required fields
            if not field.nullable:
                valid_mask = pc.and_(valid_mask, pc.is_valid(column))

        # Split into valid and invalid batches
        valid_indices = pc.filter(pc.list_indices(valid_mask), valid_mask)
        invalid_indices = pc.filter(
            pc.list_indices(valid_mask),
            pc.invert(valid_mask)
        )

        valid_batch = pc.take(batch, valid_indices)
        invalid_batch = pc.take(batch, invalid_indices)

        return valid_batch, invalid_batch

    def _write_batch_to_parquet(self, batch: pa.RecordBatch, writer: pq.ParquetWriter):
        """Write a batch to parquet file."""
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def _create_manifest(self, output_dir: Path, files: List[str]) -> str:
        """Create a manifest file listing output files."""
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
            "created_at": pd.Timestamp.now().isoformat(),
        }

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        return str(manifest_path)

    def _create_metadata(self, output_dir: Path, results: ProcessingResults) -> str:
        """Create metadata file with processing statistics."""
        metadata_path = output_dir / "metadata.json"

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
                "header_mode": self.config.header_mode.value,
                "batch_size": self.config.batch_size,
            },
            "output_files": results.output_files,
            "created_at": pd.Timestamp.now().isoformat(),
        }

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        return str(metadata_path)

    def process_csv(self) -> ProcessingResults:
        """Process CSV file with streaming and validation."""
        import time
        start_time = time.time()

        results = ProcessingResults()

        try:
            # Load schema if provided
            self.schema = self._load_schema()

            # Detect header
            input_path = Path(self.config.input_path)
            self.header_row_index, self.column_names = self._detect_header_row(input_path)

            # Prepare output paths
            output_dir = Path(self.config.output_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            good_file = output_dir / "data.parquet"
            bad_file = output_dir / "bad_rows.parquet"

            # Initialize parquet writers
            good_writer = None
            bad_writer = None

            # Process batches
            for batch in self._create_batch_reader(input_path):
                # Validate and split batch
                valid_batch, invalid_batch = self._validate_batch(batch)

                # Initialize writers on first batch (to get schema)
                if good_writer is None:
                    good_writer = pq.ParquetWriter(
                        good_file,
                        valid_batch.schema,
                        compression=self.config.compression
                    )
                if bad_writer is None and len(invalid_batch) > 0:
                    bad_writer = pq.ParquetWriter(
                        bad_file,
                        invalid_batch.schema,
                        compression=self.config.compression
                    )

                # Write batches
                if len(valid_batch) > 0:
                    self._write_batch_to_parquet(valid_batch, good_writer)
                    results.valid_rows += len(valid_batch)

                if len(invalid_batch) > 0:
                    if bad_writer is None:
                        bad_writer = pq.ParquetWriter(
                            bad_file,
                            invalid_batch.schema,
                            compression=self.config.compression
                        )
                    self._write_batch_to_parquet(invalid_batch, bad_writer)
                    results.invalid_rows += len(invalid_batch)

                results.total_rows += len(batch)

            # Close writers
            if good_writer:
                good_writer.close()
                results.output_files.append(str(good_file))

            if bad_writer:
                bad_writer.close()
                results.output_files.append(str(bad_file))

            # Create manifest and metadata
            if self.config.create_manifest:
                results.manifest_file = self._create_manifest(output_dir, results.output_files)

            if self.config.create_metadata:
                results.metadata_file = self._create_metadata(output_dir, results)

            results.execution_time = time.time() - start_time

        except Exception as e:
            results.errors.append(str(e))
            results.execution_time = time.time() - start_time
            raise

        return results


# Public API functions
def import_csv(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import CSV file with streaming and validation.

    Args:
        input_path: Path to input CSV file
        output_path: Directory for output files
        schema_file: Optional JSON schema file
        **kwargs: Additional configuration options

    Returns:
        ProcessingResults with summary statistics
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
    """Import Fixed Width File (placeholder for future implementation)."""
    raise NotImplementedError("FWF import not yet implemented")


def import_excel(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
) -> ProcessingResults:
    """Import Excel file (placeholder for future implementation)."""
    raise NotImplementedError("Excel import not yet implemented")
