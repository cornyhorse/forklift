"""Row hash processor for adding row-level hash columns and metadata."""

from __future__ import annotations
import hashlib
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


@dataclass
class RowHashConfig:
    """Configuration for row hash column generation and metadata.

    Attributes:
        enabled: Whether to generate row hash column (default: False)
        column_name: Name of the hash column (default: "row_hash")
        algorithm: Hash algorithm to use (default: "sha256")
        include_columns: List of columns to include in hash (None = all columns)
        exclude_columns: List of columns to exclude from hash
        null_value: String to use for NULL values in hash calculation (default: "NULL")
        separator: Separator between column values (default: "||")
        input_hash_enabled: Whether to generate input row hash (default: False)
        input_hash_column_name: Name of the input hash column (default: "_input_hash")
        source_uri_enabled: Whether to add source URI column (default: False)
        source_uri_column_name: Name of the source URI column (default: "_source_uri")
        ingested_at_enabled: Whether to add ingestion timestamp (default: False)
        ingested_at_column_name: Name of the ingestion timestamp column (default: "_ingested_at_utc")
        row_number_enabled: Whether to add row numbers (default: False)
        source_row_number_column_name: Name of source row number column (default: "_rownum_in_source_file")
        processing_row_number_column_name: Name of processing row number column (default: "_rownum")
    """
    enabled: bool = False
    column_name: str = "row_hash"
    algorithm: str = "sha256"
    include_columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None
    null_value: str = "NULL"
    separator: str = "||"

    # New input hash options
    input_hash_enabled: bool = False
    input_hash_column_name: str = "_input_hash"

    # New metadata columns
    source_uri_enabled: bool = False
    source_uri_column_name: str = "_source_uri"
    ingested_at_enabled: bool = False
    ingested_at_column_name: str = "_ingested_at_utc"
    row_number_enabled: bool = False
    source_row_number_column_name: str = "_rownum_in_source_file"
    processing_row_number_column_name: str = "_rownum"

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.exclude_columns is None:
            self.exclude_columns = []

        # Validate algorithm
        supported_algorithms = ["md5", "sha1", "sha256", "sha384", "sha512"]
        if self.algorithm not in supported_algorithms:
            raise ValueError(f"Unsupported hash algorithm: {self.algorithm}. "
                           f"Supported algorithms: {supported_algorithms}")


class RowHashProcessor(BaseProcessor):
    """Processor for adding row-level hash columns and metadata.

    This processor generates hash columns and metadata for each row including:
    - Output row hash (after transformations)
    - Input row hash (before transformations)
    - Source URI/file path
    - Ingestion timestamp
    - Row numbers (source file and processing sequence)

    The processor supports multiple hash algorithms and flexible column
    inclusion/exclusion rules.
    """

    def __init__(self, config: RowHashConfig):
        """Initialize the row hash processor."""
        super().__init__()
        self.config = config
        self.source_uri = None
        self.ingestion_timestamp = None
        self.source_row_offset = 0
        self.processing_row_counter = 0

    def set_source_context(self, source_uri: str, source_row_offset: int = 0):
        """Set source context for metadata generation."""
        self.source_uri = source_uri
        self.source_row_offset = source_row_offset

        if self.config.ingested_at_enabled:
            from datetime import datetime, timezone
            self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    def process_batch(self, batch: pa.RecordBatch, input_batch: Optional[pa.RecordBatch] = None) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding hash columns and metadata."""
        validation_results = []
        processed_batch = batch

        try:
            # Add output row hash if enabled
            if self.config.enabled:
                hash_columns = self._get_hash_columns(batch.schema)
                if hash_columns:
                    hash_values = self._compute_row_hashes(batch, hash_columns)
                    processed_batch = self._add_column(processed_batch, self.config.column_name, hash_values)

            # Add input row hash if enabled and input batch provided
            if self.config.input_hash_enabled and input_batch is not None:
                input_hash_columns = self._get_input_hash_columns(input_batch.schema)
                if input_hash_columns:
                    input_hash_values = self._compute_row_hashes(input_batch, input_hash_columns)
                    processed_batch = self._add_column(processed_batch, self.config.input_hash_column_name, input_hash_values)

            # Add source URI if enabled
            if self.config.source_uri_enabled and self.source_uri:
                source_uri_values = pa.array([self.source_uri] * batch.num_rows, type=pa.string())
                processed_batch = self._add_column(processed_batch, self.config.source_uri_column_name, source_uri_values)

            # Add ingestion timestamp if enabled
            if self.config.ingested_at_enabled and self.ingestion_timestamp:
                timestamp_values = pa.array([self.ingestion_timestamp] * batch.num_rows, type=pa.string())
                processed_batch = self._add_column(processed_batch, self.config.ingested_at_column_name, timestamp_values)

            # Add row numbers if enabled
            if self.config.row_number_enabled:
                # Source file row numbers
                source_row_numbers = list(range(
                    self.source_row_offset + self.processing_row_counter + 1,
                    self.source_row_offset + self.processing_row_counter + batch.num_rows + 1
                ))
                source_row_array = pa.array(source_row_numbers, type=pa.int64())
                processed_batch = self._add_column(processed_batch, self.config.source_row_number_column_name, source_row_array)

                # Processing sequence row numbers
                processing_row_numbers = list(range(
                    self.processing_row_counter + 1,
                    self.processing_row_counter + batch.num_rows + 1
                ))
                processing_row_array = pa.array(processing_row_numbers, type=pa.int64())
                processed_batch = self._add_column(processed_batch, self.config.processing_row_number_column_name, processing_row_array)

                # Update counter
                self.processing_row_counter += batch.num_rows

            return processed_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Row metadata processing failed: {str(e)}",
                error_code="ROW_METADATA_ERROR"
            ))
            return batch, validation_results

    def _get_hash_columns(self, schema: pa.Schema) -> List[str]:
        """Determine which columns to include in hash calculation."""
        all_columns = [field.name for field in schema]

        if self.config.include_columns is not None:
            hash_columns = [col for col in self.config.include_columns if col in all_columns]
        else:
            hash_columns = [col for col in all_columns if col not in self.config.exclude_columns]

        # Don't include metadata columns if they already exist
        metadata_columns = [
            self.config.column_name,
            self.config.input_hash_column_name,
            self.config.source_uri_column_name,
            self.config.ingested_at_column_name,
            self.config.source_row_number_column_name,
            self.config.processing_row_number_column_name
        ]

        hash_columns = [col for col in hash_columns if col not in metadata_columns]
        return hash_columns

    def _get_input_hash_columns(self, schema: pa.Schema) -> List[str]:
        """Get columns for input hash calculation (all original columns)."""
        return [field.name for field in schema]

    def _compute_row_hashes(self, batch: pa.RecordBatch, hash_columns: List[str]) -> pa.Array:
        """Compute hash values for each row."""
        num_rows = batch.num_rows
        hash_values = []

        for row_idx in range(num_rows):
            row_parts = []
            for col_name in hash_columns:
                column = batch.column(col_name)
                value = column[row_idx]

                if value.is_valid:
                    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                        row_parts.append(str(value.as_py()))
                    elif pa.types.is_binary(column.type):
                        row_parts.append(value.as_py().hex() if value.as_py() else self.config.null_value)
                    else:
                        row_parts.append(str(value.as_py()))
                else:
                    row_parts.append(self.config.null_value)

            row_string = self.config.separator.join(row_parts)
            hash_value = self._compute_hash(row_string)
            hash_values.append(hash_value)

        return pa.array(hash_values, type=pa.string())

    def _compute_hash(self, data: str) -> str:
        """Compute hash of the given string."""
        data_bytes = data.encode('utf-8')

        if self.config.algorithm == "md5":
            return hashlib.md5(data_bytes).hexdigest()
        elif self.config.algorithm == "sha1":
            return hashlib.sha1(data_bytes).hexdigest()
        elif self.config.algorithm == "sha256":
            return hashlib.sha256(data_bytes).hexdigest()
        elif self.config.algorithm == "sha384":
            return hashlib.sha384(data_bytes).hexdigest()
        elif self.config.algorithm == "sha512":
            return hashlib.sha512(data_bytes).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {self.config.algorithm}")

    def _add_column(self, batch: pa.RecordBatch, column_name: str, column_values: pa.Array) -> pa.RecordBatch:
        """Add a column to the batch."""
        new_fields = list(batch.schema)
        new_fields.append(pa.field(column_name, column_values.type))
        new_schema = pa.schema(new_fields)

        new_columns = list(batch.columns)
        new_columns.append(column_values)

        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def get_output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Get the output schema with hash column added."""
        if not self.config.enabled:
            return input_schema

        new_fields = list(input_schema)
        new_fields.append(pa.field(self.config.column_name, pa.string()))
        return pa.schema(new_fields)
