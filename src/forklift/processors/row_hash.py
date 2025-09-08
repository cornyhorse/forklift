"""Row hash processor for adding row-level hash columns and metadata."""

from __future__ import annotations
import hashlib
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone

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
        self.row_counter = 0  # Add this attribute that tests expect

    def set_source_context(self, source_uri: str, source_row_offset: int = 0):
        """Set source context for metadata generation."""
        self.source_uri = source_uri
        self.source_row_offset = source_row_offset

        if self.config.ingested_at_enabled:
            self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    def process_batch(self, batch: pa.RecordBatch, input_batch: Optional[pa.RecordBatch] = None) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and add hash/metadata columns.

        Args:
            batch: Transformed batch to process
            input_batch: Original input batch (for input hash generation)

        Returns:
            Tuple of (enhanced_batch, validation_results)
        """
        validation_results = []

        try:
            arrays = []
            fields = []

            # Add existing columns
            for i, field in enumerate(batch.schema):
                arrays.append(batch.column(i))
                fields.append(field)

            # Add row hash column if enabled
            if self.config.enabled:
                hash_array = self._generate_hash_column(batch)
                arrays.append(hash_array)
                fields.append(pa.field(self.config.column_name, pa.string()))

            # Add input hash column if enabled and input_batch provided
            if self.config.input_hash_enabled and input_batch is not None:
                input_hash_array = self._generate_hash_column(input_batch)
                arrays.append(input_hash_array)
                fields.append(pa.field(self.config.input_hash_column_name, pa.string()))

            # Add source URI column if enabled
            if self.config.source_uri_enabled:
                source_uri_array = pa.array([self.source_uri] * len(batch))
                arrays.append(source_uri_array)
                fields.append(pa.field(self.config.source_uri_column_name, pa.string()))

            # Add ingested_at column if enabled
            if self.config.ingested_at_enabled:
                if self.ingestion_timestamp is None:
                    self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()
                ingested_at_array = pa.array([self.ingestion_timestamp] * len(batch))
                arrays.append(ingested_at_array)
                fields.append(pa.field(self.config.ingested_at_column_name, pa.string()))

            # Add row number columns if enabled
            if self.config.row_number_enabled:
                # Source row numbers
                source_row_numbers = list(range(self.source_row_offset, self.source_row_offset + len(batch)))
                source_row_array = pa.array(source_row_numbers)
                arrays.append(source_row_array)
                fields.append(pa.field(self.config.source_row_number_column_name, pa.int64()))

                # Processing row numbers
                processing_row_numbers = list(range(self.processing_row_counter, self.processing_row_counter + len(batch)))
                processing_row_array = pa.array(processing_row_numbers)
                arrays.append(processing_row_array)
                fields.append(pa.field(self.config.processing_row_number_column_name, pa.int64()))

                # Update counters
                self.processing_row_counter += len(batch)
                self.row_counter += len(batch)

            # Create new schema and batch
            new_schema = pa.schema(fields)
            new_batch = pa.RecordBatch.from_arrays(arrays, schema=new_schema)

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Row hash processing failed: {str(e)}",
                error_code="HASH_PROCESSING_ERROR"
            ))
            return batch, validation_results

    def _generate_hash_column(self, batch: pa.RecordBatch) -> pa.Array:
        """Generate hash values for each row in the batch."""
        hash_values = []

        # Determine which columns to include in hash
        columns_to_hash = self._get_columns_to_hash(batch.schema)

        for row_idx in range(len(batch)):
            # Collect values for this row
            row_values = []
            for col_name in columns_to_hash:
                col_idx = batch.schema.get_field_index(col_name)
                value = batch.column(col_idx)[row_idx]

                if value.is_valid:
                    row_values.append(str(value.as_py()))
                else:
                    row_values.append(self.config.null_value)

            # Create hash
            row_string = self.config.separator.join(row_values)
            hash_obj = hashlib.new(self.config.algorithm)
            hash_obj.update(row_string.encode('utf-8'))
            hash_values.append(hash_obj.hexdigest())

        return pa.array(hash_values)

    def _get_columns_to_hash(self, schema: pa.Schema) -> List[str]:
        """Determine which columns to include in hash calculation."""
        if self.config.include_columns:
            # Use explicitly specified columns
            return [col for col in self.config.include_columns if col in schema.names]
        else:
            # Use all columns except excluded ones
            return [field.name for field in schema if field.name not in self.config.exclude_columns]

    def get_output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Get the output schema with added hash/metadata columns."""
        fields = list(input_schema)

        # Add row hash column if enabled
        if self.config.enabled:
            fields.append(pa.field(self.config.column_name, pa.string()))

        # Add input hash column if enabled
        if self.config.input_hash_enabled:
            fields.append(pa.field(self.config.input_hash_column_name, pa.string()))

        # Add source URI column if enabled
        if self.config.source_uri_enabled:
            fields.append(pa.field(self.config.source_uri_column_name, pa.string()))

        # Add ingested_at column if enabled
        if self.config.ingested_at_enabled:
            fields.append(pa.field(self.config.ingested_at_column_name, pa.string()))

        # Add row number columns if enabled
        if self.config.row_number_enabled:
            fields.append(pa.field(self.config.source_row_number_column_name, pa.int64()))
            fields.append(pa.field(self.config.processing_row_number_column_name, pa.int64()))

        return pa.schema(fields)

    def reset_counters(self):
        """Reset row counters (useful for processing multiple files)."""
        self.processing_row_counter = 0
        self.row_counter = 0
        self.source_row_offset = 0


# Factory function for common configurations
def create_basic_hash_processor(enabled: bool = True, include_metadata: bool = True) -> RowHashProcessor:
    """Create a basic row hash processor with common settings.

    Args:
        enabled: Whether to enable row hashing
        include_metadata: Whether to include metadata columns

    Returns:
        Configured RowHashProcessor
    """
    config = RowHashConfig(
        enabled=enabled,
        input_hash_enabled=include_metadata,
        source_uri_enabled=include_metadata,
        ingested_at_enabled=include_metadata,
        row_number_enabled=include_metadata
    )
    return RowHashProcessor(config)
