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

    Examples:
        # Basic SHA256 hash with metadata
        config = RowHashConfig(
            enabled=True,
            input_hash_enabled=True,
            source_uri_enabled=True,
            ingested_at_enabled=True,
            row_number_enabled=True
        )

        # Hash only specific columns
        config = RowHashConfig(
            enabled=True,
            column_name="key_hash",
            algorithm="sha256",
            include_columns=["id", "name", "email"]
        )
    """
        self.source_uri = None
        self.ingestion_timestamp = None
        self.source_row_offset = 0  # Starting row number in source file
        self.processing_row_counter = 0  # Sequential processing counter

    def set_source_context(self, source_uri: str, source_row_offset: int = 0):
        """Set source context for metadata generation.
        
        Args:
            source_uri: URI/path of the source file
            source_row_offset: Starting row number in source file (for batch processing)
        """
        self.source_uri = source_uri
        self.source_row_offset = source_row_offset
        
        # Set ingestion timestamp when source context is set
        if self.config.ingested_at_enabled:
            from datetime import datetime, timezone
            self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    def process_batch(self, batch: pa.RecordBatch, input_batch: Optional[pa.RecordBatch] = None) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding hash columns and metadata.

        Args:
            batch: PyArrow RecordBatch to process (output/transformed data)
            input_batch: Original input batch for input hash calculation (optional)
        """
        self.config = config
            Tuple of (batch_with_metadata_columns, validation_results)
        """
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
        """Process a batch by adding row hash column.
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
        """Determine which columns to include in hash calculation.

        Args:
            schema: PyArrow schema of the batch

        Returns:
            List of column names to include in hash
        """
        all_columns = [field.name for field in schema]

        if self.config.include_columns is not None:
        Returns:
            hash_columns = [col for col in self.config.include_columns if col in all_columns]
        else:
            # Use all columns except excluded ones
            hash_columns = [col for col in all_columns if col not in self.config.exclude_columns]
        validation_results = []
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
        """Get columns for input hash calculation (all original columns).
        
        Args:
            schema: PyArrow schema of the input batch
            
        Returns:
            List of column names to include in input hash
        """
        # For input hash, include all original columns
        return [field.name for field in schema]

    def _compute_row_hashes(self, batch: pa.RecordBatch, hash_columns: List[str]) -> pa.Array:
        """Compute hash values for each row.

        Args:
            batch: PyArrow RecordBatch
            hash_columns: List of column names to include in hash

        Returns:
            PyArrow Array of hash values
        """
        num_rows = batch.num_rows
        hash_values = []

        for row_idx in range(num_rows):
            # Build concatenated string for this row
            row_parts = []
            for col_name in hash_columns:
                column = batch.column(col_name)
                value = column[row_idx]

                if value.is_valid:
        if not self.config.enabled:
            return batch, validation_results

        try:
                        # Handle binary data
                        row_parts.append(value.as_py().hex() if value.as_py() else self.config.null_value)

            if not hash_columns:
                validation_results.append(ValidationResult(
                    is_valid=False,
                    error_message="No columns available for hash calculation",
                    error_code="ROW_HASH_NO_COLUMNS"
                ))
                return batch, validation_results

            # Generate hash values for each row
            hash_values = self._compute_row_hashes(batch, hash_columns)

            # Create new batch with hash column added
            new_batch = self._add_hash_column(batch, hash_values)

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Row hash calculation failed: {str(e)}",
                error_code="ROW_HASH_ERROR"
            ))
            return batch, validation_results

    def _get_hash_columns(self, schema: pa.Schema) -> List[str]:
        """Determine which columns to include in hash calculation.

        Args:
            schema: PyArrow schema of the batch

        Returns:
            List of column names to include in hash
        """
        all_columns = [field.name for field in schema]

    def _add_column(self, batch: pa.RecordBatch, column_name: str, values: pa.Array) -> pa.RecordBatch:
        """Add a column to the batch.

        Args:
            batch: Original PyArrow RecordBatch
            column_name: Name of the new column
            values: Array of values for the new column

        Returns:
            New RecordBatch with column added
        """
        # Create new schema with the column
        new_fields = list(batch.schema)
        new_fields.append(pa.field(column_name, values.type))
        new_schema = pa.schema(new_fields)

        # Create new batch with the column
        new_columns = list(batch.columns)
        new_columns.append(values)

        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)
        if self.config.include_columns is not None:
            # Use only specified columns
        """Get the output schema with all metadata columns added.
        else:
            # Use all columns except excluded ones
            hash_columns = [col for col in all_columns if col not in self.config.exclude_columns]

        # Don't include the hash column itself if it already exists
            Output schema with metadata columns
            hash_columns.remove(self.config.column_name)
        """Compute hash values for each row.

        # Add output hash column
        if self.config.enabled:
            new_fields.append(pa.field(self.config.column_name, pa.string()))

        # Add input hash column
        if self.config.input_hash_enabled:
            new_fields.append(pa.field(self.config.input_hash_column_name, pa.string()))

        # Add source URI column
        if self.config.source_uri_enabled:
            new_fields.append(pa.field(self.config.source_uri_column_name, pa.string()))

        # Add ingestion timestamp column
        if self.config.ingested_at_enabled:
            new_fields.append(pa.field(self.config.ingested_at_column_name, pa.string()))

        # Add row number columns
        if self.config.row_number_enabled:
            new_fields.append(pa.field(self.config.source_row_number_column_name, pa.int64()))
            new_fields.append(pa.field(self.config.processing_row_number_column_name, pa.int64()))

        Args:
            batch: PyArrow RecordBatch
            hash_columns: List of column names to include in hash

        Returns:
            PyArrow Array of hash values
        """
        num_rows = batch.num_rows
        hash_values = []

        for row_idx in range(num_rows):
            # Build concatenated string for this row
            row_parts = []
            for col_name in hash_columns:
                column = batch.column(col_name)
                value = column[row_idx]

                if value.is_valid:
                    # Convert value to string representation
                    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                        row_parts.append(str(value.as_py()))
                    elif pa.types.is_binary(column.type):
                        # Handle binary data
                        row_parts.append(value.as_py().hex() if value.as_py() else self.config.null_value)
                    else:
                        row_parts.append(str(value.as_py()))
                else:
                    row_parts.append(self.config.null_value)

            # Join with separator and compute hash
            row_string = self.config.separator.join(row_parts)
            hash_value = self._compute_hash(row_string)
            hash_values.append(hash_value)

        return pa.array(hash_values, type=pa.string())

    def _compute_hash(self, data: str) -> str:
        """Compute hash of the given string.

        Args:
            data: String data to hash

        Returns:
            Hexadecimal hash string
        """
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

    def _add_hash_column(self, batch: pa.RecordBatch, hash_values: pa.Array) -> pa.RecordBatch:
        """Add hash column to the batch.

        Args:
            batch: Original PyArrow RecordBatch
            hash_values: Array of hash values

        Returns:
            New RecordBatch with hash column added
        """
        # Create new schema with hash column
        new_fields = list(batch.schema)
        new_fields.append(pa.field(self.config.column_name, pa.string()))
        new_schema = pa.schema(new_fields)

        # Create new batch with hash column
        new_columns = list(batch.columns)
        new_columns.append(hash_values)

        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def get_output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Get the output schema with hash column added.

        Args:
            input_schema: Input PyArrow schema

        Returns:
            Output schema with hash column
        """
        if not self.config.enabled:
            return input_schema

        # Add hash column to schema
        new_fields = list(input_schema)
        new_fields.append(pa.field(self.config.column_name, pa.string()))
        return pa.schema(new_fields)
