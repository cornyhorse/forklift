"""Row hash processor for adding row-level hash columns."""

from __future__ import annotations
import hashlib
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


@dataclass
class RowHashConfig:
    """Configuration for row hash column generation.

    Attributes:
        enabled: Whether to generate row hash column (default: False)
        column_name: Name of the hash column (default: "row_hash")
        algorithm: Hash algorithm to use (default: "sha256")
        include_columns: List of columns to include in hash (None = all columns)
        exclude_columns: List of columns to exclude from hash
        null_value: String to use for NULL values in hash calculation (default: "NULL")
        separator: Separator between column values (default: "||")
    """
    enabled: bool = False
    column_name: str = "row_hash"
    algorithm: str = "sha256"
    include_columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None
    null_value: str = "NULL"
    separator: str = "||"

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
    """Processor for adding row-level hash columns.

    This processor generates a hash column for each row based on the values
    of specified columns. The hash can be used for:
    - Change detection (comparing row hashes between datasets)
    - Data integrity verification
    - Deduplication based on content
    - ETL auditing and lineage tracking

    The processor supports multiple hash algorithms and flexible column
    inclusion/exclusion rules.

    Examples:
        # Basic SHA256 hash of all columns
        config = RowHashConfig(
            enabled=True,
            column_name="data_hash",
            algorithm="sha256"
        )

        # MD5 hash excluding certain columns
        config = RowHashConfig(
            enabled=True,
            column_name="content_hash",
            algorithm="md5",
            exclude_columns=["created_timestamp", "updated_timestamp"]
        )

        # Hash only specific columns
        config = RowHashConfig(
            enabled=True,
            column_name="key_hash",
            algorithm="sha256",
            include_columns=["id", "name", "email"]
        )
    """

    def __init__(self, config: RowHashConfig):
        """Initialize the row hash processor.

        Args:
            config: Row hash configuration
        """
        self.config = config

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding row hash column.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (batch_with_hash_column, validation_results)
        """
        validation_results = []

        if not self.config.enabled:
            return batch, validation_results

        try:
            # Determine which columns to include in hash
            hash_columns = self._get_hash_columns(batch.schema)

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

        if self.config.include_columns is not None:
            # Use only specified columns
            hash_columns = [col for col in self.config.include_columns if col in all_columns]
        else:
            # Use all columns except excluded ones
            hash_columns = [col for col in all_columns if col not in self.config.exclude_columns]

        # Don't include the hash column itself if it already exists
        if self.config.column_name in hash_columns:
            hash_columns.remove(self.config.column_name)

        return hash_columns

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
