"""Write-time validation processor for ensuring data quality before writing."""

from __future__ import annotations
from typing import List, Optional, Set, Tuple, Any
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


@dataclass
class WriteTimeConfig:
    """Configuration for write-time validation."""
    # Schema validation
    expected_schema: Optional[pa.Schema] = None
    fail_on_schema_mismatch: bool = False
    required_columns: Optional[List[str]] = None

    # Data quality checks
    check_empty_tables: bool = True
    check_duplicate_rows: bool = False
    check_null_primary_keys: bool = False
    check_null_percentages: bool = False

    # Primary key configuration
    primary_key_columns: List[str] = None

    # Thresholds
    max_null_percentage: float = 50.0
    min_row_count: int = 1

    def __post_init__(self):
        if self.primary_key_columns is None:
            self.primary_key_columns = []


class WriteTimeValidator(BaseProcessor):
    """Processor for validating data quality before writing."""

    def __init__(self, config: WriteTimeConfig):
        super().__init__()
        self.config = config
        self._seen_primary_keys: Set[Tuple[Any, ...]] = set()

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and return validation results."""
        all_results = []

        try:
            # Run all configured validations
            if self.config.check_empty_tables:
                all_results.extend(self._validate_not_empty(batch))

            if self.config.expected_schema:
                all_results.extend(self._validate_schema_compliance(batch))

            if self.config.required_columns:
                all_results.extend(self._validate_required_columns(batch))

            if self.config.check_null_percentages:
                all_results.extend(self._validate_null_percentages(batch))

            if self.config.check_null_primary_keys:
                all_results.extend(self._validate_primary_key_nulls(batch))

            if self.config.check_duplicate_rows:
                all_results.extend(self._validate_duplicate_rows(batch))

            # Always check for write readiness
            all_results.extend(self._validate_write_readiness(batch))

        except Exception as e:
            # Handle any unexpected validation errors
            all_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Write validation error: {str(e)}",
                error_code="WRITE_VALIDATION_ERROR"
            ))

        return batch, all_results

    def _validate_not_empty(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that the batch is not empty."""
        results = []

        if batch.num_rows == 0:
            results.append(ValidationResult(
                is_valid=False,
                error_message="Empty table detected",
                error_code="EMPTY_TABLE"
            ))
        elif batch.num_rows < self.config.min_row_count:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Table has {batch.num_rows} rows, minimum required: {self.config.min_row_count}",
                error_code="INSUFFICIENT_ROWS"
            ))

        return results

    def _validate_schema_compliance(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate schema matches expected schema."""
        results = []

        if not self.config.expected_schema:
            return results

        # Check if schemas match - this will raise TypeError if expected_schema is not a Schema
        if not batch.schema.equals(self.config.expected_schema):
            error_msg = f"Schema mismatch. Expected: {self.config.expected_schema}, Got: {batch.schema}"

            if self.config.fail_on_schema_mismatch:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=error_msg,
                    error_code="SCHEMA_MISMATCH"
                ))
            else:
                # Just warn about schema differences
                results.append(ValidationResult(
                    is_valid=True,
                    error_message=f"Schema warning: {error_msg}",
                    error_code="SCHEMA_WARNING"
                ))

        return results

    def _validate_required_columns(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that all required columns are present."""
        results = []

        if not self.config.required_columns:
            return results

        present_columns = set(batch.schema.names)
        missing_columns = set(self.config.required_columns) - present_columns

        if missing_columns:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Missing required columns: {sorted(missing_columns)}",
                error_code="MISSING_REQUIRED_COLUMNS"
            ))

        return results

    def _validate_null_percentages(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate null percentages don't exceed thresholds."""
        results = []

        if batch.num_rows == 0:
            return results

        for i, column_name in enumerate(batch.schema.names):
            column = batch.column(i)
            null_count = pc.sum(pc.is_null(column)).as_py()
            null_percentage = (null_count / batch.num_rows) * 100

            if null_percentage > self.config.max_null_percentage:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Column '{column_name}' has {null_percentage:.1f}% null values, exceeds threshold of {self.config.max_null_percentage}%",
                    error_code="EXCESSIVE_NULLS",
                    column_name=column_name
                ))

        return results

    def _validate_primary_key_nulls(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that primary key columns don't contain nulls."""
        results = []

        schema_names = batch.schema.names

        for pk_column in self.config.primary_key_columns:
            if pk_column not in schema_names:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Primary key column '{pk_column}' not found in schema",
                    error_code="MISSING_PRIMARY_KEY_COLUMN",
                    column_name=pk_column
                ))
                continue

            column_index = schema_names.index(pk_column)
            column = batch.column(column_index)
            null_count = pc.sum(pc.is_null(column)).as_py()

            if null_count > 0:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Primary key column '{pk_column}' contains {null_count} null values",
                    error_code="NULL_PRIMARY_KEY",
                    column_name=pk_column
                ))

        return results

    def _validate_duplicate_rows(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that there are no duplicate rows based on primary key."""
        results = []

        if not self.config.primary_key_columns or batch.num_rows == 0:
            return results

        schema_names = batch.schema.names

        # Check if all primary key columns exist
        missing_pk_columns = set(self.config.primary_key_columns) - set(schema_names)
        if missing_pk_columns:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Primary key columns not found: {sorted(missing_pk_columns)}",
                error_code="MISSING_PRIMARY_KEY_COLUMNS"
            ))
            return results

        # Extract primary key values
        pk_indices = [schema_names.index(col) for col in self.config.primary_key_columns]

        duplicate_rows = []
        current_batch_keys = set()

        for row_idx in range(batch.num_rows):
            # Create primary key tuple for this row
            pk_values = tuple(
                batch.column(col_idx)[row_idx].as_py() if batch.column(col_idx)[row_idx].is_valid else None
                for col_idx in pk_indices
            )

            # Check for duplicates within this batch
            if pk_values in current_batch_keys:
                duplicate_rows.append(row_idx)
            else:
                current_batch_keys.add(pk_values)

            # Check for duplicates across batches
            if pk_values in self._seen_primary_keys:
                duplicate_rows.append(row_idx)
            else:
                self._seen_primary_keys.add(pk_values)

        if duplicate_rows:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Found {len(duplicate_rows)} duplicate primary key values in rows: {duplicate_rows[:10]}{'...' if len(duplicate_rows) > 10 else ''}",
                error_code="DUPLICATE_PRIMARY_KEYS"
            ))

        return results

    def _validate_write_readiness(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that data is ready for writing (general checks)."""
        results = []

        # Check for unsupported data types that might cause write issues
        for field in batch.schema:
            if field.type == pa.null():
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Column '{field.name}' has null type which may cause write issues",
                    error_code="NULL_TYPE_COLUMN",
                    column_name=field.name
                ))

        # Check for extremely large strings that might cause issues
        for i, field in enumerate(batch.schema):
            if field.type == pa.string():
                column = batch.column(i)
                try:
                    max_length = pc.max(pc.utf8_length(column)).as_py()
                    if max_length and max_length > 1000000:  # 1MB limit
                        results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Column '{field.name}' contains very large strings (max: {max_length} chars) that may cause write issues",
                            error_code="LARGE_STRING_VALUES",
                            column_name=field.name
                        ))
                except Exception:
                    # Skip if we can't compute string lengths
                    pass

        return results

    def reset_state(self):
        """Reset internal state for processing new datasets."""
        self._seen_primary_keys.clear()


def create_basic_write_validator(primary_key_columns: Optional[List[str]] = None) -> WriteTimeValidator:
    """Create a basic write-time validator with common settings.

    Args:
        primary_key_columns: List of primary key column names

    Returns:
        WriteTimeValidator with basic configuration
    """
    config = WriteTimeConfig(
        check_empty_tables=True,
        check_duplicate_rows=bool(primary_key_columns),
        check_null_primary_keys=bool(primary_key_columns),
        primary_key_columns=primary_key_columns or [],
        max_null_percentage=90.0  # Allow high null percentage for basic validation
    )
    return WriteTimeValidator(config)


def create_strict_write_validator(
    primary_key_columns: Optional[List[str]] = None,
    required_columns: Optional[List[str]] = None,
    expected_schema: Optional[pa.Schema] = None
) -> WriteTimeValidator:
    """Create a strict write-time validator with comprehensive checks.

    Args:
        primary_key_columns: List of primary key column names
        required_columns: List of required column names
        expected_schema: Expected schema for validation

    Returns:
        WriteTimeValidator with strict configuration
    """
    config = WriteTimeConfig(
        check_empty_tables=True,
        check_duplicate_rows=bool(primary_key_columns),
        check_null_primary_keys=bool(primary_key_columns),
        check_null_percentages=True,
        primary_key_columns=primary_key_columns or [],
        required_columns=required_columns,
        expected_schema=expected_schema,
        fail_on_schema_mismatch=bool(expected_schema),
        max_null_percentage=10.0  # Strict null percentage
    )
    return WriteTimeValidator(config)


__all__ = [
    "WriteTimeValidator",
    "WriteTimeConfig",
    "create_basic_write_validator",
    "create_strict_write_validator"
]
