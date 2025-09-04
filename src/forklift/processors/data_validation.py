"""Data validation processor with bad rows handling for required, unique, and range validation."""

from __future__ import annotations
from typing import List, Tuple, Dict, Optional, Any, Set, Union
from dataclasses import dataclass
from datetime import datetime, date
import re
import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


@dataclass
class RangeValidation:
    """Range validation configuration for numeric and date fields."""
    min_value: Optional[Union[int, float, str]] = None
    max_value: Optional[Union[int, float, str]] = None
    inclusive: bool = True


@dataclass
class StringValidation:
    """String validation configuration."""
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allow_empty: bool = True


@dataclass
class EnumValidation:
    """Enumeration validation configuration."""
    allowed_values: List[Any]
    case_sensitive: bool = True


@dataclass
class DateValidation:
    """Date validation configuration."""
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    formats: Optional[List[str]] = None


@dataclass
class FieldValidationRule:
    """Validation rule for a single field."""
    field_name: str
    required: bool = False
    unique: bool = False
    range_validation: Optional[RangeValidation] = None
    string_validation: Optional[StringValidation] = None
    enum_validation: Optional[EnumValidation] = None
    date_validation: Optional[DateValidation] = None
    on_violation: Dict[str, str] = None

    def __post_init__(self):
        if self.on_violation is None:
            self.on_violation = {}


@dataclass
class BadRowsConfig:
    """Configuration for bad rows handling."""
    enabled: bool = True
    output_path: str = "bad_rows"
    file_format: str = "parquet"
    include_original_row: bool = True
    include_validation_errors: bool = True
    max_bad_rows_percent: float = 10.0
    fail_on_exceed_threshold: bool = True


@dataclass
class ValidationConfig:
    """Configuration for data validation processor."""
    field_validations: List[FieldValidationRule]
    bad_rows_config: BadRowsConfig
    uniqueness_strategy: str = "first_wins"  # first_wins, last_wins, fail_on_duplicate, mark_all_duplicates

    def __post_init__(self):
        valid_strategies = ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
        if self.uniqueness_strategy not in valid_strategies:
            raise ValueError(f"Invalid uniqueness strategy: {self.uniqueness_strategy}")


class DataValidationProcessor(BaseProcessor):
    """Processor for data validation with bad rows handling.

    This processor enforces:
    - Required field validation (null checks)
    - Unique field validation (duplicate detection)
    - Range validation (min/max for numeric and date fields)
    - String validation (length, pattern matching)
    - Enum validation (allowed values)

    Violations are handled by routing bad rows to a separate output file.
    """

    def __init__(self, config: ValidationConfig):
        """Initialize the validation processor.

        Args:
            config: Validation configuration
        """
        self.config = config
        self.unique_value_tracker: Dict[str, Set[Any]] = {}
        self.bad_rows: List[Dict[str, Any]] = []
        self.total_rows_processed = 0

        # Initialize unique value trackers for unique fields
        for rule in config.field_validations:
            if rule.unique:
                self.unique_value_tracker[rule.field_name] = set()

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch with validation and bad row handling.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (clean_batch, validation_results)
        """
        validation_results = []
        good_row_indices = []

        try:
            # Process each row
            for row_idx in range(len(batch)):
                is_valid, errors = self._validate_row(batch, row_idx)

                if is_valid:
                    good_row_indices.append(row_idx)
                else:
                    self._handle_bad_row(batch, row_idx, errors)

                    # Add validation results for bad rows
                    for error in errors:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=error,
                            error_code="VALIDATION_ERROR",
                            row_index=row_idx
                        ))

            # Create clean batch with only good rows
            if good_row_indices:
                arrays = []
                for i in range(batch.num_columns):
                    column_array = batch.column(i)
                    good_values = [column_array[idx].as_py() for idx in good_row_indices]
                    arrays.append(pa.array(good_values, type=column_array.type))

                clean_batch = pa.RecordBatch.from_arrays(arrays, schema=batch.schema)
            else:
                # No good rows - create empty batch with same schema
                arrays = [pa.array([], type=field.type) for field in batch.schema]
                clean_batch = pa.RecordBatch.from_arrays(arrays, schema=batch.schema)

            self.total_rows_processed += len(batch)

            # Check if bad rows exceed threshold
            if self.config.bad_rows_config.fail_on_exceed_threshold:
                bad_rows_percent = (len(self.bad_rows) / self.total_rows_processed) * 100
                if bad_rows_percent > self.config.bad_rows_config.max_bad_rows_percent:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Bad rows ({bad_rows_percent:.1f}%) exceed threshold ({self.config.bad_rows_config.max_bad_rows_percent}%)",
                        error_code="BAD_ROWS_THRESHOLD_EXCEEDED"
                    ))

            return clean_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Validation processing failed: {str(e)}",
                error_code="VALIDATION_PROCESSOR_ERROR"
            ))
            return batch, validation_results

    def _validate_row(self, batch: pa.RecordBatch, row_idx: int) -> Tuple[bool, List[str]]:
        """Validate a single row against all validation rules.

        Args:
            batch: PyArrow RecordBatch
            row_idx: Index of row to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        for rule in self.config.field_validations:
            if rule.field_name not in batch.schema.names:
                continue

            column = batch.column(rule.field_name)
            value = column[row_idx].as_py()

            # Required validation
            if rule.required and self._is_null_or_empty(value):
                errors.append(f"Field '{rule.field_name}' is required but is null/empty")
                continue

            # Skip other validations if value is null (unless required)
            if self._is_null_or_empty(value):
                continue

            # Unique validation
            if rule.unique:
                if value in self.unique_value_tracker[rule.field_name]:
                    if self.config.uniqueness_strategy == "first_wins":
                        errors.append(f"Field '{rule.field_name}' value '{value}' is not unique (duplicate found)")
                    elif self.config.uniqueness_strategy == "fail_on_duplicate":
                        errors.append(f"Field '{rule.field_name}' value '{value}' violates uniqueness constraint")
                else:
                    self.unique_value_tracker[rule.field_name].add(value)

            # Range validation
            if rule.range_validation:
                range_error = self._validate_range(rule.field_name, value, rule.range_validation)
                if range_error:
                    errors.append(range_error)

            # String validation
            if rule.string_validation:
                string_error = self._validate_string(rule.field_name, value, rule.string_validation)
                if string_error:
                    errors.append(string_error)

            # Enum validation
            if rule.enum_validation:
                enum_error = self._validate_enum(rule.field_name, value, rule.enum_validation)
                if enum_error:
                    errors.append(enum_error)

            # Date validation
            if rule.date_validation:
                date_error = self._validate_date(rule.field_name, value, rule.date_validation)
                if date_error:
                    errors.append(date_error)

        return len(errors) == 0, errors

    def _is_null_or_empty(self, value: Any) -> bool:
        """Check if a value is null or empty."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False

    def _validate_range(self, field_name: str, value: Any, range_val: RangeValidation) -> Optional[str]:
        """Validate value against range constraints."""
        try:
            # Convert value to appropriate type for comparison
            if isinstance(value, str):
                # Try to parse as number if it looks like one
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    return f"Field '{field_name}' value '{value}' cannot be converted to numeric for range validation"

            # Check minimum
            if range_val.min_value is not None:
                min_val = range_val.min_value
                if isinstance(min_val, str):
                    min_val = float(min_val) if '.' in min_val else int(min_val)

                if range_val.inclusive:
                    if value < min_val:
                        return f"Field '{field_name}' value {value} is below minimum {min_val}"
                else:
                    if value <= min_val:
                        return f"Field '{field_name}' value {value} is not greater than {min_val}"

            # Check maximum
            if range_val.max_value is not None:
                max_val = range_val.max_value
                if isinstance(max_val, str):
                    max_val = float(max_val) if '.' in max_val else int(max_val)

                if range_val.inclusive:
                    if value > max_val:
                        return f"Field '{field_name}' value {value} is above maximum {max_val}"
                else:
                    if value >= max_val:
                        return f"Field '{field_name}' value {value} is not less than {max_val}"

            return None

        except Exception as e:
            return f"Field '{field_name}' range validation error: {str(e)}"

    def _validate_string(self, field_name: str, value: Any, string_val: StringValidation) -> Optional[str]:
        """Validate string constraints."""
        if not isinstance(value, str):
            value = str(value)

        # Check minimum length
        if string_val.min_length is not None and len(value) < string_val.min_length:
            return f"Field '{field_name}' length {len(value)} is below minimum {string_val.min_length}"

        # Check maximum length
        if string_val.max_length is not None and len(value) > string_val.max_length:
            return f"Field '{field_name}' length {len(value)} exceeds maximum {string_val.max_length}"

        # Check pattern
        if string_val.pattern is not None:
            try:
                if not re.match(string_val.pattern, value):
                    return f"Field '{field_name}' value '{value}' does not match required pattern"
            except re.error as e:
                return f"Field '{field_name}' pattern validation error: {str(e)}"

        # Check empty
        if not string_val.allow_empty and value.strip() == "":
            return f"Field '{field_name}' cannot be empty"

        return None

    def _validate_enum(self, field_name: str, value: Any, enum_val: EnumValidation) -> Optional[str]:
        """Validate enumeration constraints."""
        allowed_values = enum_val.allowed_values

        if enum_val.case_sensitive:
            if value not in allowed_values:
                return f"Field '{field_name}' value '{value}' not in allowed values: {allowed_values}"
        else:
            # Case-insensitive comparison
            value_lower = str(value).lower()
            allowed_lower = [str(v).lower() for v in allowed_values]
            if value_lower not in allowed_lower:
                return f"Field '{field_name}' value '{value}' not in allowed values: {allowed_values}"

        return None

    def _validate_date(self, field_name: str, value: Any, date_val: DateValidation) -> Optional[str]:
        """Validate date constraints."""
        # This is a simplified date validation - in practice you'd want more robust date parsing
        if isinstance(value, str):
            try:
                # Try to parse the date
                parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return f"Field '{field_name}' value '{value}' is not a valid date"
        elif isinstance(value, (date, datetime)):
            parsed_date = value.date() if isinstance(value, datetime) else value
        else:
            return f"Field '{field_name}' value '{value}' is not a valid date type"

        # Check date range
        if date_val.min_date:
            min_date = datetime.strptime(date_val.min_date, "%Y-%m-%d").date()
            if parsed_date < min_date:
                return f"Field '{field_name}' date {parsed_date} is before minimum {min_date}"

        if date_val.max_date:
            max_date = datetime.strptime(date_val.max_date, "%Y-%m-%d").date()
            if parsed_date > max_date:
                return f"Field '{field_name}' date {parsed_date} is after maximum {max_date}"

        return None

    def _handle_bad_row(self, batch: pa.RecordBatch, row_idx: int, errors: List[str]):
        """Handle a bad row by adding it to the bad rows collection."""
        if not self.config.bad_rows_config.enabled:
            return

        # Extract row data
        row_data = {}
        for i, field_name in enumerate(batch.schema.names):
            row_data[field_name] = batch.column(i)[row_idx].as_py()

        # Add validation errors if configured
        if self.config.bad_rows_config.include_validation_errors:
            row_data["_validation_errors"] = "; ".join(errors)
            row_data["_error_count"] = len(errors)
            row_data["_processed_timestamp"] = datetime.now().isoformat()

        self.bad_rows.append(row_data)

    def get_bad_rows_batch(self) -> Optional[pa.RecordBatch]:
        """Get bad rows as a PyArrow RecordBatch."""
        if not self.bad_rows:
            return None

        # Create schema with error columns if needed
        original_fields = []
        error_fields = []

        if self.bad_rows:
            # Get fields from first bad row (excluding error fields)
            sample_row = self.bad_rows[0]
            for key, value in sample_row.items():
                if not key.startswith("_"):
                    if isinstance(value, bool):
                        field_type = pa.bool_()
                    elif isinstance(value, int):
                        field_type = pa.int64()
                    elif isinstance(value, float):
                        field_type = pa.float64()
                    else:
                        field_type = pa.string()
                    original_fields.append(pa.field(key, field_type))

            # Add error fields if present
            if "_validation_errors" in sample_row:
                error_fields = [
                    pa.field("_validation_errors", pa.string()),
                    pa.field("_error_count", pa.int32()),
                    pa.field("_processed_timestamp", pa.string())
                ]

        schema = pa.schema(original_fields + error_fields)

        # Convert bad rows to arrays
        arrays = []
        for field in schema:
            field_values = [row.get(field.name) for row in self.bad_rows]
            arrays.append(pa.array(field_values, type=field.type))

        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get validation processing summary."""
        return {
            "total_rows_processed": self.total_rows_processed,
            "bad_rows_count": len(self.bad_rows),
            "bad_rows_percent": (len(self.bad_rows) / max(self.total_rows_processed, 1)) * 100,
            "unique_fields_tracked": list(self.unique_value_tracker.keys()),
            "unique_values_counts": {field: len(values) for field, values in self.unique_value_tracker.items()}
        }
