"""Main data validation processor."""

from typing import List, Tuple, Dict, Optional, Any, Set
import pyarrow as pa

from ..base import BaseProcessor, ValidationResult
from .validation_config import ValidationConfig
from .validation_rules import ValidationRules
from .bad_rows_handler import BadRowsHandler


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
        self.bad_rows_handler = BadRowsHandler(config.bad_rows_config)
        self.total_rows_processed = 0
        self.validation_rules = ValidationRules()

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
                    self.bad_rows_handler.add_bad_row(batch, row_idx, errors)

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
            if self.bad_rows_handler.is_threshold_exceeded(self.total_rows_processed):
                bad_rows_percent = self.bad_rows_handler.get_bad_rows_percentage(self.total_rows_processed)
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
            if rule.required and self.validation_rules.is_null_or_empty(value):
                errors.append(f"Field '{rule.field_name}' is required but is null/empty")
                continue

            # Skip other validations if value is null (unless required)
            if self.validation_rules.is_null_or_empty(value):
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
                range_error = self.validation_rules.validate_range(rule.field_name, value, rule.range_validation)
                if range_error:
                    errors.append(range_error)

            # String validation
            if rule.string_validation:
                string_error = self.validation_rules.validate_string(rule.field_name, value, rule.string_validation)
                if string_error:
                    errors.append(string_error)

            # Enum validation
            if rule.enum_validation:
                enum_error = self.validation_rules.validate_enum(rule.field_name, value, rule.enum_validation)
                if enum_error:
                    errors.append(enum_error)

            # Date validation
            if rule.date_validation:
                date_error = self.validation_rules.validate_date(rule.field_name, value, rule.date_validation)
                if date_error:
                    errors.append(date_error)

        return len(errors) == 0, errors

    def get_bad_rows_batch(self) -> Optional[pa.RecordBatch]:
        """Get bad rows as a PyArrow RecordBatch.

        Returns:
            PyArrow RecordBatch containing bad rows, or None if no bad rows
        """
        return self.bad_rows_handler.get_bad_rows_batch()

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get validation processing summary.

        Returns:
            Dictionary containing validation statistics
        """
        return {
            "total_rows_processed": self.total_rows_processed,
            "bad_rows_count": self.bad_rows_handler.get_bad_rows_count(),
            "bad_rows_percent": self.bad_rows_handler.get_bad_rows_percentage(self.total_rows_processed),
            "unique_fields_tracked": list(self.unique_value_tracker.keys()),
            "unique_values_counts": {field: len(values) for field, values in self.unique_value_tracker.items()}
        }

    # Backward compatibility methods and properties for tests
    @property
    def bad_rows(self):
        """Backward compatibility property for bad_rows."""
        return self.bad_rows_handler.bad_rows

    @bad_rows.setter
    def bad_rows(self, value):
        """Backward compatibility setter for bad_rows."""
        self.bad_rows_handler.bad_rows = value

    def _is_null_or_empty(self, value):
        """Backward compatibility wrapper for _is_null_or_empty."""
        return self.validation_rules.is_null_or_empty(value)

    def _validate_range(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_range."""
        return self.validation_rules.validate_range(field_name, value, validation_config)

    def _validate_string(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_string."""
        return self.validation_rules.validate_string(field_name, value, validation_config)

    def _validate_enum(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_enum."""
        return self.validation_rules.validate_enum(field_name, value, validation_config)

    def _validate_date(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_date."""
        return self.validation_rules.validate_date(field_name, value, validation_config)

    def _handle_bad_row(self, batch, row_idx, errors):
        """Backward compatibility wrapper for _handle_bad_row."""
        return self.bad_rows_handler.add_bad_row(batch, row_idx, errors)

    def _infer_field_type(self, field_name, bad_rows):
        """Backward compatibility wrapper for _infer_field_type."""
        return self.bad_rows_handler._infer_field_type(field_name, bad_rows)
