"""Main data validation processor."""

from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

from ..base import BaseProcessor, ValidationResult
from .bad_rows_handler import BadRowsHandler
from .validation_config import FieldValidationRule, ValidationConfig
from .validation_rules import ValidationRules


class ValidationProcessingError(RuntimeError):
    """Validation could not be completed; the batch was not emitted."""


class BadRowsThresholdExceededError(ValidationProcessingError):
    """More rows were rejected than ``BadRowsConfig.max_bad_rows_percent`` allows."""


class _RuleError(str):
    """An error message (a plain ``str``) that remembers the column it is about."""

    column: Optional[str]

    def __new__(cls, message: str, column: Optional[str] = None) -> "_RuleError":
        error = super().__new__(cls, message)
        error.column = column
        return error


def _hashable(value: Any) -> Any:
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


class DataValidationProcessor(BaseProcessor):
    """Processor for data validation with bad rows handling.

    This processor enforces:
    - Required field validation (null checks)
    - Unique field validation (duplicate detection)
    - Range validation (min/max for numeric and date fields)
    - String validation (length, pattern matching)
    - Enum validation (allowed values)

    Violations are handled by routing bad rows to a separate output file.

    Failures are never swallowed: an internal error raises ``ValidationProcessingError`` (the
    batch is not emitted), and exceeding ``max_bad_rows_percent`` raises
    ``BadRowsThresholdExceededError`` when ``fail_on_exceed_threshold`` is set. A ``required``
    rule for a column that the batch does not have is an error, not a skipped rule.

    Uniqueness strategies (``ValidationConfig.uniqueness_strategy``):
    - ``first_wins``: the first valid row with a key is kept, later duplicates are rejected.
    - ``fail_on_duplicate``: like ``first_wins`` but reported as a constraint violation.
    - ``last_wins``: within a batch the last valid row with a key is kept and earlier ones are
      rejected as superseded. Rows already emitted by earlier batches cannot be retracted, so a
      duplicate of such a key is rejected.
    - ``mark_all_duplicates``: every valid row of a key that occurs more than once in the batch
      (or whose key was already emitted or already marked) is rejected, the first one included.
    A row only claims its keys when it passes *all* rules, so a row rejected for another reason
    never causes a later valid row to be rejected as a duplicate.

    Every error of a rejected row becomes a ``ValidationResult`` with ``row_index`` (position in
    the batch passed in), ``error_code`` ``VALIDATION_ERROR`` and ``column_name`` (the field the
    rule belongs to). Messages do not contain cell values unless
    ``ValidationConfig.include_values_in_errors`` is set.

    Rejected rows are kept in ``bad_rows_handler`` only when ``BadRowsConfig.enabled`` is true;
    with ``enabled=False`` they are only counted (the percentage threshold keeps working), which
    keeps memory bounded when the caller writes the rejected rows itself.
    """

    def __init__(self, config: ValidationConfig):
        """Initialize the validation processor.

        Args:
            config: Validation configuration
        """
        self.config = config
        self.unique_value_tracker: Dict[str, Set[Any]] = {}
        self._marked_duplicate_keys: Dict[str, Set[Any]] = {}
        self.bad_rows_handler = BadRowsHandler(config.bad_rows_config)
        self.total_rows_processed = 0
        self.validation_rules = ValidationRules()

        # Initialize unique value trackers for unique fields
        for rule in config.field_validations:
            if rule.unique:
                self.unique_value_tracker[rule.field_name] = set()
                self._marked_duplicate_keys[rule.field_name] = set()
            # Bad bounds are configuration errors, reported when the processor is created
            if rule.range_validation is not None:
                ValidationRules.check_range_config(rule.range_validation)

    # ------------------------------------------------------------------ batch processing

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch with validation and bad row handling.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (clean_batch, validation_results)

        Raises:
            ValidationProcessingError: If validation could not be carried out.
            BadRowsThresholdExceededError: If too many rows are bad and
                ``fail_on_exceed_threshold`` is set.
        """
        validation_results: List[ValidationResult] = []

        try:
            self._check_required_columns(batch)

            # Per-row errors from the field rules, then uniqueness among the surviving rows
            if self._uses_batch_level_uniqueness():
                row_errors = [self._validate_row_rules(batch, i) for i in range(len(batch))]
                self._apply_uniqueness(batch, row_errors)
            else:
                row_errors = [self._validate_row(batch, i)[1] for i in range(len(batch))]

            good_row_indices = []
            for row_idx, errors in enumerate(row_errors):
                if not errors:
                    good_row_indices.append(row_idx)
                    continue

                self.bad_rows_handler.add_bad_row(
                    batch, row_idx, errors, row_number=self.total_rows_processed + row_idx
                )
                for error in errors:
                    validation_results.append(
                        ValidationResult(
                            is_valid=False,
                            error_message=error,
                            error_code="VALIDATION_ERROR",
                            row_index=row_idx,
                            column_name=getattr(error, "column", None),
                        )
                    )

            # Create clean batch with only good rows
            if len(good_row_indices) == len(batch):
                clean_batch = batch
            else:
                clean_batch = batch.take(pa.array(good_row_indices, type=pa.int64()))

            self.total_rows_processed += len(batch)

        except ValidationProcessingError:
            raise
        except Exception as exc:
            # Fail closed: a batch that could not be validated must not be emitted
            raise ValidationProcessingError(
                f"Validation processing failed ({type(exc).__name__}); the batch was not emitted"
            ) from exc

        # Check if bad rows exceed threshold
        if self.bad_rows_handler.is_threshold_exceeded(self.total_rows_processed):
            bad_rows_percent = self.bad_rows_handler.get_bad_rows_percentage(
                self.total_rows_processed
            )
            raise BadRowsThresholdExceededError(
                f"Bad rows ({bad_rows_percent:.1f}%) exceed "
                f"threshold ({self.config.bad_rows_config.max_bad_rows_percent}%)"
            )

        return clean_batch, validation_results

    def _check_required_columns(self, batch: pa.RecordBatch) -> None:
        names = set(batch.schema.names)
        missing = [
            rule.field_name
            for rule in self.config.field_validations
            if rule.required and rule.field_name not in names
        ]
        if missing:
            raise ValidationProcessingError(
                f"Required column(s) missing from the batch: {sorted(set(missing))}"
            )

    # -------------------------------------------------------------------- row validation

    def _validate_row(self, batch: pa.RecordBatch, row_idx: int) -> Tuple[bool, List[str]]:
        """Validate a single row against all validation rules.

        Uniqueness for ``first_wins``/``fail_on_duplicate`` is tracked here, in row order; the
        other strategies need to see the whole batch and are applied by ``process_batch``.

        Args:
            batch: PyArrow RecordBatch
            row_idx: Index of row to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = self._validate_row_rules(batch, row_idx)

        if not self._uses_batch_level_uniqueness():
            keys = self._row_unique_keys(batch, row_idx)
            for field_name, key in keys.items():
                if key in self.unique_value_tracker[field_name]:
                    errors.append(self._duplicate_message(field_name))
            # Claim the keys only if the row passed everything
            if not errors:
                for field_name, key in keys.items():
                    self.unique_value_tracker[field_name].add(key)

        return len(errors) == 0, errors

    def _validate_row_rules(self, batch: pa.RecordBatch, row_idx: int) -> List[str]:
        """Errors from every rule except uniqueness."""
        errors: List[str] = []
        names = batch.schema.names

        for rule in self.config.field_validations:
            if rule.field_name not in names:
                if rule.required:
                    errors.append(
                        _RuleError(
                            f"Field '{rule.field_name}' is required but the column is missing",
                            rule.field_name,
                        )
                    )
                continue

            value = batch.column(rule.field_name)[row_idx].as_py()
            errors.extend(self._field_errors(rule, value))

        return errors

    def _field_errors(self, rule: FieldValidationRule, value: Any) -> List[str]:
        """Errors for one value against the non-uniqueness rules of a field."""
        return [_RuleError(error, rule.field_name) for error in self._field_messages(rule, value)]

    def _field_messages(self, rule: FieldValidationRule, value: Any) -> List[str]:
        include_values = self.config.include_values_in_errors

        # Required validation
        if rule.required and self.validation_rules.is_null_or_empty(value):
            return [f"Field '{rule.field_name}' is required but is null/empty"]

        # NULL skips the remaining rules (use ``required`` to forbid it). Empty and
        # whitespace-only strings are values: length/pattern/enum/range rules apply to them.
        if value is None:
            return []

        errors: List[str] = []

        if rule.range_validation:
            error = self.validation_rules.validate_range(
                rule.field_name, value, rule.range_validation, include_values
            )
            if error:
                errors.append(error)

        if rule.string_validation:
            error = self.validation_rules.validate_string(
                rule.field_name, value, rule.string_validation, include_values
            )
            if error:
                errors.append(error)

        if rule.enum_validation:
            error = self.validation_rules.validate_enum(
                rule.field_name, value, rule.enum_validation, include_values
            )
            if error:
                errors.append(error)

        if rule.date_validation:
            error = self.validation_rules.validate_date(
                rule.field_name, value, rule.date_validation, include_values
            )
            if error:
                errors.append(error)

        return errors

    # ------------------------------------------------------------------------ uniqueness

    def _uses_batch_level_uniqueness(self) -> bool:
        return self.config.uniqueness_strategy in ("last_wins", "mark_all_duplicates")

    def _unique_rules(self, batch: pa.RecordBatch) -> List[FieldValidationRule]:
        names = batch.schema.names
        return [r for r in self.config.field_validations if r.unique and r.field_name in names]

    def _row_unique_keys(self, batch: pa.RecordBatch, row_idx: int) -> Dict[str, Any]:
        """Key per unique field for one row (NULL and blank values are not keys)."""
        keys: Dict[str, Any] = {}
        for rule in self._unique_rules(batch):
            value = batch.column(rule.field_name)[row_idx].as_py()
            if not self.validation_rules.is_null_or_empty(value):
                keys[rule.field_name] = _hashable(value)
        return keys

    def _duplicate_message(self, field_name: str, detail: Optional[str] = None) -> str:
        if detail:
            message = f"Field '{field_name}' {detail}"
        elif self.config.uniqueness_strategy == "fail_on_duplicate":
            message = f"Field '{field_name}' value violates uniqueness constraint"
        else:
            message = f"Field '{field_name}' value is not unique (duplicate found)"
        return _RuleError(message, field_name)

    def _apply_uniqueness(self, batch: pa.RecordBatch, row_errors: List[List[str]]) -> None:
        """Batch-level uniqueness for ``last_wins`` and ``mark_all_duplicates``.

        Only rows without any other error take part, and only rows that end up accepted claim
        their keys.
        """
        rules = self._unique_rules(batch)
        if not rules:
            return

        keys = [self._row_unique_keys(batch, i) for i in range(len(batch))]
        candidates = [i for i in range(len(batch)) if not row_errors[i]]
        accepted: List[int] = []

        if self.config.uniqueness_strategy == "last_wins":
            local: Dict[str, Set[Any]] = {r.field_name: set() for r in rules}
            for i in reversed(candidates):  # the last row of a key is seen first
                problems = []
                for field_name, key in keys[i].items():
                    if key in self.unique_value_tracker[field_name]:
                        problems.append(
                            self._duplicate_message(
                                field_name, "value was already used by an earlier batch"
                            )
                        )
                    elif key in local[field_name]:
                        problems.append(
                            self._duplicate_message(
                                field_name, "value is superseded by a later row (last_wins)"
                            )
                        )
                if problems:
                    row_errors[i].extend(problems)
                else:
                    accepted.append(i)
                    for field_name, key in keys[i].items():
                        local[field_name].add(key)
        else:  # mark_all_duplicates
            counts: Dict[str, Dict[Any, int]] = {r.field_name: {} for r in rules}
            for i in candidates:
                for field_name, key in keys[i].items():
                    counts[field_name][key] = counts[field_name].get(key, 0) + 1
            for i in candidates:
                problems = []
                for field_name, key in keys[i].items():
                    if (
                        counts[field_name][key] > 1
                        or key in self.unique_value_tracker[field_name]
                        or key in self._marked_duplicate_keys[field_name]
                    ):
                        problems.append(
                            self._duplicate_message(
                                field_name, "value is not unique (all duplicates are rejected)"
                            )
                        )
                if problems:
                    row_errors[i].extend(problems)
                else:
                    accepted.append(i)
            for field_name, per_key in counts.items():
                self._marked_duplicate_keys[field_name].update(
                    key for key, count in per_key.items() if count > 1
                )

        for i in accepted:
            for field_name, key in keys[i].items():
                self.unique_value_tracker[field_name].add(key)

    # -------------------------------------------------------------------------- reporting

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
            "bad_rows_percent": self.bad_rows_handler.get_bad_rows_percentage(
                self.total_rows_processed
            ),
            "unique_fields_tracked": list(self.unique_value_tracker.keys()),
            "unique_values_counts": {
                field: len(values) for field, values in self.unique_value_tracker.items()
            },
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
        return self.validation_rules.validate_range(
            field_name, value, validation_config, self.config.include_values_in_errors
        )

    def _validate_string(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_string."""
        return self.validation_rules.validate_string(
            field_name, value, validation_config, self.config.include_values_in_errors
        )

    def _validate_enum(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_enum."""
        return self.validation_rules.validate_enum(
            field_name, value, validation_config, self.config.include_values_in_errors
        )

    def _validate_date(self, field_name, value, validation_config):
        """Backward compatibility wrapper for _validate_date."""
        return self.validation_rules.validate_date(
            field_name, value, validation_config, self.config.include_values_in_errors
        )

    def _handle_bad_row(self, batch, row_idx, errors):
        """Backward compatibility wrapper for _handle_bad_row."""
        return self.bad_rows_handler.add_bad_row(batch, row_idx, errors)

    def _infer_field_type(self, field_name, bad_rows):
        """Backward compatibility wrapper for _infer_field_type."""
        return self.bad_rows_handler._infer_field_type(field_name, bad_rows)
