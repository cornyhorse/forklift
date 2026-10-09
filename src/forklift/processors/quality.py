"""Data quality processor for performing quality checks and validation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pyarrow as pa

from ._columns import column_index, is_text_type
from ._regex import compile_pattern, pattern_matches
from .base import BaseProcessor, ValidationResult


class DataQualityProcessor(BaseProcessor):
    """Performs data quality checks and cleaning.

    This processor applies configurable data quality rules to validate
    and clean data, including length validation, pattern matching, and
    range checking for different data types.

    ``pattern`` rules are unanchored searches (JSON Schema semantics); anchor with ``^``/``$`` to
    match the whole value. Patterns are compiled when the processor is created: an invalid
    pattern, or one with nested unbounded quantifiers such as ``(a+)+``, raises ``ValueError``
    unless ``allow_unsafe_regex`` is true (per column rule, in ``rules``, or as the argument).

    The processor only reports: every violation is a ``ValidationResult`` with ``row_index``
    (position in the batch), ``error_code`` and ``column_name``; the batch is returned unchanged.

    Args:
        rules: Dictionary containing quality rules organized by column name
        allow_unsafe_regex: Accept patterns flagged as prone to catastrophic backtracking
        include_values: Put the offending cell value into the pattern and range messages
            (default, for backward compatibility). Pass ``False`` to keep cell values (possibly
            personal data) out of messages and logs.

    Attributes:
        rules: Dictionary of data quality rules to apply
    """

    def __init__(
        self,
        rules: Dict[str, Any],
        allow_unsafe_regex: bool = False,
        *,
        include_values: bool = True,
    ):
        """Initialize the data quality processor.

        Args:
            rules: Dictionary containing quality rules organized by column name.
                   Each column can have rules like min_length, max_length, pattern, etc.
            allow_unsafe_regex: Accept regular expressions flagged as ReDoS-prone
            include_values: Include cell values in the messages (see the class docstring)
        """
        self.rules = rules
        self.include_values = include_values
        self.allow_unsafe_regex = allow_unsafe_regex or bool(
            rules.get("allow_unsafe_regex", False)
        )

        # Compile every pattern up front so a bad pattern fails at configuration time
        for column_name, column_rules in rules.get("column_rules", {}).items():
            if "pattern" in column_rules:
                compile_pattern(column_rules["pattern"], self._allow_unsafe(column_rules))

    def _allow_unsafe(self, column_rules: Dict[str, Any]) -> bool:
        return bool(column_rules.get("allow_unsafe_regex", self.allow_unsafe_regex))

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply data quality rules to batch.

        Evaluates all configured quality rules against the batch data,
        generating validation results for any failures while preserving
        the original data structure.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (original_batch, validation_results) where validation_results
            contains any quality rule violations found

        Raises:
            ValueError: If a rule refers to a column name that appears more than once.
        """
        validation_results = []

        # Apply column-specific rules
        for column_name, column_rules in self.rules.get("column_rules", {}).items():
            column_idx = column_index(batch.schema, column_name)
            if column_idx is not None:
                column = batch.column(column_idx)

                self._apply_column_rules(column, column_rules, column_name, validation_results)

        # For now, return the original batch (no filtering based on quality rules)
        # In a more sophisticated implementation, you might filter out rows
        # that fail quality checks
        return batch, validation_results

    def _apply_column_rules(
        self,
        column: pa.Array,
        rules: Dict[str, Any],
        column_name: str,
        validation_results: List[ValidationResult],
    ):
        """Apply rules to a specific column.

        Evaluates all configured rules for a single column, adding validation
        results for any violations found.

        Args:
            column: PyArrow Array containing column data
            rules: Dictionary of rules to apply to this column
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        # Length validation
        if "min_length" in rules or "max_length" in rules:
            self._validate_string_length(column, rules, column_name, validation_results)

        # Pattern validation
        if "pattern" in rules:
            self._validate_pattern(
                column,
                rules["pattern"],
                column_name,
                validation_results,
                self._allow_unsafe(rules),
            )

        # Range validation for numeric types
        if "min_value" in rules or "max_value" in rules:
            self._validate_numeric_range(column, rules, column_name, validation_results)

    def _validate_string_length(
        self,
        column: pa.Array,
        rules: Dict[str, Any],
        column_name: str,
        validation_results: List[ValidationResult],
    ):
        """Validate string length constraints.

        Checks minimum and maximum length constraints for string columns.

        Args:
            column: PyArrow Array containing string data
            rules: Dictionary containing min_length and/or max_length constraints
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        if not is_text_type(column.type):
            return

        min_len = rules.get("min_length")
        max_len = rules.get("max_length")

        for i, value in enumerate(column.to_pylist()):
            if value is None:
                continue
            length = len(value)

            if min_len is not None and length < min_len:
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Value length {length} below minimum {min_len}",
                        error_code="MIN_LENGTH_VIOLATION",
                        row_index=i,
                        column_name=column_name,
                    )
                )

            if max_len is not None and length > max_len:
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Value length {length} exceeds maximum {max_len}",
                        error_code="MAX_LENGTH_VIOLATION",
                        row_index=i,
                        column_name=column_name,
                    )
                )

    def _validate_pattern(
        self,
        column: pa.Array,
        pattern: str,
        column_name: str,
        validation_results: List[ValidationResult],
        allow_unsafe_regex: bool = False,
    ):
        """Validate string pattern constraints.

        Checks that string values match a specified regular expression pattern
        (unanchored search, see the class docstring).

        Args:
            column: PyArrow Array containing string data
            pattern: Regular expression pattern to match against
            column_name: Name of the column being validated
            validation_results: List to append validation results to
            allow_unsafe_regex: Accept a pattern flagged as ReDoS-prone

        Raises:
            ValueError: If the pattern is invalid or flagged as unsafe.
        """
        if not is_text_type(column.type):
            return

        compiled_pattern = compile_pattern(pattern, allow_unsafe_regex or self.allow_unsafe_regex)

        for i, value in enumerate(column.to_pylist()):
            if value is not None and not pattern_matches(compiled_pattern, value):
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=(
                            f"Value '{value}' does not match pattern '{pattern}'"
                            if self.include_values
                            else f"Value does not match pattern '{pattern}'"
                        ),
                        error_code="PATTERN_VIOLATION",
                        row_index=i,
                        column_name=column_name,
                    )
                )

    def _validate_numeric_range(
        self,
        column: pa.Array,
        rules: Dict[str, Any],
        column_name: str,
        validation_results: List[ValidationResult],
    ):
        """Validate numeric range constraints.

        Checks minimum and maximum value constraints for numeric columns (integer, floating
        point and decimal; decimals are compared exactly).

        Args:
            column: PyArrow Array containing numeric data
            rules: Dictionary containing min_value and/or max_value constraints
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        is_decimal = pa.types.is_decimal(column.type)
        if not (
            pa.types.is_integer(column.type) or pa.types.is_floating(column.type) or is_decimal
        ):
            return

        min_val = rules.get("min_value")
        max_val = rules.get("max_value")
        if is_decimal:
            # Decimal cannot be compared with float; compare exactly via str()
            min_cmp = None if min_val is None else Decimal(str(min_val))
            max_cmp = None if max_val is None else Decimal(str(max_val))
        else:
            min_cmp, max_cmp = min_val, max_val

        for i, value in enumerate(column.to_pylist()):
            if value is None:
                continue
            if min_cmp is not None and value < min_cmp:
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=(
                            f"Value {value} below minimum {min_val}"
                            if self.include_values
                            else f"Value below minimum {min_val}"
                        ),
                        error_code="MIN_VALUE_VIOLATION",
                        row_index=i,
                        column_name=column_name,
                    )
                )

            if max_cmp is not None and value > max_cmp:
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=(
                            f"Value {value} exceeds maximum {max_val}"
                            if self.include_values
                            else f"Value exceeds maximum {max_val}"
                        ),
                        error_code="MAX_VALUE_VIOLATION",
                        row_index=i,
                        column_name=column_name,
                    )
                )
