"""Data quality processor for performing quality checks and validation."""

from __future__ import annotations
from typing import Dict, List, Tuple, Any
import re

import pyarrow as pa

from .base import BaseProcessor, ValidationResult


class DataQualityProcessor(BaseProcessor):
    """Performs data quality checks and cleaning.

    This processor applies configurable data quality rules to validate
    and clean data, including length validation, pattern matching, and
    range checking for different data types.

    Args:
        rules: Dictionary containing quality rules organized by column name

    Attributes:
        rules: Dictionary of data quality rules to apply
    """

    def __init__(self, rules: Dict[str, Any]):
        """Initialize the data quality processor.

        Args:
            rules: Dictionary containing quality rules organized by column name.
                   Each column can have rules like min_length, max_length, pattern, etc.
        """
        self.rules = rules

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply data quality rules to batch.

        Evaluates all configured quality rules against the batch data,
        generating validation results for any failures while preserving
        the original data structure.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (original_batch, validation_results) where validation_results
            contains any quality rule violations found
        """
        validation_results = []

        # Apply column-specific rules
        for column_name, column_rules in self.rules.get("column_rules", {}).items():
            if column_name in batch.schema.names:
                column_index = batch.schema.get_field_index(column_name)
                column = batch.column(column_index)

                self._apply_column_rules(column, column_rules, column_name, validation_results)

        # For now, return the original batch (no filtering based on quality rules)
        # In a more sophisticated implementation, you might filter out rows that fail quality checks
        return batch, validation_results

    def _apply_column_rules(self, column: pa.Array, rules: Dict[str, Any],
                           column_name: str, validation_results: List[ValidationResult]):
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
            self._validate_pattern(column, rules["pattern"], column_name, validation_results)

        # Range validation for numeric types
        if "min_value" in rules or "max_value" in rules:
            self._validate_numeric_range(column, rules, column_name, validation_results)

    def _validate_string_length(self, column: pa.Array, rules: Dict[str, Any],
                               column_name: str, validation_results: List[ValidationResult]):
        """Validate string length constraints.

        Checks minimum and maximum length constraints for string columns.

        Args:
            column: PyArrow Array containing string data
            rules: Dictionary containing min_length and/or max_length constraints
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        if not pa.types.is_string(column.type):
            return

        min_len = rules.get("min_length")
        max_len = rules.get("max_length")

        for i in range(len(column)):
            if column[i].is_valid:
                value = column[i].as_py()
                if value is not None:
                    length = len(value)

                    if min_len is not None and length < min_len:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value length {length} below minimum {min_len}",
                            error_code="STRING_TOO_SHORT",
                            row_index=i,
                            column_name=column_name
                        ))

                    if max_len is not None and length > max_len:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value length {length} exceeds maximum {max_len}",
                            error_code="STRING_TOO_LONG",
                            row_index=i,
                            column_name=column_name
                        ))

    def _validate_pattern(self, column: pa.Array, pattern: str,
                         column_name: str, validation_results: List[ValidationResult]):
        """Validate string pattern constraints.

        Checks that string values match a specified regular expression pattern.

        Args:
            column: PyArrow Array containing string data
            pattern: Regular expression pattern to match against
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        if not pa.types.is_string(column.type):
            return

        try:
            compiled_pattern = re.compile(pattern)
        except re.error:
            # Invalid regex pattern - raise the error as expected by tests
            raise

        for i in range(len(column)):
            if column[i].is_valid:
                value = column[i].as_py()
                if value is not None and not compiled_pattern.match(value):
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Value '{value}' does not match pattern '{pattern}'",
                        error_code="PATTERN_MISMATCH",
                        row_index=i,
                        column_name=column_name
                    ))

    def _validate_numeric_range(self, column: pa.Array, rules: Dict[str, Any],
                               column_name: str, validation_results: List[ValidationResult]):
        """Validate numeric range constraints.

        Checks minimum and maximum value constraints for numeric columns.

        Args:
            column: PyArrow Array containing numeric data
            rules: Dictionary containing min_value and/or max_value constraints
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        if not pa.types.is_numeric(column.type):
            return

        min_val = rules.get("min_value")
        max_val = rules.get("max_value")

        for i in range(len(column)):
            if column[i].is_valid:
                value = column[i].as_py()
                if value is not None:
                    if min_val is not None and value < min_val:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value {value} below minimum {min_val}",
                            error_code="VALUE_TOO_LOW",
                            row_index=i,
                            column_name=column_name
                        ))

                    if max_val is not None and value > max_val:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value {value} exceeds maximum {max_val}",
                            error_code="VALUE_TOO_HIGH",
                            row_index=i,
                            column_name=column_name
                        ))

    def validate_string_length_min_length(self, value: str, min_length: int) -> str:
        """Validate minimum string length and return appropriate error code.

        Args:
            value: String value to validate
            min_length: Minimum required length

        Returns:
            Error code if validation fails, empty string if passes
        """
        if len(value) < min_length:
            return "STRING_TOO_SHORT"
        return ""

    def validate_string_length_max_length(self, value: str, max_length: int) -> str:
        """Validate maximum string length and return appropriate error code.

        Args:
            value: String value to validate
            max_length: Maximum allowed length

        Returns:
            Error code if validation fails, empty string if passes
        """
        if len(value) > max_length:
            return "STRING_TOO_LONG"
        return ""

    def validate_pattern_valid_pattern(self, value: str, pattern: str) -> str:
        """Validate string against pattern and return appropriate error code.

        Args:
            value: String value to validate
            pattern: Regular expression pattern

        Returns:
            Error code if validation fails, empty string if passes
        """
        try:
            if not re.match(pattern, value):
                return "PATTERN_MISMATCH"
        except re.error:
            raise
        return ""

    def validate_numeric_range_min_value(self, value: float, min_value: float) -> str:
        """Validate minimum numeric value and return appropriate error code.

        Args:
            value: Numeric value to validate
            min_value: Minimum required value

        Returns:
            Error code if validation fails, empty string if passes
        """
        if value < min_value:
            return "VALUE_TOO_LOW"
        return ""

    def validate_numeric_range_max_value(self, value: float, max_value: float) -> str:
        """Validate maximum numeric value and return appropriate error code.

        Args:
            value: Numeric value to validate
            max_value: Maximum allowed value

        Returns:
            Error code if validation fails, empty string if passes
        """
        if value > max_value:
            return "VALUE_TOO_HIGH"
        return ""

    def get_quality_summary(self) -> Dict[str, Any]:
        """Get a summary of data quality rules and their configuration.

        Returns:
            Dictionary containing summary of configured quality rules
        """
        summary = {
            "total_rules": 0,
            "column_rules": {},
            "rule_types": set()
        }

        for column_name, column_rules in self.rules.get("column_rules", {}).items():
            summary["column_rules"][column_name] = list(column_rules.keys())
            summary["total_rules"] += len(column_rules)
            summary["rule_types"].update(column_rules.keys())

        summary["rule_types"] = list(summary["rule_types"])
        return summary
