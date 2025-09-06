"""Constraint validators for different types of data validation."""

from typing import List, Any, Dict
import pyarrow as pa
import pyarrow.compute as pc
import re

from .base_local import ValidationResult
from .type_converter import TypeConverter


class ConstraintValidator:
    """Handles validation of various data constraints."""

    @staticmethod
    def validate_range_constraints(column: pa.Array, col_name: str, constraints: Dict[str, Any]) -> List[ValidationResult]:
        """Validate range constraints for numeric columns."""
        results = []

        if not TypeConverter.is_numeric_type(column.type):
            return results

        min_val = constraints.get("min")
        max_val = constraints.get("max")

        if min_val is not None:
            violations = pc.less(column, min_val)
            null_mask = pc.is_null(column)
            for i in range(len(column)):
                if violations[i].as_py() and not null_mask[i].as_py():
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value {column[i].as_py()} is below minimum {min_val}",
                        error_code="MIN_VALUE_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

        if max_val is not None:
            violations = pc.greater(column, max_val)
            null_mask = pc.is_null(column)
            for i in range(len(column)):
                if violations[i].as_py() and not null_mask[i].as_py():
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value {column[i].as_py()} exceeds maximum {max_val}",
                        error_code="MAX_VALUE_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

        return results

    @staticmethod
    def validate_enum_constraints(column: pa.Array, col_name: str, allowed_values: List[Any]) -> List[ValidationResult]:
        """Validate enum constraints."""
        results = []

        allowed_set = set(allowed_values)
        null_mask = pc.is_null(column)

        for i in range(len(column)):
            if not null_mask[i].as_py():
                value = column[i].as_py()
                if value not in allowed_set:
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value '{value}' is not in allowed values: {allowed_values}",
                        error_code="ENUM_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

        return results

    @staticmethod
    def validate_pattern_constraints(column: pa.Array, col_name: str, pattern: str) -> List[ValidationResult]:
        """Validate regex pattern constraints."""
        results = []

        if not pa.types.is_string(column.type):
            return results

        try:
            regex = re.compile(pattern)
        except re.error as e:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Invalid regex pattern for column '{col_name}': {e}",
                error_code="INVALID_PATTERN",
                column_name=col_name
            ))
            return results

        null_mask = pc.is_null(column)
        for i in range(len(column)):
            if not null_mask[i].as_py():
                value = str(column[i].as_py())
                if not regex.match(value):
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value '{value}' does not match pattern '{pattern}'",
                        error_code="PATTERN_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

        return results

    @staticmethod
    def validate_length_constraints(column: pa.Array, col_name: str, constraints: Dict[str, Any]) -> List[ValidationResult]:
        """Validate string length constraints."""
        results = []

        if not pa.types.is_string(column.type):
            return results

        min_length = constraints.get("minLength")
        max_length = constraints.get("maxLength")
        null_mask = pc.is_null(column)

        for i in range(len(column)):
            if not null_mask[i].as_py():
                value = str(column[i].as_py())
                length = len(value)

                if min_length is not None and length < min_length:
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value length {length} is below minimum {min_length}",
                        error_code="MIN_LENGTH_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

                if max_length is not None and length > max_length:
                    results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value length {length} exceeds maximum {max_length}",
                        error_code="MAX_LENGTH_VIOLATION",
                        column_name=col_name,
                        row_index=i
                    ))

        return results
