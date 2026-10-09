"""Constraint validators for different types of data validation.

Match semantics of ``pattern``: an unanchored search (JSON Schema), see
``forklift.processors._regex``.
"""

from decimal import InvalidOperation
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.compute as pc

from .._columns import is_text_type
from .._regex import compile_pattern, pattern_matches
from .._values import compare_temporal, parse_temporal, to_decimal
from .base_local import ValidationResult
from .type_converter import TypeConverter


def _is_date_like(data_type: pa.DataType) -> bool:
    return pa.types.is_date(data_type) or pa.types.is_timestamp(data_type)


def _plain_number(bound: Any) -> Any:
    """Bound for int/float columns: numbers as they are, numeric strings as Decimal."""
    if bound is None or (isinstance(bound, (int, float)) and not isinstance(bound, bool)):
        return bound
    return to_decimal(bound)


def _temporal_values(column: pa.Array) -> List[Any]:
    """date/datetime values; nanosecond timestamps are read at microsecond precision."""
    if pa.types.is_timestamp(column.type) and column.type.unit == "ns":
        column = column.cast(pa.timestamp("us", tz=column.type.tz), safe=False)
    return column.to_pylist()


class ConstraintValidator:
    """Handles validation of various data constraints."""

    @staticmethod
    def validate_range_constraints(
        column: pa.Array, col_name: str, constraints: Dict[str, Any]
    ) -> List[ValidationResult]:
        """Validate range constraints for numeric (incl. decimal) and date/timestamp columns."""
        results: List[ValidationResult] = []

        min_val = constraints.get("min")
        max_val = constraints.get("max")
        if min_val is None and max_val is None:
            return results

        if TypeConverter.is_numeric_type(column.type):
            is_decimal = pa.types.is_decimal(column.type)
            try:
                if is_decimal:
                    # Exact: Decimal("0.01") must not fall below a float bound of 0.01
                    low = None if min_val is None else to_decimal(str(min_val))
                    high = None if max_val is None else to_decimal(str(max_val))
                else:
                    low = _plain_number(min_val)
                    high = _plain_number(max_val)
            except (InvalidOperation, ValueError, TypeError):
                raise ValueError(
                    f"Range constraint of column '{col_name}' is not numeric"
                ) from None
            compare = None
        elif _is_date_like(column.type):
            low = None if min_val is None else parse_temporal(min_val)
            high = None if max_val is None else parse_temporal(max_val)
            if (min_val is not None and low is None) or (max_val is not None and high is None):
                raise ValueError(f"Range constraint of column '{col_name}' is not a date")
            compare = compare_temporal
        else:
            return results

        values = _temporal_values(column) if compare else column.to_pylist()
        for i, value in enumerate(values):
            if value is None:
                continue

            if compare:
                try:
                    below = low is not None and compare(value, low) < 0
                    above = high is not None and compare(value, high) > 0
                except TypeError:
                    raise ValueError(
                        f"Range constraint of column '{col_name}' cannot be compared with "
                        f"its values (timezone-aware vs naive)"
                    ) from None
            else:
                below = low is not None and value < low
                above = high is not None and value > high

            if below:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value {value} "
                        f"is below minimum {min_val}",
                        error_code="MIN_VALUE_VIOLATION",
                        column_name=col_name,
                        row_index=i,
                    )
                )
            if above:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value"
                        f" {value} exceeds maximum {max_val}",
                        error_code="MAX_VALUE_VIOLATION",
                        column_name=col_name,
                        row_index=i,
                    )
                )

        return results

    @staticmethod
    def validate_enum_constraints(
        column: pa.Array, col_name: str, allowed_values: List[Any]
    ) -> List[ValidationResult]:
        """Validate enum constraints."""
        results = []

        try:
            allowed_set = set(allowed_values)
        except TypeError:
            allowed_set = None  # unhashable members: fall back to list membership
        null_mask = pc.is_null(column)

        for i in range(len(column)):
            if not null_mask[i].as_py():
                value = column[i].as_py()
                allowed = value in (allowed_set if allowed_set is not None else allowed_values)
                if not allowed:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            error_message=f"Column '{col_name}' value '{value}' "
                            f"is not in allowed values: {allowed_values}",
                            error_code="ENUM_VIOLATION",
                            column_name=col_name,
                            row_index=i,
                        )
                    )

        return results

    @staticmethod
    def validate_pattern_constraints(
        column: pa.Array,
        col_name: str,
        pattern: str,
        allow_unsafe_regex: bool = False,
    ) -> List[ValidationResult]:
        """Validate regex pattern constraints (unanchored search; JSON Schema semantics).

        Raises:
            ValueError: If the pattern is invalid or looks prone to catastrophic backtracking
                (unless ``allow_unsafe_regex``). ``SchemaValidator`` checks patterns when it is
                created, so this normally never happens while validating.
        """
        results: List[ValidationResult] = []

        if not is_text_type(column.type):
            return results

        compiled = compile_pattern(pattern, allow_unsafe_regex)

        for i, value in enumerate(column.to_pylist()):
            if value is not None and not pattern_matches(compiled, value):
                results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' "
                        f"value '{value}' does not match pattern '{pattern}'",
                        error_code="PATTERN_VIOLATION",
                        column_name=col_name,
                        row_index=i,
                    )
                )

        return results

    @staticmethod
    def validate_length_constraints(
        column: pa.Array, col_name: str, constraints: Dict[str, Any]
    ) -> List[ValidationResult]:
        """Validate string length constraints."""
        results = []

        if not is_text_type(column.type):
            return results

        min_length = constraints.get("minLength")
        max_length = constraints.get("maxLength")

        for i, value in enumerate(column.to_pylist()):
            if value is None:
                continue
            length = len(value)

            if min_length is not None and length < min_length:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value length {length} "
                        f"is below minimum {min_length}",
                        error_code="MIN_LENGTH_VIOLATION",
                        column_name=col_name,
                        row_index=i,
                    )
                )

            if max_length is not None and length > max_length:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Column '{col_name}' value length {length} "
                        f"exceeds maximum {max_length}",
                        error_code="MAX_LENGTH_VIOLATION",
                        column_name=col_name,
                        row_index=i,
                    )
                )

        return results
