"""Constraint validation classes for data quality checks."""

from __future__ import annotations
from typing import List, Any, Optional, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum
import pyarrow as pa
import re
from datetime import datetime

from .base import BaseProcessor, ValidationResult


class ErrorMode(Enum):
    """Error handling modes for constraint validation."""
    FAIL_FAST = "fail_fast"
    FAIL_COMPLETE = "fail_complete"
    BAD_ROWS = "bad_rows"
    STRICT = "strict"
    LENIENT = "lenient"
    IGNORE = "ignore"


@dataclass
class ConstraintConfig:
    """Configuration for constraint validation."""
    error_mode: ErrorMode = ErrorMode.BAD_ROWS
    check_constraints: Dict[str, Any] = field(default_factory=dict)
    unique_constraints: List[str] = field(default_factory=list)
    foreign_key_constraints: Dict[str, Any] = field(default_factory=dict)
    field_constraints: Dict[str, Any] = field(default_factory=dict)
    max_violations: int = 1000


@dataclass
class ConstraintViolation:
    """Represents a constraint violation found during data validation."""
    constraint_name: str
    violation_type: str
    error_message: str
    columns: List[str] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)
    row_index: Optional[int] = None
    column_name: Optional[str] = None
    actual_value: Any = None
    expected_value: Any = None


class ConstraintValidator(BaseProcessor):
    """Validates data against constraints defined in the schema."""

    def __init__(self, config: ConstraintConfig):
        """Initialize the constraint validator.

        Args:
            config: Constraint configuration
        """
        self.config = config
        self.violations: List[ConstraintViolation] = []
        self.violation_counts: Dict[str, int] = {}

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch and validate against constraints.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (valid_batch, validation_results)
        """
        validation_results = []
        self.violations.clear()

        # Process field constraints if any
        if self.config.field_constraints:
            self._validate_field_constraints(batch)

        # For now, return the batch as-is since constraint validation
        # would require more complex implementation
        return batch, validation_results

    def _validate_field_constraints(self, batch: pa.RecordBatch):
        """Validate field-level constraints."""
        for field_name, constraints in self.config.field_constraints.items():
            if field_name not in batch.schema.names:
                continue

            column = batch.column(field_name)

            for constraint in constraints:
                constraint_type = constraint.get('type')
                constraint_name = constraint.get('name', f"{field_name}_{constraint_type}")

                if constraint_type == 'minimum':
                    self._check_minimum_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'maximum':
                    self._check_maximum_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'min_length':
                    self._check_min_length_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'max_length':
                    self._check_max_length_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'pattern':
                    self._check_pattern_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'enum':
                    self._check_enum_constraint(column, field_name, constraint, constraint_name)
                elif constraint_type == 'required':
                    self._check_required_constraint(column, field_name, constraint, constraint_name)

    def _check_minimum_constraint(self, column, field_name, constraint, constraint_name):
        """Check minimum value constraint."""
        min_value = constraint.get('value')
        if min_value is not None:
            for i, value in enumerate(column.to_pylist()):
                if value is not None and value < min_value:
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='minimum',
                        error_message=f"Value {value} is below minimum {min_value}",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value,
                        expected_value=f">= {min_value}"
                    )

    def _check_maximum_constraint(self, column, field_name, constraint, constraint_name):
        """Check maximum value constraint."""
        max_value = constraint.get('value')
        if max_value is not None:
            for i, value in enumerate(column.to_pylist()):
                if value is not None and value > max_value:
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='maximum',
                        error_message=f"Value {value} is above maximum {max_value}",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value,
                        expected_value=f"<= {max_value}"
                    )

    def _check_min_length_constraint(self, column, field_name, constraint, constraint_name):
        """Check minimum length constraint."""
        min_length = constraint.get('value')
        if min_length is not None:
            for i, value in enumerate(column.to_pylist()):
                if value is not None and len(str(value)) < min_length:
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='min_length',
                        error_message=f"Value length {len(str(value))} is below minimum {min_length}",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value
                    )

    def _check_max_length_constraint(self, column, field_name, constraint, constraint_name):
        """Check maximum length constraint."""
        max_length = constraint.get('value')
        if max_length is not None:
            for i, value in enumerate(column.to_pylist()):
                if value is not None and len(str(value)) > max_length:
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='max_length',
                        error_message=f"Value length {len(str(value))} is above maximum {max_length}",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value
                    )

    def _check_pattern_constraint(self, column, field_name, constraint, constraint_name):
        """Check pattern constraint."""
        pattern = constraint.get('value')
        if pattern is not None:
            try:
                regex = re.compile(pattern)
                for i, value in enumerate(column.to_pylist()):
                    if value is not None and not regex.match(str(value)):
                        self._add_violation(
                            constraint_name=constraint_name,
                            violation_type='pattern',
                            error_message=f"Value '{value}' does not match pattern '{pattern}'",
                            column_name=field_name,
                            row_index=i,
                            actual_value=value
                        )
            except re.error:
                # Invalid regex pattern
                pass

    def _check_enum_constraint(self, column, field_name, constraint, constraint_name):
        """Check enum constraint."""
        allowed_values = constraint.get('values', [])
        if allowed_values:
            for i, value in enumerate(column.to_pylist()):
                if value is not None and value not in allowed_values:
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='enum',
                        error_message=f"Value '{value}' is not in allowed values {allowed_values}",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value
                    )

    def _check_required_constraint(self, column, field_name, constraint, constraint_name):
        """Check required constraint."""
        if constraint.get('value', False):
            for i, value in enumerate(column.to_pylist()):
                if value is None or (isinstance(value, str) and not value.strip()):
                    self._add_violation(
                        constraint_name=constraint_name,
                        violation_type='required',
                        error_message=f"Required field '{field_name}' is missing or empty",
                        column_name=field_name,
                        row_index=i,
                        actual_value=value
                    )

    def _add_violation(self, constraint_name: str, violation_type: str, error_message: str,
                      column_name: Optional[str] = None, row_index: Optional[int] = None,
                      actual_value: Any = None, expected_value: Any = None):
        """Add a constraint violation."""
        if len(self.violations) >= self.config.max_violations:
            return

        violation = ConstraintViolation(
            constraint_name=constraint_name,
            violation_type=violation_type,
            error_message=error_message,
            column_name=column_name,
            row_index=row_index,
            actual_value=actual_value,
            expected_value=expected_value
        )

        self.violations.append(violation)

        # Update violation counts
        if constraint_name not in self.violation_counts:
            self.violation_counts[constraint_name] = 0
        self.violation_counts[constraint_name] += 1

    def get_all_violations(self) -> List[ConstraintViolation]:
        """Get all constraint violations found during validation."""
        return self.violations.copy()

    def get_violation_summary(self) -> Dict[str, Any]:
        """Get a summary of constraint violations."""
        return {
            'total_violations': len(self.violations),
            'violation_counts': self.violation_counts.copy(),
            'violations_by_type': self._get_violations_by_type()
        }

    def _get_violations_by_type(self) -> Dict[str, int]:
        """Get violations grouped by type."""
        by_type = {}
        for violation in self.violations:
            if violation.violation_type not in by_type:
                by_type[violation.violation_type] = 0
            by_type[violation.violation_type] += 1
        return by_type

    def finalize(self):
        """Finalize validation and potentially raise exceptions based on error mode."""
        if self.violations and self.config.error_mode in [ErrorMode.FAIL_FAST, ErrorMode.FAIL_COMPLETE]:
            violation_count = len(self.violations)
            raise ValueError(f"Constraint validation failed with {violation_count} violations")


def create_constraint_config_from_schema(schema_dict: Dict[str, Any]) -> ConstraintConfig:
    """Create constraint configuration from schema dictionary.

    Args:
        schema_dict: Schema dictionary containing constraint definitions

    Returns:
        ConstraintConfig instance
    """
    if schema_dict is None:
        return ConstraintConfig()

    # Extract error mode
    error_mode_str = "bad_rows"
    if "x-constraintHandling" in schema_dict:
        error_mode_str = schema_dict["x-constraintHandling"].get("errorMode", "bad_rows")

    try:
        error_mode = ErrorMode(error_mode_str)
    except ValueError:
        error_mode = ErrorMode.BAD_ROWS

    # Extract constraints from schema (simplified implementation)
    check_constraints = {}
    unique_constraints = []
    foreign_key_constraints = {}
    field_constraints = {}

    # Look for constraints in the schema properties
    properties = schema_dict.get("properties", {})
    for field_name, field_def in properties.items():
        constraints = []

        # Check for minimum/maximum constraints
        if "minimum" in field_def:
            constraints.append({
                'type': 'minimum',
                'value': field_def["minimum"],
                'name': f"{field_name}_minimum"
            })
        if "maximum" in field_def:
            constraints.append({
                'type': 'maximum',
                'value': field_def["maximum"],
                'name': f"{field_name}_maximum"
            })

        # Check for length constraints
        if "minLength" in field_def:
            constraints.append({
                'type': 'min_length',
                'value': field_def["minLength"],
                'name': f"{field_name}_min_length"
            })
        if "maxLength" in field_def:
            constraints.append({
                'type': 'max_length',
                'value': field_def["maxLength"],
                'name': f"{field_name}_max_length"
            })

        # Check for pattern constraints
        if "pattern" in field_def:
            constraints.append({
                'type': 'pattern',
                'value': field_def["pattern"],
                'name': f"{field_name}_pattern"
            })

        # Check for enum constraints
        if "enum" in field_def:
            constraints.append({
                'type': 'enum',
                'values': field_def["enum"],
                'name': f"{field_name}_enum"
            })

        # Check for required constraints
        required_fields = schema_dict.get("required", [])
        if field_name in required_fields:
            constraints.append({
                'type': 'required',
                'value': True,
                'name': f"{field_name}_required"
            })

        if constraints:
            field_constraints[field_name] = constraints

        # Check for unique constraints
        if field_def.get("x-unique", False):
            unique_constraints.append(field_name)

    return ConstraintConfig(
        error_mode=error_mode,
        check_constraints=check_constraints,
        unique_constraints=unique_constraints,
        foreign_key_constraints=foreign_key_constraints,
        field_constraints=field_constraints
    )
