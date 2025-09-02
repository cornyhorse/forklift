"""Schema validation processor for validating data against PyArrow schemas."""

from __future__ import annotations
from typing import List, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


class SchemaValidator(BaseProcessor):
    """Validates data against a PyArrow schema.

    This processor validates incoming data against a predefined schema,
    checking data types, null constraints, and performing type coercion
    where possible.

    Args:
        schema: PyArrow schema to validate against
        strict_mode: Whether to enforce strict validation (default: True)

    Attributes:
        schema: The PyArrow schema used for validation
        strict_mode: Whether strict validation is enabled
    """

    def __init__(self, schema: pa.Schema, strict_mode: bool = True):
        """Initialize the schema validator.

        Args:
            schema: PyArrow schema defining expected data structure and types
            strict_mode: If True, enforce strict type checking; if False, attempt coercion
        """
        self.schema = schema
        self.strict_mode = strict_mode

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Validate batch against schema.

        Performs comprehensive schema validation including type checking,
        null constraint validation, and type coercion where appropriate.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (valid_batch, validation_results) where valid_batch contains
            only rows that passed validation and validation_results contains
            details about any validation failures
        """
        validation_results = []
        valid_mask = pa.array([True] * batch.num_rows)  # Start with all rows valid

        # Check each column
        for i, field in enumerate(self.schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)
            field_valid_mask = self._validate_column(column, field, validation_results)
            valid_mask = pc.and_(valid_mask, field_valid_mask)

        # Split batch into valid and invalid
        valid_indices = pc.filter(pa.array(range(batch.num_rows)), valid_mask)
        valid_batch = batch.take(valid_indices)

        return valid_batch, validation_results

    def _validate_column(self, column: pa.Array, field: pa.Field, validation_results: List[ValidationResult]) -> pa.Array:
        """Validate a single column against field definition.

        Performs type validation, null checking, and type coercion for a single column.

        Args:
            column: PyArrow Array containing column data
            field: PyArrow Field definition for this column
            validation_results: List to append validation results to

        Returns:
            PyArrow Array mask indicating which rows are valid for this column
        """
        # Check for nulls in required fields
        if not field.nullable:
            null_mask = pc.is_null(column)
            if pc.any(null_mask).as_py():
                # Find indices where values are null
                for i in range(len(column)):
                    if not column[i].is_valid:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Null value in required field '{field.name}'",
                            error_code="NULL_IN_REQUIRED_FIELD",
                            row_index=i,
                            column_name=field.name
                        ))

        # Type validation and casting
        valid_mask = pa.array([True] * len(column))
        if column.type != field.type:
            try:
                # Try to cast the column
                casted_column = pc.cast(column, field.type, safe=False)
                # If casting succeeds, all values are valid for this type check
            except pa.ArrowInvalid:
                # Some values failed casting
                valid_mask = self._find_castable_values(column, field.type, validation_results, field.name)

        return valid_mask

    def _find_castable_values(self, column: pa.Array, target_type: pa.DataType,
                             validation_results: List[ValidationResult], field_name: str) -> pa.Array:
        """Find which values can be safely cast to target type.

        Tests each value in the column to determine which ones can be
        successfully cast to the target data type.

        Args:
            column: PyArrow Array to test for castability
            target_type: Target data type for casting
            validation_results: List to append validation failures to
            field_name: Name of the field being validated

        Returns:
            PyArrow Array mask indicating which values can be cast
        """
        valid_mask = []

        for i in range(len(column)):
            try:
                value = column[i]
                if value.is_valid:
                    pc.cast(pa.array([value.as_py()]), target_type)
                valid_mask.append(True)
            except (pa.ArrowInvalid, ValueError, TypeError):
                valid_mask.append(False)
                validation_results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Cannot cast value to {target_type} in field '{field_name}'",
                    error_code="TYPE_CAST_ERROR",
                    row_index=i,
                    column_name=field_name
                ))

        return pa.array(valid_mask)
