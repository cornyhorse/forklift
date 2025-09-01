"""Data processors for validation, transformation, and quality checks."""

from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.compute as pc
import re
import json


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    row_index: Optional[int] = None
    column_name: Optional[str] = None


class BaseProcessor(ABC):
    """Base class for all data processors."""

    @abstractmethod
    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and return valid data and validation results."""
        pass


class SchemaValidator(BaseProcessor):
    """Validates data against a PyArrow schema."""

    def __init__(self, schema: pa.Schema, strict_mode: bool = True):
        self.schema = schema
        self.strict_mode = strict_mode

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Validate batch against schema."""
        validation_results = []
        valid_mask = pc.true()  # Start with all rows valid

        # Check each column
        for i, field in enumerate(self.schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)
            field_valid_mask = self._validate_column(column, field, validation_results)
            valid_mask = pc.and_(valid_mask, field_valid_mask)

        # Split batch into valid and invalid
        valid_indices = pc.filter(pc.list_indices(valid_mask), valid_mask)
        valid_batch = pc.take(batch, valid_indices)

        return valid_batch, validation_results

    def _validate_column(self, column: pa.Array, field: pa.Field, validation_results: List[ValidationResult]) -> pa.Array:
        """Validate a single column against field definition."""
        # Check for nulls in required fields
        if not field.nullable:
            null_mask = pc.is_null(column)
            if pc.any(null_mask).as_py():
                null_indices = pc.filter(pc.list_indices(null_mask), null_mask)
                for idx in null_indices.to_pylist():
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Null value in required field '{field.name}'",
                        error_code="NULL_IN_REQUIRED_FIELD",
                        row_index=idx,
                        column_name=field.name
                    ))

        # Type validation and casting
        valid_mask = pc.true()
        if not pa.types.is_compatible(column.type, field.type):
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
        """Find which values can be safely cast to target type."""
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


class DataQualityProcessor(BaseProcessor):
    """Performs data quality checks and cleaning."""

    def __init__(self, rules: Dict[str, Any]):
        self.rules = rules

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply data quality rules to batch."""
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
        """Apply rules to a specific column."""
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
        """Validate string length constraints."""
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
                            error_code="MIN_LENGTH_VIOLATION",
                            row_index=i,
                            column_name=column_name
                        ))

                    if max_len is not None and length > max_len:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value length {length} exceeds maximum {max_len}",
                            error_code="MAX_LENGTH_VIOLATION",
                            row_index=i,
                            column_name=column_name
                        ))

    def _validate_pattern(self, column: pa.Array, pattern: str,
                         column_name: str, validation_results: List[ValidationResult]):
        """Validate string pattern constraints."""
        if not pa.types.is_string(column.type):
            return

        compiled_pattern = re.compile(pattern)

        for i in range(len(column)):
            if column[i].is_valid:
                value = column[i].as_py()
                if value is not None and not compiled_pattern.match(value):
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Value '{value}' does not match pattern '{pattern}'",
                        error_code="PATTERN_VIOLATION",
                        row_index=i,
                        column_name=column_name
                    ))

    def _validate_numeric_range(self, column: pa.Array, rules: Dict[str, Any],
                               column_name: str, validation_results: List[ValidationResult]):
        """Validate numeric range constraints."""
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
                            error_code="MIN_VALUE_VIOLATION",
                            row_index=i,
                            column_name=column_name
                        ))

                    if max_val is not None and value > max_val:
                        validation_results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Value {value} exceeds maximum {max_val}",
                            error_code="MAX_VALUE_VIOLATION",
                            row_index=i,
                            column_name=column_name
                        ))


class ColumnTransformer(BaseProcessor):
    """Transforms column data (standardization, cleaning, etc.)."""

    def __init__(self, transformations: Dict[str, List[Callable]]):
        self.transformations = transformations

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply transformations to batch columns."""
        validation_results = []

        # Apply transformations to each configured column
        for column_name, transforms in self.transformations.items():
            if column_name in batch.schema.names:
                column_index = batch.schema.get_field_index(column_name)
                column = batch.column(column_index)

                try:
                    transformed_column = self._apply_transforms(column, transforms)
                    batch = batch.set_column(column_index, column_name, transformed_column)
                except Exception as e:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Transformation failed for column '{column_name}': {str(e)}",
                        error_code="TRANSFORMATION_ERROR",
                        column_name=column_name
                    ))

        return batch, validation_results

    def _apply_transforms(self, column: pa.Array, transforms: List[Callable]) -> pa.Array:
        """Apply a list of transformations to a column."""
        result = column
        for transform in transforms:
            result = transform(result)
        return result


class ProcessorPipeline:
    """Pipeline for chaining multiple processors."""

    def __init__(self, processors: List[BaseProcessor]):
        self.processors = processors

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch through all processors in sequence."""
        current_batch = batch
        all_validation_results = []

        for processor in self.processors:
            current_batch, validation_results = processor.process_batch(current_batch)
            all_validation_results.extend(validation_results)

        return current_batch, all_validation_results


# Common transformation functions
def trim_whitespace(column: pa.Array) -> pa.Array:
    """Remove leading and trailing whitespace from string column."""
    if pa.types.is_string(column.type):
        return pc.utf8_trim_whitespace(column)
    return column


def uppercase(column: pa.Array) -> pa.Array:
    """Convert string column to uppercase."""
    if pa.types.is_string(column.type):
        return pc.utf8_upper(column)
    return column


def lowercase(column: pa.Array) -> pa.Array:
    """Convert string column to lowercase."""
    if pa.types.is_string(column.type):
        return pc.utf8_lower(column)
    return column
