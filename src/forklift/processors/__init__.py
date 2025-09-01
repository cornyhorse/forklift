"""Data processors for validation, transformation, and quality checks.

This module provides processor classes for validating, transforming, and
performing quality checks on data during the import process. Processors can
be chained together in pipelines for complex data processing workflows.
"""

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
    """Result of data validation operation.

    Attributes:
        is_valid: Whether the validation passed
        error_message: Human-readable error message (if validation failed)
        error_code: Machine-readable error code for categorization
        row_index: Index of the row that failed validation (if applicable)
        column_name: Name of the column that failed validation (if applicable)
    """
    is_valid: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    row_index: Optional[int] = None
    column_name: Optional[str] = None


class BaseProcessor(ABC):
    """Base class for all data processors.

    This abstract base class defines the interface that all data processors
    must implement. Processors take PyArrow RecordBatch objects and return
    processed data along with validation results.
    """

    @abstractmethod
    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and return valid data and validation results.

        Args:
            batch: PyArrow RecordBatch containing data to process

        Returns:
            Tuple of (processed_batch, validation_results)

        Note:
            Implementations should handle both data transformation and validation,
            returning the processed data and any validation issues encountered.
        """
        pass


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
        """Validate numeric range constraints.

        Checks minimum and maximum value constraints for numeric columns.

        Args:
            column: PyArrow Array containing numeric data
            rules: Dictionary containing min_value and/or max_value constraints
            column_name: Name of the column being validated
            validation_results: List to append validation results to
        """
        # Check if column type is numeric (integer or floating point)
        if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
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
    """Transforms column data (standardization, cleaning, etc.).

    This processor applies configurable transformations to column data,
    such as trimming whitespace, changing case, or applying custom
    transformation functions.

    Args:
        transformations: Dictionary mapping column names to lists of transformation functions

    Attributes:
        transformations: Dictionary of column transformations to apply
    """

    def __init__(self, transformations: Dict[str, List[Callable]]):
        """Initialize the column transformer.

        Args:
            transformations: Dictionary where keys are column names and values are
                           lists of transformation functions to apply in order
        """
        self.transformations = transformations

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply transformations to batch columns.

        Applies all configured transformations to their respective columns,
        returning the transformed batch along with any errors encountered.

        Args:
            batch: PyArrow RecordBatch to transform

        Returns:
            Tuple of (transformed_batch, validation_results) where transformed_batch
            contains the data with transformations applied and validation_results
            contains any transformation errors
        """
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
        """Apply a list of transformations to a column.

        Applies transformation functions in sequence to the column data.

        Args:
            column: PyArrow Array to transform
            transforms: List of transformation functions to apply

        Returns:
            PyArrow Array with transformations applied
        """
        result = column
        for transform in transforms:
            result = transform(result)
        return result


class ProcessorPipeline:
    """Pipeline for chaining multiple processors.

    This class allows multiple processors to be chained together in a
    pipeline, with data flowing through each processor in sequence.

    Args:
        processors: List of BaseProcessor instances to chain together

    Attributes:
        processors: List of processors in the pipeline
    """

    def __init__(self, processors: List[BaseProcessor]):
        """Initialize the processor pipeline.

        Args:
            processors: List of BaseProcessor instances that will process data in order
        """
        self.processors = processors

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch through all processors in sequence.

        Passes the batch through each processor in the pipeline, accumulating
        validation results and applying transformations sequentially.

        Args:
            batch: PyArrow RecordBatch to process through the pipeline

        Returns:
            Tuple of (final_batch, all_validation_results) where final_batch
            is the result of all transformations and all_validation_results
            contains validation results from all processors
        """
        current_batch = batch
        all_validation_results = []

        for processor in self.processors:
            current_batch, validation_results = processor.process_batch(current_batch)
            all_validation_results.extend(validation_results)

        return current_batch, all_validation_results


# Common transformation functions
def trim_whitespace(column: pa.Array) -> pa.Array:
    """Remove leading and trailing whitespace from string column.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with whitespace trimmed from string values
    """
    if pa.types.is_string(column.type):
        return pc.utf8_trim_whitespace(column)
    return column


def uppercase(column: pa.Array) -> pa.Array:
    """Convert string column to uppercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to uppercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_upper(column)
    return column


def lowercase(column: pa.Array) -> pa.Array:
    """Convert string column to lowercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to lowercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_lower(column)
    return column
