"""Core schema validation logic."""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Union
import pyarrow as pa
import pyarrow.compute as pc

from .base_local import BaseProcessor, ValidationResult
from .config import SchemaValidatorConfig, SchemaValidationMode, NullabilityMode
from .schema import ColumnSchema
from .type_converter import TypeConverter
from .constraints import ConstraintValidator


class SchemaValidator(BaseProcessor):
    """Validates PyArrow record batches against schema definitions."""

    def __init__(self, schema_definition: Union[Dict[str, Any], pa.Schema], config: Optional[Union[SchemaValidatorConfig, bool]] = None, strict_mode: Optional[bool] = None):
        """Initialize the schema validator.

        Args:
            schema_definition: Schema definition dictionary or PyArrow Schema
            config: Validation configuration or legacy strict_mode boolean
            strict_mode: Legacy parameter for backwards compatibility
        """
        # Handle legacy interface where second parameter might be strict_mode boolean
        if isinstance(config, bool):
            # Legacy interface: SchemaValidator(schema, strict_mode)
            strict_mode = config
            config = None

        # Handle legacy interface
        if isinstance(schema_definition, pa.Schema):
            self.schema = schema_definition
            self.schema_definition = TypeConverter.convert_arrow_schema_to_dict(schema_definition)
        else:
            self.schema_definition = schema_definition
            self.schema = TypeConverter.convert_dict_to_arrow_schema(schema_definition) if schema_definition else None

        # Handle legacy strict_mode parameter
        if config is None:
            config = SchemaValidatorConfig()

        if strict_mode is not None:
            config.validation_mode = SchemaValidationMode.STRICT if strict_mode else SchemaValidationMode.PERMISSIVE
            # In strict mode, we don't allow extra columns
            config.extra_columns_allowed = not strict_mode

        self.config = config
        self.strict_mode = (config.validation_mode == SchemaValidationMode.STRICT)  # Legacy attribute
        self.expected_columns = self._parse_schema_definition()
        self._validation_cache: Dict[str, bool] = {}

    def _parse_schema_definition(self) -> Dict[str, ColumnSchema]:
        """Parse schema definition into ColumnSchema objects."""
        columns = {}

        if self.schema_definition and "columns" in self.schema_definition:
            for col_def in self.schema_definition["columns"]:
                if isinstance(col_def, dict):
                    name = col_def.get("name", "")
                    data_type = col_def.get("type", "string")
                    nullable = col_def.get("nullable", True)
                    constraints = col_def.get("constraints", {})
                    description = col_def.get("description")

                    columns[name] = ColumnSchema(
                        name=name,
                        data_type=data_type,
                        nullable=nullable,
                        constraints=constraints,
                        description=description
                    )

        return columns

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and validate against schema.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (processed_batch, validation_results)
        """
        validation_results = []

        # Validate batch structure
        validation_results.extend(self._validate_batch_structure(batch))

        # Validate column presence
        validation_results.extend(self._validate_column_presence(batch))

        # Validate data types
        validation_results.extend(self._validate_data_types(batch))

        # Validate nullability
        validation_results.extend(self._validate_nullability(batch))

        # Validate constraints
        validation_results.extend(self._validate_constraints(batch))

        # Validate row counts if specified
        validation_results.extend(self._validate_row_counts(batch))

        # Return original or processed batch based on configuration
        processed_batch = self._process_batch_based_on_mode(batch, validation_results)

        return processed_batch, validation_results

    def _validate_batch_structure(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate basic batch structure."""
        results = []

        if batch is None:
            results.append(ValidationResult(
                is_valid=False,
                error_message="Batch is None",
                error_code="NULL_BATCH"
            ))
            return results

        if batch.num_rows == 0 and self.config.min_row_count and self.config.min_row_count > 0:
            results.append(ValidationResult(
                is_valid=False,
                error_message="Batch is empty but minimum row count is required",
                error_code="EMPTY_BATCH"
            ))

        return results

    def _validate_column_presence(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that required columns are present."""
        results = []
        batch_columns = set(batch.column_names)
        expected_columns = set(self.expected_columns.keys())

        # Check for missing columns
        missing_columns = expected_columns - batch_columns
        for missing_col in missing_columns:
            col_schema = self.expected_columns[missing_col]
            if not col_schema.nullable or self.config.validation_mode == SchemaValidationMode.STRICT:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Required column '{missing_col}' is missing",
                    error_code="MISSING_COLUMN",
                    column_name=missing_col
                ))

        # Check for extra columns - only flag as error if we're in strict mode and extra columns aren't allowed
        if not self.config.extra_columns_allowed and self.config.validation_mode == SchemaValidationMode.STRICT:
            extra_columns = batch_columns - expected_columns
            for extra_col in extra_columns:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Unexpected column '{extra_col}' found",
                    error_code="EXTRA_COLUMN",
                    column_name=extra_col
                ))

        # Check column order if required
        if self.config.check_column_order and len(missing_columns) == 0:
            expected_order = list(self.expected_columns.keys())
            actual_order = [col for col in batch.column_names if col in expected_columns]

            if expected_order != actual_order:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Column order mismatch. Expected: {expected_order}, Got: {actual_order}",
                    error_code="COLUMN_ORDER_MISMATCH"
                ))

        return results

    def _validate_data_types(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate column data types."""
        results = []

        for col_name in batch.column_names:
            if col_name in self.expected_columns:
                expected_schema = self.expected_columns[col_name]
                actual_type = batch.column(col_name).type

                if not self._is_type_compatible(actual_type, expected_schema.data_type):
                    if self.config.allow_type_coercion:
                        # Check if coercion is possible
                        if not TypeConverter.can_coerce_type(actual_type, expected_schema.data_type):
                            results.append(ValidationResult(
                                is_valid=False,
                                error_message=f"Column '{col_name}' type mismatch: expected {expected_schema.data_type}, got {actual_type}, coercion not possible",
                                error_code="TYPE_MISMATCH_NO_COERCION",
                                column_name=col_name
                            ))
                    else:
                        results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Column '{col_name}' type mismatch: expected {expected_schema.data_type}, got {actual_type}",
                            error_code="TYPE_MISMATCH",
                            column_name=col_name
                        ))

        return results

    def _validate_nullability(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate nullability constraints."""
        results = []

        if self.config.nullability_mode == NullabilityMode.IGNORE:
            return results

        for col_name in batch.column_names:
            if col_name in self.expected_columns:
                expected_schema = self.expected_columns[col_name]
                column = batch.column(col_name)

                # Check if column should not be nullable
                if not expected_schema.nullable:
                    # Check for nulls using PyArrow compute
                    null_mask = pc.is_null(column)

                    for i in range(batch.num_rows):
                        if null_mask[i].as_py():
                            is_error = self.config.nullability_mode == NullabilityMode.ERROR
                            results.append(ValidationResult(
                                is_valid=not is_error,
                                error_message=f"Column '{col_name}' contains null value but is marked as non-nullable",
                                error_code="NULL_IN_REQUIRED_FIELD" if is_error else "NULL_WARNING",
                                column_name=col_name,
                                row_index=i
                            ))

                # Check null percentage thresholds
                if self.config.max_null_percentage is not None:
                    null_mask = pc.is_null(column)
                    null_count = pc.sum(null_mask).as_py()
                    null_percentage = (null_count / batch.num_rows) * 100

                    if null_percentage > self.config.max_null_percentage:
                        results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Column '{col_name}' null percentage ({null_percentage:.2f}%) exceeds threshold ({self.config.max_null_percentage}%)",
                            error_code="NULL_PERCENTAGE_EXCEEDED",
                            column_name=col_name
                        ))

        return results

    def _validate_constraints(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate column constraints."""
        results = []

        for col_name in batch.column_names:
            if col_name in self.expected_columns:
                expected_schema = self.expected_columns[col_name]
                column = batch.column(col_name)

                # Validate range constraints
                if "min" in expected_schema.constraints or "max" in expected_schema.constraints:
                    results.extend(ConstraintValidator.validate_range_constraints(column, col_name, expected_schema.constraints))

                # Validate enum constraints
                if "enum" in expected_schema.constraints:
                    results.extend(ConstraintValidator.validate_enum_constraints(column, col_name, expected_schema.constraints["enum"]))

                # Validate pattern constraints
                if "pattern" in expected_schema.constraints:
                    results.extend(ConstraintValidator.validate_pattern_constraints(column, col_name, expected_schema.constraints["pattern"]))

                # Validate length constraints
                if "minLength" in expected_schema.constraints or "maxLength" in expected_schema.constraints:
                    results.extend(ConstraintValidator.validate_length_constraints(column, col_name, expected_schema.constraints))

        return results

    def _validate_row_counts(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate row count constraints."""
        results = []

        if self.config.min_row_count is not None and batch.num_rows < self.config.min_row_count:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Batch has {batch.num_rows} rows, below minimum {self.config.min_row_count}",
                error_code="MIN_ROW_COUNT_VIOLATION"
            ))

        if self.config.max_row_count is not None and batch.num_rows > self.config.max_row_count:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Batch has {batch.num_rows} rows, exceeds maximum {self.config.max_row_count}",
                error_code="MAX_ROW_COUNT_VIOLATION"
            ))

        return results

    def _is_type_compatible(self, actual_type: pa.DataType, expected_type_str: str) -> bool:
        """Check if actual type is compatible with expected type."""
        # Cache results for performance
        cache_key = f"{actual_type}:{expected_type_str}"
        if cache_key in self._validation_cache:
            return self._validation_cache[cache_key]

        result = TypeConverter.is_type_compatible(actual_type, expected_type_str)
        self._validation_cache[cache_key] = result
        return result

    def _process_batch_based_on_mode(self, batch: pa.RecordBatch, validation_results: List[ValidationResult]) -> pa.RecordBatch:
        """Process batch based on validation mode and results."""
        has_errors = any(not result.is_valid for result in validation_results)

        if has_errors and self.config.validation_mode == SchemaValidationMode.STRICT:
            # In strict mode, could potentially filter out invalid rows
            # For now, return original batch
            pass

        if self.config.validation_mode == SchemaValidationMode.COERCE:
            # Could attempt type coercions here
            pass

        return batch

    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of the expected schema."""
        return {
            "total_columns": len(self.expected_columns),
            "nullable_columns": sum(1 for col in self.expected_columns.values() if col.nullable),
            "non_nullable_columns": sum(1 for col in self.expected_columns.values() if not col.nullable),
            "columns_with_constraints": sum(1 for col in self.expected_columns.values() if col.constraints),
            "column_details": {
                name: {
                    "type": col.data_type,
                    "nullable": col.nullable,
                    "has_constraints": bool(col.constraints),
                    "description": col.description
                }
                for name, col in self.expected_columns.items()
            }
        }

    def reset_cache(self):
        """Reset the validation cache."""
        self._validation_cache.clear()
