"""Main calculated columns processor."""

from typing import Any, Dict, List, Tuple

import pyarrow as pa

from ..base import BaseProcessor, ValidationResult
from .evaluator import ExpressionEvaluator
from .models import CalculatedColumn, CalculatedColumnsConfig


class CalculatedColumnsProcessor(BaseProcessor):
    """Processor for adding calculated columns to data.

    This processor supports various types of calculations:
    - Arithmetic operations (add, subtract, multiply, divide)
    - String operations (concatenation, substring, case conversion)
    - Date/time operations (date arithmetic, formatting)
    - Conditional logic (if-then-else)
    - Mathematical functions (round, abs, sqrt, etc.)
    - Type conversions
    - Null handling
    """

    def __init__(self, config: CalculatedColumnsConfig):
        """Initialize the calculated columns processor.

        Args:
            config: Configuration for calculated columns
        """
        self.config = config
        self.evaluator = ExpressionEvaluator(fail_on_error=config.fail_on_error)

        # Validate configuration: unique names, safe/parsable expressions, dependencies.
        # These are configuration errors, so they raise regardless of ``fail_on_error``.
        self._validate_column_names()
        self._compile_expressions()
        if self.config.validate_dependencies:
            self._validate_dependencies()

    @property
    def _available_functions(self) -> Dict[str, Any]:
        """Backward compatibility property for accessing available functions."""
        return self.evaluator._available_functions

    def _init_functions(self) -> Dict[str, Any]:
        """Backward compatibility method for initializing functions."""
        return self.evaluator._available_functions

    def _calculate_column(
        self, batch: pa.RecordBatch, column_config: CalculatedColumn
    ) -> pa.Array:
        """Backward compatibility method for calculating column values."""
        return self.evaluator.calculate_column_values(batch, column_config)

    def _evaluate_expression(self, batch: pa.RecordBatch, row_idx: int, expression: str) -> Any:
        """Backward compatibility method for evaluating expressions."""
        return self.evaluator.evaluate_expression(batch, row_idx, expression)

    def _validate_column_names(self):
        """Reject two calculated columns with the same name."""
        seen = set()
        for col in self.config.columns:
            if col.name in seen:
                raise ValueError(f"Duplicate calculated column name '{col.name}'")
            seen.add(col.name)

    def _compile_expressions(self):
        """Compile every expression up front so unsafe or malformed ones fail at configuration."""
        for col in self.config.columns:
            if col.is_constant:
                continue
            try:
                self.evaluator.compile(col.expression)
            except ValueError as exc:
                raise ValueError(f"Invalid expression for column '{col.name}': {exc}") from None

    def _validate_dependencies(self):
        """Validate that all column dependencies exist and detect circular dependencies."""
        column_names = {col.name for col in self.config.columns}

        for col in self.config.columns:
            # Check for circular dependencies
            if self._has_circular_dependency(col, column_names, set()):
                raise ValueError(f"Circular dependency detected for column '{col.name}'")

    def _has_circular_dependency(
        self, column: CalculatedColumn, all_columns: set, visited: set
    ) -> bool:
        """Check for circular dependencies in column calculations."""
        if column.name in visited:
            return True

        visited.add(column.name)

        for dep in column.dependencies:
            if dep in all_columns:
                # Find the dependent column
                dep_column = next((col for col in self.config.columns if col.name == dep), None)
                if dep_column and self._has_circular_dependency(
                    dep_column, all_columns, visited.copy()
                ):
                    return True

        return False

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch and add calculated columns.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (processed_batch, validation_results)
        """
        validation_results = []

        # A calculated column must not shadow an input column (or another calculated column):
        # the result would carry two columns with the same name.
        existing_names = set(batch.schema.names)
        for column_config in self.config.columns:
            if column_config.name in existing_names:
                raise ValueError(
                    f"Calculated column '{column_config.name}' collides with a column "
                    f"of the same name"
                )
            existing_names.add(column_config.name)

        try:
            # Create a copy of the batch to work with
            result_batch = batch

            # Process columns in dependency order
            sorted_columns = self._sort_columns_by_dependencies()

            # One snapshot of now()/today() for the whole batch
            with self.evaluator.run():
                for column_config in sorted_columns:
                    if not column_config.is_constant:
                        # Unsafe/malformed expressions are configuration errors: always raise
                        self.evaluator.compile(column_config.expression)

                    try:
                        calculated_column = self.evaluator.calculate_column_values(
                            result_batch, column_config
                        )

                        # Add the new column to the batch
                        result_batch = self._add_column_to_batch(
                            result_batch, column_config.name, calculated_column
                        )

                        if self.config.add_metadata:
                            validation_results.append(
                                ValidationResult(
                                    is_valid=True,
                                    error_message=(
                                        f"Successfully calculated column '{column_config.name}'"
                                    ),
                                    error_code="CALCULATION_SUCCESS",
                                    column_name=column_config.name,
                                )
                            )

                    except Exception as e:
                        error_msg = f"Failed to calculate column '{column_config.name}': {str(e)}"

                        if self.config.fail_on_error:
                            # Fail closed: never hand back a batch that is missing its columns
                            raise ValueError(error_msg) from None

                        validation_results.append(
                            ValidationResult(
                                is_valid=False,
                                error_message=error_msg,
                                error_code="CALCULATION_ERROR",
                                column_name=column_config.name,
                            )
                        )

                        # Add null column when not failing on error
                        null_column = pa.array([None] * len(batch), type=column_config.data_type)
                        result_batch = self._add_column_to_batch(
                            result_batch, column_config.name, null_column
                        )

            return result_batch, validation_results

        except Exception as e:
            if self.config.fail_on_error:
                raise
            validation_results.append(
                ValidationResult(
                    is_valid=False,
                    error_message=f"Calculated columns processing failed: {str(e)}",
                    error_code="PROCESSOR_ERROR",
                )
            )
            return batch, validation_results

    def _sort_columns_by_dependencies(self) -> List[CalculatedColumn]:
        """Sort columns by their dependencies using topological sort."""
        # Simple topological sort implementation
        sorted_columns = []
        remaining_columns = self.config.columns.copy()

        while remaining_columns:
            # Find columns with no unresolved dependencies
            ready_columns = []
            for col in remaining_columns:
                resolved_deps = {sorted_col.name for sorted_col in sorted_columns}
                if all(
                    dep in resolved_deps or dep not in {c.name for c in self.config.columns}
                    for dep in col.dependencies
                ):
                    ready_columns.append(col)

            if not ready_columns:
                # If no columns are ready, there might be circular dependencies
                # Add remaining columns anyway to avoid infinite loop
                sorted_columns.extend(remaining_columns)
                break

            # Add ready columns to sorted list
            sorted_columns.extend(ready_columns)
            for col in ready_columns:
                remaining_columns.remove(col)

        return sorted_columns

    def _add_column_to_batch(
        self, batch: pa.RecordBatch, column_name: str, column_array: pa.Array
    ) -> pa.RecordBatch:
        """Add a new column to the batch."""
        # Create new schema with the additional field
        new_fields = list(batch.schema)
        new_fields.append(pa.field(column_name, column_array.type))
        new_schema = pa.schema(new_fields)

        # Create new arrays list with the additional column
        new_arrays = [batch.column(i) for i in range(batch.num_columns)]
        new_arrays.append(column_array)

        return pa.RecordBatch.from_arrays(new_arrays, new_schema)

    def get_calculated_columns_info(self) -> Dict[str, Any]:
        """Get information about calculated columns configuration."""
        return {
            "total_columns": len(self.config.columns),
            "column_names": [col.name for col in self.config.columns],
            "has_dependencies": any(col.dependencies for col in self.config.columns),
            "fail_on_error": self.config.fail_on_error,
            "add_metadata": self.config.add_metadata,
            "available_functions": list(self.evaluator._available_functions.keys()),
        }

    def validate_expressions(self, sample_data: Dict[str, Any]) -> List[ValidationResult]:
        """Validate expressions against sample data."""
        validation_results = []

        for column_config in self.config.columns:
            is_valid = column_config.is_constant or self.evaluator.validate_expression(
                column_config.expression, sample_data
            )

            if is_valid:
                validation_results.append(
                    ValidationResult(
                        is_valid=True,
                        error_message=f"Expression for '{column_config.name}' is valid",
                        error_code="EXPRESSION_VALID",
                        column_name=column_config.name,
                    )
                )
            else:
                validation_results.append(
                    ValidationResult(
                        is_valid=False,
                        error_message=f"Invalid expression for '{column_config.name}'",
                        error_code="EXPRESSION_INVALID",
                        column_name=column_config.name,
                    )
                )

        return validation_results
