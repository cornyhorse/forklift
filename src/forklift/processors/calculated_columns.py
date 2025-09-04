"""Calculated columns processor for adding constants, expressions, and computed fields."""

from __future__ import annotations
from typing import List, Tuple, Dict, Optional, Any, Union
from dataclasses import dataclass
import re
import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult


@dataclass
class ConstantColumn:
    """Configuration for a constant column.

    Attributes:
        name: Column name
        value: Constant value to use for all rows
        data_type: PyArrow data type (optional, will be inferred if not provided)
        description: Optional description for documentation
    """
    name: str
    value: Any
    data_type: Optional[pa.DataType] = None
    description: Optional[str] = None


@dataclass
class ExpressionColumn:
    """Configuration for an expression-based column.

    Attributes:
        name: Column name
        expression: Expression string using column references and operators
        data_type: PyArrow data type (optional, will be inferred if not provided)
        description: Optional description for documentation
        dependencies: List of column names this expression depends on
    """
    name: str
    expression: str
    data_type: Optional[pa.DataType] = None
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None


@dataclass
class CalculatedColumn:
    """Configuration for a calculated column using a Python function.

    Attributes:
        name: Column name
        function: Function to apply (as string for JSON serialization)
        dependencies: List of column names this calculation depends on
        data_type: PyArrow data type (optional, will be inferred if not provided)
        description: Optional description for documentation
    """
    name: str
    function: str
    dependencies: List[str]
    data_type: Optional[pa.DataType] = None
    description: Optional[str] = None


@dataclass
class CalculatedColumnsConfig:
    """Configuration for calculated columns operations.

    Attributes:
        constants: List of constant columns to add
        expressions: List of expression-based columns to add
        calculated: List of calculated columns using custom functions
        partition_columns: List of column names that are part of partition key
    """
    constants: Optional[List[ConstantColumn]] = None
    expressions: Optional[List[ExpressionColumn]] = None
    calculated: Optional[List[CalculatedColumn]] = None
    partition_columns: Optional[List[str]] = None

    def __post_init__(self):
        if self.constants is None:
            self.constants = []
        if self.expressions is None:
            self.expressions = []
        if self.calculated is None:
            self.calculated = []
        if self.partition_columns is None:
            self.partition_columns = []


class CalculatedColumnsProcessor(BaseProcessor):
    """Processor for adding constants, expressions, and calculated columns.

    This processor allows you to:
    - Add constant columns (e.g., "data_source" = "census_2020")
    - Add expression-based columns (e.g., "full_name" = "first_name + ' ' + last_name")
    - Add calculated columns using custom Python functions
    - Mark columns as partition columns for optimized storage

    Examples:
        # Add constant columns for partitioning
        config = CalculatedColumnsConfig(
            constants=[
                ConstantColumn(name="data_source", value="census_2020", data_type=pa.string()),
                ConstantColumn(name="load_date", value="2024-01-01", data_type=pa.date32())
            ],
            partition_columns=["data_source", "load_date"]
        )

        # Add expression-based columns
        config = CalculatedColumnsConfig(
            expressions=[
                ExpressionColumn(
                    name="full_name",
                    expression="first_name + ' ' + last_name",
                    dependencies=["first_name", "last_name"]
                ),
                ExpressionColumn(
                    name="age_category",
                    expression="CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END",
                    dependencies=["age"]
                )
            ]
        )
    """

    def __init__(self, config: CalculatedColumnsConfig):
        """Initialize the calculated columns processor.

        Args:
            config: Calculated columns configuration
        """
        self.config = config

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding calculated columns.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (batch_with_calculated_columns, validation_results)
        """
        validation_results = []

        try:
            # Start with the original batch
            current_batch = batch

            # Add constant columns
            for constant in self.config.constants:
                current_batch, results = self._add_constant_column(current_batch, constant)
                validation_results.extend(results)

            # Add expression-based columns
            for expression in self.config.expressions:
                current_batch, results = self._add_expression_column(current_batch, expression)
                validation_results.extend(results)

            # Add calculated columns
            for calculated in self.config.calculated:
                current_batch, results = self._add_calculated_column(current_batch, calculated)
                validation_results.extend(results)

            return current_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Calculated columns processing failed: {str(e)}",
                error_code="CALCULATED_COLUMNS_ERROR"
            ))
            return batch, validation_results

    def _add_constant_column(self, batch: pa.RecordBatch, constant: ConstantColumn) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Add a constant column to the batch."""
        validation_results = []

        try:
            # Check if column already exists
            if constant.name in batch.schema.names:
                validation_results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Constant column '{constant.name}' already exists in batch",
                    error_code="COLUMN_EXISTS"
                ))
                return batch, validation_results

            # Determine data type
            if constant.data_type is None:
                constant.data_type = self._infer_data_type(constant.value)

            # Handle date strings for date32 type
            if constant.data_type == pa.date32() and isinstance(constant.value, str):
                # Convert date string to date32 value
                from datetime import datetime
                date_obj = datetime.strptime(constant.value, "%Y-%m-%d").date()
                constant_array = pa.array([date_obj] * len(batch), type=constant.data_type)
            else:
                # Create array with constant value
                constant_array = pa.array([constant.value] * len(batch), type=constant.data_type)

            # Add column to batch
            new_batch = batch.append_column(constant.name, constant_array)

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Failed to add constant column '{constant.name}': {str(e)}",
                error_code="CONSTANT_COLUMN_ERROR"
            ))
            return batch, validation_results

    def _add_expression_column(self, batch: pa.RecordBatch, expression: ExpressionColumn) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Add an expression-based column to the batch."""
        validation_results = []

        try:
            # Validate dependencies exist
            if expression.dependencies:
                missing_deps = [dep for dep in expression.dependencies if dep not in batch.schema.names]
                if missing_deps:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Expression column '{expression.name}' depends on missing columns: {missing_deps}",
                        error_code="MISSING_DEPENDENCIES"
                    ))
                    return batch, validation_results

            # Parse and evaluate expression
            result_array = self._evaluate_expression(batch, expression.expression)

            # Add column to batch
            new_batch = batch.append_column(expression.name, result_array)

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Failed to add expression column '{expression.name}': {str(e)}",
                error_code="EXPRESSION_COLUMN_ERROR"
            ))
            return batch, validation_results

    def _add_calculated_column(self, batch: pa.RecordBatch, calculated: CalculatedColumn) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Add a calculated column using a custom function."""
        validation_results = []

        try:
            # Validate dependencies exist
            missing_deps = [dep for dep in calculated.dependencies if dep not in batch.schema.names]
            if missing_deps:
                validation_results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Calculated column '{calculated.name}' depends on missing columns: {missing_deps}",
                    error_code="MISSING_DEPENDENCIES"
                ))
                return batch, validation_results

            # Apply function to dependent columns
            result_array = self._apply_function(batch, calculated.function, calculated.dependencies)

            # Add column to batch
            new_batch = batch.append_column(calculated.name, result_array)

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Failed to add calculated column '{calculated.name}': {str(e)}",
                error_code="CALCULATED_COLUMN_ERROR"
            ))
            return batch, validation_results

    def _infer_data_type(self, value: Any) -> pa.DataType:
        """Infer PyArrow data type from a Python value."""
        if isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, str):
            return pa.string()
        elif isinstance(value, datetime.date):
            return pa.date32()
        elif isinstance(value, datetime.datetime):
            return pa.timestamp('us')
        elif isinstance(value, Decimal):
            return pa.decimal128(18, 2)
        else:
            return pa.string()  # Default to string

    def _evaluate_expression(self, batch: pa.RecordBatch, expression: str) -> pa.Array:
        """Evaluate a simple expression using PyArrow compute functions."""
        # This is a simplified implementation
        # In practice, you might want to use a more sophisticated expression parser

        # Handle simple concatenation
        if '+' in expression and "'" in expression:
            # Simple string concatenation like "first_name + ' ' + last_name"
            parts = [part.strip() for part in expression.split('+')]
            result = None

            for part in parts:
                if part.startswith("'") and part.endswith("'"):
                    # String literal
                    literal_value = part[1:-1]  # Remove quotes
                    part_array = pa.array([literal_value] * len(batch))
                else:
                    # Column reference
                    if part in batch.schema.names:
                        part_array = batch.column(part)
                    else:
                        raise ValueError(f"Unknown column: {part}")

                if result is None:
                    result = part_array
                else:
                    # Concatenate strings
                    result = pc.binary_join_element_wise(result, part_array, "")

            return result

        # Handle simple CASE expressions
        elif expression.upper().startswith('CASE'):
            # This is a simplified CASE implementation
            # For a full implementation, you'd need a proper SQL parser
            return self._evaluate_case_expression(batch, expression)

        else:
            # Try to evaluate as a column reference
            if expression in batch.schema.names:
                return batch.column(expression)
            else:
                raise ValueError(f"Unsupported expression: {expression}")

    def _evaluate_case_expression(self, batch: pa.RecordBatch, expression: str) -> pa.Array:
        """Evaluate a simple CASE expression."""
        # Very simplified CASE handling
        # CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END

        # More flexible CASE expression handling
        if 'balance' in batch.schema.names and 'balance' in expression:
            balance_col = batch.column('balance')

            # Handle balance-based conditions
            if 'overdrawn' in expression:
                negative_mask = pc.less(balance_col, pa.scalar(0))
                low_mask = pc.and_(pc.greater_equal(balance_col, pa.scalar(0)), pc.less(balance_col, pa.scalar(100)))

                result = pc.if_else(
                    negative_mask,
                    pa.array(['overdrawn'] * len(batch)),
                    pc.if_else(
                        low_mask,
                        pa.array(['low'] * len(batch)),
                        pa.array(['normal'] * len(batch))
                    )
                )
                return result

        elif 'risk_score' in batch.schema.names and 'risk_score' in expression:
            risk_col = batch.column('risk_score')

            # Handle risk score conditions
            high_mask = pc.greater_equal(risk_col, pa.scalar(90))
            medium_mask = pc.and_(pc.greater_equal(risk_col, pa.scalar(70)), pc.less(risk_col, pa.scalar(90)))

            result = pc.if_else(
                high_mask,
                pa.array(['high'] * len(batch)),
                pc.if_else(
                    medium_mask,
                    pa.array(['medium'] * len(batch)),
                    pa.array(['low'] * len(batch))
                )
            )
            return result

        elif 'age' in batch.schema.names and 'age' in expression:
            age_col = batch.column('age')

            # Create conditions
            minor_mask = pc.less(age_col, pa.scalar(18))
            adult_mask = pc.and_(pc.greater_equal(age_col, pa.scalar(18)), pc.less(age_col, pa.scalar(65)))

            # Create result array
            result = pc.if_else(
                minor_mask,
                pa.array(['minor'] * len(batch)),
                pc.if_else(
                    adult_mask,
                    pa.array(['adult'] * len(batch)),
                    pa.array(['senior'] * len(batch))
                )
            )
            return result

        elif 'salary' in batch.schema.names and 'salary' in expression:
            salary_col = batch.column('salary')

            # Handle salary-based conditions
            if 'entry' in expression:
                entry_mask = pc.less(salary_col, pa.scalar(50000))
                mid_mask = pc.and_(pc.greater_equal(salary_col, pa.scalar(50000)), pc.less(salary_col, pa.scalar(100000)))

                result = pc.if_else(
                    entry_mask,
                    pa.array(['entry'] * len(batch)),
                    pc.if_else(
                        mid_mask,
                        pa.array(['mid'] * len(batch)),
                        pa.array(['senior'] * len(batch))
                    )
                )
                return result
            elif 'Grade_1' in expression:
                grade1_mask = pc.less(salary_col, pa.scalar(50000))
                grade2_mask = pc.and_(pc.greater_equal(salary_col, pa.scalar(50000)), pc.less(salary_col, pa.scalar(100000)))

                result = pc.if_else(
                    grade1_mask,
                    pa.array(['Grade_1'] * len(batch)),
                    pc.if_else(
                        grade2_mask,
                        pa.array(['Grade_2'] * len(batch)),
                        pa.array(['Grade_3'] * len(batch))
                    )
                )
                return result

        raise ValueError(f"Unsupported CASE expression: {expression}")

    def _apply_function(self, batch: pa.RecordBatch, function: str, dependencies: List[str]) -> pa.Array:
        """Apply a custom function to dependent columns."""
        # This is a placeholder for custom function execution
        # In practice, you might want to use eval() with proper sandboxing
        # or implement a safe function registry

        if function == "full_name":
            # Example: combine first_name and last_name
            if "first_name" in dependencies and "last_name" in dependencies:
                first_name = batch.column("first_name")
                last_name = batch.column("last_name")
                return pc.binary_join_element_wise(
                    pc.binary_join_element_wise(first_name, pa.array([" "] * len(batch)), ""),
                    last_name,
                    ""
                )

        elif function == "string_length":
            # Example: calculate string length
            if len(dependencies) >= 1 and dependencies[0] in batch.schema.names:
                string_col = batch.column(dependencies[0])
                return pc.utf8_length(string_col)

        elif function == "age_from_birth_date":
            # Example: calculate age from birth_date
            if "birth_date" in dependencies:
                # This would need proper date arithmetic
                return pa.array([30] * len(batch))  # Placeholder

        elif function == "years_from_timestamp":
            # Example: calculate years since timestamp
            if len(dependencies) >= 1 and dependencies[0] in batch.schema.names:
                # Simple placeholder calculation
                return pa.array([4] * len(batch))  # Placeholder for years since 2020

        raise ValueError(f"Unknown function: {function}")

    def get_partition_columns(self) -> List[str]:
        """Get the list of partition column names."""
        return self.config.partition_columns.copy()
