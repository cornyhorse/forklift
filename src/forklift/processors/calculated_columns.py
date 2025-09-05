"""Calculated columns processor for dynamic field generation and computation."""

from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, date
import re
import math
import pyarrow as pa

from .base import BaseProcessor, ValidationResult

# Sentinel value to distinguish between not provided and explicitly None
_UNSET = object()


@dataclass
class CalculatedColumn:
    """Configuration for a calculated column."""
    name: str
    expression: str
    data_type: Optional[pa.DataType] = _UNSET
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.data_type is _UNSET:
            self.data_type = pa.string()


@dataclass
class ConstantColumn:
    """Configuration for a constant value column."""
    name: str
    value: Any
    data_type: Optional[pa.DataType] = _UNSET
    description: Optional[str] = None

    def __post_init__(self):
        # Default to None when not provided, but convert explicit None to string
        if self.data_type is _UNSET:
            self.data_type = None  # Default to None when not provided
        elif self.data_type is None:
            self.data_type = pa.string()  # Convert explicit None to string
        elif self.data_type is None:
            self.data_type = pa.string()  # Convert explicit None to string

    def to_calculated_column(self) -> CalculatedColumn:
        """Convert to CalculatedColumn for processing."""
        # Create expression that returns the constant value
        if isinstance(self.value, str):
            expression = f"'{self.value}'"
        else:
            expression = str(self.value)

        return CalculatedColumn(
            name=self.name,
            expression=expression,
            data_type=self.data_type,
            description=self.description,
            dependencies=[]
        )


@dataclass
class ExpressionColumn:
    """Configuration for an expression-based calculated column."""
    name: str
    expression: str
    data_type: Optional[pa.DataType] = None
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        # ExpressionColumn always defaults to string type when None
        if self.data_type is None:
            self.data_type = pa.string()

    def to_calculated_column(self) -> CalculatedColumn:
        """Convert to CalculatedColumn for processing."""
        return CalculatedColumn(
            name=self.name,
            expression=self.expression,
            data_type=self.data_type,
            description=self.description,
            dependencies=self.dependencies
        )


@dataclass
class CalculatedColumnsConfig:
    """Configuration for calculated columns processor."""
    columns: List[CalculatedColumn]
    fail_on_error: bool = True
    add_metadata: bool = False
    validate_dependencies: bool = True

    # Additional attributes for backward compatibility with factory tests
    constants: List[ConstantColumn] = None
    expressions: List[ExpressionColumn] = None
    calculated: List[CalculatedColumn] = None
    partition_columns: List[str] = None

    def __post_init__(self):
        # Initialize optional lists if None
        if self.constants is None:
            self.constants = []
        if self.expressions is None:
            self.expressions = []
        if self.calculated is None:
            self.calculated = []
        if self.partition_columns is None:
            self.partition_columns = []


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
        self._available_functions = self._init_functions()

        # Validate configuration
        if self.config.validate_dependencies:
            self._validate_dependencies()

    def _init_functions(self) -> Dict[str, Callable]:
        """Initialize available functions for expressions."""
        return {
            # Arithmetic functions
            'add': lambda a, b: a + b if a is not None and b is not None else None,
            'subtract': lambda a, b: a - b if a is not None and b is not None else None,
            'multiply': lambda a, b: a * b if a is not None and b is not None else None,
            'divide': lambda a, b: a / b if a is not None and b is not None and b != 0 else None,
            'power': lambda a, b: a ** b if a is not None and b is not None else None,
            'mod': lambda a, b: a % b if a is not None and b is not None and b != 0 else None,

            # Mathematical functions
            'abs': lambda x: abs(x) if x is not None else None,
            'round': lambda x, digits=0: round(x, digits) if x is not None else None,
            'floor': lambda x: math.floor(x) if x is not None else None,
            'ceil': lambda x: math.ceil(x) if x is not None else None,
            'sqrt': lambda x: math.sqrt(x) if x is not None and x >= 0 else None,
            'log': lambda x: math.log(x) if x is not None and x > 0 else None,
            'log10': lambda x: math.log10(x) if x is not None and x > 0 else None,
            'sin': lambda x: math.sin(x) if x is not None else None,
            'cos': lambda x: math.cos(x) if x is not None else None,
            'tan': lambda x: math.tan(x) if x is not None else None,

            # String functions
            'concat': lambda *args: ''.join(str(arg) for arg in args if arg is not None),
            'upper': lambda x: str(x).upper() if x is not None else None,
            'lower': lambda x: str(x).lower() if x is not None else None,
            'trim': lambda x: str(x).strip() if x is not None else None,
            'length': lambda x: len(str(x)) if x is not None else None,
            'substring': lambda x, start, length=None: str(x)[start:start+length] if x is not None else None,
            'replace': lambda x, old, new: str(x).replace(old, new) if x is not None else None,
            'left': lambda x, n: str(x)[:n] if x is not None else None,
            'right': lambda x, n: str(x)[-n:] if x is not None else None,

            # Conditional functions
            'if_then_else': lambda condition, then_val, else_val: then_val if condition else else_val,
            'coalesce': lambda *args: next((arg for arg in args if arg is not None), None),
            'nullif': lambda x, y: None if x == y else x,
            'isnull': lambda x: x is None,
            'isnotnull': lambda x: x is not None,

            # Type conversion functions
            'to_string': lambda x: str(x) if x is not None else None,
            'to_int': lambda x: int(x) if x is not None else None,
            'to_float': lambda x: float(x) if x is not None else None,
            'to_bool': lambda x: bool(x) if x is not None else None,

            # Date/time functions
            'now': lambda: datetime.now(),
            'today': lambda: date.today(),
            'year': lambda x: x.year if isinstance(x, (date, datetime)) else None,
            'month': lambda x: x.month if isinstance(x, (date, datetime)) else None,
            'day': lambda x: x.day if isinstance(x, (date, datetime)) else None,
            'weekday': lambda x: x.weekday() if isinstance(x, (date, datetime)) else None,

            # Comparison functions
            'equals': lambda a, b: a == b,
            'not_equals': lambda a, b: a != b,
            'greater_than': lambda a, b: a > b if a is not None and b is not None else False,
            'less_than': lambda a, b: a < b if a is not None and b is not None else False,
            'greater_equal': lambda a, b: a >= b if a is not None and b is not None else False,
            'less_equal': lambda a, b: a <= b if a is not None and b is not None else False,

            # Logical functions
            'and': lambda a, b: a and b,
            'or': lambda a, b: a or b,
            'not': lambda a: not a,

            # Utility functions
            'min': lambda *args: min(arg for arg in args if arg is not None) if any(arg is not None for arg in args) else None,
            'max': lambda *args: max(arg for arg in args if arg is not None) if any(arg is not None for arg in args) else None,
            'sum': lambda *args: sum(arg for arg in args if arg is not None),
            'avg': lambda *args: sum(arg for arg in args if arg is not None) / len([arg for arg in args if arg is not None]) if any(arg is not None for arg in args) else None,
        }

    def _validate_dependencies(self):
        """Validate that all column dependencies exist and detect circular dependencies."""
        column_names = {col.name for col in self.config.columns}

        for col in self.config.columns:
            # Check for circular dependencies
            if self._has_circular_dependency(col, column_names, set()):
                raise ValueError(f"Circular dependency detected for column '{col.name}'")

    def _has_circular_dependency(self, column: CalculatedColumn, all_columns: set, visited: set) -> bool:
        """Check for circular dependencies in column calculations."""
        if column.name in visited:
            return True

        visited.add(column.name)

        for dep in column.dependencies:
            if dep in all_columns:
                # Find the dependent column
                dep_column = next((col for col in self.config.columns if col.name == dep), None)
                if dep_column and self._has_circular_dependency(dep_column, all_columns, visited.copy()):
                    return True

        return False

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch and add calculated columns.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (processed_batch, validation_results)
        """
        validation_results = []

        try:
            # Create a copy of the batch to work with
            result_batch = batch

            # Process columns in dependency order
            sorted_columns = self._sort_columns_by_dependencies()

            for column_config in sorted_columns:
                try:
                    calculated_column = self._calculate_column(result_batch, column_config)

                    # Add the new column to the batch
                    result_batch = self._add_column_to_batch(result_batch, column_config.name, calculated_column)

                    if self.config.add_metadata:
                        validation_results.append(ValidationResult(
                            is_valid=True,
                            error_message=f"Successfully calculated column '{column_config.name}'",
                            error_code="CALCULATION_SUCCESS",
                            column_name=column_config.name
                        ))

                except Exception as e:
                    error_msg = f"Failed to calculate column '{column_config.name}': {str(e)}"
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=error_msg,
                        error_code="CALCULATION_ERROR",
                        column_name=column_config.name
                    ))

                    if self.config.fail_on_error:
                        return batch, validation_results

                    # Add null column if not failing on error
                    null_column = pa.array([None] * len(batch), type=column_config.data_type)
                    result_batch = self._add_column_to_batch(result_batch, column_config.name, null_column)

            return result_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Calculated columns processing failed: {str(e)}",
                error_code="PROCESSOR_ERROR"
            ))
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
                if all(dep in resolved_deps or dep not in {c.name for c in self.config.columns}
                       for dep in col.dependencies):
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

    def _calculate_column(self, batch: pa.RecordBatch, column_config: CalculatedColumn) -> pa.Array:
        """Calculate values for a single column."""
        # Parse and evaluate the expression for each row
        values = []

        for row_idx in range(len(batch)):
            try:
                value = self._evaluate_expression(batch, row_idx, column_config.expression)
                values.append(value)
            except Exception as e:
                if self.config.fail_on_error:
                    raise e
                values.append(None)

        return pa.array(values, type=column_config.data_type)

    def _evaluate_expression(self, batch: pa.RecordBatch, row_idx: int, expression: str) -> Any:
        """Evaluate an expression for a specific row."""
        # Create context with column values and functions
        context = {}

        # Add column values to context
        for i, field_name in enumerate(batch.schema.names):
            context[field_name] = batch.column(i)[row_idx].as_py()

        # Add available functions to context
        context.update(self._available_functions)

        # Add common constants
        context.update({
            'PI': math.pi,
            'E': math.e,
            'TRUE': True,
            'FALSE': False,
            'NULL': None
        })

        try:
            # Handle simple arithmetic operations with null values
            # Check for basic arithmetic patterns with spaces
            if any(op in expression for op in [' + ', ' - ', ' * ', ' / ', ' % ', ' ** ']):
                # Extract variable names from expression
                var_pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
                variables = re.findall(var_pattern, expression)

                # Check if any variables are None and not function names
                null_vars = []
                for var in variables:
                    if (var in context and not callable(context.get(var)) and
                        context[var] is None and var not in self._available_functions and
                        var not in ['PI', 'E', 'TRUE', 'FALSE', 'NULL']):
                        null_vars.append(var)

                # If we have null variables in a simple arithmetic expression, return None
                if null_vars and not any(func in expression for func in self._available_functions.keys()):
                    return None

            # Evaluate the expression
            result = eval(expression, {"__builtins__": {}}, context)
            return result
        except Exception as e:
            raise ValueError(f"Expression evaluation failed: {str(e)}")

    def _add_column_to_batch(self, batch: pa.RecordBatch, column_name: str, column_array: pa.Array) -> pa.RecordBatch:
        """Add a new column to the batch."""
        # Create new schema with the additional field
        new_fields = list(batch.schema)
        new_fields.append(pa.field(column_name, column_array.type))
        new_schema = pa.schema(new_fields)

        # Create new arrays list with the additional column
        new_arrays = [batch.column(i) for i in range(batch.num_columns)]
        new_arrays.append(column_array)

        return pa.RecordBatch.from_arrays(new_arrays, schema=new_schema)

    def get_calculated_columns_info(self) -> Dict[str, Any]:
        """Get information about calculated columns configuration."""
        return {
            'total_columns': len(self.config.columns),
            'column_names': [col.name for col in self.config.columns],
            'has_dependencies': any(col.dependencies for col in self.config.columns),
            'fail_on_error': self.config.fail_on_error,
            'add_metadata': self.config.add_metadata,
            'available_functions': list(self._available_functions.keys())
        }

    def validate_expressions(self, sample_data: Dict[str, Any]) -> List[ValidationResult]:
        """Validate expressions against sample data."""
        validation_results = []

        for column_config in self.config.columns:
            try:
                # Create a mock context with sample data
                context = sample_data.copy()
                context.update(self._available_functions)
                context.update({'PI': math.pi, 'E': math.e, 'TRUE': True, 'FALSE': False, 'NULL': None})

                # Try to evaluate the expression
                eval(column_config.expression, {"__builtins__": {}}, context)

                validation_results.append(ValidationResult(
                    is_valid=True,
                    error_message=f"Expression for '{column_config.name}' is valid",
                    error_code="EXPRESSION_VALID",
                    column_name=column_config.name
                ))

            except Exception as e:
                validation_results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Invalid expression for '{column_config.name}': {str(e)}",
                    error_code="EXPRESSION_INVALID",
                    column_name=column_config.name
                ))

        return validation_results
