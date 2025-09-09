"""Expression evaluation logic for calculated columns."""

from typing import Any, Dict
import re
import pyarrow as pa

from .functions import get_available_functions, get_constants
from .models import CalculatedColumn


class ExpressionEvaluator:
    """Handles evaluation of expressions for calculated columns."""

    def __init__(self, fail_on_error: bool = True):
        """Initialize the expression evaluator.

        Args:
            fail_on_error: Whether to fail on evaluation errors
        """
        self.fail_on_error = fail_on_error
        self._available_functions = get_available_functions()
        self._constants = get_constants()

    def evaluate_expression(self, batch: pa.RecordBatch, row_idx: int, expression: str) -> Any:
        """Evaluate an expression for a specific row.

        Args:
            batch: PyArrow RecordBatch containing the data
            row_idx: Index of the row to evaluate
            expression: Expression string to evaluate

        Returns:
            Evaluated result

        Raises:
            ValueError: If expression evaluation fails
        """
        # Create context with column values and functions
        context = {}

        # Add column values to context
        for i, field_name in enumerate(batch.schema.names):
            context[field_name] = batch.column(i)[row_idx].as_py()

        # Add available functions to context
        context.update(self._available_functions)

        # Add common constants
        context.update(self._constants)

        try:
            # Handle simple arithmetic operations with null values
            if self._has_arithmetic_operations(expression):
                if self._has_null_variables_in_arithmetic(expression, context):
                    return None

            # Evaluate the expression
            result = eval(expression, {"__builtins__": {}}, context)
            return result
        except Exception as e:
            raise ValueError(f"Expression evaluation failed: {str(e)}")

    def _has_arithmetic_operations(self, expression: str) -> bool:
        """Check if expression contains basic arithmetic operations."""
        return any(op in expression for op in [' + ', ' - ', ' * ', ' / ', ' % ', ' ** '])

    def _has_null_variables_in_arithmetic(self, expression: str, context: Dict[str, Any]) -> bool:
        """Check if expression has null variables in arithmetic operations."""
        # Extract variable names from expression
        var_pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
        variables = re.findall(var_pattern, expression)

        # Check if any variables are None and not function names
        null_vars = []
        for var in variables:
            if (var in context and not callable(context.get(var)) and
                context[var] is None and var not in self._available_functions and
                var not in self._constants):
                null_vars.append(var)

        # If we have null variables in a simple arithmetic expression, return None
        return bool(null_vars and not any(func in expression for func in self._available_functions.keys()))

    def validate_expression(self, expression: str, sample_data: Dict[str, Any]) -> bool:
        """Validate an expression against sample data.

        Args:
            expression: Expression to validate
            sample_data: Sample data for validation

        Returns:
            True if expression is valid, False otherwise
        """
        try:
            # Create a mock context with sample data
            context = sample_data.copy()
            context.update(self._available_functions)
            context.update(self._constants)

            # Try to evaluate the expression
            eval(expression, {"__builtins__": {}}, context)
            return True
        except Exception:
            return False

    def calculate_column_values(self, batch: pa.RecordBatch, column_config: CalculatedColumn) -> pa.Array:
        """Calculate values for a single column.

        Args:
            batch: PyArrow RecordBatch to process
            column_config: Configuration for the column to calculate

        Returns:
            PyArrow Array with calculated values
        """
        values = []

        for row_idx in range(len(batch)):
            try:
                value = self.evaluate_expression(batch, row_idx, column_config.expression)
                values.append(value)
            except Exception as e:
                if self.fail_on_error:
                    raise e
                values.append(None)

        return pa.array(values, type=column_config.data_type)
