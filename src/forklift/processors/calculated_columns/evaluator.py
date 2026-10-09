"""Expression evaluation logic for calculated columns.

Expressions are compiled once into a validated AST and interpreted per row by
:mod:`.safe_eval`; nothing is ever passed to ``eval``.
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from .functions import get_available_functions, get_constants
from .limits import MAX_EXPRESSION_LENGTH, MAX_EXPRESSION_NODES, ExpressionError
from .models import CalculatedColumn
from .safe_eval import CompiledExpression, compile_expression


class _RunClock:
    """Source of ``now()``/``today()``: one snapshot per evaluation run (batch)."""

    def __init__(self):
        self.snapshot: Optional[datetime] = None

    def now(self) -> datetime:
        return self.snapshot if self.snapshot is not None else datetime.now()


class ExpressionEvaluator:
    """Handles evaluation of expressions for calculated columns."""

    def __init__(
        self,
        fail_on_error: bool = True,
        max_expression_length: int = MAX_EXPRESSION_LENGTH,
        max_expression_nodes: int = MAX_EXPRESSION_NODES,
    ):
        """Initialize the expression evaluator.

        Args:
            fail_on_error: Whether to fail on evaluation errors
            max_expression_length: Longest accepted expression, in characters
            max_expression_nodes: Largest accepted expression, in AST nodes
        """
        self.fail_on_error = fail_on_error
        self.max_expression_length = max_expression_length
        self.max_expression_nodes = max_expression_nodes
        self._run_clock = _RunClock()
        self._available_functions = get_available_functions(clock=self._run_clock.now)
        self._constants = get_constants()

    # ------------------------------------------------------------------ compilation / runs

    def compile(self, expression: str) -> CompiledExpression:
        """Compile (parse + validate) an expression; cached.

        Raises:
            ValueError: If the expression is unsafe, malformed or too large.
        """
        try:
            return compile_expression(
                expression, self.max_expression_length, self.max_expression_nodes
            )
        except ExpressionError as exc:
            raise ValueError(f"Expression evaluation failed: {exc}") from None

    @contextmanager
    def run(self) -> Iterator[None]:
        """Evaluation run in which ``now()``/``today()`` return a single snapshot.

        Nested runs share the outermost snapshot. Not thread-safe (one run per evaluator at a
        time).
        """
        outermost = self._run_clock.snapshot is None
        if outermost:
            self._run_clock.snapshot = datetime.now()
        try:
            yield
        finally:
            if outermost:
                self._run_clock.snapshot = None

    # ----------------------------------------------------------------------- evaluation

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
        compiled = self.compile(expression)
        variables = {}
        for i, field_name in enumerate(batch.schema.names):
            if field_name in compiled.names:
                self._reject_duplicate(variables, field_name)
                variables[field_name] = batch.column(i)[row_idx].as_py()

        try:
            return compiled.evaluate(variables, self._available_functions, self._constants)
        except ExpressionError as exc:
            raise ValueError(f"Expression evaluation failed: {exc}") from None

    @staticmethod
    def _reject_duplicate(variables: Dict[str, Any], name: str) -> None:
        if name in variables:
            raise ValueError(
                f"Expression evaluation failed: column name '{name}' is ambiguous (duplicated)"
            )

    def validate_expression(self, expression: str, sample_data: Dict[str, Any]) -> bool:
        """Validate an expression against sample data.

        Args:
            expression: Expression to validate
            sample_data: Sample data for validation

        Returns:
            True if expression is valid, False otherwise
        """
        try:
            compiled = self.compile(expression)
            compiled.evaluate(sample_data, self._available_functions, self._constants)
            return True
        except Exception:
            return False

    def calculate_column_values(
        self, batch: pa.RecordBatch, column_config: CalculatedColumn
    ) -> pa.Array:
        """Calculate values for a single column.

        Args:
            batch: PyArrow RecordBatch to process
            column_config: Configuration for the column to calculate

        Returns:
            PyArrow Array with calculated values

        Raises:
            ValueError: If the expression is unsafe/malformed, or (with ``fail_on_error``) if
                it fails for a row, or the results do not fit ``data_type``.
        """
        num_rows = batch.num_rows

        if column_config.is_constant:
            values: List[Any] = [column_config.constant_value] * num_rows
        else:
            compiled = self.compile(column_config.expression)  # config errors always raise
            columns = self._referenced_columns(batch, compiled)
            values = []
            with self.run():
                for row_idx in range(num_rows):
                    variables = {name: col[row_idx] for name, col in columns.items()}
                    try:
                        values.append(
                            compiled.evaluate(
                                variables, self._available_functions, self._constants
                            )
                        )
                    except ExpressionError as exc:
                        if self.fail_on_error:
                            raise ValueError(
                                f"Expression evaluation failed at row {row_idx}: {exc}"
                            ) from None
                        values.append(None)

        return self._to_array(values, column_config)

    @staticmethod
    def _referenced_columns(
        batch: pa.RecordBatch, compiled: CompiledExpression
    ) -> Dict[str, List[Any]]:
        """Python lists for just the columns an expression refers to (converted once)."""
        columns: Dict[str, List[Any]] = {}
        for i, field_name in enumerate(batch.schema.names):
            if field_name in compiled.names:
                if field_name in columns:
                    raise ValueError(
                        f"Expression evaluation failed: column name '{field_name}' "
                        f"is ambiguous (duplicated)"
                    )
                columns[field_name] = batch.column(i).to_pylist()
        return columns

    @staticmethod
    def _to_array(values: List[Any], column_config: CalculatedColumn) -> pa.Array:
        data_type = column_config.data_type
        try:
            try:
                return pa.array(values, type=data_type)
            except (pa.ArrowException, TypeError, ValueError, OverflowError):
                # A constant such as "2024-08-26" for a date32 column: ISO text is read as the
                # temporal type (other conversions from text stay errors)
                if (
                    data_type is not None
                    and (pa.types.is_temporal(data_type) and not pa.types.is_duration(data_type))
                    and all(v is None or isinstance(v, str) for v in values)
                ):
                    text = pa.array(values, type=pa.string())
                    try:
                        return text.cast(data_type)
                    except pa.ArrowInvalid:
                        if not (pa.types.is_timestamp(data_type) and data_type.tz is None):
                            raise
                        # "2024-08-26T10:30:00Z" for a timestamp without time zone: keep the
                        # UTC wall time (as the engine does for timestamp columns)
                        return text.cast(pa.timestamp(data_type.unit, tz="UTC")).cast(data_type)
                raise
        except (pa.ArrowException, TypeError, ValueError, OverflowError):
            # The Arrow message quotes the offending value; report only the target type.
            raise ValueError(
                f"Expression result for column '{column_config.name}' "
                f"cannot be converted to {column_config.data_type}"
            ) from None
