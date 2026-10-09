"""Constraint validation classes for data quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

from ._columns import column_index
from ._regex import compile_pattern
from .base import BaseProcessor, ValidationResult
from .schema_validator.constraints import ConstraintValidator as ValueConstraintValidator

#: Default for ``ConstraintConfig.max_retained_violations``.
DEFAULT_MAX_RETAINED_VIOLATIONS = 1000


class ErrorMode(Enum):
    """Error handling modes for constraint validation."""

    FAIL_FAST = "fail_fast"
    FAIL_COMPLETE = "fail_complete"
    BAD_ROWS = "bad_rows"


def coerce_error_mode(value: Any) -> ErrorMode:
    """``ErrorMode`` from an ``ErrorMode`` or its string value (case-insensitive).

    Raises:
        ValueError: For anything else, naming the valid values.
    """
    if isinstance(value, ErrorMode):
        return value
    if isinstance(value, str):
        try:
            return ErrorMode(value.strip().lower())
        except ValueError:
            pass
    shown = repr(value) if isinstance(value, str) else type(value).__name__
    raise ValueError(f"Invalid error mode {shown}; expected one of {_VALID_ERROR_MODES}")


_VALID_ERROR_MODES = " | ".join(
    mode.value for mode in (ErrorMode.BAD_ROWS, ErrorMode.FAIL_FAST, ErrorMode.FAIL_COMPLETE)
)


@dataclass
class ConstraintConfig:
    """Configuration for constraint validation.

    ``check_constraints`` maps a constraint name to a dict naming a ``column`` plus any of
    ``min``/``max``, ``enum``, ``pattern`` (unanchored search), ``minLength``/``maxLength`` and
    ``nullable`` (``False`` forbids NULL). ``unique_constraints`` lists column names (or tuples
    of names for composite keys) that must be unique; NULL keys are not compared.

    ``error_mode`` accepts an ``ErrorMode`` or its string value (``"bad_rows"``, ``"fail_fast"``,
    ``"fail_complete"``, case-insensitive) and is stored as an ``ErrorMode``; anything else raises
    ``ValueError``.

    Memory: the validator keeps an exact running total of violations but retains at most
    ``max_retained_violations`` of them (the first ones; ``None`` = no limit), and does not keep
    the offending cell values in them unless ``include_values`` is true (they may be personal
    data). The set of unique keys seen so far is inherently proportional to the number of
    distinct keys.
    """

    error_mode: ErrorMode = ErrorMode.BAD_ROWS
    check_constraints: Dict[str, Any] = None
    unique_constraints: List[str] = None
    foreign_key_constraints: Dict[str, Any] = None
    max_retained_violations: Optional[int] = DEFAULT_MAX_RETAINED_VIOLATIONS
    include_values: bool = False

    def __post_init__(self):
        self.error_mode = coerce_error_mode(self.error_mode)
        limit = self.max_retained_violations
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
        ):
            raise ValueError("max_retained_violations must be a non-negative integer or None")
        if not isinstance(self.include_values, bool):
            raise ValueError("include_values must be a boolean")
        if self.check_constraints is None:
            self.check_constraints = {}
        if self.unique_constraints is None:
            self.unique_constraints = []
        if self.foreign_key_constraints is None:
            self.foreign_key_constraints = {}


@dataclass
class ConstraintViolation:
    """Represents a constraint violation found during data validation.

    ``row_index`` is the position of the row in the batch that was validated. ``values`` holds
    the offending cell value(s) only when ``ConstraintConfig.include_values`` is true (else it is
    an empty list).
    """

    violation_type: str
    error_message: str
    columns: List[str]
    values: List[Any]
    constraint_name: str
    row_index: Optional[int] = None


# error codes of the shared value-constraint helpers -> violation type
_HELPER_VIOLATION_TYPES = {
    "MIN_VALUE_VIOLATION": "range",
    "MAX_VALUE_VIOLATION": "range",
    "ENUM_VIOLATION": "enum",
    "PATTERN_VIOLATION": "pattern",
    "MIN_LENGTH_VIOLATION": "length",
    "MAX_LENGTH_VIOLATION": "length",
}
_CHECK_KEYS = ("min", "max", "enum", "pattern", "minLength", "maxLength", "nullable")


class ConstraintValidator(BaseProcessor):
    """Validates data against constraints defined in the schema.

    Per error mode:
    - ``bad_rows``: violating rows are removed from the returned batch and reported;
    - ``fail_complete``: every row is kept, all violations are collected and ``finalize()``
      raises ``ValueError`` if there were any;
    - ``fail_fast``: the first violation raises ``ValueError`` from ``process_batch``.

    ``violation_count`` is the exact number of violations found so far. ``violations``
    accumulates across batches but retains at most ``config.max_retained_violations`` of them (the
    first ones; ``violations_truncated`` tells whether some were not kept), whereas
    ``batch_violations`` holds *all* of the last batch's. Rows are removed in ``bad_rows`` mode and
    ``finalize()`` fails in ``fail_complete`` mode regardless of the retention limit.
    A row only registers its unique keys when it violates nothing, so a row rejected for another
    reason never makes a later valid row look like a duplicate.
    """

    def __init__(self, config: ConstraintConfig):
        """Initialize the constraint validator.

        Args:
            config: Constraint configuration
        """
        self.config = config
        self.violations: List[ConstraintViolation] = []
        self.batch_violations: List[ConstraintViolation] = []
        self.violation_count = 0
        self.rows_seen = 0
        self._seen_keys: Dict[str, Set[Any]] = {}

        # Patterns are compiled when the validator is created
        for spec in config.check_constraints.values():
            if isinstance(spec, dict) and spec.get("pattern") is not None:
                compile_pattern(spec["pattern"], bool(spec.get("allow_unsafe_regex", False)))

    # ------------------------------------------------------------------------ processing

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch and validate against constraints.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (valid_batch, validation_results). In ``bad_rows`` mode ``valid_batch``
            holds only the rows without violations.

        Raises:
            ValueError: In ``fail_fast`` mode, on the first violation.
        """
        found: List[ConstraintViolation] = []
        self._check_constraints(batch, found)
        self._unique_constraints(batch, found)

        self.batch_violations = found
        self.violation_count += len(found)
        limit = self.config.max_retained_violations
        room = len(found) if limit is None else max(0, limit - len(self.violations))
        self.violations.extend(found[:room])
        self.rows_seen += batch.num_rows

        validation_results = [
            ValidationResult(
                is_valid=False,
                error_message=violation.error_message,
                error_code=f"{violation.violation_type.upper()}_VIOLATION",
                row_index=violation.row_index,
                column_name=violation.columns[0] if violation.columns else None,
            )
            for violation in found
        ]

        if found and self.config.error_mode == ErrorMode.FAIL_FAST:
            first = found[0]
            raise ValueError(
                f"Constraint '{first.constraint_name}' violated "
                f"(row {self.rows_seen - batch.num_rows + (first.row_index or 0)}); "
                f"{len(found)} violation(s) in the batch"
            )

        if found and self.config.error_mode == ErrorMode.BAD_ROWS:
            bad_rows = {v.row_index for v in found if v.row_index is not None}
            keep = [i for i in range(batch.num_rows) if i not in bad_rows]
            batch = batch.take(pa.array(keep, type=pa.int64()))

        return batch, validation_results

    def _check_constraints(self, batch: pa.RecordBatch, found: List[ConstraintViolation]) -> None:
        for name, spec in self.config.check_constraints.items():
            if not isinstance(spec, dict) or not any(key in spec for key in _CHECK_KEYS):
                continue
            col_name = spec.get("column")
            idx = column_index(batch.schema, col_name) if col_name else None
            if idx is None:
                continue
            column = batch.column(idx)

            helper_results: List[ValidationResult] = []
            if spec.get("min") is not None or spec.get("max") is not None:
                helper_results += ValueConstraintValidator.validate_range_constraints(
                    column, col_name, spec
                )
            if spec.get("enum") is not None:
                helper_results += ValueConstraintValidator.validate_enum_constraints(
                    column, col_name, spec["enum"]
                )
            if spec.get("pattern") is not None:
                helper_results += ValueConstraintValidator.validate_pattern_constraints(
                    column,
                    col_name,
                    spec["pattern"],
                    bool(spec.get("allow_unsafe_regex", False)),
                )
            if spec.get("minLength") is not None or spec.get("maxLength") is not None:
                helper_results += ValueConstraintValidator.validate_length_constraints(
                    column, col_name, spec
                )

            for result in helper_results:
                found.append(
                    ConstraintViolation(
                        violation_type=_HELPER_VIOLATION_TYPES.get(result.error_code, "check"),
                        error_message=self._describe(result.error_code, col_name, name),
                        columns=[col_name],
                        values=(
                            [column[result.row_index].as_py()]
                            if self.config.include_values
                            else []
                        ),
                        constraint_name=name,
                        row_index=result.row_index,
                    )
                )

            if spec.get("nullable") is False:
                for row_idx in range(len(column)):
                    if not column[row_idx].is_valid:
                        found.append(
                            ConstraintViolation(
                                violation_type="null",
                                error_message=f"Column '{col_name}' must not be null "
                                f"(constraint '{name}')",
                                columns=[col_name],
                                values=[None] if self.config.include_values else [],
                                constraint_name=name,
                                row_index=row_idx,
                            )
                        )

    @staticmethod
    def _describe(error_code: Optional[str], col_name: str, constraint_name: str) -> str:
        what = {
            "MIN_VALUE_VIOLATION": "is below the minimum",
            "MAX_VALUE_VIOLATION": "exceeds the maximum",
            "ENUM_VIOLATION": "is not an allowed value",
            "PATTERN_VIOLATION": "does not match the required pattern",
            "MIN_LENGTH_VIOLATION": "is shorter than the minimum length",
            "MAX_LENGTH_VIOLATION": "is longer than the maximum length",
        }.get(error_code or "", "violates the constraint")
        return f"Column '{col_name}' value {what} (constraint '{constraint_name}')"

    def _unique_constraints(self, batch: pa.RecordBatch, found: List[ConstraintViolation]) -> None:
        constraints = []
        for spec in self.config.unique_constraints:
            columns = [spec] if isinstance(spec, str) else list(spec)
            indices = [column_index(batch.schema, c) for c in columns]
            if any(i is None for i in indices):
                continue
            key_name = "+".join(columns)
            values = [batch.column(i).to_pylist() for i in indices]
            constraints.append((key_name, columns, values))
        if not constraints:
            return

        already_bad = {v.row_index for v in found if v.row_index is not None}
        for row_idx in range(batch.num_rows):
            if row_idx in already_bad:
                continue  # a rejected row does not claim any key

            claims = []
            duplicates = []
            for key_name, columns, values in constraints:
                key = tuple(column_values[row_idx] for column_values in values)
                if any(part is None for part in key):
                    continue  # NULL keys are not compared
                key = key[0] if len(key) == 1 else key
                if key in self._seen_keys.get(key_name, ()):
                    duplicates.append((key_name, columns, key))
                else:
                    claims.append((key_name, key))

            if duplicates:
                for key_name, columns, key in duplicates:
                    found.append(
                        ConstraintViolation(
                            violation_type="unique",
                            error_message=f"Duplicate value in unique column(s) "
                            f"{', '.join(columns)}",
                            columns=columns,
                            values=(
                                (list(key) if isinstance(key, tuple) else [key])
                                if self.config.include_values
                                else []
                            ),
                            constraint_name=f"{key_name}_unique",
                            row_index=row_idx,
                        )
                    )
            else:
                for key_name, key in claims:
                    self._seen_keys.setdefault(key_name, set()).add(key)

    # --------------------------------------------------------------------------- results

    @property
    def violations_truncated(self) -> bool:
        """Whether more violations were found than ``violations`` retains."""
        return self.violation_count > len(self.violations)

    def get_all_violations(self) -> List[ConstraintViolation]:
        """Get the retained constraint violations (at most ``max_retained_violations``).

        The exact total is ``violation_count``.
        """
        return self.violations.copy()

    def reset(self) -> None:
        """Forget violations, row counts and unique keys (start a new dataset)."""
        self.violations.clear()
        self.batch_violations = []
        self.violation_count = 0
        self.rows_seen = 0
        self._seen_keys.clear()

    def finalize(self):
        """Finalize validation and potentially raise exceptions based on error mode."""
        # the exact count; the retained list can only be longer if a caller appended to it
        violation_count = max(self.violation_count, len(self.violations))
        if violation_count and self.config.error_mode in [
            ErrorMode.FAIL_FAST,
            ErrorMode.FAIL_COMPLETE,
        ]:
            raise ValueError(f"Constraint validation failed with {violation_count} violations")


def create_constraint_config_from_schema(schema_dict: Dict[str, Any]) -> ConstraintConfig:
    """Create constraint configuration from schema dictionary.

    Reads, per property: ``minimum``/``maximum`` (``<field>_range``), ``enum`` (``<field>_enum``),
    ``pattern`` (``<field>_pattern``), ``minLength``/``maxLength`` (``<field>_length``) and
    ``x-unique``.

    Args:
        schema_dict: Schema dictionary containing constraint definitions

    Returns:
        ConstraintConfig instance

    Raises:
        ValueError: If ``x-constraintHandling.errorMode`` is not a known mode (a typo must not
            silently turn into ``bad_rows``).
    """
    # Extract error mode
    error_mode_str = "bad_rows"
    if "x-constraintHandling" in schema_dict:
        error_mode_str = schema_dict["x-constraintHandling"].get("errorMode", "bad_rows")

    try:
        error_mode = coerce_error_mode(error_mode_str)
    except ValueError:
        shown = repr(error_mode_str) if isinstance(error_mode_str, str) else "(not a string)"
        raise ValueError(
            f"Invalid x-constraintHandling errorMode {shown}; "
            f"expected one of {_VALID_ERROR_MODES}"
        ) from None

    check_constraints = {}
    unique_constraints = []
    foreign_key_constraints = {}

    # Look for constraints in the schema properties
    properties = schema_dict.get("properties", {})
    for field_name, field_def in properties.items():
        # Check for minimum/maximum constraints
        if "minimum" in field_def or "maximum" in field_def:
            check_constraints[f"{field_name}_range"] = {
                "column": field_name,
                "min": field_def.get("minimum"),
                "max": field_def.get("maximum"),
            }

        if "enum" in field_def:
            check_constraints[f"{field_name}_enum"] = {
                "column": field_name,
                "enum": field_def["enum"],
            }

        if "pattern" in field_def:
            check_constraints[f"{field_name}_pattern"] = {
                "column": field_name,
                "pattern": field_def["pattern"],
            }

        if "minLength" in field_def or "maxLength" in field_def:
            check_constraints[f"{field_name}_length"] = {
                "column": field_name,
                "minLength": field_def.get("minLength"),
                "maxLength": field_def.get("maxLength"),
            }

        # Check for unique constraints
        if field_def.get("x-unique", False):
            unique_constraints.append(field_name)

    return ConstraintConfig(
        error_mode=error_mode,
        check_constraints=check_constraints,
        unique_constraints=unique_constraints,
        foreign_key_constraints=foreign_key_constraints,
    )
