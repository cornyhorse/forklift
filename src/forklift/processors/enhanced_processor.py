"""Enhanced data processor that combines schema validation,
constraint checking, and bad rows handling."""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa

from .bad_rows_handler import BadRowsConfig, BadRowsHandler
from .base import BaseProcessor, ValidationResult
from .constraint_validator import (
    ConstraintConfig,
    ConstraintValidator,
    ConstraintViolation,
    create_constraint_config_from_schema,
)
from .schema_validator import SchemaValidator

logger = logging.getLogger(__name__)


class EnhancedDataProcessor(BaseProcessor):
    """Enhanced data processor with comprehensive validation and error handling.

    This processor combines schema validation, constraint checking, and bad rows
    handling to provide complete data quality validation according to schema
    standards.
    """

    def __init__(
        self,
        schema: pa.Schema,
        schema_dict: Optional[Dict[str, Any]] = None,
        constraint_config: Optional[ConstraintConfig] = None,
        bad_rows_config: Optional[BadRowsConfig] = None,
        strict_mode: bool = True,
    ):
        """Initialize the enhanced data processor.

        Args:
            schema: PyArrow schema for type validation
            schema_dict: Schema dictionary containing constraint definitions
            constraint_config: Optional constraint configuration override
            bad_rows_config: Configuration for bad rows handling
            strict_mode: Whether to enforce strict validation
        """
        self.schema = schema
        self.schema_dict = schema_dict or {}
        self.strict_mode = strict_mode

        # Initialize schema validator
        self.schema_validator = SchemaValidator(schema, strict_mode)

        # Initialize constraint validator
        if constraint_config:
            self.constraint_config = constraint_config
        else:
            self.constraint_config = create_constraint_config_from_schema(self.schema_dict)

        self.constraint_validator = ConstraintValidator(self.constraint_config)

        # Initialize bad rows handler
        if bad_rows_config is None:
            bad_rows_config = BadRowsConfig()
        self.bad_rows_handler = BadRowsHandler(bad_rows_config)

        # Extract error handling mode from schema
        self.error_mode = self._extract_error_handling_mode()

        # Bookkeeping: violations already attributed to batches, row-level schema failures
        self._violations_attributed = 0
        self._schema_row_errors = 0

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch with comprehensive validation.

        In ``bad_rows`` mode (the default) rows that fail schema or constraint validation are
        removed from the returned batch and routed to the bad rows handler (with their position
        in the whole input). ``fail_fast`` raises on the first violation; ``fail_complete``
        keeps all rows and raises from ``finalize()``.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (valid_batch, validation_results). ``row_index`` of every result is the
            position of the row in ``batch``.
        """
        all_validation_results = []
        mode = self._error_mode_value()

        # Track original batch for bad rows
        original_batch = batch

        # Step 1: Schema validation
        schema_valid_batch, schema_validation_results = self.schema_validator.process_batch(batch)
        all_validation_results.extend(schema_validation_results)

        schema_bad_rows = self._row_level_failures(schema_validation_results, batch.num_rows)
        if schema_bad_rows:
            self._schema_row_errors += len(schema_bad_rows)
            if mode == "fail_fast":
                raise ValueError(f"Schema validation failed for {len(schema_bad_rows)} row(s)")

        # Rows that failed schema validation must not take part in constraint checks (a rejected
        # row must not claim a unique key), so they are removed first in bad_rows mode.
        keep_idx: Optional[List[int]] = None
        working_batch = schema_valid_batch
        if (
            schema_bad_rows
            and mode == "bad_rows"
            and schema_valid_batch.num_rows == batch.num_rows
        ):
            rejected = set(schema_bad_rows)
            keep_idx = [i for i in range(batch.num_rows) if i not in rejected]
            working_batch = schema_valid_batch.take(pa.array(keep_idx, type=pa.int64()))

        # Step 2: Constraint validation on schema-valid data
        already_attributed = self._violations_attributed
        constraint_valid_batch, constraint_validation_results = (
            self.constraint_validator.process_batch(working_batch)
        )
        violations = self.constraint_validator.get_all_violations()
        new_violations = list(violations[already_attributed:])
        self._violations_attributed = len(violations)

        # Report positions relative to the batch that was passed in
        if keep_idx is not None:
            for violation in new_violations:
                violation.row_index = self._original_index(violation.row_index, keep_idx)
            constraint_validation_results = [
                dataclasses.replace(
                    result, row_index=self._original_index(result.row_index, keep_idx)
                )
                for result in constraint_validation_results
            ]
        all_validation_results.extend(constraint_validation_results)

        # Step 3: Handle bad rows
        self._handle_bad_rows(
            original_batch, constraint_valid_batch, all_validation_results, new_violations
        )

        # Update row count
        self.bad_rows_handler.increment_row_count(batch.num_rows)

        return constraint_valid_batch, all_validation_results

    @staticmethod
    def _original_index(row_index: Any, keep_idx: List[int]) -> Any:
        if isinstance(row_index, int) and 0 <= row_index < len(keep_idx):
            return keep_idx[row_index]
        return row_index

    @staticmethod
    def _as_row_index(value: Any) -> Optional[int]:
        """Row index as int, or None when the value is not a usable index."""
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _row_level_failures(
        self, validation_results: List[ValidationResult], num_rows: int
    ) -> List[int]:
        """Sorted indices of rows that have at least one failed, row-attributed result."""
        rows = set()
        for result in validation_results:
            row_idx = self._as_row_index(result.row_index)
            if not result.is_valid and row_idx is not None and 0 <= row_idx < num_rows:
                rows.add(row_idx)
        return sorted(rows)

    def _error_mode_value(self) -> str:
        """Effective error mode: the constraint configuration's, else the schema's."""
        mode = getattr(getattr(self.constraint_config, "error_mode", None), "value", None)
        return mode if isinstance(mode, str) else self.error_mode

    def _handle_bad_rows(
        self,
        original_batch: pa.RecordBatch,
        valid_batch: pa.RecordBatch,
        validation_results: List[ValidationResult],
        constraint_violations: Optional[List[ConstraintViolation]] = None,
    ):
        """Handle bad rows collection and processing.

        Args:
            original_batch: The batch as it was passed to ``process_batch``
            valid_batch: The rows that passed validation
            validation_results: Results whose ``row_index`` refers to ``original_batch``
            constraint_violations: Violations of this batch (``row_index`` relative to
                ``original_batch``); defaults to everything the constraint validator holds
        """
        if constraint_violations is None:
            constraint_violations = self.constraint_validator.get_all_violations()

        # Collect validation errors by row
        validation_by_row: Dict[int, List[ValidationResult]] = {}
        for result in validation_results:
            row_idx = self._as_row_index(result.row_index)
            if row_idx is not None and not result.is_valid:
                validation_by_row.setdefault(row_idx, []).append(result)

        # Collect every constraint violation of a row (not just the first)
        constraint_violations_by_row: Dict[int, List[ConstraintViolation]] = {}
        for violation in constraint_violations:
            row_idx = self._as_row_index(violation.row_index)
            if row_idx is not None:
                constraint_violations_by_row.setdefault(row_idx, []).append(violation)

        # Add bad rows to handler
        invalid_row_indices = sorted(set(validation_by_row) | set(constraint_violations_by_row))
        first_row_of_batch = self.bad_rows_handler.row_count

        for row_idx in invalid_row_indices:
            if row_idx < 0 or row_idx >= original_batch.num_rows:
                continue

            # Extract row data
            row_data = {}
            for i, field in enumerate(original_batch.schema):
                if i < original_batch.num_columns:
                    value = original_batch.column(i)[row_idx]
                    row_data[field.name] = value.as_py() if value.is_valid else None

            # Add to bad rows handler; the index is the position in the whole input
            self.bad_rows_handler.add_bad_row(
                row_data=row_data,
                row_index=first_row_of_batch + row_idx,
                validation_results=validation_by_row.get(row_idx, []),
                constraint_violations=constraint_violations_by_row.get(row_idx, []),
            )

    def _extract_error_handling_mode(self) -> str:
        """Extract error handling mode from schema configuration."""
        if "x-constraintHandling" in self.schema_dict:
            return self.schema_dict["x-constraintHandling"].get("errorMode", "bad_rows")
        return "bad_rows"

    def finalize(self) -> Dict[str, Any]:
        """Finalize processing and return summary information.

        Returns:
            Dictionary containing processing summary and file paths
        """
        results = {
            "processing_summary": self.bad_rows_handler.get_summary(),
            "constraint_violations": len(self.constraint_validator.get_all_violations()),
            "has_bad_rows": self.bad_rows_handler.has_bad_rows(),
        }

        # Finalize constraint validation (may raise exception in FAIL_COMPLETE mode)
        try:
            self.constraint_validator.finalize()
            results["constraint_validation_passed"] = True
        except Exception as e:
            results["constraint_validation_passed"] = False
            results["constraint_error"] = str(e)

            # Re-raise if we're supposed to fail
            if self.constraint_config.error_mode.value in ["fail_fast", "fail_complete"]:
                raise

        # In the fail modes, row-level schema failures fail the run as well
        if self._schema_row_errors and self._error_mode_value() in ["fail_fast", "fail_complete"]:
            results["constraint_validation_passed"] = False
            raise ValueError(
                f"Schema validation failed for {self._schema_row_errors} row(s) "
                f"({self._error_mode_value()} mode)"
            )

        # Write bad rows if any exist
        if self.bad_rows_handler.has_bad_rows():
            bad_rows_file = self.bad_rows_handler.write_bad_rows()
            if bad_rows_file:
                results["bad_rows_file"] = str(bad_rows_file)

        return results

    def get_constraint_violations_summary(self) -> Dict[str, Any]:
        """Get a summary of constraint violations."""
        violations = self.constraint_validator.get_all_violations()

        summary = {
            "total_violations": len(violations),
            "violation_types": {},
            "affected_constraints": set(),
            "sample_violations": [],
        }

        for violation in violations:
            # Count by type
            vtype = violation.violation_type
            summary["violation_types"][vtype] = summary["violation_types"].get(vtype, 0) + 1

            # Track affected constraints
            if violation.constraint_name:
                summary["affected_constraints"].add(violation.constraint_name)

            # Sample violations (first 5 of each type)
            if len([v for v in summary["sample_violations"] if v["type"] == vtype]) < 5:
                summary["sample_violations"].append(
                    {
                        "type": vtype,
                        "row_index": violation.row_index,
                        "columns": violation.columns,
                        "values": violation.values,
                        "message": violation.error_message,
                    }
                )

        summary["affected_constraints"] = list(summary["affected_constraints"])
        return summary


def create_enhanced_processor_from_schema_file(
    schema_file_path: Union[str, Path],
    bad_rows_output_path: Optional[Union[str, Path]] = None,
    error_mode: str = "bad_rows",
) -> EnhancedDataProcessor:
    """Create an enhanced processor from a schema file.

    Args:
        schema_file_path: Path to schema JSON file
        bad_rows_output_path: Optional path for bad rows output
        error_mode: Error handling mode ("fail_fast", "fail_complete", "bad_rows")

    Returns:
        Configured EnhancedDataProcessor
    """
    import json

    # Load schema
    with open(schema_file_path, "r") as f:
        schema_dict = json.load(f)

    # Create PyArrow schema from JSON schema (simplified conversion)
    # This would need to be more sophisticated in practice
    fields = []
    properties = schema_dict.get("properties", {})

    for field_name, field_def in properties.items():
        # Basic type mapping - would need enhancement for complex types
        field_type = _json_type_to_arrow_type(field_def)
        nullable = field_name not in schema_dict.get("required", [])
        fields.append(pa.field(field_name, field_type, nullable=nullable))

    arrow_schema = pa.schema(fields)

    # Configure bad rows handling
    bad_rows_config = BadRowsConfig(
        output_path=bad_rows_output_path,
        output_format="parquet",
        include_original_data=True,
        include_error_details=True,
        create_summary=True,
    )

    # Override error mode if specified
    if "x-constraintHandling" not in schema_dict:
        schema_dict["x-constraintHandling"] = {}
    schema_dict["x-constraintHandling"]["errorMode"] = error_mode

    return EnhancedDataProcessor(
        schema=arrow_schema, schema_dict=schema_dict, bad_rows_config=bad_rows_config
    )


def _json_type_to_arrow_type(field_def: Dict[str, Any]) -> pa.DataType:
    """Convert JSON Schema type to PyArrow type."""
    field_type = field_def.get("type", "string")

    if field_type == "integer":
        return pa.int64()
    elif field_type == "number":
        return pa.float64()
    elif field_type == "boolean":
        return pa.bool_()
    elif field_type == "string":
        format_type = field_def.get("format")
        if format_type == "date":
            return pa.date32()
        elif format_type == "date-time":
            return pa.timestamp("us")
        else:
            return pa.string()
    elif field_type == "array":
        # Simplified array handling
        return pa.list_(pa.string())
    else:
        return pa.string()  # Default fallback
