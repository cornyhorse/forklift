"""Bad rows handler for managing invalid data during processing."""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq

from .base import ValidationResult
from .constraint_validator import ConstraintViolation

logger = logging.getLogger(__name__)


@dataclass
class BadRowsConfig:
    """Configuration for bad rows handling."""

    output_path: Optional[Union[str, Path]] = None  # a file, or a directory for generated names
    output_format: str = "parquet"  # parquet, csv, json
    include_original_data: bool = True
    include_error_details: bool = True
    max_bad_rows: Optional[int] = None  # Maximum number of bad rows to collect (0 collects none)
    create_summary: bool = True
    # CSV output: prefix cells that start with = + - @ (or tab/CR) with ' so spreadsheet
    # programs do not run them as formulas
    csv_formula_protection: bool = True


class BadRowsHandler:
    """Handles collection and output of invalid rows during data processing.

    This class collects rows that fail validation (schema, constraint, or other
    validation checks) and outputs them to a separate file with error details
    for debugging and data quality analysis.
    """

    def __init__(self, config: BadRowsConfig):
        """Initialize the bad rows handler.

        Args:
            config: Configuration for bad rows handling
        """
        self.config = config
        self.bad_rows: List[Dict[str, Any]] = []
        self.validation_errors: List[ValidationResult] = []
        self.constraint_violations: List[ConstraintViolation] = []
        self.row_count = 0
        self.bad_row_count = 0  # every bad row seen, whether it was collected or not
        self.dropped_bad_row_count = 0  # bad rows not collected because of max_bad_rows
        self._limit_warning_logged = False

    def add_bad_row(
        self,
        row_data: Dict[str, Any],
        row_index: int,
        validation_results: Optional[List[ValidationResult]] = None,
        constraint_violations: Optional[List[ConstraintViolation]] = None,
    ):
        """Add a bad row with its validation errors.

        Args:
            row_data: Original row data
            row_index: Index of the row in the original data
            validation_results: Schema validation errors for this row
            constraint_violations: Constraint violations for this row
        """
        self.bad_row_count += 1

        max_bad_rows = self.config.max_bad_rows
        if max_bad_rows is not None and len(self.bad_rows) >= max_bad_rows:
            # Count the row (so percentages stay right) but do not keep it; warn once
            self.dropped_bad_row_count += 1
            if not self._limit_warning_logged:
                self._limit_warning_logged = True
                logger.warning(
                    f"Maximum bad rows limit ({max_bad_rows}) reached. "
                    f"Subsequent bad rows will be counted but not collected."
                )
            return

        bad_row_entry = {"row_index": row_index, "timestamp": datetime.now().isoformat()}

        # Include original data if configured
        if self.config.include_original_data:
            bad_row_entry["original_data"] = row_data

        # Include error details if configured
        if self.config.include_error_details:
            errors = []

            # Add validation errors
            if validation_results:
                for result in validation_results:
                    if not result.is_valid:
                        errors.append(
                            {
                                "type": "validation_error",
                                "error_code": result.error_code,
                                "error_message": result.error_message,
                                "column_name": result.column_name,
                                "row_index": result.row_index,
                            }
                        )

            # Add constraint violations
            if constraint_violations:
                for violation in constraint_violations:
                    errors.append(
                        {
                            "type": "constraint_violation",
                            "violation_type": violation.violation_type,
                            "error_message": violation.error_message,
                            "columns": violation.columns,
                            "values": violation.values,
                            "constraint_name": violation.constraint_name,
                        }
                    )

            bad_row_entry["errors"] = errors

        self.bad_rows.append(bad_row_entry)

        # Store validation results and violations for summary
        if validation_results:
            self.validation_errors.extend([r for r in validation_results if not r.is_valid])
        if constraint_violations:
            self.constraint_violations.extend(constraint_violations)

    def add_bad_rows_from_batch(
        self,
        batch: pa.RecordBatch,
        invalid_indices: List[int],
        validation_results: List[ValidationResult],
        constraint_violations: Optional[List[ConstraintViolation]] = None,
    ):
        """Add multiple bad rows from a batch.

        Args:
            batch: Original batch containing the data
            invalid_indices: Indices of invalid rows in the batch
            validation_results: Validation results for the batch
            constraint_violations: Constraint violations for the batch
        """
        # Group validation results by row index
        validation_by_row = {}
        for result in validation_results:
            if result.row_index is not None and not result.is_valid:
                if result.row_index not in validation_by_row:
                    validation_by_row[result.row_index] = []
                validation_by_row[result.row_index].append(result)

        # Group constraint violations by row index
        violations_by_row = {}
        if constraint_violations:
            for violation in constraint_violations:
                if violation.row_index not in violations_by_row:
                    violations_by_row[violation.row_index] = []
                violations_by_row[violation.row_index].append(violation)

        # Add each invalid row
        for row_idx in invalid_indices:
            if row_idx >= batch.num_rows:
                continue

            # Extract row data
            row_data = {}
            for i, field in enumerate(batch.schema):
                value = batch.column(i)[row_idx]
                row_data[field.name] = value.as_py() if value.is_valid else None

            # Get validation results and violations for this row
            row_validations = validation_by_row.get(row_idx, [])
            row_violations = violations_by_row.get(row_idx, [])

            self.add_bad_row(
                row_data=row_data,
                row_index=self.row_count + row_idx,
                validation_results=row_validations,
                constraint_violations=row_violations,
            )

    def increment_row_count(self, count: int = 1):
        """Increment the total row count."""
        self.row_count += count

    def has_bad_rows(self) -> bool:
        """Check if any bad rows have been collected."""
        return len(self.bad_rows) > 0

    def get_bad_row_count(self) -> int:
        """Get the number of bad rows collected."""
        return len(self.bad_rows)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of bad rows and errors."""
        # Count error types
        validation_error_counts = {}
        constraint_violation_counts = {}

        for error in self.validation_errors:
            error_type = error.error_code or "unknown"
            validation_error_counts[error_type] = validation_error_counts.get(error_type, 0) + 1

        for violation in self.constraint_violations:
            violation_type = violation.violation_type
            constraint_violation_counts[violation_type] = (
                constraint_violation_counts.get(violation_type, 0) + 1
            )

        return {
            "total_rows_processed": self.row_count,
            "bad_rows_count": self.bad_row_count,
            "bad_rows_collected": len(self.bad_rows),
            "bad_rows_dropped": self.dropped_bad_row_count,
            "bad_rows_percentage": (
                (self.bad_row_count / self.row_count * 100) if self.row_count > 0 else 0
            ),
            "validation_errors": validation_error_counts,
            "constraint_violations": constraint_violation_counts,
            "timestamp": datetime.now().isoformat(),
        }

    def write_bad_rows(self, output_path: Optional[Union[str, Path]] = None) -> Optional[Path]:
        """Write bad rows to file.

        Args:
            output_path: Optional output path override

        Returns:
            Path to the written file, or None if no bad rows to write
        """
        if not self.has_bad_rows():
            logger.info("No bad rows to write")
            return None

        file_path = self._resolve_output_path(output_path)

        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if self.config.output_format.lower() == "parquet":
                self._write_parquet(file_path)
            elif self.config.output_format.lower() == "csv":
                self._write_csv(file_path)
            elif self.config.output_format.lower() == "json":
                self._write_json(file_path)
            else:
                raise ValueError(f"Unsupported output format: {self.config.output_format}")

            logger.info(f"Written {self.bad_row_count} bad rows to {file_path}")

            # Write summary if configured
            if self.config.create_summary:
                summary_path = file_path.with_suffix(".summary.json")
                self._write_summary(summary_path)

            return file_path

        except Exception as e:
            logger.error(f"Failed to write bad rows to {file_path}: {e}")
            raise

    def _resolve_output_path(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """File to write: an explicit file path, or a unique generated name.

        ``output_path`` (argument or configuration) may name a file or a directory; for a
        directory (existing, or written with a trailing separator) and when nothing is
        configured, a unique ``bad_rows_<timestamp with microseconds>_<pid>_<id>`` file name
        is generated, so concurrent jobs never overwrite each other.
        """
        configured = output_path or self.config.output_path
        extension = self.config.output_format.lower()

        if configured:
            candidate = Path(configured)
            is_directory = candidate.is_dir() or str(configured).endswith(("/", os.sep))
            if not is_directory:
                return candidate
            directory = candidate
        else:
            directory = Path(".")

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return directory / f"bad_rows_{stamp}_{os.getpid()}_{uuid.uuid4().hex[:8]}.{extension}"

    @staticmethod
    def _table_from_rows(rows: List[Dict[str, Any]]) -> pa.Table:
        """Arrow table with the union of the rows' keys (a column that cannot hold its mixed
        values becomes a string column)."""
        names: List[str] = []
        seen = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    names.append(key)

        columns = []
        for name in names:
            values = [row.get(name) for row in rows]
            try:
                columns.append(pa.array(values))
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                columns.append(
                    pa.array([None if v is None else str(v) for v in values], pa.string())
                )
        return pa.Table.from_arrays(columns, names=names)

    @staticmethod
    def _protect_formulas(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prefix string cells that a spreadsheet would run as formulas with an apostrophe."""
        triggers = ("=", "+", "-", "@", "\t", "\r")
        return [
            {
                key: (
                    "'" + value if isinstance(value, str) and value.startswith(triggers) else value
                )
                for key, value in row.items()
            }
            for row in rows
        ]

    def _write_parquet(self, file_path: Path):
        """Write bad rows in Parquet format."""
        # Flatten the data for Parquet
        flattened_rows = []
        for bad_row in self.bad_rows:
            flattened = {"row_index": bad_row["row_index"], "timestamp": bad_row["timestamp"]}

            # Add original data columns
            if "original_data" in bad_row:
                for key, value in bad_row["original_data"].items():
                    flattened[f"original_{key}"] = value

            # Add error information
            if "errors" in bad_row:
                error_messages = []
                error_codes = []
                error_types = []
                for error in bad_row["errors"]:
                    error_messages.append(error.get("error_message", ""))
                    error_codes.append(error.get("error_code") or error.get("violation_type", ""))
                    error_types.append(error.get("type", ""))

                flattened["error_messages"] = "; ".join(error_messages)
                flattened["error_codes"] = "; ".join(error_codes)
                flattened["error_types"] = "; ".join(error_types)

            flattened_rows.append(flattened)

        # Convert to Arrow table
        table = self._table_from_rows(flattened_rows)
        pq.write_table(table, file_path)

    def _write_csv(self, file_path: Path):
        """Write bad rows in CSV format."""
        # Use the same flattening logic as Parquet
        flattened_rows = []
        for bad_row in self.bad_rows:
            flattened = {"row_index": bad_row["row_index"], "timestamp": bad_row["timestamp"]}

            if "original_data" in bad_row:
                for key, value in bad_row["original_data"].items():
                    flattened[f"original_{key}"] = value

            if "errors" in bad_row:
                error_messages = []
                error_codes = []
                for error in bad_row["errors"]:
                    error_messages.append(error.get("error_message", ""))
                    error_codes.append(error.get("error_code") or error.get("violation_type", ""))

                flattened["error_messages"] = "; ".join(error_messages)
                flattened["error_codes"] = "; ".join(error_codes)

            flattened_rows.append(flattened)

        # Convert to Arrow table and write as CSV
        if self.config.csv_formula_protection:
            flattened_rows = self._protect_formulas(flattened_rows)
        table = self._table_from_rows(flattened_rows)
        pv_csv.write_csv(table, file_path)

    def _write_json(self, file_path: Path):
        """Write bad rows in JSON format."""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.bad_rows, f, indent=2, default=str)

    def _write_summary(self, file_path: Path):
        """Write summary information."""
        summary = self.get_summary()
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
