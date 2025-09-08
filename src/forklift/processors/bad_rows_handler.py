"""Bad rows handler for managing invalid data during processing."""

from __future__ import annotations
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
from pathlib import Path
import json
import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pv_csv

from .base import ValidationResult
from .constraint_validator import ConstraintViolation

logger = logging.getLogger(__name__)


@dataclass
class BadRowsConfig:
    """Configuration for bad rows handling."""
    output_path: Optional[Union[str, Path]] = None
    output_format: str = "parquet"  # parquet, csv, json
    include_original_data: bool = True
    include_error_details: bool = True
    max_bad_rows: Optional[int] = None  # Maximum number of bad rows to collect
    create_summary: bool = True


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
        self.bad_row_count = 0

    def add_bad_row(self, row_data: Dict[str, Any], row_index: int,
                   validation_results: Optional[List[ValidationResult]] = None,
                   constraint_violations: Optional[List[ConstraintViolation]] = None):
        """Add a bad row with its validation errors.

        Args:
            row_data: Original row data
            row_index: Index of the row in the original data
            validation_results: Schema validation errors for this row
            constraint_violations: Constraint violations for this row
        """
        if self.config.max_bad_rows and self.bad_row_count >= self.config.max_bad_rows:
            logger.warning(f"Maximum bad rows limit ({self.config.max_bad_rows}) reached. "
                         f"Subsequent bad rows will not be collected.")
            return

        bad_row_entry = {
            "row_index": row_index,
            "timestamp": datetime.now().isoformat()
        }

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
                        errors.append({
                            "type": "validation_error",
                            "error_code": result.error_code,
                            "error_message": result.error_message,
                            "column_name": result.column_name,
                            "row_index": result.row_index
                        })

            # Add constraint violations
            if constraint_violations:
                for violation in constraint_violations:
                    errors.append({
                        "type": "constraint_violation",
                        "violation_type": violation.violation_type,
                        "error_message": violation.error_message,
                        "columns": violation.columns,
                        "values": violation.values,
                        "constraint_name": violation.constraint_name,
                        "row_index": violation.row_index
                    })

            bad_row_entry["errors"] = errors

        self.bad_rows.append(bad_row_entry)
        self.bad_row_count += 1

        # Store validation results and constraint violations separately for aggregation
        if validation_results:
            self.validation_errors.extend([r for r in validation_results if not r.is_valid])
        if constraint_violations:
            self.constraint_violations.extend(constraint_violations)

    def add_bad_rows_from_batch(self, batch_data: List[Dict[str, Any]],
                               validation_results: List[ValidationResult]):
        """Add multiple bad rows from a batch.

        Args:
            batch_data: List of row data dictionaries
            validation_results: List of validation results for the batch
        """
        # Group validation results by row index
        errors_by_row = {}
        for result in validation_results:
            if not result.is_valid and result.row_index is not None:
                if result.row_index not in errors_by_row:
                    errors_by_row[result.row_index] = []
                errors_by_row[result.row_index].append(result)

        # Add bad rows
        for row_index, row_data in enumerate(batch_data):
            if row_index in errors_by_row:
                self.add_bad_row(row_data, row_index, errors_by_row[row_index])

    def increment_row_count(self, count: int = 1):
        """Increment the total row count.

        Args:
            count: Number of rows to add to the count
        """
        self.row_count += count

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of bad rows and errors.

        Returns:
            Dictionary containing summary statistics
        """
        # Calculate constraint violations by type
        constraint_violations_summary = {}
        for violation in self.constraint_violations:
            violation_type = violation.violation_type
            if violation_type not in constraint_violations_summary:
                constraint_violations_summary[violation_type] = 0
            constraint_violations_summary[violation_type] += 1

        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_rows_processed": self.row_count,
            "bad_rows_count": self.bad_row_count,
            "bad_row_percentage": round((self.bad_row_count / max(self.row_count, 1)) * 100, 2),
            "constraint_violations": constraint_violations_summary,
            "validation_errors_count": len(self.validation_errors),
            "total_errors": len(self.validation_errors) + len(self.constraint_violations)
        }

        return summary

    def write_bad_rows(self, output_path: Optional[Union[str, Path]] = None) -> str:
        """Write bad rows to file.

        Args:
            output_path: Optional output path override

        Returns:
            Path to the written file
        """
        if not self.bad_rows:
            logger.info("No bad rows to write.")
            return ""

        output_path = output_path or self.config.output_path
        if not output_path:
            raise ValueError("Output path must be specified either in config or as parameter")

        output_path = Path(output_path)

        if self.config.output_format.lower() == "parquet":
            return self.write_parquet(output_path)
        elif self.config.output_format.lower() == "csv":
            return self.write_csv(output_path)
        elif self.config.output_format.lower() == "json":
            return self.write_json(output_path)
        else:
            raise ValueError(f"Unsupported output format: {self.config.output_format}")

    def write_parquet(self, output_path: Path) -> str:
        """Write bad rows as Parquet file."""
        # Convert bad rows to PyArrow table
        if not self.bad_rows:
            return str(output_path)

        # Flatten the data structure for Parquet
        flattened_rows = []
        for row in self.bad_rows:
            flat_row = {
                "row_index": row["row_index"],
                "timestamp": row["timestamp"],
                "error_count": len(row.get("errors", [])),
                "error_types": ",".join([e["type"] for e in row.get("errors", [])]),
                "error_messages": "; ".join([e["error_message"] for e in row.get("errors", [])])
            }

            # Add original data columns if present
            if "original_data" in row:
                for key, value in row["original_data"].items():
                    flat_row[f"original_{key}"] = value

            flattened_rows.append(flat_row)

        table = pa.Table.from_pylist(flattened_rows)
        pq.write_table(table, output_path)
        return str(output_path)

    def write_csv(self, output_path: Path) -> str:
        """Write bad rows as CSV file."""
        if not self.bad_rows:
            return str(output_path)

        # Convert to table first
        flattened_rows = []
        for row in self.bad_rows:
            flat_row = {
                "row_index": row["row_index"],
                "timestamp": row["timestamp"],
                "error_count": len(row.get("errors", [])),
                "error_types": ",".join([e["type"] for e in row.get("errors", [])]),
                "error_messages": "; ".join([e["error_message"] for e in row.get("errors", [])])
            }

            # Add original data columns if present
            if "original_data" in row:
                for key, value in row["original_data"].items():
                    flat_row[f"original_{key}"] = value

            flattened_rows.append(flat_row)

        table = pa.Table.from_pylist(flattened_rows)
        pv_csv.write_csv(table, output_path)
        return str(output_path)

    def write_json(self, output_path: Path) -> str:
        """Write bad rows as JSON file."""
        output_path = output_path.with_suffix('.json')

        with open(output_path, 'w') as f:
            json.dump(self.bad_rows, f, indent=2, default=str)

        return str(output_path)

    def write_summary(self, output_path: Optional[Union[str, Path]] = None) -> str:
        """Write summary to JSON file.

        Args:
            output_path: Optional output path override

        Returns:
            Path to the written summary file
        """
        if not self.config.create_summary:
            return ""

        summary = self.get_summary()

        if output_path is None:
            if self.config.output_path:
                output_path = Path(self.config.output_path).with_suffix('.summary.json')
            else:
                output_path = Path("bad_rows_summary.json")
        else:
            output_path = Path(output_path)

        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)

        return str(output_path)

    def clear(self):
        """Clear all bad rows and reset counters."""
        self.bad_rows.clear()
        self.validation_errors.clear()
        self.constraint_violations.clear()
        self.bad_row_count = 0
        # Note: We don't reset row_count as it tracks total processed rows
