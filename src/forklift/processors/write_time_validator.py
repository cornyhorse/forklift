"""Write-time constraint validator that validates constraints after all transformations are complete."""

from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional
import logging

import pyarrow as pa

from .constraint_validator import ConstraintValidator, ConstraintConfig, ConstraintViolation
from .bad_rows_handler import BadRowsHandler, BadRowsConfig
from .base import ValidationResult

logger = logging.getLogger(__name__)


class WriteTimeConstraintValidator:
    """Validates constraints at write time, after all transformations are complete.

    This validator should be the final step before writing data to ensure that
    constraints are validated on the final, transformed data rather than raw input.
    """

    def __init__(self,
                 constraint_config: ConstraintConfig,
                 bad_rows_config: Optional[BadRowsConfig] = None):
        """Initialize write-time constraint validator.

        Args:
            constraint_config: Configuration for constraint validation
            bad_rows_config: Configuration for bad rows handling
        """
        self.constraint_validator = ConstraintValidator(constraint_config)
        self.bad_rows_handler = BadRowsHandler(bad_rows_config or BadRowsConfig())

    def validate_and_split(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch, List[ValidationResult]]:
        """Validate constraints and split into valid and invalid rows.

        Args:
            batch: Final transformed batch ready for writing

        Returns:
            Tuple of (valid_batch, invalid_batch, validation_results)
        """
        # Perform constraint validation
        valid_batch, validation_results = self.constraint_validator.process_batch(batch)

        # Determine invalid rows
        invalid_row_indices = []
        violations_by_row = {}

        for result in validation_results:
            if not result.is_valid and result.row_index is not None:
                if result.row_index not in invalid_row_indices:
                    invalid_row_indices.append(result.row_index)

        for violation in self.constraint_validator.get_all_violations():
            if violation.row_index not in violations_by_row:
                violations_by_row[violation.row_index] = []
            violations_by_row[violation.row_index].append(violation)

        # Create invalid batch if there are invalid rows
        invalid_batch = None
        if invalid_row_indices:
            invalid_batch = batch.take(pa.array(invalid_row_indices))

            # Add invalid rows to bad rows handler
            self._add_invalid_rows_to_handler(batch, invalid_row_indices, validation_results, violations_by_row)

        return valid_batch, invalid_batch, validation_results

    def _add_invalid_rows_to_handler(self,
                                   original_batch: pa.RecordBatch,
                                   invalid_indices: List[int],
                                   validation_results: List[ValidationResult],
                                   violations_by_row: Dict[int, List[ConstraintViolation]]):
        """Add invalid rows to the bad rows handler."""
        # Group validation results by row
        validation_by_row = {}
        for result in validation_results:
            if result.row_index is not None and not result.is_valid:
                if result.row_index not in validation_by_row:
                    validation_by_row[result.row_index] = []
                validation_by_row[result.row_index].append(result)

        # Add each invalid row
        for row_idx in invalid_indices:
            if row_idx >= original_batch.num_rows:
                continue

            # Extract row data
            row_data = {}
            for i, field in enumerate(original_batch.schema):
                if i < original_batch.num_columns:
                    value = original_batch.column(i)[row_idx]
                    row_data[field.name] = value.as_py() if value.is_valid else None

            # Add to bad rows handler
            self.bad_rows_handler.add_bad_row(
                row_data=row_data,
                row_index=row_idx,
                validation_results=validation_by_row.get(row_idx, []),
                constraint_violations=violations_by_row.get(row_idx, [])
            )

    def finalize(self) -> Dict[str, Any]:
        """Finalize validation and return summary."""
        try:
            self.constraint_validator.finalize()
            constraint_validation_passed = True
            constraint_error = None
        except Exception as e:
            constraint_validation_passed = False
            constraint_error = str(e)

        # Write bad rows if any
        bad_rows_file = None
        if self.bad_rows_handler.has_bad_rows():
            bad_rows_file = self.bad_rows_handler.write_bad_rows()

        return {
            "constraint_validation_passed": constraint_validation_passed,
            "constraint_error": constraint_error,
            "bad_rows_summary": self.bad_rows_handler.get_summary(),
            "bad_rows_file": str(bad_rows_file) if bad_rows_file else None,
            "total_violations": len(self.constraint_validator.get_all_violations())
        }


class TransformationAwareProcessor:
    """Processor that coordinates transformations and write-time constraint validation.

    This processor ensures that constraint validation happens after all transformations
    are complete, providing accurate validation on the final data that will be written.
    """

    def __init__(self,
                 transformation_processors: List,
                 constraint_config: Optional[ConstraintConfig] = None,
                 bad_rows_config: Optional[BadRowsConfig] = None):
        """Initialize transformation-aware processor.

        Args:
            transformation_processors: List of transformation processors to apply
            constraint_config: Configuration for constraint validation
            bad_rows_config: Configuration for bad rows handling
        """
        self.transformation_processors = transformation_processors
        self.write_time_validator = None

        if constraint_config:
            self.write_time_validator = WriteTimeConstraintValidator(
                constraint_config, bad_rows_config
            )

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch, List[ValidationResult]]:
        """Process batch through transformations and validate constraints at write time.

        Args:
            batch: Input batch to process

        Returns:
            Tuple of (valid_batch_for_writing, invalid_batch, validation_results)
        """
        # Apply all transformations first
        current_batch = batch
        all_validation_results = []

        for processor in self.transformation_processors:
            if hasattr(processor, 'process_batch'):
                current_batch, validation_results = processor.process_batch(current_batch)
                all_validation_results.extend(validation_results)
            else:
                # Handle processors that might not follow the standard interface
                current_batch = processor.transform(current_batch)

        # After all transformations, validate constraints
        if self.write_time_validator:
            valid_batch, invalid_batch, constraint_results = self.write_time_validator.validate_and_split(current_batch)
            all_validation_results.extend(constraint_results)
            return valid_batch, invalid_batch, all_validation_results
        else:
            # No constraint validation, return all data as valid
            return current_batch, None, all_validation_results

    def finalize(self) -> Dict[str, Any]:
        """Finalize processing and return summary."""
        if self.write_time_validator:
            return self.write_time_validator.finalize()
        else:
            return {"constraint_validation_passed": True, "total_violations": 0}
