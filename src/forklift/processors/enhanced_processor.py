"""Enhanced data processor that combines schema validation, constraint checking, and bad rows handling."""

from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional, Union
from pathlib import Path
import logging

import pyarrow as pa

from .base import BaseProcessor, ValidationResult
from .schema_validator import SchemaValidator
from .constraint_validator import ConstraintValidator, ConstraintConfig, create_constraint_config_from_schema
from .bad_rows_handler import BadRowsHandler, BadRowsConfig

logger = logging.getLogger(__name__)


class EnhancedDataProcessor(BaseProcessor):
    """Enhanced data processor with comprehensive validation and error handling.

    This processor combines schema validation, constraint checking, and bad rows
    handling to provide complete data quality validation according to schema
    standards.
    """

    def __init__(self,
                 schema: pa.Schema,
                 schema_dict: Optional[Dict[str, Any]] = None,
                 constraint_config: Optional[ConstraintConfig] = None,
                 bad_rows_config: Optional[BadRowsConfig] = None,
                 strict_mode: bool = True):
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

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch with comprehensive validation.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (valid_batch, validation_results)
        """
        all_validation_results = []

        # Track original batch for bad rows
        original_batch = batch

        # Step 1: Schema validation
        schema_valid_batch, schema_validation_results = self.schema_validator.process_batch(batch)
        all_validation_results.extend(schema_validation_results)

        # Step 2: Constraint validation on schema-valid data
        constraint_valid_batch, constraint_validation_results = self.constraint_validator.process_batch(schema_valid_batch)
        all_validation_results.extend(constraint_validation_results)

        # Step 3: Handle bad rows
        self._handle_bad_rows(original_batch, constraint_valid_batch, all_validation_results)

        # Update row count
        self.bad_rows_handler.increment_row_count(batch.num_rows)

        return constraint_valid_batch, all_validation_results

    def _handle_bad_rows(self, original_batch: pa.RecordBatch, valid_batch: pa.RecordBatch,
                        validation_results: List[ValidationResult]):
        """Handle bad rows collection and processing."""
        # Determine which rows are invalid
        valid_row_indices = set()
        if valid_batch.num_rows > 0:
            # This is a simplified approach - in practice, we'd need to track
            # the mapping between original and valid rows more precisely
            pass

        # Collect validation errors by row
        validation_by_row = {}
        for result in validation_results:
            if result.row_index is not None and not result.is_valid:
                if result.row_index not in validation_by_row:
                    validation_by_row[result.row_index] = []
                validation_by_row[result.row_index].append(result)

        # Collect constraint violations by row
        constraint_violations_by_row = {}
        for violation in self.constraint_validator.get_all_violations():
            if violation.row_index is not None and violation.row_index not in constraint_violations_by_row:
                constraint_violations_by_row[violation.row_index] = []
                constraint_violations_by_row[violation.row_index].append(violation)

        # Add bad rows to handler
        invalid_row_indices = set(validation_by_row.keys()) | set(constraint_violations_by_row.keys())

        for row_idx in invalid_row_indices:
            # Ensure row_idx is an integer - handle various types
            original_row_idx = row_idx
            if isinstance(row_idx, str):
                try:
                    row_idx = int(row_idx)
                except (ValueError, TypeError):
                    continue
            elif row_idx is None:
                continue
            elif not isinstance(row_idx, int):
                try:
                    row_idx = int(row_idx)
                except (ValueError, TypeError):
                    continue

            if row_idx < 0 or row_idx >= original_batch.num_rows:
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
                validation_results=validation_by_row.get(original_row_idx, []),
                constraint_violations=constraint_violations_by_row.get(original_row_idx, [])
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
            "has_bad_rows": self.bad_rows_handler.has_bad_rows()
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
            "sample_violations": []
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
                summary["sample_violations"].append({
                    "type": vtype,
                    "row_index": violation.row_index,
                    "columns": violation.columns,
                    "values": violation.values,
                    "message": violation.error_message
                })

        summary["affected_constraints"] = list(summary["affected_constraints"])
        return summary


def create_enhanced_processor_from_schema_file(schema_file_path: Union[str, Path],
                                              bad_rows_output_path: Optional[Union[str, Path]] = None,
                                              error_mode: str = "bad_rows") -> EnhancedDataProcessor:
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
    with open(schema_file_path, 'r') as f:
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
        create_summary=True
    )

    # Override error mode if specified
    if "x-constraintHandling" not in schema_dict:
        schema_dict["x-constraintHandling"] = {}
    schema_dict["x-constraintHandling"]["errorMode"] = error_mode

    return EnhancedDataProcessor(
        schema=arrow_schema,
        schema_dict=schema_dict,
        bad_rows_config=bad_rows_config
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
            return pa.timestamp('us')
        else:
            return pa.string()
    elif field_type == "array":
        # Simplified array handling
        return pa.list_(pa.string())
    else:
        return pa.string()  # Default fallback
