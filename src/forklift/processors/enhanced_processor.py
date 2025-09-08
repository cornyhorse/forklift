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
            valid_row_indices = set(range(valid_batch.num_rows))

        # Collect bad rows
        for i in range(original_batch.num_rows):
            if i not in valid_row_indices:
                # Extract row data
                row_data = {}
                for j, field in enumerate(original_batch.schema):
                    try:
                        value = original_batch.column(j)[i].as_py()
                        row_data[field.name] = value
                    except Exception:
                        row_data[field.name] = None

                # Get validation results for this row
                row_validation_results = [vr for vr in validation_results if vr.row_index == i]

                # Get constraint violations for this row
                row_constraint_violations = [cv for cv in self.constraint_validator.get_all_violations() if cv.row_index == i]

                self.bad_rows_handler.add_bad_row(
                    row_data=row_data,
                    row_index=i,
                    validation_results=row_validation_results,
                    constraint_violations=row_constraint_violations
                )

    def _extract_error_handling_mode(self) -> str:
        """Extract error handling mode from schema."""
        if not self.schema_dict:
            return "bad_rows"

        constraint_handling = self.schema_dict.get("x-constraintHandling", {})
        return constraint_handling.get("errorMode", "bad_rows")

    def extract_error_handling_mode(self) -> str:
        """Extract error handling mode from schema (public method for tests)."""
        return self._extract_error_handling_mode()

    def handle_bad_rows(self, original_batch: pa.RecordBatch, valid_batch: pa.RecordBatch,
                       validation_results: List[ValidationResult]):
        """Handle bad rows (public method for tests)."""
        return self._handle_bad_rows(original_batch, valid_batch, validation_results)

    def finalize(self) -> Dict[str, Any]:
        """Finalize processing and return summary results."""
        # Finalize constraint validator
        self.constraint_validator.finalize()

        # Get bad rows summary
        bad_rows_summary = self.bad_rows_handler.get_summary()

        # Get constraint violations summary
        constraint_violations_summary = self.constraint_validator.get_violation_summary()

        results = {
            "constraint_validation_passed": len(self.constraint_validator.get_all_violations()) == 0,
            "constraint_violations_count": len(self.constraint_validator.get_all_violations()),
            "constraint_violations_summary": constraint_violations_summary,
            "bad_rows_summary": bad_rows_summary,
            "total_rows_processed": self.bad_rows_handler.row_count,
            "bad_rows_count": self.bad_rows_handler.bad_row_count,
            "error_handling_mode": self.error_mode
        }

        # Write bad rows if configured
        if self.bad_rows_handler.config.output_path:
            try:
                bad_rows_file = self.bad_rows_handler.write_bad_rows()
                results["bad_rows_file"] = bad_rows_file
            except Exception as e:
                logger.error(f"Failed to write bad rows: {e}")
                results["bad_rows_file_error"] = str(e)

        return results

    def get_constraint_violations_summary(self) -> Dict[str, Any]:
        """Get summary of constraint violations."""
        return self.constraint_validator.get_violation_summary()

    def get_bad_rows_summary(self) -> Dict[str, Any]:
        """Get summary of bad rows."""
        return self.bad_rows_handler.get_summary()


def create_enhanced_processor_from_schema_file(
    schema_file_path: Union[str, Path],
    bad_rows_config: Optional[BadRowsConfig] = None,
    strict_mode: bool = True
) -> EnhancedDataProcessor:
    """Create an enhanced processor from a schema file.

    Args:
        schema_file_path: Path to JSON schema file
        bad_rows_config: Configuration for bad rows handling
        strict_mode: Whether to enforce strict validation

    Returns:
        Configured EnhancedDataProcessor

    Raises:
        FileNotFoundError: If schema file doesn't exist
        ValueError: If schema file is invalid
    """
    import json

    schema_path = Path(schema_file_path)
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file_path}")

    try:
        with open(schema_path, 'r') as f:
            schema_dict = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file: {e}")

    # Convert schema dict to PyArrow schema
    # This is a simplified conversion - in practice, you'd want more robust handling
    fields = []
    properties = schema_dict.get("properties", {})

    for field_name, field_def in properties.items():
        field_type = field_def.get("type", "string")

        if field_type == "string":
            pa_type = pa.string()
        elif field_type == "integer":
            pa_type = pa.int64()
        elif field_type == "number":
            pa_type = pa.float64()
        elif field_type == "boolean":
            pa_type = pa.bool_()
        else:
            pa_type = pa.string()  # Default to string

        fields.append(pa.field(field_name, pa_type))

    schema = pa.schema(fields)

    return EnhancedDataProcessor(
        schema=schema,
        schema_dict=schema_dict,
        bad_rows_config=bad_rows_config,
        strict_mode=strict_mode
    )


# Add missing utility function that tests expect
def _json_type_to_arrow_type(json_type: str) -> pa.DataType:
    """Convert JSON schema type to PyArrow data type.

    Args:
        json_type: JSON schema type string

    Returns:
        Corresponding PyArrow data type
    """
    type_mapping = {
        "string": pa.string(),
        "integer": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
        "array": pa.list_(pa.string()),  # Default to list of strings
        "object": pa.string(),  # Serialize objects as JSON strings
        "null": pa.null()
    }

    return type_mapping.get(json_type, pa.string())
