"""Bad rows handling functionality."""

from typing import List, Dict, Any, Optional
from datetime import datetime
import pyarrow as pa

from .validation_config import BadRowsConfig


class BadRowsHandler:
    """Handles collection and processing of bad rows."""

    def __init__(self, config: BadRowsConfig):
        """Initialize the bad rows handler.

        Args:
            config: Bad rows configuration
        """
        self.config = config
        self.bad_rows: List[Dict[str, Any]] = []

    def add_bad_row(self, batch: pa.RecordBatch, row_idx: int, errors: List[str]):
        """Add a bad row to the collection.

        Args:
            batch: PyArrow RecordBatch containing the row
            row_idx: Index of the bad row
            errors: List of validation error messages
        """
        if not self.config.enabled:
            return

        # Extract row data
        row_data = {}
        for i, field_name in enumerate(batch.schema.names):
            row_data[field_name] = batch.column(i)[row_idx].as_py()

        # Add validation errors if configured
        if self.config.include_validation_errors:
            row_data["_validation_errors"] = "; ".join(errors)
            row_data["_error_count"] = len(errors)
            row_data["_processed_timestamp"] = datetime.now().isoformat()

        self.bad_rows.append(row_data)

    def get_bad_rows_batch(self) -> Optional[pa.RecordBatch]:
        """Get bad rows as a PyArrow RecordBatch.

        Returns:
            PyArrow RecordBatch containing bad rows, or None if no bad rows
        """
        if not self.bad_rows:
            return None

        # Create schema with error columns if needed
        original_fields = []
        error_fields = []

        if self.bad_rows:
            # Get fields from first bad row (excluding error fields)
            sample_row = self.bad_rows[0]
            for key, value in sample_row.items():
                if not key.startswith("_"):
                    # Improved type inference that handles None values
                    field_type = self._infer_field_type(key, self.bad_rows)
                    original_fields.append(pa.field(key, field_type))

            # Add error fields if present
            if "_validation_errors" in sample_row:
                error_fields = [
                    pa.field("_validation_errors", pa.string()),
                    pa.field("_error_count", pa.int32()),
                    pa.field("_processed_timestamp", pa.string())
                ]

        schema = pa.schema(original_fields + error_fields)

        # Convert bad rows to arrays
        arrays = []
        for field in schema:
            field_values = [row.get(field.name) for row in self.bad_rows]
            # Let PyArrow infer the type from the actual values
            arrays.append(pa.array(field_values))

        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def _infer_field_type(self, field_name: str, bad_rows: List[Dict[str, Any]]) -> pa.DataType:
        """Infer PyArrow data type for a field from bad rows data.

        Args:
            field_name: Name of the field to infer type for
            bad_rows: List of bad row dictionaries

        Returns:
            Inferred PyArrow data type
        """
        # Collect all non-None values for this field
        values = []
        for row in bad_rows:
            value = row.get(field_name)
            if value is not None:
                values.append(value)

        # If all values are None, default to string
        if not values:
            return pa.string()

        # Check types of non-None values
        types_seen = set()
        for value in values:
            if isinstance(value, bool):
                types_seen.add('bool')
            elif isinstance(value, int):
                types_seen.add('int')
            elif isinstance(value, float):
                types_seen.add('float')
            else:
                types_seen.add('string')

        # Return most appropriate type
        if 'float' in types_seen:
            return pa.float64()
        elif 'int' in types_seen and 'string' not in types_seen:
            return pa.int64()
        elif 'bool' in types_seen and len(types_seen) == 1:
            return pa.bool_()
        else:
            return pa.string()

    def get_bad_rows_count(self) -> int:
        """Get the number of bad rows collected.

        Returns:
            Number of bad rows
        """
        return len(self.bad_rows)

    def clear_bad_rows(self):
        """Clear all collected bad rows."""
        self.bad_rows.clear()

    def is_threshold_exceeded(self, total_rows: int) -> bool:
        """Check if bad rows exceed the configured threshold.

        Args:
            total_rows: Total number of rows processed

        Returns:
            True if threshold is exceeded
        """
        if not self.config.fail_on_exceed_threshold or total_rows == 0:
            return False

        bad_rows_percent = (len(self.bad_rows) / total_rows) * 100
        return bad_rows_percent > self.config.max_bad_rows_percent

    def get_bad_rows_percentage(self, total_rows: int) -> float:
        """Get the percentage of bad rows.

        Args:
            total_rows: Total number of rows processed

        Returns:
            Percentage of bad rows
        """
        if total_rows == 0:
            return 0.0
        return (len(self.bad_rows) / total_rows) * 100
