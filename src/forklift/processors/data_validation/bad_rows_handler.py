"""Bad rows handling functionality."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pyarrow as pa

from .validation_config import BadRowsConfig

#: Columns the handler adds to every bad row; original columns with these names are renamed.
RESERVED_COLUMNS = ("_validation_errors", "_error_count", "_processed_timestamp", "_row_number")


class BadRowsHandler:
    """Handles collection and processing of bad rows."""

    def __init__(self, config: BadRowsConfig):
        """Initialize the bad rows handler.

        Args:
            config: Bad rows configuration
        """
        self.config = config
        self.bad_rows: List[Dict[str, Any]] = []
        # Rows rejected so far. Counted even when collection is disabled so the threshold and
        # the percentage stay correct.
        self.bad_row_total = 0

    def add_bad_row(
        self,
        batch: pa.RecordBatch,
        row_idx: int,
        errors: List[str],
        row_number: Optional[int] = None,
    ):
        """Add a bad row to the collection.

        Args:
            batch: PyArrow RecordBatch containing the row
            row_idx: Index of the bad row
            errors: List of validation error messages
            row_number: Position of the row in the whole input (0-based), when known
        """
        self.bad_row_total += 1

        if not self.config.enabled:
            return

        row_data: Dict[str, Any] = {}

        # Extract the original row unless it must be kept out of the output
        if self.config.include_original_row:
            for i, field_name in enumerate(batch.schema.names):
                name = field_name
                # An original column that looks like one of our error columns is renamed, never
                # dropped or overwritten
                while name in RESERVED_COLUMNS or name in row_data:
                    name = f"original_{name}"
                row_data[name] = batch.column(i)[row_idx].as_py()
        else:
            row_data["_row_number"] = row_number if row_number is not None else row_idx

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

        # Original columns: union of the keys of all rows, in order of first appearance
        original_names: List[str] = []
        seen = set()
        for row in self.bad_rows:
            for key in row:
                if key not in RESERVED_COLUMNS and key not in seen:
                    seen.add(key)
                    original_names.append(key)
        if not self.config.include_original_row and any(
            "_row_number" in row for row in self.bad_rows
        ):
            original_names = ["_row_number"]

        fields = []
        columns = []
        for key in original_names:
            field_type = self._infer_field_type(key, self.bad_rows)
            fields.append(pa.field(key, field_type))
            values = [row.get(key) for row in self.bad_rows]
            columns.append(pa.array(self._conform(values, field_type), field_type))

        # Add error fields if present
        if any("_validation_errors" in row for row in self.bad_rows):
            error_fields = [
                pa.field("_validation_errors", pa.string()),
                pa.field("_error_count", pa.int32()),
                pa.field("_processed_timestamp", pa.string()),
            ]
            for field in error_fields:
                fields.append(field)
                columns.append(
                    pa.array([row.get(field.name) for row in self.bad_rows], field.type)
                )

        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    @staticmethod
    def _conform(values: List[Any], field_type: pa.DataType) -> List[Any]:
        """Values as the inferred column type needs them (non-str -> str, int -> float)."""
        if pa.types.is_string(field_type):
            return [v if v is None or isinstance(v, str) else str(v) for v in values]
        if pa.types.is_floating(field_type):
            return [None if v is None else float(v) for v in values]
        return values

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
                types_seen.add("bool")
            elif isinstance(value, int):
                types_seen.add("int")
            elif isinstance(value, float):
                types_seen.add("float")
            else:
                types_seen.add("string")

        # Return most appropriate type
        if "float" in types_seen:
            return pa.float64()
        elif "int" in types_seen and "string" not in types_seen:
            return pa.int64()
        elif "bool" in types_seen and len(types_seen) == 1:
            return pa.bool_()
        else:
            return pa.string()

    def get_bad_rows_count(self) -> int:
        """Get the number of bad rows seen (collected or, with collection disabled, dropped).

        Returns:
            Number of bad rows
        """
        return max(self.bad_row_total, len(self.bad_rows))

    def clear_bad_rows(self):
        """Clear all collected bad rows."""
        self.bad_rows.clear()
        self.bad_row_total = 0

    def is_threshold_exceeded(self, total_rows: int) -> bool:
        """Check if bad rows exceed the configured threshold.

        Args:
            total_rows: Total number of rows processed

        Returns:
            True if threshold is exceeded
        """
        if not self.config.fail_on_exceed_threshold or total_rows == 0:
            return False

        return self.get_bad_rows_percentage(total_rows) > self.config.max_bad_rows_percent

    def get_bad_rows_percentage(self, total_rows: int) -> float:
        """Get the percentage of bad rows.

        Args:
            total_rows: Total number of rows processed

        Returns:
            Percentage of bad rows
        """
        if total_rows == 0:
            return 0.0
        return (max(self.bad_row_total, len(self.bad_rows)) / total_rows) * 100
