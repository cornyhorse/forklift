"""Position calculation utilities for FWF fields."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class PositionCalculator:
    """Handles position calculations for FWF fields."""

    @staticmethod
    def get_field_positions(fields: List[Dict[str, Any]]) -> List[Tuple[int, int]]:
        """Get field positions as (start, end) tuples for parsing.

        Args:
            fields: List of field configurations

        Returns:
            List of (start, end) tuples using 0-based indexing
        """
        positions = []
        for field in fields:
            start = field.get("start", 1)
            length = field.get("length", 1)
            end = start + length - 1
            positions.append((start - 1, end))  # Convert to 0-based indexing
        return positions

    @staticmethod
    def get_field_positions_for_flag_value(
        flag_column: Optional[Dict[str, Any]], variant_fields: List[Dict[str, Any]]
    ) -> List[Tuple[int, int]]:
        """Get field positions for a specific flag value including flag column.

        Args:
            flag_column: The flag column configuration
            variant_fields: List of fields for the specific variant

        Returns:
            List of (start, end) tuples using 0-based indexing
        """
        positions = []

        # Add flag column position first
        if flag_column:
            flag_start = flag_column.get("start", 1)
            flag_length = flag_column.get("length", 1)
            positions.append((flag_start - 1, flag_start + flag_length - 1))

        # Add variant-specific field positions. A variant may repeat the flag column as one
        # of its own fields; it is already listed above (and the column-name helper skips it by
        # name), so skip it here to keep names and positions aligned.
        flag_name = flag_column.get("name") if flag_column else None
        for field in variant_fields:
            if flag_name and field.get("name") == flag_name:
                continue
            start = field.get("start", 1)
            length = field.get("length", 1)
            end = start + length - 1
            positions.append((start - 1, end))  # Convert to 0-based indexing

        return positions

    @staticmethod
    def extract_flag_value_from_row(row_data: str, flag_column: Dict[str, Any]) -> Optional[str]:
        """Extract flag value from a row using flag column configuration.

        Args:
            row_data: The raw row data string
            flag_column: Flag column configuration

        Returns:
            The extracted flag value, or None if extraction fails
        """
        flag_start = flag_column.get("start", 1) - 1  # Convert to 0-based
        flag_length = flag_column.get("length", 1)

        # >= : a flag that ends exactly at the end of the row (e.g. a one-character row, or a
        # flag in the last column) is still present
        if len(row_data) >= flag_start + flag_length:
            return row_data[flag_start : flag_start + flag_length].strip()

        return None
