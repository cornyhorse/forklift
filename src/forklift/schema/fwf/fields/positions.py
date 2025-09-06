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
        flag_column: Optional[Dict[str, Any]],
        variant_fields: List[Dict[str, Any]]
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

        # Add variant-specific field positions
        for field in variant_fields:
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

        if len(row_data) > flag_start + flag_length:
            return row_data[flag_start:flag_start + flag_length].strip()

        return None
