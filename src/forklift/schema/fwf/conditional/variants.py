"""Schema variant management functionality."""

from __future__ import annotations
from typing import Any, Dict, List

from ..fields.positions import PositionCalculator
from ..fields.parser import FieldParser


class VariantManager:
    """Manages individual schema variants and their operations."""

    def __init__(self, schema_variants: List[Dict[str, Any]], flag_column: Dict[str, Any]):
        """Initialize the variant manager.

        Args:
            schema_variants: List of schema variant configurations
            flag_column: Flag column configuration
        """
        self.schema_variants = schema_variants
        self.flag_column = flag_column

    def get_field_positions_for_flag_value(self, flag_value: str) -> List[tuple[int, int]]:
        """Get field positions for a specific flag value.

        Args:
            flag_value: The flag value to get positions for

        Returns:
            List of (start, end) position tuples
        """
        variant = self._get_variant_by_flag_value(flag_value)
        if not variant:
            return []

        variant_fields = variant.get("fields", [])
        return PositionCalculator.get_field_positions_for_flag_value(
            self.flag_column, variant_fields
        )

    def get_column_names_for_flag_value(
        self,
        flag_value: str,
        standardize_names: str = None,
        dedupe_names: str = None
    ) -> List[str]:
        """Get column names for a specific flag value.

        Args:
            flag_value: The flag value to get column names for
            standardize_names: Name standardization method
            dedupe_names: Name deduplication method

        Returns:
            List of column names
        """
        variant = self._get_variant_by_flag_value(flag_value)
        if not variant:
            return []

        variant_fields = variant.get("fields", [])
        return FieldParser.get_column_names_for_flag_value(
            self.flag_column, variant_fields, standardize_names, dedupe_names
        )

    def _get_variant_by_flag_value(self, flag_value: str) -> Dict[str, Any]:
        """Get a specific schema variant by flag value.

        Args:
            flag_value: The flag value to search for

        Returns:
            The matching variant configuration, or empty dict if not found
        """
        for variant in self.schema_variants:
            if variant.get("flagValue") == flag_value:
                return variant
        return {}
