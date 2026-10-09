"""Variant management functionality."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..fields.parser import FieldParser
from ..fields.positions import PositionCalculator


class VariantManager:
    """Manages schema variant operations and configurations."""

    def __init__(
        self, schema_variants: List[Dict[str, Any]], flag_column_info: Optional[Dict[str, Any]]
    ):
        """Initialize the variant manager.

        Args:
            schema_variants: List of schema variant configurations
            flag_column_info: Configuration for the flag column
        """
        self.schema_variants = schema_variants or []
        self.flag_column_info = flag_column_info

    def get_variant_by_flag_value(self, flag_value: str) -> Optional[Dict[str, Any]]:
        """Get a specific schema variant by flag value.

        Args:
            flag_value: The flag value to search for

        Returns:
            The matching variant configuration, or None if not found
        """
        for variant in self.schema_variants:
            if variant.get("flagValue") == flag_value:
                return variant
        return None

    def get_all_flag_values(self) -> List[str]:
        """Get all possible flag values defined in the variants.

        Returns:
            List of all valid flag values
        """
        return [
            variant.get("flagValue")
            for variant in self.schema_variants
            if variant.get("flagValue") is not None
        ]

    def get_variant_fields(self, flag_value: str) -> List[Dict[str, Any]]:
        """Get the fields for a specific variant.

        Args:
            flag_value: The flag value to get fields for

        Returns:
            List of field configurations for the variant
        """
        variant = self.get_variant_by_flag_value(flag_value)
        if variant:
            return variant.get("fields", [])
        return []

    def get_field_positions_for_flag_value(self, flag_value: str) -> List[Tuple[int, int]]:
        """Get the field positions (flag column first) for a specific variant.

        Args:
            flag_value: The flag value to get positions for

        Returns:
            List of (start, end) tuples using 0-based indexing; empty for an unknown flag value
        """
        variant = self.get_variant_by_flag_value(flag_value)
        if variant is None:
            return []
        return PositionCalculator.get_field_positions_for_flag_value(
            self.flag_column_info, variant.get("fields", [])
        )

    def get_column_names_for_flag_value(
        self,
        flag_value: str,
        standardize_names: Optional[str] = None,
        dedupe_names: Optional[str] = None,
    ) -> List[str]:
        """Get the column names (flag column first) for a specific variant.

        Args:
            flag_value: The flag value to get column names for
            standardize_names: Name standardization method (postgres, snake_case, camelCase)
            dedupe_names: Name deduplication method (suffix, prefix, error)

        Returns:
            List of column names; empty for an unknown flag value
        """
        variant = self.get_variant_by_flag_value(flag_value)
        if variant is None:
            return []
        return FieldParser.get_column_names_for_flag_value(
            self.flag_column_info, variant.get("fields", []), standardize_names, dedupe_names
        )

    def has_variants(self) -> bool:
        """Check if any variants are defined.

        Returns:
            True if variants exist, False otherwise
        """
        return len(self.schema_variants) > 0

    def get_flag_column_name(self) -> Optional[str]:
        """Get the name of the flag column.

        Returns:
            The flag column name, or None if not defined
        """
        if self.flag_column_info:
            return self.flag_column_info.get("name")
        return None

    def get_flag_column_position(self) -> Optional[int]:
        """Get the position of the flag column.

        Returns:
            The flag column position, or None if not defined
        """
        if self.flag_column_info:
            return self.flag_column_info.get("start")
        return None

    def validate_flag_value(self, flag_value: str) -> bool:
        """Validate if a flag value is defined in the variants.

        Args:
            flag_value: The flag value to validate

        Returns:
            True if the flag value is valid, False otherwise
        """
        return flag_value in self.get_all_flag_values()
