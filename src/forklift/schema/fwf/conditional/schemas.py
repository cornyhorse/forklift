"""Conditional schema management functionality."""

from __future__ import annotations
from typing import Any, Dict, List, Optional

from ..fields.positions import PositionCalculator


class ConditionalSchemaManager:
    """Manages conditional schema configurations and operations."""

    def __init__(self, conditional_schemas: Dict[str, Any]):
        """Initialize the conditional schema manager.

        Args:
            conditional_schemas: The conditional schemas configuration
        """
        self.conditional_schemas = conditional_schemas
        self.flag_column = conditional_schemas.get("flagColumn")
        self.schema_variants = conditional_schemas.get("schemas", [])

    def get_flag_column_info(self) -> Optional[Dict[str, Any]]:
        """Get the flag column configuration."""
        return self.flag_column

    def get_schema_variants(self) -> List[Dict[str, Any]]:
        """Get all schema variants."""
        return self.schema_variants

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

    def get_all_possible_flag_values(self) -> List[str]:
        """Get all possible flag values defined in the schema.

        Returns:
            List of all valid flag values
        """
        return [variant.get("flagValue") for variant in self.schema_variants if variant.get("flagValue")]

    def validate_flag_value(self, flag_value: str) -> bool:
        """Check if a flag value is valid according to the schema.

        Args:
            flag_value: The flag value to validate

        Returns:
            True if the flag value is valid, False otherwise
        """
        return flag_value in self.get_all_possible_flag_values()

    def get_fields_for_flag_value(self, flag_value: str) -> List[Dict[str, Any]]:
        """Get field definitions for a specific flag value.

        Args:
            flag_value: The flag value to get fields for

        Returns:
            List of field configurations for the flag value
        """
        variant = self.get_variant_by_flag_value(flag_value)
        if variant:
            return variant.get("fields", [])
        return []

    def get_record_mapping_for_row(self, row_data: str) -> Optional[Dict[str, Any]]:
        """Get the appropriate field mapping for a given row based on its flag value.

        Args:
            row_data: The raw row data string

        Returns:
            Dictionary with mapping information, or None if no mapping found
        """
        if not self.flag_column:
            return None

        # Extract flag value from the row
        flag_value = PositionCalculator.extract_flag_value_from_row(row_data, self.flag_column)

        if flag_value:
            # Find the appropriate variant
            variant = self.get_variant_by_flag_value(flag_value)
            if variant:
                return {
                    "flagValue": flag_value,
                    "variant": variant,
                    "fields": variant.get("fields", [])
                }

        return None
