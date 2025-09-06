"""Field parsing utilities for FWF schemas."""

from __future__ import annotations
from typing import Any, Dict, List, Optional

from ..utils.column_names import ColumnNameProcessor


class FieldParser:
    """Handles parsing and processing of FWF field configurations."""

    @staticmethod
    def get_column_names(fields: List[Dict[str, Any]],
                        standardize_names: Optional[str] = None,
                        dedupe_names: Optional[str] = None) -> List[str]:
        """Get column names from field configurations.

        Args:
            fields: List of field configurations
            standardize_names: Name standardization method (postgres, snake_case, camelCase)
            dedupe_names: Name deduplication method (suffix, prefix, error)

        Returns:
            List of column names
        """
        names = []
        for field in fields:
            name = field.get("name", "")
            names.append(name)

        if standardize_names or dedupe_names:
            return ColumnNameProcessor.standardize_column_names(
                names, standardize_names, dedupe_names
            )

        return names

    @staticmethod
    def get_column_names_for_flag_value(
        flag_column: Optional[Dict[str, Any]],
        variant_fields: List[Dict[str, Any]],
        standardize_names: Optional[str] = None,
        dedupe_names: Optional[str] = None
    ) -> List[str]:
        """Get column names for a specific flag value including flag column.

        Args:
            flag_column: The flag column configuration
            variant_fields: List of fields for the specific variant
            standardize_names: Name standardization method
            dedupe_names: Name deduplication method

        Returns:
            List of column names
        """
        names = []

        # Add flag column name first
        if flag_column and flag_column.get("name"):
            names.append(flag_column["name"])

        # Add variant-specific field names
        for field in variant_fields:
            name = field.get("name", "")
            if name and name != flag_column.get("name") if flag_column else True:
                names.append(name)

        if standardize_names or dedupe_names:
            return ColumnNameProcessor.standardize_column_names(
                names, standardize_names, dedupe_names
            )

        return names

    @staticmethod
    def should_trim_field(field_name: str, trim_config: Dict[str, bool]) -> bool:
        """Check if a field should be trimmed.

        Args:
            field_name: Name of the field to check
            trim_config: Trim configuration dictionary

        Returns:
            True if field should be trimmed, False otherwise
        """
        return trim_config.get(field_name, True)  # Default to trim

    @staticmethod
    def get_null_values(column_name: Optional[str], nulls_config: Dict[str, Any]) -> List[str]:
        """Get null values for a specific column or global defaults.

        Args:
            column_name: Name of the column (None for global defaults)
            nulls_config: Nulls configuration dictionary

        Returns:
            List of null value strings
        """
        global_nulls = nulls_config.get("global", [""])

        if column_name:
            per_column = nulls_config.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls
