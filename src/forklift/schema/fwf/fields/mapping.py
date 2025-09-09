"""Field mapping utilities for FWF schemas."""

from __future__ import annotations
from typing import Any, Dict, List, Optional


class FieldMapper:
    """Handles field mapping and unified schema generation."""

    @staticmethod
    def get_all_possible_fields(
        has_conditional_schemas: bool,
        traditional_fields: List[Dict[str, Any]],
        flag_column: Optional[Dict[str, Any]],
        schema_variants: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Get all possible fields from all schema variants combined.

        Args:
            has_conditional_schemas: Whether schema has conditional support
            traditional_fields: Traditional field configurations
            flag_column: Flag column configuration
            schema_variants: List of schema variants

        Returns:
            Dictionary mapping field names to field configurations
        """
        if not has_conditional_schemas:
            # Return traditional fields
            all_fields = {}
            for field in traditional_fields:
                field_name = field.get("name")
                if field_name:
                    all_fields[field_name] = field
            return all_fields

        # Combine fields from all variants
        all_fields = {}

        # Add flag column first
        if flag_column and flag_column.get("name"):
            all_fields[flag_column["name"]] = flag_column

        # Add fields from all variants
        for variant in schema_variants:
            fields = variant.get("fields", [])
            for field in fields:
                field_name = field.get("name")
                if field_name and field_name not in all_fields:
                    # Store the field with additional metadata about which variants contain it
                    all_fields[field_name] = {
                        **field,
                        "_appears_in_variants": [variant.get("flagValue")]
                    }
                elif field_name and field_name in all_fields:
                    # Add to existing field's variant list
                    if "_appears_in_variants" not in all_fields[field_name]:
                        all_fields[field_name]["_appears_in_variants"] = []
                    all_fields[field_name]["_appears_in_variants"].append(variant.get("flagValue"))

        return all_fields

    @staticmethod
    def get_unified_parquet_schema(
        all_fields: Dict[str, Dict[str, Any]],
        flag_column: Optional[Dict[str, Any]],
        schema_variants: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Get a unified Parquet schema that accommodates all variants.

        Args:
            all_fields: All possible fields from variants
            flag_column: Flag column configuration
            schema_variants: List of schema variants

        Returns:
            Dictionary mapping field names to Parquet types
        """
        unified_schema = {}

        for field_name, field_info in all_fields.items():
            if field_name == flag_column.get("name") if flag_column else None:
                # Flag column
                unified_schema[field_name] = field_info.get("parquetType", "string")
            else:
                # Determine the best unified type for this field
                variants_with_field = field_info.get("_appears_in_variants", [])
                if len(variants_with_field) == len(schema_variants):
                    # Field appears in all variants, use its Parquet type
                    unified_schema[field_name] = field_info.get("parquetType", "string")
                else:
                    # Field doesn't appear in all variants, make it nullable
                    base_type = field_info.get("parquetType", "string")
                    unified_schema[field_name] = base_type  # Parquet handles nullability

        return unified_schema
