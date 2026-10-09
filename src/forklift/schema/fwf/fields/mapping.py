"""Field mapping utilities for FWF schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...types.data_types import unify_parquet_types
from ..exceptions import ConditionalSchemaError


class FieldMapper:
    """Handles field mapping and unified schema generation."""

    @staticmethod
    def get_all_possible_fields(
        has_conditional_schemas: bool,
        traditional_fields: List[Dict[str, Any]],
        flag_column: Optional[Dict[str, Any]],
        schema_variants: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Get all possible fields from all schema variants combined.

        The returned dictionaries are copies: the caller's schema is never modified.

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
                    all_fields[field_name] = dict(field)
            return all_fields

        # Combine fields from all variants
        all_fields = {}
        appears_in: Dict[str, List[Any]] = {}

        # Add flag column first
        if flag_column and flag_column.get("name"):
            all_fields[flag_column["name"]] = dict(flag_column)

        # Add fields from all variants
        for variant in schema_variants:
            flag_value = variant.get("flagValue")
            for field in variant.get("fields", []):
                field_name = field.get("name")
                if not field_name:
                    continue
                if field_name not in all_fields:
                    all_fields[field_name] = dict(field)
                # Remember which variants contain the field
                variants_with_field = appears_in.setdefault(field_name, [])
                if flag_value not in variants_with_field:
                    variants_with_field.append(flag_value)

        for field_name, variants_with_field in appears_in.items():
            all_fields[field_name]["_appears_in_variants"] = variants_with_field

        return all_fields

    @staticmethod
    def get_unified_parquet_schema(
        all_fields: Dict[str, Dict[str, Any]],
        flag_column: Optional[Dict[str, Any]],
        schema_variants: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Get a unified Parquet schema that accommodates all variants.

        A field that several variants declare with different (but compatible) Parquet types gets
        the common wider type, e.g. ``int32`` and ``double`` unify to ``double``; incompatible
        types raise. A field missing from some variants keeps its type (Parquet columns are
        nullable).

        Args:
            all_fields: All possible fields from variants
            flag_column: Flag column configuration
            schema_variants: List of schema variants

        Returns:
            Dictionary mapping field names to Parquet types

        Raises:
            ConditionalSchemaError: if the variants declare incompatible types for one field
        """
        unified_schema = {}
        flag_name = flag_column.get("name") if flag_column else None

        for field_name, field_info in all_fields.items():
            if flag_name and field_name == flag_name:
                # Flag column
                unified_schema[field_name] = field_info.get("parquetType", "string")
                continue

            # Every variant's declaration of this field
            declared_types = [
                field.get("parquetType", "string")
                for variant in schema_variants
                for field in variant.get("fields", [])
                if field.get("name") == field_name
            ] or [field_info.get("parquetType", "string")]

            unified_type = unify_parquet_types(declared_types)
            if unified_type is None:
                raise ConditionalSchemaError(
                    f"Field '{field_name}' has incompatible Parquet types across variants: "
                    f"{sorted(set(map(str, declared_types)))}"
                )
            unified_schema[field_name] = unified_type

        return unified_schema
