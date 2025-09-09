"""Compatibility validation for schema variants."""

from __future__ import annotations
from typing import Any, Dict, List, Tuple

from .parquet_types import ParquetTypeValidator


class CompatibilityValidator:
    """Validates compatibility between different schema variants."""

    @staticmethod
    def validate_schema_compatibility(schema_variants: List[Dict[str, Any]]) -> List[str]:
        """Validate compatibility between different schema variants.

        Args:
            schema_variants: List of schema variant configurations

        Returns:
            List of validation error messages
        """
        errors = []

        # Collect all fields from all variants
        all_fields = {}  # field_name -> list of field definitions

        for i, variant in enumerate(schema_variants):
            fields = variant.get("fields", [])
            for field in fields:
                field_name = field.get("name")
                if field_name:
                    if field_name not in all_fields:
                        all_fields[field_name] = []
                    all_fields[field_name].append((i, field))

        # Check for compatibility issues
        for field_name, field_defs in all_fields.items():
            if len(field_defs) > 1:  # Field appears in multiple variants
                errors.extend(CompatibilityValidator._validate_field_compatibility(field_name, field_defs))

        return errors

    @staticmethod
    def _validate_field_compatibility(field_name: str, field_defs: List[Tuple[int, Dict[str, Any]]]) -> List[str]:
        """Validate compatibility of a field across multiple schema variants.

        Args:
            field_name: Name of the field being validated
            field_defs: List of (variant_index, field_definition) tuples

        Returns:
            List of validation error messages
        """
        errors = []

        # Get Parquet types for this field across variants
        parquet_types = set()
        position_ranges = []

        for variant_idx, field_def in field_defs:
            parquet_type = field_def.get("parquetType")
            if parquet_type:
                parquet_types.add(parquet_type)

            start = field_def.get("start")
            length = field_def.get("length")
            if isinstance(start, int) and isinstance(length, int):
                position_ranges.append((variant_idx, start, start + length - 1))

        # Check Parquet type compatibility
        if len(parquet_types) > 1:
            compatible = ParquetTypeValidator.are_types_compatible(list(parquet_types))
            if not compatible:
                errors.append(f"Field '{field_name}' has incompatible Parquet types across variants: {list(parquet_types)}")

        # Check for overlapping positions that might cause issues
        for i, (v1_idx, v1_start, v1_end) in enumerate(position_ranges):
            for v2_idx, v2_start, v2_end in position_ranges[i+1:]:
                # Check if positions overlap in a way that could cause data corruption
                if CompatibilityValidator._positions_overlap_incompatibly(v1_start, v1_end, v2_start, v2_end):
                    parquet_type_1 = next((field_def.get("parquetType") for variant_idx, field_def in field_defs if variant_idx == v1_idx), None)
                    parquet_type_2 = next((field_def.get("parquetType") for variant_idx, field_def in field_defs if variant_idx == v2_idx), None)

                    if parquet_type_1 and parquet_type_2 and not ParquetTypeValidator.are_types_compatible([parquet_type_1, parquet_type_2]):
                        errors.append(f"Field '{field_name}' has incompatible overlapping positions and types between variants {v1_idx} and {v2_idx}")

        return errors

    @staticmethod
    def _positions_overlap_incompatibly(start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two position ranges overlap in an incompatible way.

        Args:
            start1: Start position of first range
            end1: End position of first range
            start2: Start position of second range
            end2: End position of second range

        Returns:
            True if the ranges overlap incompatibly
        """
        # Check for any overlap
        return not (end1 < start2 or end2 < start1)
