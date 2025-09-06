"""Parquet mapping utilities."""

from __future__ import annotations
from typing import Any, Dict, List

from ..validation.parquet_types import ParquetTypeValidator


class ParquetMappingUtils:
    """Utilities for Parquet type mapping and schema operations."""

    @staticmethod
    def validate_parquet_types_in_fields(fields: List[Dict[str, Any]]) -> List[str]:
        """Validate Parquet type mappings in field configurations.

        Args:
            fields: List of field configurations to validate

        Returns:
            List of validation error messages
        """
        errors = []

        for i, field in enumerate(fields):
            if isinstance(field, dict):
                parquet_type = field.get("parquetType")
                if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
                    errors.append(f"Field {i} invalid Parquet type '{parquet_type}'")

        return errors

    @staticmethod
    def validate_parquet_types_in_variants(schema_variants: List[Dict[str, Any]]) -> List[str]:
        """Validate Parquet types in conditional schema variants.

        Args:
            schema_variants: List of schema variant configurations

        Returns:
            List of validation error messages
        """
        errors = []

        for i, variant in enumerate(schema_variants):
            fields = variant.get("fields", [])
            for j, field in enumerate(fields):
                if isinstance(field, dict):
                    parquet_type = field.get("parquetType")
                    if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
                        errors.append(f"Variant {i} field {j} invalid Parquet type '{parquet_type}'")

        return errors
