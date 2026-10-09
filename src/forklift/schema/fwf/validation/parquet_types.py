"""Parquet type validation and utilities."""

from __future__ import annotations

from typing import List, Set

from ...types.data_types import is_valid_parquet_type, unify_parquet_types


class ParquetTypeValidator:
    """Validates Parquet data types."""

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES: Set[str] = {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "double",
        "bool",
        "string",
        "binary",
        "date32",
        "date64",
        "timestamp[s]",
        "timestamp[ms]",
        "timestamp[us]",
        "timestamp[ns]",
        "duration[s]",
        "duration[ms]",
        "duration[us]",
        "duration[ns]",
        "decimal128(10,2)",
        "list<string>",
        "struct",
        "dictionary<values=string, indices=int32>",
    }

    @classmethod
    def is_valid_parquet_type(cls, parquet_type: str) -> bool:
        """Check if a Parquet type is valid.

        Delegates to the strict parser shared by all schema importers, so units, precision/scale
        and nested types are checked, not just the type-name prefix.

        Args:
            parquet_type: The Parquet type string to validate

        Returns:
            True if the type is valid, False otherwise
        """
        return is_valid_parquet_type(parquet_type)

    @classmethod
    def are_types_compatible(cls, types: List[str]) -> bool:
        """Check if different Parquet types are compatible for the same logical field.

        Types are compatible when they can be unified into one common type (see
        ``unify_parquet_types``): all numeric, all decimal, all date/timestamp (same time zone),
        all duration, or all string/binary types.

        Args:
            types: List of Parquet type strings to check for compatibility

        Returns:
            True if all types are compatible, False otherwise
        """
        if not types:
            return True
        return unify_parquet_types(types) is not None
