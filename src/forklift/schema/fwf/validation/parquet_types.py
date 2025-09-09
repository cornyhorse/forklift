"""Parquet type validation and utilities."""

from __future__ import annotations
from typing import List, Set


class ParquetTypeValidator:
    """Validates Parquet data types."""

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES: Set[str] = {
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "double", "bool", "string", "binary",
        "date32", "date64", "timestamp[s]", "timestamp[ms]",
        "timestamp[us]", "timestamp[ns]", "duration[s]", "duration[ms]",
        "duration[us]", "duration[ns]", "decimal128(10,2)",
        "list<string>", "struct", "dictionary<values=string, indices=int32>"
    }

    @classmethod
    def is_valid_parquet_type(cls, parquet_type: str) -> bool:
        """Check if a Parquet type is valid.

        Args:
            parquet_type: The Parquet type string to validate

        Returns:
            True if the type is valid, False otherwise
        """
        if parquet_type in cls.SUPPORTED_PARQUET_TYPES:
            return True

        # Check for parameterized types like decimal128(precision,scale)
        if parquet_type.startswith("decimal128(") and parquet_type.endswith(")"):
            return True

        # Check for timestamp with timezone
        if parquet_type.startswith("timestamp[") and parquet_type.endswith("]"):
            return True

        # Check for duration types
        if parquet_type.startswith("duration[") and parquet_type.endswith("]"):
            return True

        # Check for list types
        if parquet_type.startswith("list<") and parquet_type.endswith(">"):
            return True

        # Check for dictionary types
        if parquet_type.startswith("dictionary<") and parquet_type.endswith(">"):
            return True

        return False

    @classmethod
    def are_types_compatible(cls, types: List[str]) -> bool:
        """Check if different Parquet types are compatible for the same logical field.

        Args:
            types: List of Parquet type strings to check for compatibility

        Returns:
            True if all types are compatible, False otherwise
        """
        if len(set(types)) == 1:
            return True

        # Define compatibility groups
        numeric_types = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "double"}
        temporal_types = {"date32", "date64", "timestamp[s]", "timestamp[ms]", "timestamp[us]", "timestamp[ns]"}
        duration_types = {"duration[s]", "duration[ms]", "duration[us]", "duration[ns]"}
        string_types = {"string", "binary"}

        # Check if all types belong to the same compatibility group
        types_set = set(types)

        if types_set.issubset(numeric_types):
            return True
        elif types_set.issubset(temporal_types):
            return True
        elif types_set.issubset(duration_types):
            return True
        elif types_set.issubset(string_types):
            return True

        # Special cases for decimal types
        decimal_types = [t for t in types if t.startswith("decimal128")]
        if len(decimal_types) == len(types):
            return True  # All decimal types are compatible

        return False
