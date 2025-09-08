"""Type analysis utilities for determining data types and characteristics."""

from __future__ import annotations
import pyarrow as pa


class TypeAnalyzer:
    """Handles data type analysis and classification."""

    @staticmethod
    def is_numeric_type(data_type: pa.DataType) -> bool:
        """Check if data type is numeric."""
        return pa.types.is_integer(data_type) or pa.types.is_floating(data_type)

    @staticmethod
    def is_string_type(data_type: pa.DataType) -> bool:
        """Check if data type is string-like."""
        return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)

    @staticmethod
    def is_temporal_type(data_type: pa.DataType) -> bool:
        """Check if data type is temporal."""
        return (pa.types.is_date(data_type) or
                pa.types.is_timestamp(data_type) or
                pa.types.is_time(data_type))
