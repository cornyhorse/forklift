"""Common transformation functions for basic string operations."""

import pyarrow as pa
import pyarrow.compute as pc


def trim_whitespace(column: pa.Array) -> pa.Array:
    """Remove leading and trailing whitespace from string column.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with whitespace trimmed from string values
    """
    if pa.types.is_string(column.type):
        return pc.utf8_trim_whitespace(column)
    return column


def uppercase(column: pa.Array) -> pa.Array:
    """Convert string column to uppercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to uppercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_upper(column)
    return column


def lowercase(column: pa.Array) -> pa.Array:
    """Convert string column to lowercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to lowercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_lower(column)
    return column