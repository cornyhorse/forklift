"""Small PyArrow helpers shared by the transformation modules.

The transformers work on ``pa.Array`` / ``pa.ChunkedArray`` columns without going through pandas.
Nulls are ``None`` (``Array.to_pylist()``), never NaN, so ``pa.array(values, type=...)`` is safe.
"""

from __future__ import annotations

import pyarrow as pa


def is_string_like(arrow_type: pa.DataType) -> bool:
    """Return True for ``string`` and ``large_string`` columns.

    Transformations such as trimming or regex replacement are free to return either flavour
    (kernels keep the input type), so every guard has to accept both. Otherwise the next
    transformation in a chain silently skips the column.
    """
    return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)


def string_array(values, like: pa.DataType) -> pa.Array:
    """Build a string array of the same Arrow type as ``like`` (``string``/``large_string``).

    The explicit ``type=`` matters: an all-null result would otherwise come back as ``null``.
    """
    return pa.array(values, type=like if is_string_like(like) else pa.string())
