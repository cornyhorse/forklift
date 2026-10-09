"""Helpers that give the same result on every supported pyarrow version."""

from __future__ import annotations

from typing import Union

import pyarrow as pa
import pyarrow.compute as pc

ArrowValues = Union[pa.Array, pa.ChunkedArray]


def set_null_where(values: ArrowValues, mask: ArrowValues) -> ArrowValues:
    """Return ``values`` with every position where ``mask`` is true replaced by NULL.

    Do not write ``pc.if_else(mask, pa.scalar(None, type), values)`` for this: on pyarrow 16 to
    22 it returns wrong data for a *sliced* string array (``'\\x00'`` instead of the value, which
    reaches the output without an error), and the engine hands sliced arrays to its converters
    whenever a block of the input has more rows than ``batch_size``. A null array of the same
    length gives the right result on every version.

    Args:
        values: Array or chunked array of any type
        mask: Boolean array (chunked if ``values`` is) of the same length; NULL counts as false

    Returns:
        Array or chunked array of the same type as ``values``
    """
    if isinstance(values, pa.ChunkedArray):
        nulls: ArrowValues = pa.chunked_array(
            [pa.nulls(len(values), values.type)], type=values.type
        )
    else:
        nulls = pa.nulls(len(values), values.type)
    return pc.if_else(mask, nulls, values)
