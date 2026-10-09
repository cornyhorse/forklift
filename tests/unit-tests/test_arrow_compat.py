"""``set_null_where`` must be right for sliced arrays on every supported pyarrow version.

``pc.if_else(mask, <null scalar>, sliced_string_array)`` returns ``'\\x00'`` for the masked-out
neighbours on pyarrow 16 - 22; these tests fail there if the helper is replaced by that call.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from forklift.utils.arrow_compat import set_null_where

VALUES = [str(i) if i % 4 else "NA" for i in range(24)]


def expected(values):
    return [None if v == "NA" else v for v in values]


@pytest.mark.parametrize("offset", [0, 1, 3, 5, 11])
@pytest.mark.parametrize("length", [1, 6, 12])
def test_sliced_string_array(offset, length):
    sliced = pa.array(VALUES).slice(offset, length)
    mask = pc.is_in(sliced, value_set=pa.array(["NA"]))

    result = set_null_where(sliced, mask)

    assert result.to_pylist() == expected(sliced.to_pylist())
    assert result.type == pa.string()


def test_sliced_record_batch_column():
    batch = pa.RecordBatch.from_pydict({"x": VALUES}).slice(7, 9)
    column = batch.column(0)

    result = set_null_where(column, pc.is_in(column, value_set=pa.array(["NA"])))

    assert result.to_pylist() == expected(VALUES[7:16])


def test_chunked_array_keeps_its_type_and_values():
    chunked = pa.chunked_array([pa.array(VALUES[:10]), pa.array(VALUES[10:]).slice(2)])
    mask = pc.is_in(chunked, value_set=pa.array(["NA"]))

    result = set_null_where(chunked, mask)

    assert isinstance(result, pa.ChunkedArray)
    assert result.to_pylist() == expected(VALUES[:10] + VALUES[12:])


@pytest.mark.parametrize(
    "array",
    [
        pa.array([1, 2, 3, 4], pa.int64()),
        pa.array([1.5, 2.5, 3.5, 4.5]),
        pa.array([True, False, True, False]),
        pa.array(["a", "b", "c", "d"], pa.large_string()),
        pa.array([[1], [2], [3], [4]]),
    ],
)
def test_other_types(array):
    sliced = array.slice(1, 3)
    mask = pa.array([True, False, True])

    result = set_null_where(sliced, mask)

    values = sliced.to_pylist()
    assert result.to_pylist() == [None, values[1], None]
    assert result.type == sliced.type


def test_nothing_masked_returns_the_values():
    array = pa.array(["a", "b", "c"]).slice(1)

    assert set_null_where(array, pa.array([False, False])).to_pylist() == ["b", "c"]


def test_empty_array():
    result = set_null_where(pa.array([], pa.string()), pa.array([], pa.bool_()))

    assert len(result) == 0 and result.type == pa.string()
