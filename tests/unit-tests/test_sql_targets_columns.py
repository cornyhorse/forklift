"""The source's columns for ``forklift.outputs.sql``: kinds, key columns and bound values."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest

from forklift.outputs.sql.columns import SourceColumn, column_kind, source_columns, to_parameters
from forklift.outputs.sql.errors import TableWriteError


@pytest.mark.parametrize(
    "arrow_type, kind",
    [
        (pa.bool_(), "boolean"),
        (pa.int8(), "integer"),
        (pa.uint64(), "integer"),
        (pa.float16(), "float"),
        (pa.float64(), "float"),
        (pa.decimal128(10, 2), "decimal"),
        (pa.decimal256(50, 2), "decimal"),
        (pa.string(), "string"),
        (pa.large_string(), "string"),
        (pa.string_view(), "string"),
        (pa.binary(), "binary"),
        (pa.large_binary(), "binary"),
        (pa.binary(4), "binary"),
        (pa.binary_view(), "binary"),
        (pa.date32(), "date"),
        (pa.date64(), "date"),
        (pa.timestamp("ns"), "timestamp"),
        (pa.timestamp("us", tz="UTC"), "timestamp_tz"),
        (pa.time32("s"), "time"),
        (pa.time64("ns"), "time"),
        (pa.dictionary(pa.int8(), pa.string()), "string"),
        (pa.list_(pa.int8()), None),
        (pa.struct([("a", pa.int8())]), None),
        (pa.duration("s"), None),
        (pa.null(), None),
    ],
)
def test_column_kind(arrow_type, kind):
    assert column_kind(arrow_type) == kind


class TestSourceColumns:
    def test_columns_keep_the_schema_order_and_mark_keys(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                ("name", pa.dictionary(pa.int32(), pa.string())),
            ]
        )

        columns = source_columns(schema, ["id"])

        assert [(c.name, c.kind, c.nullable, c.key) for c in columns] == [
            ("id", "integer", False, True),
            ("name", "string", True, False),
        ]
        assert columns[1].arrow_type == pa.string()  # the dictionary's value type

    def test_unsupported_types_are_all_named(self):
        schema = pa.schema([("tags", pa.list_(pa.string())), ("ok", pa.int8()), ("x", pa.null())])

        with pytest.raises(TableWriteError) as raised:
            source_columns(schema, [])

        assert "'tags' (list<item: string>)" in str(raised.value)
        assert "'x' (null)" in str(raised.value)
        assert "'ok'" not in str(raised.value)

    def test_names_that_differ_only_in_case_are_refused(self):
        with pytest.raises(TableWriteError, match="'Id' and 'ID'"):
            source_columns(pa.schema([("Id", pa.int8()), ("ID", pa.int8())]), [])

    def test_key_columns_must_be_in_the_source(self):
        with pytest.raises(TableWriteError, match="'code', 'region' are not in the source"):
            source_columns(pa.schema([("id", pa.int8())]), ["code", "region"])


def _values(array, kind, *, decimal_integers=False, timestamps_as_text=False):
    warnings = []
    column = SourceColumn("c", kind, array.type)
    values = to_parameters(
        array,
        column,
        decimal_integers=decimal_integers,
        timestamps_as_text=timestamps_as_text,
        warn=warnings.append,
    )
    return values, warnings


class TestToParameters:
    def test_small_integers_stay_integers(self):
        assert _values(pa.array([1, None], pa.int16()), "integer", decimal_integers=True) == (
            [1, None],
            [],
        )

    @pytest.mark.parametrize("arrow_type", [pa.int32(), pa.int64(), pa.uint32()])
    def test_wide_integers_become_decimals_where_the_driver_needs_it(self, arrow_type):
        values, _ = _values(
            pa.array([-5, None], pa.int64()).cast(arrow_type, safe=False), "integer"
        )
        assert all(not isinstance(v, Decimal) for v in values)

        values, _ = _values(pa.array([5, None], arrow_type), "integer", decimal_integers=True)
        assert values == [Decimal(5), None] and isinstance(values[0], Decimal)

    def test_unsigned_64_bit_integers_are_always_decimals(self):
        values, _ = _values(pa.array([2**64 - 1], pa.uint64()), "integer")
        assert values == [Decimal(2**64 - 1)]

    def test_half_floats_become_python_floats(self):
        # (pyarrow before 19 builds half floats only from numpy.float16 values)
        half = pa.array([1.5, None], pa.float32()).cast(pa.float16())
        values, _ = _values(half, "float")
        assert values == [1.5, None] and type(values[0]) is float

    def test_dictionary_arrays_are_decoded(self):
        array = pa.array(["a", "b", "a"]).dictionary_encode()
        assert _values(array, "string")[0] == ["a", "b", "a"]

    def test_time_zone_aware_timestamps_become_utc(self):
        moment = dt.datetime(2021, 6, 30, 14, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        array = pa.array([moment, None], pa.timestamp("ms", tz="+02:00"))

        assert _values(array, "timestamp_tz")[0] == [dt.datetime(2021, 6, 30, 12, 0), None]
        assert _values(array, "timestamp_tz", timestamps_as_text=True)[0] == [
            "2021-06-30 12:00:00.000000",
            None,
        ]

    def test_nanoseconds_are_truncated_with_a_warning_without_values(self):
        array = pa.array([1_000_001_234], pa.timestamp("ns"))

        values, warnings = _values(array, "timestamp")

        assert values == [dt.datetime(1970, 1, 1, 0, 0, 1, 1)]
        assert warnings == [
            "Column 'c' has values finer than microseconds; they were truncated to microseconds"
        ]
        assert "1234" not in warnings[0]

    def test_times_are_bound_as_text(self):
        array = pa.array([dt.time(1, 2, 3), None], pa.time32("s"))
        assert _values(array, "time")[0] == ["01:02:03.000000", None]

        nanos = pa.array([3_723_000_000_789], pa.time64("ns"))
        assert _values(nanos, "time") == (
            ["01:02:03.000000"],
            ["Column 'c' has values finer than microseconds; they were truncated to microseconds"],
        )
