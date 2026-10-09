"""Schema-driven column typing for CSV batches.

The JSON schema (through ``CsvSchemaImporter``) declares a target Arrow type for each column.
Both the local Arrow reader and the S3/fallback row reader hand over *raw string* values for
those columns, and :class:`ColumnConverter` turns them into the declared types in one place,
so the same CSV produces the same Parquet schema on every path. Raw strings are essential:
letting Arrow infer a type from the first block turns ``00123`` into ``123``.

Rows holding a value that cannot be converted are split off (failure isolation) and returned as
all-string "rejected" rows so the caller can write them to bad_rows instead of aborting.
"""

from __future__ import annotations

import re
from typing import Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv_csv

# Values Arrow's own CSV reader treats as null in non-string columns
ARROW_DEFAULT_NULL_VALUES: FrozenSet[str] = frozenset(pv_csv.ConvertOptions().null_values)

_CAST_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError)

_SIMPLE_TYPES = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "string": pa.string(),
    "utf8": pa.string(),
    "binary": pa.binary(),
    "date32": pa.date32(),
    "date32[day]": pa.date32(),
    "date64": pa.date64(),
    "date64[ms]": pa.date64(),
}

_TIMESTAMP = re.compile(r"^timestamp\[\s*(s|ms|us|ns)\s*(?:,\s*tz\s*=\s*([^\]]+?)\s*)?\]$")
_DURATION = re.compile(r"^duration\[\s*(s|ms|us|ns)\s*\]$")
_DECIMAL = re.compile(r"^decimal(?:128|256)?\(\s*(\d+)\s*,\s*(-?\d+)\s*\)$")
_DICTIONARY = re.compile(
    r"^dictionary<\s*values\s*=\s*(\w+)\s*,\s*indices\s*=\s*(\w+)\s*(?:,\s*ordered\s*=\s*\d\s*)?>$"
)


def parse_arrow_type(type_str: str) -> Optional[pa.DataType]:
    """Parse a ``parquetTypeMapping`` type name (``int32``, ``decimal128(10,2)``...) into Arrow.

    Args:
        type_str: Type name as written in the schema

    Returns:
        The Arrow type, or None if the name is not understood (``list<...>``, ``struct``...)
    """
    if not isinstance(type_str, str):
        return None
    text = type_str.strip()

    simple = _SIMPLE_TYPES.get(text.lower())
    if simple is not None:
        return simple

    match = _TIMESTAMP.match(text)
    if match:
        return pa.timestamp(match.group(1), tz=match.group(2))

    match = _DURATION.match(text)
    if match:
        return pa.duration(match.group(1))

    match = _DECIMAL.match(text)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        try:
            return pa.decimal128(precision, scale)
        except (pa.ArrowInvalid, ValueError):
            return None

    match = _DICTIONARY.match(text)
    if match:
        value_type = _SIMPLE_TYPES.get(match.group(1).lower())
        index_type = _SIMPLE_TYPES.get(match.group(2).lower())
        if value_type is not None and index_type is not None:
            try:
                return pa.dictionary(index_type, value_type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError):
                return None

    return None


def csv_target_type(arrow_type: Optional[pa.DataType]) -> pa.DataType:
    """Return ``arrow_type`` if CSV text can be converted to it, ``string`` otherwise.

    Nested types (list/struct) and types Arrow cannot build from text (duration, null) keep
    the raw string rather than failing every row.
    """
    if arrow_type is None:
        return pa.string()
    if pa.types.is_string(arrow_type):
        return arrow_type
    try:
        pa.array([], type=pa.string()).cast(arrow_type)
    except _CAST_ERRORS:
        return pa.string()
    return arrow_type


def _is_text(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)


class NullPolicy:
    """Which text values count as null, taken from the schema's ``x-csv.nulls`` block.

    Attributes:
        global_values: Null markers for every column (None when the schema has no ``nulls``)
        per_column: Column specific markers; they replace the global list for that column
    """

    def __init__(
        self,
        global_values: Optional[Iterable[str]] = None,
        per_column: Optional[Mapping[str, Iterable[str]]] = None,
    ):
        self.global_values: Optional[FrozenSet[str]] = (
            frozenset(str(v) for v in global_values) if global_values is not None else None
        )
        self.per_column: Dict[str, FrozenSet[str]] = {
            name: frozenset(str(v) for v in values) for name, values in (per_column or {}).items()
        }

    @property
    def configured(self) -> bool:
        """True if the schema configured null markers."""
        return self.global_values is not None or bool(self.per_column)

    def values_for(self, column: str) -> Optional[FrozenSet[str]]:
        """Null markers for ``column`` or None when the schema says nothing about it."""
        if column in self.per_column:
            return self.per_column[column]
        return self.global_values


class ColumnConverter:
    """Apply schema types and null markers to CSV batches, isolating bad values.

    Args:
        column_types: Target Arrow type per column name (from the schema)
        null_policy: Null markers from the schema, if any
    """

    def __init__(
        self,
        column_types: Optional[Mapping[str, pa.DataType]] = None,
        null_policy: Optional[NullPolicy] = None,
    ):
        self.column_types: Dict[str, pa.DataType] = dict(column_types or {})
        self.null_policy: NullPolicy = null_policy or NullPolicy()

    # ------------------------------------------------------------------ reader options
    def arrow_column_types(self, column_names: Sequence[str]) -> Dict[str, pa.DataType]:
        """``ConvertOptions.column_types``: schema columns are read as raw strings.

        Converting here instead of letting Arrow parse them keeps leading zeros and lets a bad
        value reject one row rather than abort the whole stream.
        """
        return {name: pa.string() for name in column_names if name in self.column_types}

    def arrow_null_values(self) -> Optional[List[str]]:
        """``ConvertOptions.null_values`` (None keeps Arrow's defaults)."""
        values = self.null_policy.global_values
        return sorted(values) if values is not None else None

    def empty_schema(self, column_names: Sequence[str]) -> pa.Schema:
        """Schema for an output without rows: schema types where known, strings otherwise."""
        return pa.schema(
            [pa.field(name, self.column_types.get(name, pa.string())) for name in column_names]
        )

    # ------------------------------------------------------------------ conversion
    def convert(
        self, batch: pa.RecordBatch, established: Optional[pa.Schema] = None
    ) -> Tuple[pa.RecordBatch, Optional[pa.RecordBatch]]:
        """Convert ``batch`` to the schema types.

        Args:
            batch: Batch whose schema columns hold raw strings
            established: Schema the writer already uses. Columns that are not covered by the
                JSON schema are cast to it, so a string-only fallback batch matches batches
                Arrow produced earlier.

        Returns:
            ``(converted, rejected)``. ``converted`` has the rows that converted cleanly.
            ``rejected`` is None or an all-string batch with the raw values of the rows that
            had an unconvertible value.
        """
        names = batch.schema.names
        arrays: List[pa.Array] = []
        failed: Set[int] = set()

        for index, name in enumerate(names):
            column = batch.column(index)
            target = self.column_types.get(name)
            if target is None and established is not None:
                position = established.get_field_index(name)
                if position >= 0:
                    target = established.field(position).type

            if _is_text(column.type):
                column = self._apply_nulls(column, name, target)

            if target is not None and column.type != target:
                column, bad_rows = _cast_isolated(column, target)
                failed.update(bad_rows)
            arrays.append(column)

        converted = pa.RecordBatch.from_arrays(arrays, names=names)
        if not failed:
            return converted, None

        keep = pa.array([row not in failed for row in range(batch.num_rows)], type=pa.bool_())
        rejected = to_string_batch(batch.take(pa.array(sorted(failed), type=pa.int64())))
        return converted.filter(keep), rejected

    def mark_nulls(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Replace the schema's null markers by real nulls in the text columns of ``batch``.

        ``convert`` does this too; calling it first lets a step that rewrites the text (a
        transformation) see NULL where the file said ``NA``, ``-`` or ``0.00``.
        """
        arrays = [
            (
                self._apply_nulls(column, name, self.column_types.get(name))
                if _is_text(column.type)
                else column
            )
            for name, column in zip(batch.schema.names, batch.columns)
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=batch.schema)

    def _apply_nulls(self, column: pa.Array, name: str, target: Optional[pa.DataType]):
        """Replace the schema's null markers by real nulls in a text column."""
        values = self.null_policy.values_for(name)
        if values is None and target is not None and not _is_text(target):
            # Same set Arrow would have used had it parsed the column itself
            values = ARROW_DEFAULT_NULL_VALUES
        if not values:
            return column
        mask = pc.is_in(column, value_set=pa.array(sorted(values), type=column.type))
        return pc.if_else(mask, pa.scalar(None, type=column.type), column)


def _cast_text(array: pa.Array, target: pa.DataType) -> pa.Array:
    """Cast to ``target``; timestamps without time zone also accept ``Z``/offset suffixes."""
    try:
        return array.cast(target)
    except _CAST_ERRORS:
        if (
            pa.types.is_timestamp(target)
            and target.tz is None
            and (_is_text(array.type) or pa.types.is_null(array.type))
        ):
            # "2024-01-15T10:30:00Z" / "+02:00": keep the UTC wall time
            return array.cast(pa.timestamp(target.unit, tz="UTC")).cast(target)
        raise


def _cast_isolated(
    array: pa.Array, target: pa.DataType, offset: int = 0, bad: Optional[List[int]] = None
) -> Tuple[pa.Array, List[int]]:
    """Cast ``array`` to ``target``, nulling out values that cannot be converted.

    The whole array is tried first; only when that fails is it split in halves until the
    offending values are isolated, so clean batches cost a single cast.

    Returns:
        ``(array, bad_positions)`` where bad positions index into the original array. The
        values at those positions are null in the returned array.
    """
    if bad is None:
        bad = []
    try:
        return _cast_text(array, target), bad
    except _CAST_ERRORS:
        if len(array) <= 1:
            bad.append(offset)
            return pa.nulls(len(array), type=target), bad

    middle = len(array) // 2
    left, _ = _cast_isolated(array.slice(0, middle), target, offset, bad)
    right, _ = _cast_isolated(array.slice(middle), target, offset + middle, bad)
    return pa.concat_arrays([left, right]), bad


def to_string_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Return ``batch`` with every column as ``string`` (how rejected rows are stored)."""
    arrays = []
    for column in batch.columns:
        if pa.types.is_string(column.type):
            arrays.append(column)
            continue
        try:
            arrays.append(column.cast(pa.string()))
        except _CAST_ERRORS:
            arrays.append(
                pa.array([None if v is None else str(v) for v in column.to_pylist()], pa.string())
            )
    return pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)
