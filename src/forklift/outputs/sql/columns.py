"""The source's columns: what kind of value each holds and how its values are bound.

Every Arrow type forklift writes falls into one *kind* (``boolean``, ``integer``, ``float``,
``decimal``, ``string``, ``binary``, ``date``, ``timestamp``, ``timestamp_tz``, ``time``); the
dialects map kinds to column types and placeholders. Values are converted column by column with
Arrow compute (casts), then handed to pyodbc as Python objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

import pyarrow as pa

from .errors import TableWriteError

KINDS = (
    "boolean",
    "integer",
    "float",
    "decimal",
    "string",
    "binary",
    "date",
    "timestamp",
    "timestamp_tz",
    "time",
)


@dataclass
class SourceColumn:
    """One column of the source and how it is written.

    Attributes:
        name: The Arrow field name
        kind: One of :data:`KINDS`
        arrow_type: The field's type (the value type for dictionary-encoded fields)
        nullable: False when the Arrow field is declared non-nullable
        key: True for a key column (upsert, or the primary key of a table forklift creates)
        sql_name: The column's name in the database (the table's spelling for an existing table)
        ddl: The column type in a staging table or a table forklift creates
        placeholder: How a value is bound in ``VALUES`` (``?`` or a cast around it)
        expression: Oracle only: the expression over the bound value (``{}`` is the value)
        load_type: The existing column's type when the staging column must use it (or None)
    """

    name: str
    kind: str
    arrow_type: pa.DataType
    nullable: bool = True
    key: bool = False
    sql_name: str = ""
    ddl: str = ""
    placeholder: str = "?"
    expression: str = "{}"
    load_type: Optional[str] = None


def column_kind(arrow_type: pa.DataType) -> Optional[str]:
    """The kind of value an Arrow type holds, or ``None`` when forklift cannot write it."""
    types = pa.types
    if types.is_dictionary(arrow_type):
        return column_kind(arrow_type.value_type)
    if types.is_boolean(arrow_type):
        return "boolean"
    if types.is_integer(arrow_type):
        return "integer"
    if types.is_floating(arrow_type):
        return "float"
    if types.is_decimal(arrow_type):
        return "decimal"
    if types.is_string(arrow_type) or types.is_large_string(arrow_type):
        return "string"
    if types.is_string_view(arrow_type):
        return "string"
    if types.is_binary(arrow_type) or types.is_large_binary(arrow_type):
        return "binary"
    if types.is_fixed_size_binary(arrow_type) or types.is_binary_view(arrow_type):
        return "binary"
    if types.is_date(arrow_type):
        return "date"
    if types.is_timestamp(arrow_type):
        return "timestamp_tz" if arrow_type.tz else "timestamp"
    if types.is_time(arrow_type):
        return "time"
    return None


def source_columns(schema: pa.Schema, key_columns: Sequence[str]) -> List[SourceColumn]:
    """Describe the source's columns; key columns are marked (and must be in the source).

    Raises:
        TableWriteError: A column has a type forklift cannot write to a table, two columns
            have the same name ignoring case, or a key column is not in the source
    """
    columns: List[SourceColumn] = []
    unsupported = []
    seen = {}
    for field in schema:
        kind = column_kind(field.type)
        if kind is None:
            unsupported.append(f"{field.name!r} ({field.type})")
            continue
        folded = field.name.casefold()
        if folded in seen:
            raise TableWriteError(
                f"The source has two columns named {seen[folded]!r} and {field.name!r}; "
                "databases compare column names without regard to case, so rename one of them"
            )
        seen[folded] = field.name
        value_type = field.type.value_type if pa.types.is_dictionary(field.type) else field.type
        columns.append(SourceColumn(field.name, kind, value_type, nullable=field.nullable))
    if unsupported:
        raise TableWriteError(
            "These source columns have types forklift cannot write to a database table: "
            + ", ".join(unsupported)
            + ". Supported are booleans, integers, floats, decimals, strings, binary, dates, "
            "timestamps and times."
        )
    by_name = {column.name: column for column in columns}
    missing = [name for name in key_columns if name not in by_name]
    if missing:
        raise TableWriteError(
            f"Key column(s) {', '.join(repr(name) for name in missing)} are not in the source; "
            f"its columns are {', '.join(repr(column.name) for column in columns)}"
        )
    for name in key_columns:
        by_name[name].key = True
    return columns


def _microseconds(
    array: pa.Array, target: pa.DataType, column: SourceColumn, warn: Callable[[str], None]
) -> pa.Array:
    """Cast a temporal array to microseconds (dropping any time zone; values stay UTC).

    Python's datetime and time hold microseconds, so finer values are truncated, with a
    warning that names the column (never the values).
    """
    try:
        return array.cast(target)
    except pa.ArrowInvalid:
        warn(
            f"Column {column.name!r} has values finer than microseconds; they were truncated "
            "to microseconds"
        )
        return array.cast(target, safe=False)


def to_parameters(
    array: pa.Array,
    column: SourceColumn,
    *,
    decimal_integers: bool,
    timestamps_as_text: bool,
    warn: Callable[[str], None],
) -> list:
    """The values of one column as the Python objects pyodbc binds.

    Args:
        array: The column of one record batch
        column: How the column is written
        decimal_integers: Bind 32- and 64-bit integers as ``Decimal`` (Oracle's driver rejects
            64-bit integer parameters, which pyodbc uses from -2**31 on); 64-bit unsigned
            integers always are, since they may not fit a signed 64-bit parameter
        timestamps_as_text: Bind timestamps as ``YYYY-MM-DD HH:MM:SS.ffffff`` text (MySQL's and
            Oracle's drivers drop the fractional seconds of timestamp parameters)
        warn: Called with a warning (precision lost), which never holds values

    Times are always bound as ``HH:MM:SS.ffffff`` text (ODBC's time parameters have no
    fractional seconds); the dialects cast them back on the server.
    """
    if pa.types.is_dictionary(array.type):
        array = array.dictionary_decode()
    kind = column.kind
    if kind == "integer":
        unsigned64 = array.type == pa.uint64()
        # pyodbc binds -2**31 and anything wider as a 64-bit parameter
        wide = array.type.bit_width >= 32
        if unsigned64 or (decimal_integers and wide):
            array = array.cast(pa.decimal128(20, 0))
    elif kind == "float" and array.type == pa.float16():
        array = array.cast(pa.float32())
    elif kind in ("timestamp", "timestamp_tz"):
        array = _microseconds(array, pa.timestamp("us"), column, warn)
        if timestamps_as_text:
            array = array.cast(pa.string())
    elif kind == "time":
        array = _microseconds(array, pa.time64("us"), column, warn).cast(pa.string())
    return array.to_pylist()
