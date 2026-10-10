"""What differs between PostgreSQL, MySQL/MariaDB, SQL Server and Oracle when writing a table.

A :class:`Dialect` knows a database's identifier rules, column types, catalog queries, error
codes and the SQL of every step :func:`~forklift.outputs.sql.write_table` takes. It only builds
SQL text; the writer runs it. Identifiers are always quoted (with the quote character doubled,
or rejected where the database cannot quote it), and values are always bound as parameters.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple

import pyarrow as pa

from ...engine.exceptions import SPEC_INVALID
from .columns import SourceColumn
from .errors import DriverCodes, TableWriteError

NUMBERS = frozenset({"integer", "decimal", "float"})
TEMPORAL = frozenset({"date", "timestamp", "timestamp_tz"})
# Which source kinds a column of each kind accepts (a dialect may widen the set per column)
ACCEPTS: Dict[str, FrozenSet[str]] = {
    "boolean": frozenset({"boolean"}),
    "integer": frozenset({"integer"}),
    "decimal": NUMBERS,
    "float": NUMBERS,
    "string": frozenset({"string"}),
    "binary": frozenset({"binary"}),
    "date": frozenset({"date"}),
    "timestamp": TEMPORAL,
    "timestamp_tz": TEMPORAL,
    "time": frozenset({"time"}),
    # Types forklift does not know (uuid, json, enum, xml, ...): the database converts text
    "other": frozenset({"string"}),
}


@dataclass(frozen=True)
class ExistingColumn:
    """A column of an existing table, as the catalog describes it.

    Attributes:
        name: The column's name as the catalog spells it
        type_name: Its type, for messages
        kind: The kind of value it holds (see :data:`ACCEPTS`), ``other`` when unknown
        accepts: The source kinds that may be written to it
        required: NOT NULL without a default: every row must give it a value
        writable: False for computed, generated and identity columns forklift may not set
        load_type: The type a staging column must have for values to reach this column
            (PostgreSQL types forklift does not know; Oracle LOBs), or ``None``
    """

    name: str
    type_name: str
    kind: str
    accepts: FrozenSet[str]
    required: bool = False
    writable: bool = True
    load_type: Optional[str] = None


def _truthy(value) -> bool:
    """Catalog flags arrive as bools, numbers or strings depending on the driver."""
    if isinstance(value, str):
        return value.strip().lower() in ("1", "t", "true", "y", "yes")
    return bool(value)


@dataclass
class Dialect:
    """SQL for one kind of database.

    Subclasses fill in the differences and define ``existing_column(row)`` (a catalog row of
    ``columns_sql`` as an :class:`ExistingColumn`), ``column_type(column)`` (the type of a new
    column), ``upsert_rows_sql(...)`` (upsert without staging) and, where DDL is not
    transactional, ``rename_table_sql(schema, old, new)`` and ``rename_privilege(schema)``.
    """

    name: str = ""
    label: str = ""
    #: Longest identifier, and whether the limit counts UTF-8 bytes (else characters)
    max_identifier: int = 128
    identifier_bytes: bool = False
    #: DDL inside a transaction is rolled back with it (PostgreSQL, SQL Server)
    transactional_ddl: bool = True
    #: Insert one row per statement with pyodbc's fast_executemany (parameter arrays)
    fast_executemany: bool = False
    #: Rows per multi-row INSERT statement, and the bound parameters one statement may hold
    rows_per_statement: int = 500
    max_parameters: int = 30000
    #: Driver quirks (see forklift.outputs.sql.columns.to_parameters)
    decimal_integers: bool = False
    timestamps_as_text: bool = False
    #: Upsert statements without staging fail when one statement holds a key twice
    upsert_needs_unique_rows: bool = False
    #: Upsert without staging needs a unique index on the key columns
    upsert_needs_unique_index: bool = False
    session_statements: Tuple[str, ...] = ()
    #: Send text parameters as UTF-8 (narrow) instead of UTF-16 (MariaDB Connector/ODBC
    #: corrupts longer UTF-16 parameters)
    utf8_parameters: bool = False
    codes: DriverCodes = field(default_factory=lambda: DriverCodes({}))

    # ------------------------------------------------------------------ identifiers

    def quote(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def qualified(self, schema: str, name: str) -> str:
        return f"{self.quote(schema)}.{self.quote(name)}"

    def new_name(self, name: str) -> str:
        """The name a new table or column gets (Oracle upper-cases plain lower-case names)."""
        return name

    def check_identifier(self, name: object, what: str) -> str:
        """Reject names that cannot be a quoted identifier on this database.

        Raises:
            TableWriteError: Empty, not a string, control characters, leading or trailing
                spaces, a character the database cannot quote, or longer than its limit
        """
        if not isinstance(name, str) or not name:
            raise TableWriteError(
                f"Invalid {what} name: it must be a non-empty string", error_code=SPEC_INVALID
            )
        if any(unicodedata.category(ch) == "Cc" for ch in name):
            raise TableWriteError(
                f"Invalid {what} name {name!r}: it contains control characters",
                error_code=SPEC_INVALID,
            )
        if name != name.strip():
            raise TableWriteError(
                f"Invalid {what} name {name!r}: it starts or ends with spaces",
                error_code=SPEC_INVALID,
            )
        self._check_characters(name, what)
        length = len(name.encode("utf-8")) if self.identifier_bytes else len(name)
        if length > self.max_identifier:
            unit = "bytes" if self.identifier_bytes else "characters"
            raise TableWriteError(
                f"Invalid {what} name {name!r}: {self.label} allows at most "
                f"{self.max_identifier} {unit} ({length} given)",
                error_code=SPEC_INVALID,
            )
        return name

    def _check_characters(self, name: str, what: str) -> None:
        """Characters a quoted identifier may not contain (none by default)."""

    # ------------------------------------------------------------------ privileges

    def create_privilege(self, schema: str) -> str:
        return f"CREATE on schema {schema}"

    def drop_privilege(self, schema: str) -> str:
        return "ownership of the table (only its owner may drop it)"

    # ------------------------------------------------------------------ catalog

    current_schema_sql: str = ""
    schema_sql: str = ""
    tables_sql: str = ""
    columns_sql: str = ""
    unique_keys_sql: str = ""

    # ------------------------------------------------------------------ column types

    def bind(self, column: SourceColumn) -> Tuple[str, str]:
        """The placeholder of a column's values and (Oracle) the expression over it."""
        if column.kind == "time":
            return "CAST(? AS TIME(6))", "{}"
        return "?", "{}"

    def _decimal(self, arrow_type: pa.DataType, keyword: str, max_precision: int) -> str:
        precision, scale = arrow_type.precision, arrow_type.scale
        if precision > max_precision or scale < 0 or scale > precision:
            raise TableWriteError(
                f"decimal({precision}, {scale}) does not fit {self.label}'s {keyword} "
                f"(precision 1 to {max_precision}, scale 0 to the precision)"
            )
        return f"{keyword}({precision}, {scale})"

    # ------------------------------------------------------------------ statements

    def create_table_sql(
        self,
        table: str,
        columns: Sequence[SourceColumn],
        keys: Sequence[str],
        not_null: bool = True,
    ) -> str:
        """``CREATE TABLE`` with the columns' types and, when ``keys``, a primary key.

        Key columns are NOT NULL; so are columns the source declares non-nullable, unless
        ``not_null`` is False (a staging table for an existing table, whose own constraints
        decide).
        """
        definitions = []
        for column in columns:
            required = (column.key and keys) or (not_null and not column.nullable)
            constraint = " NOT NULL" if required else ""
            definitions.append(f"{self.quote(column.sql_name)} {column.ddl}{constraint}")
        if keys:
            definitions.append(f"PRIMARY KEY ({', '.join(self.quote(k) for k in keys)})")
        return f"CREATE TABLE {table} ({', '.join(definitions)})"

    def add_primary_key_sql(self, table: str, keys: Sequence[str]) -> str:
        return f"ALTER TABLE {table} ADD PRIMARY KEY ({', '.join(self.quote(k) for k in keys)})"

    def drop_table_sql(self, table: str) -> str:
        return f"DROP TABLE {table}"

    def insert_rows_sql(
        self, table: str, names: Sequence[str], columns: Sequence[SourceColumn], rows: int
    ) -> str:
        """``INSERT INTO table (names) VALUES (...), (...)`` for ``rows`` rows."""
        row = "(" + ", ".join(column.placeholder for column in columns) + ")"
        return (
            f"INSERT INTO {table} ({', '.join(self.quote(n) for n in names)}) VALUES "
            + ", ".join([row] * rows)
        )

    def split_long_values(
        self, columns: Sequence[SourceColumn], rows: List[tuple]
    ) -> Tuple[List[tuple], List[tuple]]:
        """The rows multi-row statements can take, and the rows that need one of their own."""
        return rows, []

    def insert_select_sql(self, table: str, names: Sequence[str], staging: str, sources) -> str:
        return (
            f"INSERT INTO {table} ({', '.join(self.quote(n) for n in names)}) "
            f"SELECT {', '.join(self.quote(s) for s in sources)} FROM {staging}"
        )

    def _on(self, keys: Sequence[Tuple[str, str]]) -> str:
        return " AND ".join(f"t.{self.quote(t)} = s.{self.quote(s)}" for t, s in keys)

    def upsert_from_staging_sql(
        self, table: str, staging: str, pairs: Sequence[Tuple[str, str]], keys
    ) -> List[str]:
        """Statements that update existing rows and insert new ones from the staging table.

        ``pairs`` are ``(table column, staging column)``; ``keys`` the key pairs among them.
        Default: an UPDATE joined to the staging table, then an INSERT of the rows whose key
        is not in the table (PostgreSQL syntax).
        """
        statements = []
        others = [pair for pair in pairs if pair not in keys]
        if others:
            sets = ", ".join(f"{self.quote(t)} = s.{self.quote(s)}" for t, s in others)
            statements.append(
                f"UPDATE {table} AS t SET {sets} FROM {staging} AS s WHERE {self._on(keys)}"
            )
        statements.append(self._insert_missing_sql(table, staging, pairs, keys))
        return statements

    def _insert_missing_sql(self, table, staging, pairs, keys) -> str:
        return (
            f"INSERT INTO {table} ({', '.join(self.quote(t) for t, _ in pairs)}) "
            f"SELECT {', '.join('s.' + self.quote(s) for _, s in pairs)} FROM {staging} AS s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WHERE {self._on(keys)})"
        )

    def duplicate_keys_sql(self, staging: str, keys: Sequence[str]) -> str:
        quoted = ", ".join(self.quote(k) for k in keys)
        return (
            f"SELECT COUNT(*) FROM (SELECT {quoted} FROM {staging} "
            f"GROUP BY {quoted} HAVING COUNT(*) > 1) d"
        )

    def null_keys_sql(self, staging: str, keys: Sequence[str]) -> str:
        nulls = " OR ".join(f"{self.quote(k)} IS NULL" for k in keys)
        return f"SELECT COUNT(*) FROM {staging} WHERE {nulls}"


# ---------------------------------------------------------------------------- PostgreSQL

_PG_KINDS = {
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "numeric": "decimal",
    "float4": "float",
    "float8": "float",
    "text": "string",
    "varchar": "string",
    "bpchar": "string",
    "name": "string",
    "citext": "string",
    "bool": "boolean",
    "bytea": "binary",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "timestamp_tz",
    "time": "time",
    "timetz": "time",
}


@dataclass
class PostgreSQL(Dialect):
    name: str = "postgresql"
    label: str = "PostgreSQL"
    max_identifier: int = 63
    identifier_bytes: bool = True
    upsert_needs_unique_rows: bool = True
    upsert_needs_unique_index: bool = True
    session_statements: Tuple[str, ...] = ("SET TIME ZONE 'UTC'",)
    codes: DriverCodes = field(default_factory=lambda: DriverCodes({}))

    current_schema_sql: str = "SELECT current_schema()"
    schema_sql: str = "SELECT nspname FROM pg_catalog.pg_namespace WHERE lower(nspname) = lower(?)"
    tables_sql: str = (
        "SELECT c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = ? AND lower(c.relname) = lower(?) AND c.relkind IN ('r', 'p', 'v', 'f')"
    )
    columns_sql: str = (
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), "
        "CASE WHEN t.typtype = 'd' THEN b.typname ELSE t.typname END, "
        "a.attnotnull, a.atthasdef, a.attidentity, a.attgenerated "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
        "LEFT JOIN pg_catalog.pg_type b ON b.oid = t.typbasetype "
        "WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum"
    )
    unique_keys_sql: str = (
        "SELECT i.indexrelid, a.attname FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE n.nspname = ? AND c.relname = ? AND i.indisunique "
        "AND i.indpred IS NULL AND i.indexprs IS NULL"
    )

    def existing_column(self, row: Sequence) -> ExistingColumn:
        name, type_name, base, not_null, has_default, identity, generated = row
        kind = _PG_KINDS.get(base, "other")
        identity = (identity or "").strip()
        writable = identity != "a" and not (generated or "").strip()
        return ExistingColumn(
            name=name,
            type_name=type_name,
            kind=kind,
            accepts=ACCEPTS[kind],
            required=_truthy(not_null) and not _truthy(has_default) and not identity,
            writable=writable,
            # Text reaches uuid, json, enum, ... columns only as an untyped value or the
            # column's own type, so the staging column takes that type
            load_type=type_name if kind == "other" else None,
        )

    def column_type(self, column: SourceColumn) -> str:
        kind, arrow_type = column.kind, column.arrow_type
        if kind == "integer":
            if arrow_type == pa.uint64():
                return "NUMERIC(20)"
            width = arrow_type.bit_width + (8 if pa.types.is_unsigned_integer(arrow_type) else 0)
            return "SMALLINT" if width <= 16 else "INTEGER" if width <= 32 else "BIGINT"
        if kind == "float":
            return "DOUBLE PRECISION" if arrow_type == pa.float64() else "REAL"
        if kind == "decimal":
            return self._decimal(arrow_type, "NUMERIC", 1000)
        return {
            "boolean": "BOOLEAN",
            "string": "TEXT",
            "binary": "BYTEA",
            "date": "DATE",
            "timestamp": "TIMESTAMP(6)",
            "timestamp_tz": "TIMESTAMP(6) WITH TIME ZONE",
            "time": "TIME(6)",
        }[kind]

    def upsert_rows_sql(self, table, names, columns, keys, rows) -> str:
        insert = self.insert_rows_sql(table, names, columns, rows)
        conflict = ", ".join(self.quote(k) for k in keys)
        others = [n for n in names if n not in keys]
        if not others:
            return f"{insert} ON CONFLICT ({conflict}) DO NOTHING"
        sets = ", ".join(f"{self.quote(n)} = EXCLUDED.{self.quote(n)}" for n in others)
        return f"{insert} ON CONFLICT ({conflict}) DO UPDATE SET {sets}"


# ---------------------------------------------------------------------------- MySQL / MariaDB

_MYSQL_KINDS = {
    "tinyint": "integer",
    "smallint": "integer",
    "mediumint": "integer",
    "int": "integer",
    "integer": "integer",
    "bigint": "integer",
    "year": "integer",
    "bit": "boolean",
    "decimal": "decimal",
    "numeric": "decimal",
    "float": "float",
    "double": "float",
    "real": "float",
    "char": "string",
    "varchar": "string",
    "tinytext": "string",
    "text": "string",
    "mediumtext": "string",
    "longtext": "string",
    "binary": "binary",
    "varbinary": "binary",
    "tinyblob": "binary",
    "blob": "binary",
    "mediumblob": "binary",
    "longblob": "binary",
    "date": "date",
    "datetime": "timestamp",
    # TIMESTAMP stores instants (in UTC; forklift's sessions use UTC)
    "timestamp": "timestamp_tz",
    "time": "time",
}
_MYSQL_INTEGERS = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}


@dataclass
class MySql(Dialect):
    name: str = "mysql"
    label: str = "MySQL"
    max_identifier: int = 64
    transactional_ddl: bool = False
    timestamps_as_text: bool = True
    upsert_needs_unique_index: bool = True
    utf8_parameters: bool = True
    session_statements: Tuple[str, ...] = (
        "SET time_zone = '+00:00'",
        # Strict mode makes a value that does not fit its column an error, not a warning
        "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), "
        "'STRICT_ALL_TABLES')",
    )
    codes: DriverCodes = field(
        default_factory=lambda: DriverCodes(
            {
                1044: "access denied to the database",
                1045: "access denied for the login",
                1048: "a column that may not be empty got no value",
                1050: "the table already exists",
                1062: "duplicate key",
                1142: "command denied for the table",
                1143: "command denied for a column",
                1146: "the table does not exist",
                1205: "lock wait timeout",
                1213: "deadlock",
                1227: "access denied: a privilege is missing",
                1264: "a value is out of range for its column",
                1292: "incorrect date or time value",
                1366: "incorrect value for the column",
                1406: "a value is too long for its column",
                2006: "the server has gone away",
                2013: "the connection to the server was lost",
            },
            privilege=frozenset({1044, 1045, 1142, 1143, 1227}),
            retryable=frozenset({1205, 1213, 2006, 2013}),
        )
    )

    current_schema_sql: str = "SELECT DATABASE()"
    schema_sql: str = (
        "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = lower(?)"
    )
    tables_sql: str = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? AND lower(table_name) = lower(?)"
    )
    columns_sql: str = (
        "SELECT column_name, data_type, column_type, is_nullable, column_default, extra "
        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position"
    )
    unique_keys_sql: str = (
        "SELECT index_name, column_name FROM information_schema.statistics "
        "WHERE table_schema = ? AND table_name = ? AND non_unique = 0"
    )

    def quote(self, name: str) -> str:
        return "`" + name.replace("`", "``") + "`"

    def create_privilege(self, schema: str) -> str:
        return f"CREATE (and DROP, to remove the staging table) on database {schema}"

    def drop_privilege(self, schema: str) -> str:
        return f"DROP on database {schema}"

    def rename_privilege(self, schema: str) -> str:
        return f"ALTER, DROP, CREATE and INSERT on database {schema}"

    def existing_column(self, row: Sequence) -> ExistingColumn:
        name, data_type, column_type, is_nullable, default, extra = row
        data_type = str(data_type).lower()
        column_type = str(column_type).lower()
        extra = str(extra or "").lower()
        kind = _MYSQL_KINDS.get(data_type, "other")
        accepts = ACCEPTS[kind]
        if kind == "integer" or kind == "boolean":
            # BOOLEAN is TINYINT(1), and BIT takes numbers: both hold booleans and integers
            accepts = frozenset({"integer", "boolean"})
            if column_type.startswith("tinyint(1)"):
                kind = "boolean"
        return ExistingColumn(
            name=name,
            type_name=column_type,
            kind=kind,
            accepts=accepts,
            required=str(is_nullable).upper() == "NO"
            and default is None
            and "auto_increment" not in extra,
            writable="virtual generated" not in extra and "stored generated" not in extra,
        )

    def column_type(self, column: SourceColumn) -> str:
        kind, arrow_type = column.kind, column.arrow_type
        if kind == "integer":
            keyword = _MYSQL_INTEGERS[arrow_type.bit_width]
            return f"{keyword} UNSIGNED" if pa.types.is_unsigned_integer(arrow_type) else keyword
        if kind == "float":
            return "DOUBLE" if arrow_type == pa.float64() else "FLOAT"
        if kind == "decimal":
            if arrow_type.scale > 30:
                raise TableWriteError(
                    f"decimal({arrow_type.precision}, {arrow_type.scale}) does not fit "
                    f"{self.label}'s DECIMAL (scale at most 30)"
                )
            return self._decimal(arrow_type, "DECIMAL", 65)
        if kind == "string":
            # TEXT cannot be a key without a prefix length
            return "VARCHAR(255)" if column.key else "LONGTEXT"
        if kind == "binary":
            if pa.types.is_fixed_size_binary(arrow_type) and arrow_type.byte_width <= 255:
                return f"BINARY({arrow_type.byte_width})"
            return "VARBINARY(255)" if column.key else "LONGBLOB"
        return {
            "boolean": "BOOLEAN",
            "date": "DATE",
            # DATETIME rather than TIMESTAMP (which ends in 2038); time zone-aware values are
            # written in UTC
            "timestamp": "DATETIME(6)",
            "timestamp_tz": "DATETIME(6)",
            "time": "TIME(6)",
        }[kind]

    def bind(self, column: SourceColumn) -> Tuple[str, str]:
        if column.kind in ("timestamp", "timestamp_tz"):
            return "CAST(? AS DATETIME(6))", "{}"
        return super().bind(column)

    def rename_table_sql(self, schema, old, new) -> str:
        return f"RENAME TABLE {self.qualified(schema, old)} TO {self.qualified(schema, new)}"

    def upsert_from_staging_sql(self, table, staging, pairs, keys) -> List[str]:
        statements = []
        others = [pair for pair in pairs if pair not in keys]
        if others:
            sets = ", ".join(f"t.{self.quote(t)} = s.{self.quote(s)}" for t, s in others)
            statements.append(
                f"UPDATE {table} AS t JOIN {staging} AS s ON {self._on(keys)} SET {sets}"
            )
        statements.append(self._insert_missing_sql(table, staging, pairs, keys))
        return statements

    def upsert_rows_sql(self, table, names, columns, keys, rows) -> str:
        insert = self.insert_rows_sql(table, names, columns, rows)
        others = [n for n in names if n not in keys] or [keys[0]]
        sets = ", ".join(f"{self.quote(n)} = VALUES({self.quote(n)})" for n in others)
        return f"{insert} ON DUPLICATE KEY UPDATE {sets}"


# ---------------------------------------------------------------------------- SQL Server

_MSSQL_KINDS = {
    "bit": "boolean",
    "tinyint": "integer",
    "smallint": "integer",
    "int": "integer",
    "bigint": "integer",
    "decimal": "decimal",
    "numeric": "decimal",
    "money": "decimal",
    "smallmoney": "decimal",
    "float": "float",
    "real": "float",
    "char": "string",
    "varchar": "string",
    "nchar": "string",
    "nvarchar": "string",
    "text": "string",
    "ntext": "string",
    "binary": "binary",
    "varbinary": "binary",
    "image": "binary",
    "date": "date",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "smalldatetime": "timestamp",
    "datetimeoffset": "timestamp_tz",
    "time": "time",
}
_MSSQL_INTEGERS = {"int8": "SMALLINT", "int16": "SMALLINT", "int32": "INT", "int64": "BIGINT"}
_MSSQL_UNSIGNED = {"uint8": "TINYINT", "uint16": "INT", "uint32": "BIGINT"}


@dataclass
class SqlServer(Dialect):
    name: str = "sqlserver"
    label: str = "SQL Server"
    fast_executemany: bool = True
    rows_per_statement: int = 1
    session_statements: Tuple[str, ...] = ("SET XACT_ABORT ON",)
    codes: DriverCodes = field(
        default_factory=lambda: DriverCodes(
            {
                208: "invalid object name",
                229: "permission denied on the object",
                230: "permission denied on a column",
                262: "permission denied in the database",
                515: "a column that may not be empty got no value",
                1088: "the object does not exist or the login has no permission on it",
                1205: "deadlock",
                2601: "duplicate key",
                2627: "duplicate key",
                2628: "a value is too long for its column",
                2714: "an object with that name already exists",
                2760: "the schema does not exist or the login has no permission on it",
                3701: "the table does not exist or the login has no permission to drop it",
                4902: "the object does not exist or the login has no permission on it",
                8115: "arithmetic overflow (a value does not fit its column)",
                8152: "a value is too long for its column",
                8672: "a key occurs twice in the source of a MERGE",
                15151: "the object does not exist or the login has no permission on it",
                15247: "the login has no permission to perform this action",
                18456: "login failed",
            },
            privilege=frozenset({229, 230, 262, 1088, 2760, 3701, 4902, 15151, 15247, 18456}),
            retryable=frozenset({1205}),
        )
    )

    current_schema_sql: str = "SELECT SCHEMA_NAME()"
    schema_sql: str = "SELECT name FROM sys.schemas WHERE LOWER(name) = LOWER(?)"
    tables_sql: str = (
        "SELECT o.name FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id "
        "WHERE s.name = ? AND LOWER(o.name) = LOWER(?) AND o.type IN ('U', 'V')"
    )
    columns_sql: str = (
        "SELECT c.name, TYPE_NAME(c.system_type_id), c.is_nullable, c.is_identity, "
        "c.is_computed, CASE WHEN c.default_object_id <> 0 THEN 1 ELSE 0 END "
        "FROM sys.columns c JOIN sys.objects o ON o.object_id = c.object_id "
        "JOIN sys.schemas s ON s.schema_id = o.schema_id "
        "WHERE s.name = ? AND o.name = ? ORDER BY c.column_id"
    )

    def quote(self, name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"

    def create_privilege(self, schema: str) -> str:
        return f"CREATE TABLE in the database and ALTER on schema {schema}"

    def drop_privilege(self, schema: str) -> str:
        return f"ALTER on schema {schema}"

    def existing_column(self, row: Sequence) -> ExistingColumn:
        name, type_name, nullable, identity, computed, has_default = row
        type_name = str(type_name).lower()
        kind = _MSSQL_KINDS.get(type_name, "other")
        # rowversion (TYPE_NAME "timestamp") is set by the server, like identity columns
        generated = _truthy(identity) or _truthy(computed) or type_name == "timestamp"
        return ExistingColumn(
            name=name,
            type_name=type_name,
            kind=kind,
            accepts=ACCEPTS[kind],
            required=not _truthy(nullable) and not _truthy(has_default) and not generated,
            writable=not generated,
        )

    def column_type(self, column: SourceColumn) -> str:
        kind, arrow_type = column.kind, column.arrow_type
        if kind == "integer":
            name = str(arrow_type)
            if name == "uint64":
                return "DECIMAL(20, 0)"
            return _MSSQL_INTEGERS.get(name) or _MSSQL_UNSIGNED[name]
        if kind == "float":
            return "FLOAT" if arrow_type == pa.float64() else "REAL"
        if kind == "decimal":
            return self._decimal(arrow_type, "DECIMAL", 38)
        if kind == "string":
            # NVARCHAR(MAX) cannot be a key
            return "NVARCHAR(255)" if column.key else "NVARCHAR(MAX)"
        if kind == "binary":
            if pa.types.is_fixed_size_binary(arrow_type) and arrow_type.byte_width <= 8000:
                return f"BINARY({arrow_type.byte_width})"
            return "VARBINARY(255)" if column.key else "VARBINARY(MAX)"
        return {
            "boolean": "BIT",
            "date": "DATE",
            "timestamp": "DATETIME2(6)",
            "timestamp_tz": "DATETIMEOFFSET(6)",
            "time": "TIME(6)",
        }[kind]

    def upsert_from_staging_sql(self, table, staging, pairs, keys) -> List[str]:
        return [self._merge(table, f"{staging} AS s", pairs, keys)]

    def _merge(self, table, source, pairs, keys) -> str:
        """``MERGE`` from ``source`` (aliased ``s``); HOLDLOCK keeps concurrent upserts apart."""
        others = [pair for pair in pairs if pair not in keys]
        matched = ""
        if others:
            sets = ", ".join(f"t.{self.quote(t)} = s.{self.quote(s)}" for t, s in others)
            matched = f" WHEN MATCHED THEN UPDATE SET {sets}"
        return (
            f"MERGE INTO {table} WITH (HOLDLOCK) AS t USING {source} ON {self._on(keys)}"
            f"{matched} WHEN NOT MATCHED BY TARGET THEN INSERT "
            f"({', '.join(self.quote(t) for t, _ in pairs)}) "
            f"VALUES ({', '.join('s.' + self.quote(s) for _, s in pairs)});"
        )

    def upsert_rows_sql(self, table, names, columns, keys, rows) -> str:
        row = "(" + ", ".join(column.placeholder for column in columns) + ")"
        source = (
            f"(VALUES {', '.join([row] * rows)}) AS s "
            f"({', '.join(self.quote(n) for n in names)})"
        )
        pairs = [(n, n) for n in names]
        return self._merge(table, source, pairs, [(k, k) for k in keys])


# ---------------------------------------------------------------------------- Oracle

_ORACLE_PLAIN_NAME = re.compile(r"[a-z][a-z0-9_$#]*")
_ORACLE_INTEGERS = {8: 3, 16: 5, 32: 10, 64: 19}
_ORACLE_TIMESTAMP = "TO_TIMESTAMP({}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
_ORACLE_TIME = "CASE WHEN {0} IS NOT NULL THEN TO_DSINTERVAL('0 ' || {0}) END"
_ORACLE_LOB_BINDS = ("TO_CLOB(?)", "TO_BLOB(?)")
# The longest text or binary value Oracle's driver binds inside a SELECT; text is counted at
# four bytes a character (the most AL32UTF8 needs)
_ORACLE_MAX_BIND_BYTES = 32767


def _too_long_to_bind(value) -> bool:
    if isinstance(value, str):
        return len(value) * 4 > _ORACLE_MAX_BIND_BYTES
    return isinstance(value, bytes) and len(value) > _ORACLE_MAX_BIND_BYTES


@dataclass
class Oracle(Dialect):
    name: str = "oracle"
    label: str = "Oracle"
    identifier_bytes: bool = True
    transactional_ddl: bool = False
    rows_per_statement: int = 100
    decimal_integers: bool = True
    timestamps_as_text: bool = True
    upsert_needs_unique_rows: bool = True
    session_statements: Tuple[str, ...] = ("ALTER SESSION SET TIME_ZONE = '+00:00'",)
    codes: DriverCodes = field(
        default_factory=lambda: DriverCodes(
            {
                1: "unique constraint violated (a key already exists)",
                54: "the table is locked by another session",
                60: "deadlock",
                942: "the table or view does not exist, or the login has no privilege on it",
                955: "an object with that name already exists",
                1017: "invalid user name or password",
                1031: "insufficient privileges",
                1045: "the login lacks the CREATE SESSION privilege",
                1400: "a column that may not be empty got no value",
                1438: "a value is larger than its column's precision",
                1536: "the login's tablespace quota is exceeded",
                1722: "invalid number",
                1950: "the login has no quota on the tablespace",
                3113: "the connection to the server was lost",
                3114: "not connected to the server",
                3135: "the connection to the server was lost",
                12899: "a value is too long for its column",
                30926: "a key occurs twice in the source of a MERGE",
                41900: "missing privilege",
            },
            privilege=frozenset({942, 1017, 1031, 1045, 1536, 1950, 41900}),
            retryable=frozenset({54, 60, 3113, 3114, 3135}),
        )
    )

    current_schema_sql: str = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual"
    schema_sql: str = "SELECT username FROM all_users WHERE UPPER(username) = UPPER(?)"
    tables_sql: str = (
        "SELECT object_name FROM all_objects WHERE owner = ? "
        "AND UPPER(object_name) = UPPER(?) AND object_type IN ('TABLE', 'VIEW')"
    )
    columns_sql: str = (
        "SELECT column_name, data_type, data_precision, data_scale, nullable, default_length, "
        "virtual_column, identity_column FROM all_tab_cols "
        "WHERE owner = ? AND table_name = ? AND hidden_column = 'NO' ORDER BY column_id"
    )

    def new_name(self, name: str) -> str:
        # A plain lower-case name gets Oracle's convention for unquoted names (upper case), so
        # it can be queried without quotes; other names are kept exactly as given
        return name.upper() if _ORACLE_PLAIN_NAME.fullmatch(name) else name

    def _check_characters(self, name: str, what: str) -> None:
        if '"' in name:
            raise TableWriteError(
                f'Invalid {what} name {name!r}: Oracle names cannot contain double quotes (")',
                error_code=SPEC_INVALID,
            )

    def create_privilege(self, schema: str) -> str:
        return (
            f"CREATE TABLE (CREATE ANY TABLE for another user's schema {schema}) and a quota "
            "on the tablespace"
        )

    def drop_privilege(self, schema: str) -> str:
        return "ownership of the table (or DROP ANY TABLE)"

    def rename_privilege(self, schema: str) -> str:
        return "ownership of the staging table (or ALTER ANY TABLE)"

    def existing_column(self, row: Sequence) -> ExistingColumn:
        name, data_type, precision, scale, nullable, default_length, virtual, identity = row
        data_type = str(data_type).upper()
        load_type = None
        if data_type == "NUMBER":
            if scale is None:
                kind, accepts = "decimal", NUMBERS | {"boolean"}
            elif int(scale) == 0:
                kind, accepts = "integer", frozenset({"integer", "boolean"})
            else:
                kind, accepts = "decimal", NUMBERS
        else:
            kind = self._kind(data_type)
            accepts = ACCEPTS[kind]
            if data_type in ("CLOB", "NCLOB", "LONG"):
                load_type = "CLOB"
            elif data_type in ("BLOB", "LONG RAW"):
                load_type = "BLOB"
        identity = _truthy(identity)
        return ExistingColumn(
            name=name,
            type_name=data_type,
            kind=kind,
            accepts=accepts,
            required=str(nullable).upper() == "N" and default_length is None and not identity,
            writable=not _truthy(virtual),
            load_type=load_type,
        )

    @staticmethod
    def _kind(data_type: str) -> str:
        if data_type in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
            return "float"
        if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG"):
            return "string"
        if data_type in ("RAW", "BLOB", "LONG RAW"):
            return "binary"
        if data_type == "DATE":
            # Oracle's DATE holds a date and a time of day (whole seconds)
            return "timestamp"
        if data_type.startswith("TIMESTAMP"):
            return "timestamp_tz" if "TIME ZONE" in data_type else "timestamp"
        if data_type.startswith("INTERVAL DAY"):
            return "time"
        if data_type == "BOOLEAN":
            return "boolean"
        return "other"

    def column_type(self, column: SourceColumn) -> str:
        kind, arrow_type = column.kind, column.arrow_type
        if kind == "integer":
            digits = _ORACLE_INTEGERS[arrow_type.bit_width]
            return "NUMBER(20)" if arrow_type == pa.uint64() else f"NUMBER({digits})"
        if kind == "float":
            return "BINARY_DOUBLE" if arrow_type == pa.float64() else "BINARY_FLOAT"
        if kind == "decimal":
            return self._decimal(arrow_type, "NUMBER", 38)
        if kind == "string":
            return "VARCHAR2(255 CHAR)" if column.key else "VARCHAR2(4000 CHAR)"
        if kind == "binary":
            if pa.types.is_fixed_size_binary(arrow_type) and arrow_type.byte_width <= 2000:
                return f"RAW({arrow_type.byte_width})"
            return "RAW(255)" if column.key else "BLOB"
        return {
            "boolean": "NUMBER(1)",
            "date": "DATE",
            "timestamp": "TIMESTAMP(6)",
            "timestamp_tz": "TIMESTAMP(6) WITH TIME ZONE",
            # Oracle has no time-of-day type: the time since midnight
            "time": "INTERVAL DAY(0) TO SECOND(6)",
        }[kind]

    def bind(self, column: SourceColumn) -> Tuple[str, str]:
        """Oracle binds into ``SELECT CAST(? AS type) ... FROM dual`` rows (see insert_rows_sql).

        Every value is cast, so the ``UNION ALL`` branches agree on their types even when a
        value is NULL. Timestamps and times arrive as text and are converted on the server:
        the driver drops the fractional seconds of timestamp parameters and fails on any
        parameter that meets an INTERVAL.
        """
        kind = column.kind
        if kind == "timestamp":
            return "CAST(? AS VARCHAR2(26))", _ORACLE_TIMESTAMP
        if kind == "timestamp_tz":
            return "CAST(? AS VARCHAR2(26))", f"FROM_TZ({_ORACLE_TIMESTAMP}, 'UTC')"
        if kind == "time":
            return "CAST(? AS VARCHAR2(15))", _ORACLE_TIME
        target = column.load_type or column.ddl
        if target == "CLOB":
            return "TO_CLOB(?)", "{}"
        if target == "BLOB":
            return "TO_BLOB(?)", "{}"
        return f"CAST(? AS {target})", "{}"

    def drop_table_sql(self, table: str) -> str:
        return f"DROP TABLE {table} PURGE"

    def rename_table_sql(self, schema, old, new) -> str:
        return f"ALTER TABLE {self.qualified(schema, old)} RENAME TO {self.quote(new)}"

    def _rows(self, columns: Sequence[SourceColumn], rows: int, names: Sequence[str] = ()) -> str:
        """``SELECT <expressions> FROM (SELECT CAST(? AS ...) x1, ... FROM dual UNION ALL ...)``.

        Multi-row ``VALUES`` needs Oracle 23ai, so the rows come from ``dual`` instead. With
        ``names``, the expressions are aliased (a MERGE source).
        """
        first = ", ".join(f"{column.placeholder} x{i}" for i, column in enumerate(columns, 1))
        rest = ", ".join(column.placeholder for column in columns)
        branches = [f"SELECT {first} FROM dual"] + [f"SELECT {rest} FROM dual"] * (rows - 1)
        expressions = [column.expression.format(f"x{i}") for i, column in enumerate(columns, 1)]
        if names:
            expressions = [f"{e} {self.quote(n)}" for e, n in zip(expressions, names)]
        return f"SELECT {', '.join(expressions)} FROM ({' UNION ALL '.join(branches)})"

    def insert_rows_sql(self, table, names, columns, rows) -> str:
        return f"INSERT INTO {table} ({', '.join(self.quote(n) for n in names)}) " + self._rows(
            columns, rows
        )

    def split_long_values(self, columns, rows):
        """Rows with a CLOB or BLOB value over 32,767 bytes go into single-row statements.

        Inside ``SELECT ... FROM dual`` the driver binds text and binary values as VARCHAR2 and
        RAW, which hold at most 32,767 bytes; ``VALUES`` binds them as LOBs.
        """
        lobs = [i for i, column in enumerate(columns) if column.placeholder in _ORACLE_LOB_BINDS]
        if not lobs:
            return rows, []
        short, long = [], []
        for row in rows:
            (long if any(_too_long_to_bind(row[i]) for i in lobs) else short).append(row)
        return short, long

    def single_row_insert_sql(self, table, names, columns) -> str:
        """``INSERT ... VALUES`` for one row: binds CLOB and BLOB values of any length.

        Raises:
            TableWriteError: The row has a time (INTERVAL) column, which the driver cannot
                bind in ``VALUES``
        """
        times = [name for name, column in zip(names, columns) if column.kind == "time"]
        if times:
            raise TableWriteError(
                "a CLOB or BLOB value is longer than 32,767 bytes, and Oracle's ODBC driver "
                "binds such values only in single-row INSERT ... VALUES statements, where it "
                f"cannot bind the INTERVAL (time) column(s) {', '.join(times)}; write the "
                "times in a separate table, or as text"
            )
        values = [
            "?" if column.placeholder in _ORACLE_LOB_BINDS else column.placeholder
            for column in columns
        ]
        expressions = [
            value if value == "?" else column.expression.format(value)
            for value, column in zip(values, columns)
        ]
        return (
            f"INSERT INTO {table} ({', '.join(self.quote(n) for n in names)}) "
            f"VALUES ({', '.join(expressions)})"
        )

    def upsert_from_staging_sql(self, table, staging, pairs, keys) -> List[str]:
        return [self._merge(table, staging, pairs, keys)]

    def _merge(self, table, source, pairs, keys) -> str:
        others = [pair for pair in pairs if pair not in keys]
        matched = ""
        if others:
            sets = ", ".join(f"t.{self.quote(t)} = s.{self.quote(s)}" for t, s in others)
            matched = f" WHEN MATCHED THEN UPDATE SET {sets}"
        return (
            f"MERGE INTO {table} t USING {source} s ON ({self._on(keys)}){matched} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(self.quote(t) for t, _ in pairs)}) "
            f"VALUES ({', '.join('s.' + self.quote(s) for _, s in pairs)})"
        )

    def upsert_rows_sql(self, table, names, columns, keys, rows) -> str:
        source = f"({self._rows(columns, rows, names)})"
        pairs = [(n, n) for n in names]
        return self._merge(table, source, pairs, [(k, k) for k in keys])


#: The dialects by the prefix of the server's SQL_DBMS_NAME (lower case)
DIALECTS = (
    ("postgresql", PostgreSQL),
    ("mysql", MySql),
    ("mariadb", MySql),
    ("microsoft sql server", SqlServer),
    ("oracle", Oracle),
)


def dialect_for(dbms_name: str) -> Optional[Dialect]:
    """The dialect for a server's SQL_DBMS_NAME, or ``None`` when forklift cannot write to it."""
    dbms = (dbms_name or "").strip().lower()
    for prefix, factory in DIALECTS:
        if dbms.startswith(prefix):
            dialect = factory()
            if prefix == "mariadb":
                dialect.label = "MariaDB"
            return dialect
    return None
