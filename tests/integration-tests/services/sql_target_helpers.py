"""Helpers for the SQL target tests (test_sql_targets.py), on top of service_helpers.

* ``staging_writer(database, table)``: a login that may create, fill and drop tables in the
  namespace (so it can stage) and may SELECT and INSERT, but not UPDATE or DELETE, the
  admin's table ``table``.
* ``read_back(database, table, kinds)``: the table's rows as comparable Python values, read
  with per-database SQL (ODBC drivers return booleans, times and time zone-aware timestamps
  in their own ways, and pyodbc cannot fetch every type).
* ``column_types(database, table)``: the type of each column as the catalog names it.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Dict, List, Sequence, Tuple

from service_helpers import Database, Login


def staging_writer(database: Database, table: str) -> Login:
    """A login that may stage in the namespace, and only SELECT and INSERT ``table``."""
    kind, namespace = database.kind, database.namespace
    if kind == "postgres":
        login = database.owner_login()
        database.grant(login, table, "SELECT", "INSERT")
    elif kind == "mysql":
        login = database.create_user()
        database.admin(
            f"GRANT CREATE, DROP, ALTER, SELECT, INSERT ON {namespace}.* "
            f"TO {database.grantee(login)}"
        )
    elif kind == "mssql":
        login = database.create_user()
        database.admin(
            f"GRANT CREATE TABLE TO [{login.user}]",
            f"GRANT ALTER, SELECT, INSERT ON SCHEMA::{namespace} TO [{login.user}]",
        )
    else:
        # Only the schema's own user may create tables in it without system-wide ANY
        # privileges; this throwaway server grants them to a test login
        login = database.create_user()
        database.admin(
            "GRANT CREATE ANY TABLE, DROP ANY TABLE, ALTER ANY TABLE, SELECT ANY TABLE, "
            f"INSERT ANY TABLE TO {login.user}"
        )
    return login


def _expression(kind: str, database: str, column: str) -> str:
    """SQL that reads a column of ``kind`` as a value Python compares exactly."""
    if kind == "boolean" and database == "postgres":
        return f"CASE WHEN {column} THEN 1 WHEN NOT {column} THEN 0 END"
    if kind == "integer" and database == "mysql":
        # MariaDB Connector/ODBC reads INT UNSIGNED as a signed 32-bit number
        return f"CAST({column} AS CHAR)"
    if kind == "timestamp_tz":
        # the instant in UTC (MySQL's DATETIME already holds UTC)
        column = {
            "postgres": f"({column} AT TIME ZONE 'UTC')",
            "mysql": column,
            "mssql": f"CAST(SWITCHOFFSET({column}, '+00:00') AS DATETIME2(6))",
            "oracle": f"SYS_EXTRACT_UTC({column})",
        }[database]
    if kind in ("timestamp", "timestamp_tz"):
        return {
            "postgres": f"to_char({column}, 'YYYY-MM-DD HH24:MI:SS.US')",
            "mysql": f"DATE_FORMAT({column}, '%Y-%m-%d %H:%i:%s.%f')",
            "mssql": f"CONVERT(VARCHAR(26), {column}, 121)",
            "oracle": f"TO_CHAR({column}, 'YYYY-MM-DD HH24:MI:SS.FF6')",
        }[database]
    if kind == "time":
        # microseconds since midnight (pyodbc reads times without their fraction, and cannot
        # fetch Oracle's INTERVAL at all)
        return {
            "postgres": f"CAST(EXTRACT(EPOCH FROM {column}) * 1000000 AS BIGINT)",
            "mysql": f"TIME_TO_SEC({column}) * 1000000 + MICROSECOND({column})",
            "mssql": f"DATEDIFF_BIG(MICROSECOND, CAST('00:00' AS TIME), {column})",
            "oracle": f"(EXTRACT(HOUR FROM {column}) * 3600 + EXTRACT(MINUTE FROM {column}) * 60 "
            f"+ EXTRACT(SECOND FROM {column})) * 1000000",
        }[database]
    return column


def _value(kind: str, value):
    if value is None:
        return None
    if kind in ("boolean", "integer", "time"):
        return int(value)
    if kind == "float":
        return float(value)
    if kind == "decimal":
        return Decimal(value)
    if kind == "binary":
        return bytes(value)
    if kind == "date" and isinstance(value, dt.datetime):
        return value.date()  # Oracle's DATE has a time of day
    if kind in ("timestamp", "timestamp_tz"):
        return dt.datetime.fromisoformat(value)
    return value


def read_back(
    database: Database, table: str, kinds: Sequence[Tuple[str, str]], order_by: str
) -> List[Tuple]:
    """The table's rows (columns ``(name, kind)``) ordered by ``order_by``, as Python values."""
    names = {name.casefold(): name for name in database.column_names(table)}
    columns = [(database.quote(names[name.casefold()]), kind) for name, kind in kinds]
    select = ", ".join(_expression(kind, database.kind, column) for column, kind in columns)
    order = database.quote(names[order_by.casefold()])
    rows = database.query(f"SELECT {select} FROM {database.qualified(table)} ORDER BY {order}")
    return [tuple(_value(kind, v) for (_, kind), v in zip(columns, row)) for row in rows]


def column_types(database: Database, table: str) -> Dict[str, str]:
    """``{column: type}`` with the type as the catalog spells it (lower case)."""
    namespace, name = database.catalog_namespace, database.catalog_name(table)
    if database.kind == "postgres":
        rows = database.query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped",
            namespace,
            name,
        )
    elif database.kind == "mysql":
        rows = database.query(
            "SELECT column_name, column_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            namespace,
            name,
        )
    elif database.kind == "mssql":
        rows = database.query(
            "SELECT column_name, data_type + CASE WHEN character_maximum_length = -1 "
            "THEN '(max)' WHEN character_maximum_length IS NOT NULL THEN '(' + "
            "CAST(character_maximum_length AS VARCHAR(10)) + ')' WHEN data_type IN "
            "('decimal', 'numeric') THEN '(' + CAST(numeric_precision AS VARCHAR(3)) + ',' + "
            "CAST(numeric_scale AS VARCHAR(3)) + ')' ELSE '' END "
            "FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            namespace,
            name,
        )
    else:
        rows = database.query(
            "SELECT column_name, data_type || CASE WHEN data_type = 'NUMBER' THEN '(' || "
            "data_precision || CASE WHEN data_scale > 0 THEN ',' || data_scale END || ')' "
            "WHEN data_type = 'VARCHAR2' THEN '(' || char_length || ')' "
            "WHEN data_type = 'RAW' THEN '(' || data_length || ')' END "
            "FROM all_tab_columns WHERE owner = ? AND table_name = ?",
            namespace,
            name,
        )
    return {str(column).lower(): str(type_name).lower() for column, type_name in rows}
