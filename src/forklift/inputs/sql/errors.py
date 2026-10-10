"""Errors of the SQL input, and how a database error is described without its message text.

Driver messages can quote cell values, so forklift never repeats them. A database error is
described by its SQLSTATE, what that state means and the driver's numeric code; forklift's own
errors (:class:`SqlSourceError` and its subclasses) are built from names and those codes only,
so their messages are safe to show and to record in ``metadata.json``.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from ...engine.exceptions import COLUMN_MISSING, PERMISSION_DENIED

# What common SQLSTATEs mean, for the reason recorded with a failed table
_SQLSTATE_MEANINGS = {
    "25006": "read-only transaction: the statement would have written",
    "28000": "invalid authorization",
    "42000": "syntax error or access rule violation",
    "42501": "insufficient privilege",
    "42P01": "table not found",
    "42S02": "table not found",
    "57014": "statement cancelled (query timeout)",
    "HY008": "operation cancelled",
    "HYT00": "timeout expired",
}
_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
# pyodbc messages end with the driver's numeric code, e.g. "... (1142) (SQLExecDirectW)"
_NATIVE_CODE = re.compile(r"\((-?\d+)\)\s*\(SQL\w+\)\s*$")

# The driver codes that mean "missing privilege", by the server's SQL_DBMS_NAME (lower case,
# matched as a prefix). PostgreSQL says so with SQLSTATE 42501, which counts on every database;
# the others report the generic 42000 (MySQL, SQL Server) or HY000 (Oracle) with these codes.
_PRIVILEGE_CODES = {
    "mysql": {1142, 1143},  # SELECT denied for the table / for a column
    "mariadb": {1142, 1143},
    "microsoft sql server": {229, 230},  # permission denied on the object / on a column
    "oracle": {1031, 41900},  # ORA-01031 insufficient privileges, ORA-41900 missing privilege
}


class SqlSourceError(Exception):
    """An SQL input failure whose message forklift built from names and codes only.

    The message never holds cell values or the driver's message text, so it is safe to show
    and to record (the SQL importer records it as a failed table's ``reason``).
    """


class TableLookupError(SqlSourceError, ValueError):
    """A requested table is not in the catalog, or the request matches several tables.

    The message is built only from the requested names and the catalog's schema names, never
    from table data, so it is safe to show and to record in ``metadata.json``.
    """


class ColumnLookupError(SqlSourceError, ValueError):
    """A column declared in ``x-sql`` ``select.columns`` is not in the table's catalog entry.

    Like :class:`TableLookupError` it holds names only (the declared ones and the catalog's).
    """

    error_code = COLUMN_MISSING


class ColumnPrivilegeError(SqlSourceError, PermissionError):
    """The database refused to read a table, or some of its columns, for a missing privilege.

    The message says which columns the login may read and which ``x-sql`` declaration imports
    them; the database error it replaces is its ``__cause__``.
    """

    error_code = PERMISSION_DENIED


def listed(names: List[str], limit: int = 50) -> str:
    """Names for an error message: all of them, or the first ``limit`` and how many more."""
    shown = ", ".join(names[:limit])
    if len(names) > limit:
        shown += f" and {len(names) - limit} more"
    return shown


def database_error_codes(error: BaseException) -> Tuple[Optional[str], Optional[int]]:
    """The SQLSTATE and the driver's numeric code of a pyodbc-style error (``None`` if absent).

    pyodbc errors carry ``(SQLSTATE, message)`` in ``args``; the message ends with the driver's
    numeric code in parentheses.
    """
    args = getattr(error, "args", ())
    if len(args) < 2 or not isinstance(args[0], str) or not _SQLSTATE.fullmatch(args[0]):
        return None, None
    native = _NATIVE_CODE.search(str(args[1]))
    return args[0], int(native.group(1)) if native else None


def describe_database_error(error: BaseException) -> str:
    """Why a database call failed, without the driver's message text (which can quote data).

    The SQLSTATE, what that state means, and the driver's numeric code: enough to tell a missing
    privilege from a timeout, never the values involved. ``""`` when ``error`` carries no
    SQLSTATE.
    """
    state, native = database_error_codes(error)
    if state is None:
        return ""
    reason = f"SQLSTATE {state}"
    if state in _SQLSTATE_MEANINGS:
        reason += f", {_SQLSTATE_MEANINGS[state]}"
    if native is not None:
        reason += f", driver error {native}"
    return reason


def is_privilege_error(error: BaseException, dbms: str) -> bool:
    """True when ``error`` is the database refusing an operation for a missing privilege.

    Args:
        error: The error a database call raised
        dbms: The server's SQL_DBMS_NAME in lower case (``""`` when unknown)
    """
    state, native = database_error_codes(error)
    if state == "42501":
        return True
    for name, codes in _PRIVILEGE_CODES.items():
        if dbms.startswith(name):
            return native in codes
    return False
