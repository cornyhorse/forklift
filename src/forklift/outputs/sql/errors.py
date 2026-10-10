"""Errors of :func:`forklift.outputs.sql.write_table`, built without the driver's message text.

Driver messages can quote cell values (Oracle repeats the bound value of a failed ``CAST``, for
example) and the connection string, so forklift never repeats them. A database error is
described by its SQLSTATE, what that state means, and the driver's numeric code (with what that
code means on the database at hand): enough to tell a missing privilege from a value that does
not fit its column, never the values involved.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional, Tuple

from ...engine.exceptions import CANCELLED, PERMISSION_DENIED, TARGET_WRITE_FAILED

#: What SQLSTATEs mean (the ODBC and SQL standard classes plus PostgreSQL's own codes)
SQLSTATE_MEANINGS: Dict[str, str] = {
    "01004": "string data, right truncation",
    "08001": "the client could not connect",
    "08004": "the server rejected the connection",
    "08S01": "the connection to the server was lost",
    "21000": "cardinality violation (a key occurs twice in one statement)",
    "22001": "string data, right truncation (a value is too long for its column)",
    "22003": "numeric value out of range (a value does not fit its column)",
    "22007": "invalid date or time format",
    "22008": "date or time value out of range",
    "22018": "invalid character value for a cast",
    "22P02": "invalid text representation",
    "23000": "integrity constraint violation",
    "23502": "not-null violation (a column that may not be empty got no value)",
    "23503": "foreign key violation",
    "23505": "unique violation (a key already exists)",
    "23514": "check constraint violation",
    "25006": "read-only transaction",
    "28000": "invalid authorization (login refused)",
    "28P01": "invalid password",
    "40001": "serialization failure or deadlock (the transaction was rolled back)",
    "40P01": "deadlock detected",
    "42000": "syntax error or access rule violation",
    "42501": "insufficient privilege",
    "42703": "column not found",
    "42804": "datatype mismatch",
    "42P01": "table not found",
    "42P07": "a table with that name already exists",
    "42P10": "no unique index or constraint matches the key columns",
    "42S01": "a table with that name already exists",
    "42S02": "table not found",
    "42S22": "column not found",
    "53100": "the server ran out of disk space",
    "53200": "the server ran out of memory",
    "54000": "a program limit was exceeded",
    "55P03": "lock not available",
    "57014": "statement cancelled",
    "57P01": "the server is shutting down",
    "HY000": "general driver error",
    "HY008": "operation cancelled",
    "HYT00": "timeout expired",
    "HYT01": "connection timeout expired",
}

_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
# pyodbc messages end with the driver's numeric code, e.g. "... (1142) (SQLExecDirectW)"
_NATIVE_CODE = re.compile(r"\((-?\d+)\)\s*\(SQL\w+\)\s*$")
# Oracle's messages name the error (ORA-01031) and may end in padding bytes instead
_ORACLE_CODE = re.compile(r"ORA-(\d{5})")

# pyodbc's own errors that mean a value does not fit its parameter (sizes come from the column)
_PYODBC_STATES = (
    ("String data, right truncation", "22001"),
    ("Converting decimal loses precision", "22003"),
)

_RETRYABLE_SQLSTATE_CLASSES = ("08", "40")
_RETRYABLE_SQLSTATES = frozenset({"55P03", "57P01", "57P02", "57P03", "HYT00", "HYT01"})


class TableWriteError(Exception):
    """Writing a table failed; the table is unchanged (see the guarantees in the package docs).

    The message names the table, the mode, the step that failed and, when the database refused
    for lack of a privilege, the privilege the login needs. It is built from names and codes
    only: never cell values, the connection string or the driver's message text, so it is safe
    to show, log and store.

    Attributes:
        table: The table as ``schema.table`` (as given, or as the catalog spells it)
        mode: The write mode (``create``, ``append``, ``replace`` or ``upsert``)
        action: What forklift was doing when it failed, or ``None``
        sqlstate: The SQLSTATE the database reported, or ``None``
        native_code: The driver's numeric error code, or ``None``
        privilege: The privilege the login lacks (when the database refused for that reason)
        retryable: True when trying again may succeed (deadlock, lost connection, timeout)
        error_code: ``PERMISSION_DENIED``, ``CANCELLED`` or ``TARGET_WRITE_FAILED`` (the job
            contract's error codes)
    """

    def __init__(
        self,
        message: str,
        *,
        table: str = "",
        mode: str = "",
        action: Optional[str] = None,
        sqlstate: Optional[str] = None,
        native_code: Optional[int] = None,
        privilege: Optional[str] = None,
        retryable: bool = False,
        error_code: str = TARGET_WRITE_FAILED,
    ):
        super().__init__(message)
        self.table = table
        self.mode = mode
        self.action = action
        self.sqlstate = sqlstate
        self.native_code = native_code
        self.privilege = privilege
        self.retryable = retryable
        self.error_code = error_code


class TableWriteCancelled(TableWriteError):
    """The ``cancel`` callback asked the write to stop before the rows were published."""

    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("error_code", CANCELLED)
        super().__init__(message, **kwargs)


@dataclass(frozen=True)
class DatabaseFailure:
    """A database error reduced to its codes and what they mean (no message text)."""

    sqlstate: Optional[str]
    native_code: Optional[int]
    meaning: str
    privilege_refused: bool
    retryable: bool

    def describe(self) -> str:
        """``SQLSTATE 42501 (insufficient privilege)``, plus the driver code when there is one."""
        parts = []
        if self.sqlstate:
            state = f"SQLSTATE {self.sqlstate}"
            known = SQLSTATE_MEANINGS.get(self.sqlstate)
            parts.append(f"{state} ({known})" if known else state)
        if self.native_code is not None:
            code = f"driver error {self.native_code}"
            parts.append(f"{code} ({self.meaning})" if self.meaning else code)
        return ", ".join(parts) or "no SQLSTATE or driver code reported"


@dataclass(frozen=True)
class DriverCodes:
    """What one database's numeric error codes mean (``native`` codes in pyodbc messages)."""

    meanings: Dict[int, str]
    privilege: FrozenSet[int] = frozenset()
    retryable: FrozenSet[int] = frozenset()


def database_error_codes(error: BaseException) -> Tuple[Optional[str], Optional[int]]:
    """The SQLSTATE and the driver's numeric code of a pyodbc error (``None`` when absent).

    Errors from the driver carry ``(SQLSTATE, message)``. Errors pyodbc raises itself (while
    binding parameters, before anything reaches the database) carry ``(message, SQLSTATE)``
    instead, with a generic HY000; the ones that mean a value does not fit get the SQLSTATE a
    database would have reported.
    """
    args = getattr(error, "args", ())
    if len(args) < 2 or not all(isinstance(arg, str) for arg in args[:2]):
        return None, undecodable_message_code(error)
    if _SQLSTATE.fullmatch(args[0]):
        text = args[1]
        native = _NATIVE_CODE.search(text) or _ORACLE_CODE.search(text)
        return args[0], int(native.group(1)) if native else None
    if _SQLSTATE.fullmatch(args[1]):
        for prefix, state in _PYODBC_STATES:
            if args[0].startswith(prefix):
                return state, None
        return args[1], None
    return None, None


def driver_errors(pyodbc) -> Tuple[type, ...]:
    """What a pyodbc call raises when the driver reports an error, for ``except`` clauses.

    ``pyodbc.Error``, and ``SystemError``: pyodbc raises that instead when it cannot decode the
    driver's message (see :func:`undecodable_message_code`).
    """
    return (pyodbc.Error, SystemError)


def undecodable_message_code(error: BaseException) -> Optional[int]:
    """The ORA code in a driver message pyodbc could not decode (``None`` for any other error).

    Oracle's ODBC driver can report a message as longer than the text it wrote. pyodbc decodes
    the rest of its buffer too and, when those bytes are not valid UTF-16, raises SystemError
    (caused by a UnicodeDecodeError that holds the raw message) instead of the driver's error.
    The SQLSTATE is lost with it; the message still names the ORA code.
    """
    cause = error.__cause__ if isinstance(error, SystemError) else None
    if not isinstance(cause, UnicodeDecodeError):
        return None
    code = _ORACLE_CODE.search(bytes(cause.object).decode(cause.encoding, errors="replace"))
    return int(code.group(1)) if code else None


def describe_failure(error: BaseException, codes: DriverCodes) -> DatabaseFailure:
    """Reduce a pyodbc error to its codes, what they mean, and whether it is worth a retry."""
    sqlstate, native = database_error_codes(error)
    refused = sqlstate in ("42501", "28000", "28P01") or native in codes.privilege
    retryable = (
        (sqlstate or "").startswith(_RETRYABLE_SQLSTATE_CLASSES)
        or sqlstate in _RETRYABLE_SQLSTATES
        or native in codes.retryable
    )
    return DatabaseFailure(
        sqlstate=sqlstate,
        native_code=native,
        meaning=codes.meanings.get(native, "") if native is not None else "",
        privilege_refused=refused,
        retryable=retryable,
    )


def failure_error_code(failure: DatabaseFailure) -> str:
    """The job contract's error code for a database failure."""
    return PERMISSION_DENIED if failure.privilege_refused else TARGET_WRITE_FAILED
