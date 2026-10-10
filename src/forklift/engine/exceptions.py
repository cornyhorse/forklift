"""Exception classes for Forklift engine.

Errors raised by the importers can carry a stable ``error_code`` (one of :data:`ERROR_CODES`), so
that callers such as :func:`forklift.jobs.run_job` can tell a schema problem from an unreadable
input without parsing messages. The code is an attribute of the exception instance (or class);
the exception type and message stay what they were, so existing ``except ValueError`` handlers
keep working.
"""

from typing import Optional, TypeVar

#: The job specification is invalid (``forklift.jobs`` only)
SPEC_INVALID = "SPEC_INVALID"
#: The schema cannot be loaded or one of its settings is invalid
SCHEMA_INVALID = "SCHEMA_INVALID"
#: The input cannot be read or parsed (missing file, no header, malformed rows, ...)
INPUT_UNREADABLE = "INPUT_UNREADABLE"
#: The input holds bytes that are not valid for the configured encoding
ENCODING_ERROR = "ENCODING_ERROR"
#: A column the schema needs is not in the input
COLUMN_MISSING = "COLUMN_MISSING"
#: ``x-validation`` rejected more rows than ``maxBadRowsPercent`` allows
BAD_ROWS_THRESHOLD_EXCEEDED = "BAD_ROWS_THRESHOLD_EXCEEDED"
#: Constraints with ``errorMode`` ``fail_fast`` / ``fail_complete`` were violated
CONSTRAINT_VIOLATION = "CONSTRAINT_VIOLATION"
#: A limit set by the caller (input size, rows, run time) was exceeded
LIMIT_EXCEEDED = "LIMIT_EXCEEDED"
#: Access to the input, the output or the database was refused
PERMISSION_DENIED = "PERMISSION_DENIED"
#: Writing the output table failed (``sql_table`` outputs)
TARGET_WRITE_FAILED = "TARGET_WRITE_FAILED"
#: The caller cancelled the import
CANCELLED = "CANCELLED"
#: Anything else (a bug or an unexpected environment problem)
INTERNAL = "INTERNAL"

#: Every error code, in the order the job contract lists them
ERROR_CODES = (
    SPEC_INVALID,
    SCHEMA_INVALID,
    INPUT_UNREADABLE,
    ENCODING_ERROR,
    COLUMN_MISSING,
    BAD_ROWS_THRESHOLD_EXCEEDED,
    CONSTRAINT_VIOLATION,
    LIMIT_EXCEEDED,
    PERMISSION_DENIED,
    TARGET_WRITE_FAILED,
    CANCELLED,
    INTERNAL,
)

_E = TypeVar("_E", bound=BaseException)


def with_error_code(error: _E, code: str) -> _E:
    """Give ``error`` the error code ``code`` unless it already has one; return ``error``.

    The first code wins: the place that raised an error knows best what it means, so a caller
    further up only fills in a code that is still missing.
    """
    if getattr(error, "error_code", None) is None:
        error.error_code = code  # type: ignore[attr-defined]
    return error


class ProcessingError(Exception):
    """Raised when data processing fails."""

    error_code: Optional[str] = None


class ImportInterrupted(ProcessingError):
    """The import was stopped on purpose before it finished; its outputs are discarded.

    Raised from the progress and cancellation hooks (``progress=`` / ``cancel=`` of the
    importers). Unlike other errors it ends the whole import at once: the SQL importer does not
    go on with the next table and the Excel importer not with the next sheet.
    """


class ImportCancelled(ImportInterrupted):
    """The ``cancel`` callback asked the import to stop."""

    error_code = CANCELLED


class LimitExceededError(ImportInterrupted):
    """A limit set by the caller (input bytes, rows, seconds) was exceeded."""

    error_code = LIMIT_EXCEEDED
