"""Turning exceptions into the job contract's error codes.

Every failure of a job maps to one of :data:`ERROR_CODES` plus the engine's own (verbose)
message. Where the engine knows what went wrong it says so on the exception (``error_code``,
see ``forklift.engine.exceptions``); otherwise the type of the exception decides. Messages are
cleaned before they reach a result: Arrow messages lose the row content they quote, database
errors are described by their SQLSTATE instead of the driver's text, secrets (connection
strings, presigned URLs) are removed, and absolute paths inside the base directory become
relative ones.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pyarrow as pa

from ..engine.exceptions import (
    BAD_ROWS_THRESHOLD_EXCEEDED,
    ENCODING_ERROR,
    ERROR_CODES,
    INPUT_UNREADABLE,
    INTERNAL,
    PERMISSION_DENIED,
    SPEC_INVALID,
)
from ..engine.importers.redaction import scrub_secrets
from ..engine.processors.text_utils import sanitize_arrow_error
from ._model import ContractError

__all__ = ["ERROR_CODES", "classify_error"]

#: Longest message a result carries (a schema with thousands of columns can make long ones)
MAX_MESSAGE_LENGTH = 4000

# botocore error codes that mean "not allowed"; every other S3 error is an unreadable input
_S3_DENIED = {"AccessDenied", "403", "Forbidden", "InvalidAccessKeyId", "SignatureDoesNotMatch"}
# SQLSTATEs of a refused login or privilege, and of failures worth another attempt
_SQL_DENIED = {"28000", "28P01", "42501"}
_SQL_TRANSIENT = {"40001", "40P01", "HYT00", "HYT01"}


def classify_error(
    error: BaseException,
    *,
    secrets: Iterable[str] = (),
    base_dir: Optional[Path] = None,
) -> Tuple[str, str, bool]:
    """``(code, message, retryable)`` for an exception that ended a job.

    Args:
        error: The exception
        secrets: Strings that must not appear in the message (connection strings, URLs)
        base_dir: Absolute paths inside it are shown relative to it
    """
    code = getattr(error, "error_code", None)
    if code not in ERROR_CODES:
        code = _code_from_type(error)
    retryable = bool(getattr(error, "retryable", False)) or _transient(error)
    return code, _message(error, code, list(secrets), base_dir), retryable


def _code_from_type(error: BaseException) -> str:
    from botocore.exceptions import NoCredentialsError

    from ..processors.data_validation.data_validation_processor import (
        BadRowsThresholdExceededError,
    )

    if isinstance(error, ContractError):
        return SPEC_INVALID
    if isinstance(error, BadRowsThresholdExceededError):
        return BAD_ROWS_THRESHOLD_EXCEEDED
    if isinstance(error, UnicodeError) or (
        isinstance(error, ValueError) and isinstance(_cause(error), UnicodeError)
    ):
        return ENCODING_ERROR
    if isinstance(error, (PermissionError, NoCredentialsError)):
        return PERMISSION_DENIED
    s3_code = _s3_error_code(error)
    if s3_code is not None:
        return PERMISSION_DENIED if s3_code in _S3_DENIED else INPUT_UNREADABLE
    if _database_error(error):
        return PERMISSION_DENIED if _sqlstate(error) in _SQL_DENIED else INPUT_UNREADABLE
    if isinstance(error, (OSError, csv.Error, ValueError, EOFError)):
        # ValueError: the engine's "this value cannot be used" (a sheet that does not exist,
        # a row too wide for the output, ...); ArrowInvalid is one too
        return INPUT_UNREADABLE
    return INTERNAL


def _cause(error: BaseException) -> Optional[BaseException]:
    """The exception ``error`` was raised from or while handling (even with ``from None``)."""
    return error.__cause__ or error.__context__


def _s3_error_code(error: BaseException) -> Optional[str]:
    """The error code of a botocore ``ClientError`` (None for anything else)."""
    from botocore.exceptions import ClientError

    if not isinstance(error, ClientError):
        return None
    return str(error.response.get("Error", {}).get("Code", ""))


def _sqlstate(error: BaseException) -> str:
    args = getattr(error, "args", ())
    return args[0] if args and isinstance(args[0], str) else ""


def _transient(error: BaseException) -> bool:
    """Network trouble that a second attempt may not meet."""
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    if _database_error(error):
        state = _sqlstate(error)
        return state.startswith("08") or state in _SQL_TRANSIENT
    return _s3_error_code(error) in {"SlowDown", "RequestTimeout", "InternalError", "503", "500"}


def _message(error: BaseException, code: str, secrets: list, base_dir: Optional[Path]) -> str:
    if _database_error(error):
        # The driver's text can quote data: the SQLSTATE and what it means instead
        from ..engine.importers.sql_importer import _failure_reason

        reason = _failure_reason(error)
        text = f"{type(error).__name__} ({reason})" if reason else type(error).__name__
    else:
        if isinstance(error, pa.ArrowException):
            text = sanitize_arrow_error(str(error))
        else:
            text = str(error) or type(error).__name__
        if code == INTERNAL or isinstance(error, (OSError, KeyError)):
            # Without the type these read like riddles ("[Errno 28] ...", "'name'")
            text = f"{type(error).__name__}: {text}"
    for secret in secrets:
        if secret:
            text = scrub_secrets(text, secret)
            text = text.replace(secret, "<redacted>")
    if base_dir is not None:
        text = text.replace(f"{base_dir}/", "").replace(str(base_dir), ".")
    if len(text) > MAX_MESSAGE_LENGTH:
        text = text[: MAX_MESSAGE_LENGTH - 16] + " ... (cut short)"
    return text


def _database_error(error: BaseException) -> bool:
    """A pyodbc error: ``args`` is ``(SQLSTATE, driver message)``; the message can quote data."""
    module = type(error).__module__ or ""
    return module.split(".")[0] == "pyodbc"
