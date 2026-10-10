"""How ``forklift.outputs.sql`` describes database errors without their message text."""

from __future__ import annotations

import pytest

from forklift.outputs.sql.errors import (
    DatabaseFailure,
    DriverCodes,
    TableWriteCancelled,
    TableWriteError,
    database_error_codes,
    describe_failure,
    failure_error_code,
)


class DriverError(Exception):
    """A pyodbc-style error: ``(SQLSTATE, message)`` from the driver."""


CODES = DriverCodes(
    {1142: "command denied", 1213: "deadlock"},
    privilege=frozenset({1142}),
    retryable=frozenset({1213}),
)


class TestDatabaseErrorCodes:
    def test_driver_errors_give_their_sqlstate_and_numeric_code(self):
        error = DriverError("42000", "[42000] ... denied to user 'x' (1142) (SQLExecDirectW)")
        assert database_error_codes(error) == ("42000", 1142)

    def test_oracle_errors_give_the_ora_number_even_with_trailing_padding(self):
        error = DriverError("HY000", "[Oracle][ODBC][Ora]ORA-01031: insufficient\n\x00\x00LLL")
        assert database_error_codes(error) == ("HY000", 1031)

    def test_a_driver_error_without_a_code(self):
        assert database_error_codes(
            DriverError("HY000", "The driver did not supply an error!")
        ) == (
            "HY000",
            None,
        )

    @pytest.mark.parametrize(
        "message, state",
        [
            ("String data, right truncation: length 30 buffer 10", "22001"),
            ("Converting decimal loses precision", "22003"),
            ("Invalid parameter type.  param-index=0 param-type=dict", "HY000"),
        ],
    )
    def test_pyodbc_errors_carry_the_sqlstate_second(self, message, state):
        assert database_error_codes(DriverError(message, "HY000")) == (state, None)

    @pytest.mark.parametrize(
        "args", [(), ("only one",), ("not a state", "nor this"), (1, "x"), ("42000", None)]
    )
    def test_anything_else_has_no_codes(self, args):
        assert database_error_codes(DriverError(*args)) == (None, None)


class TestDescribeFailure:
    def test_a_privilege_refusal_by_sqlstate(self):
        failure = describe_failure(
            DriverError("42501", "permission denied (7) (SQLExecute)"), CODES
        )

        assert failure.privilege_refused and not failure.retryable
        assert failure.describe() == "SQLSTATE 42501 (insufficient privilege), driver error 7"
        assert failure_error_code(failure) == "PERMISSION_DENIED"

    def test_a_privilege_refusal_by_driver_code(self):
        failure = describe_failure(DriverError("42000", "denied (1142) (SQLExecute)"), CODES)

        assert failure.privilege_refused
        assert failure.describe() == (
            "SQLSTATE 42000 (syntax error or access rule violation), "
            "driver error 1142 (command denied)"
        )

    @pytest.mark.parametrize(
        "args",
        [
            ("40001", "serialization failure"),
            ("08S01", "link failure"),
            ("HYT00", "timeout"),
            ("HY000", "Deadlock found (1213) (SQLExecute)"),
        ],
    )
    def test_temporary_failures_are_retryable(self, args):
        failure = describe_failure(DriverError(*args), CODES)
        assert failure.retryable and failure_error_code(failure) == "TARGET_WRITE_FAILED"

    def test_unknown_codes_are_reported_as_they_are(self):
        failure = describe_failure(DriverError("XX123", "x (99) (SQLExecute)"), CODES)
        assert failure.describe() == "SQLSTATE XX123, driver error 99"

    def test_no_codes_at_all(self):
        failure = describe_failure(ValueError("boom"), CODES)
        assert failure == DatabaseFailure(None, None, "", False, False)
        assert failure.describe() == "no SQLSTATE or driver code reported"


def test_errors_carry_what_failed():
    error = TableWriteError(
        "message", table="s.t", mode="append", action="x", sqlstate="42501", privilege="INSERT"
    )
    assert (error.table, error.mode, error.action, error.sqlstate, error.privilege) == (
        "s.t",
        "append",
        "x",
        "42501",
        "INSERT",
    )
    assert error.error_code == "TARGET_WRITE_FAILED" and not error.retryable

    cancelled = TableWriteCancelled("stopped", table="s.t")
    assert isinstance(cancelled, TableWriteError) and cancelled.error_code == "CANCELLED"
