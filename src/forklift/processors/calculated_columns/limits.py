"""Resource limits and the error type shared by the expression evaluator and its functions.

Expressions come from schema files, so the evaluator must not let a (possibly malicious or
merely careless) expression exhaust memory or CPU. Every limit below is checked *before* the
expensive operation is carried out.
"""

from __future__ import annotations

from typing import Any, Iterable

# Parser / AST limits
MAX_EXPRESSION_LENGTH = 4096  # characters of expression source
MAX_EXPRESSION_NODES = 512  # AST nodes in a single expression
MAX_CALL_ARGUMENTS = 64  # positional + keyword arguments in a single call

# Value limits
MAX_POWER_EXPONENT = 10_000  # largest |exponent| accepted by ** and power()
MAX_INT_BITS = 65_536  # largest integer produced by ** and * (bit length)
MAX_SEQUENCE_LENGTH = 1_000_000  # largest str/bytes/list/tuple produced by *, +, replace(), ...


class ExpressionError(ValueError):
    """Raised for expressions that are unsafe, malformed or fail while being evaluated.

    Messages never contain cell values (they may be personal data); they only name
    the construct, function, column or limit involved.
    """


def _length(value: Any) -> int:
    return len(value) if isinstance(value, (str, bytes, list, tuple)) else 0


def check_result(value: Any, inputs: Iterable[Any] = ()) -> Any:
    """Reject results that are unreasonably large compared with their inputs.

    A result may be as large as the configured limit plus the largest input, so working with
    genuinely large cell values keeps working while amplification (``'x' * n``,
    ``replace(x, '', 'abc')``, repeated ``+``) is bounded.
    """
    if isinstance(value, complex):
        raise ExpressionError("Result is not a real number")

    if isinstance(value, (str, bytes)):
        if len(value) > MAX_SEQUENCE_LENGTH:
            allowance = MAX_SEQUENCE_LENGTH + max((_length(i) for i in inputs), default=0)
            if len(value) > allowance:
                raise ExpressionError(
                    f"Result exceeds the maximum length of {MAX_SEQUENCE_LENGTH} characters"
                )
    elif isinstance(value, int) and not isinstance(value, bool):
        if value.bit_length() > MAX_INT_BITS:
            allowance = MAX_INT_BITS + max(
                (i.bit_length() for i in inputs if isinstance(i, int)), default=0
            )
            if value.bit_length() > allowance:
                raise ExpressionError(f"Integer result exceeds the limit of {MAX_INT_BITS} bits")
    return value


def checked_mul(a: Any, b: Any) -> Any:
    """``a * b`` with bounds on sequence repetition and integer growth."""
    seq_types = (str, bytes, list, tuple)
    if isinstance(a, seq_types) and isinstance(b, int):
        _check_repeat(len(a), b)
    elif isinstance(b, seq_types) and isinstance(a, int):
        _check_repeat(len(b), a)
    elif isinstance(a, int) and isinstance(b, int):
        if a.bit_length() + b.bit_length() > MAX_INT_BITS + max(a.bit_length(), b.bit_length()):
            raise ExpressionError(f"Integer result exceeds the limit of {MAX_INT_BITS} bits")
    return check_result(a * b, (a, b))


def _check_repeat(length: int, times: int) -> None:
    if length * max(times, 0) > max(MAX_SEQUENCE_LENGTH, length):
        raise ExpressionError(
            f"Sequence repetition would exceed the maximum length of {MAX_SEQUENCE_LENGTH}"
        )


def checked_pow(a: Any, b: Any) -> Any:
    """``a ** b`` with bounds on the exponent and on the size of integer results."""
    try:
        too_large = abs(b) > MAX_POWER_EXPONENT
    except TypeError:
        too_large = False  # the operator below raises the natural TypeError
    if too_large:
        raise ExpressionError(f"Exponent magnitude exceeds the limit of {MAX_POWER_EXPONENT}")

    if (
        isinstance(a, int)
        and isinstance(b, int)
        and b > 0
        and abs(a) > 1
        and abs(a).bit_length() * b > MAX_INT_BITS
    ):
        raise ExpressionError(f"Integer result exceeds the limit of {MAX_INT_BITS} bits")

    return check_result(a**b, (a,))
