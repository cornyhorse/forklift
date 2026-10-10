"""Helpers shared by the /api/v1 routers."""

from __future__ import annotations

from forklift_web.api.payloads import ErrorOut

ERRORS = {
    frozenset({400, 401, 403, 404, 409, 410, 502}): ErrorOut,
}


def responses(ok: dict) -> dict:
    """``ok`` (status -> schema) plus the error responses every operation can give."""
    return {**ok, **ERRORS}
