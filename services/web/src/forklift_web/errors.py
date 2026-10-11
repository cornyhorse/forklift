"""Errors raised by the service layer, each with the HTTP status the API answers with.

The API and the HTML views both call the service layer; the API turns these into
``{"detail": message, "code": code}`` responses, a view can show ``message`` to the person.
Messages are specific (what was wrong, with which object, what to do) but never contain cell
values, passwords, tokens or other secrets.
"""

from __future__ import annotations


class ServiceError(Exception):
    status = 400
    code = "invalid_request"

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class InvalidRequest(ServiceError):
    """The request is malformed or names something that cannot be used (400)."""


class NotAuthenticated(ServiceError):
    status = 401
    code = "not_authenticated"


class PermissionDenied(ServiceError):
    status = 403
    code = "permission_denied"


class NotFound(ServiceError):
    status = 404
    code = "not_found"


class Conflict(ServiceError):
    """The request conflicts with the object's current state (409)."""

    status = 409
    code = "conflict"


class Gone(ServiceError):
    """The object existed but its data was deleted, for example by retention (410)."""

    status = 410
    code = "gone"


class TooManyAttempts(ServiceError):
    """Too many failed attempts: refused, without being looked at, until ``retry_at`` (429)."""

    status = 429
    code = "too_many_attempts"

    def __init__(self, message: str, *, retry_at):
        super().__init__(message)
        self.retry_at = retry_at


class StoreUnavailable(ServiceError):
    """The object store or another upstream service did not answer as expected (502)."""

    status = 502
    code = "store_unavailable"
