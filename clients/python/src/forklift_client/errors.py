"""The exceptions of forklift-client: one hierarchy under :class:`ForkliftError`.

Messages say what was asked (the method and API path, or the host of a presigned URL), what came
back (the status, the API's ``code`` and ``detail``) and the request id to quote to an operator.
They never contain the API token, presigned URLs (their query strings are signatures) or data.
"""

from __future__ import annotations

from typing import Any, Optional


class ForkliftError(Exception):
    """Base class of every error raised by forklift-client."""


class TransportError(ForkliftError):
    """No HTTP answer: the connection was refused or reset, DNS or TLS failed, or it timed out.

    ``sent`` is False when the request certainly did not reach the server (the connection was
    refused, the host name did not resolve, the TLS handshake failed), so even a request that is
    not safe to repeat may be sent again. ``retryable`` is False for TLS failures (a certificate
    that does not verify does not get better by asking again).
    """

    def __init__(
        self, what: str, host: str, reason: str, *, sent: bool = True, retryable: bool = True
    ):
        super().__init__(f"{what}: no answer from {host} ({reason})")
        self.what = what
        self.host = host
        self.reason = reason
        self.sent = sent
        self.retryable = retryable


class UnexpectedResponse(ForkliftError):
    """The API answered, but not with what this client understands: a body that is not JSON
    (is the base URL the gateway's?), or a document without a field this client needs (a gateway
    of another version)."""


class ApiError(ForkliftError):
    """The API answered with an error status.

    ``code`` and ``detail`` are the API's (``{"detail": ..., "code": ...}``); ``request_id`` is
    the gateway's ``X-Request-ID`` for the request, which its logs and audit entries carry;
    ``retry_after`` is the ``Retry-After`` the API sent, in seconds.
    """

    def __init__(
        self,
        method: str,
        path: str,
        status: int,
        *,
        code: str = "",
        detail: str = "",
        request_id: str = "",
        retry_after: Optional[float] = None,
    ):
        text = f"{method} {path} failed with HTTP {status}"
        if code:
            text += f" ({code})"
        if detail:
            text += f": {detail}"
        if request_id:
            text += f" [request id {request_id}]"
        super().__init__(text)
        self.method = method
        self.path = path
        self.status = status
        self.code = code
        self.detail = detail
        self.request_id = request_id
        self.retry_after = retry_after

    @property
    def transient(self) -> bool:
        """Whether asking again later may succeed (429, 502, 503, 504)."""
        return self.status in (429, 502, 503, 504)


class BadRequest(ApiError):
    """400: the request is malformed or names something that cannot be used; 422: the body
    does not have the API's shape (``detail`` lists the fields)."""


class Unauthorized(ApiError):
    """401: no token, or an unknown, revoked or expired one."""


class Forbidden(ApiError):
    """403: the token's role or scopes do not allow this."""


class NotFound(ApiError):
    """404: there is no such object (or the caller may not see it)."""


class Conflict(ApiError):
    """409: the request conflicts with the object's state, for example cancelling a job that
    has finished, or an Idempotency-Key used before for a different request
    (``code == "idempotency_key_reused"``)."""


class Gone(ApiError):
    """410: the object existed but its data was deleted, for example by retention."""


class TooManyRequests(ApiError):
    """429: too many requests; ``retry_after`` says when to come back."""


class ServerError(ApiError):
    """5xx: the gateway (502: the object store behind it) could not answer."""


_BY_STATUS = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    409: Conflict,
    410: Gone,
    422: BadRequest,
    429: TooManyRequests,
}


def api_error(method: str, path: str, status: int, **fields: Any) -> ApiError:
    """The ApiError subclass for ``status``."""
    kind = _BY_STATUS.get(status) or (ServerError if status >= 500 else ApiError)
    return kind(method, path, status, **fields)


class StoreError(ForkliftError):
    """The object store refused a request to a presigned URL (an upload or a download).

    ``code`` is the store's error code (``<Code>`` of an S3 error, such as ``AccessDenied`` or
    ``ExpiredToken``) when it sent one.
    """

    def __init__(self, what: str, host: str, status: int, code: str = ""):
        detail = f": {code}" if code else ""
        super().__init__(f"{what}: the object store at {host} answered HTTP {status}{detail}")
        self.what = what
        self.host = host
        self.status = status
        self.code = code

    @property
    def transient(self) -> bool:
        """Whether sending the same request again may succeed (408, 429, 5xx)."""
        return self.status in (408, 429) or self.status >= 500


class VerificationError(ForkliftError):
    """Bytes are not what they should be: a download's size or sha256 differs from the
    artifact's, or a file changed size while it was uploaded."""


class JobFailed(ForkliftError):
    """A job ended ``failed`` or ``cancelled``; ``job`` is the job, ``code`` and ``message``
    its error (empty when it has none)."""

    def __init__(self, job: Any):
        error = job.error
        self.job = job
        self.code = error.code if error is not None else ""
        self.message = error.message if error is not None else ""
        reason = f": {self.code}: {self.message}" if error is not None else ""
        super().__init__(f"Job {job.id} ({job.kind}) {job.status}{reason}")


class JobTimeout(ForkliftError, TimeoutError):
    """The job had not ended when the wait's timeout passed; ``job`` is its last state (it goes
    on running: cancel it if it should not)."""

    def __init__(self, job: Any, timeout: float):
        super().__init__(
            f"Job {job.id} ({job.kind}) is still {job.status} after {timeout:g} seconds"
        )
        self.job = job
        self.timeout = timeout
