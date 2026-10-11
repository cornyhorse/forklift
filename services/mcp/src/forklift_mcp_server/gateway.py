"""Calls to the gateway's /api/v1 with the caller's own token, through forklift-client.

The client is synchronous (urllib), so every call runs in a worker thread and a tool waiting for a
job sleeps in the event loop, not in a thread. A client is made per call: it holds the token for
that call only, and nothing outlives the request.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Awaitable, Callable, TypeVar

import anyio
from forklift_client import (
    ApiError,
    BadRequest,
    Conflict,
    Forbidden,
    ForkliftClient,
    ForkliftError,
    Gone,
    NotFound,
    RetryPolicy,
    TooManyRequests,
    TransportError,
    Unauthorized,
)
from mcp.server.mcpserver.exceptions import ToolError

from . import __version__
from .settings import Settings

log = logging.getLogger("forklift_mcp_server")

T = TypeVar("T")

#: A tool call is interactive: retry briefly, and pass long Retry-After waits on to the agent
RETRY = RetryPolicy(attempts=3, initial_delay=0.5, max_delay=4.0, max_retry_after=10.0)
USER_AGENT = f"forklift-mcp-server/{__version__}"


class ToolInputError(ToolError):
    """A call the proxy refuses, or the gateway's refusal, in words for the agent: an MCP tool
    error whose text is this message."""


class Gateway:
    """Makes forklift-client clients for one gateway (the settings) and runs their calls."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def client(self, token: str) -> ForkliftClient:
        """A client of the gateway with ``token``; ValueError for a token no header can carry."""
        return ForkliftClient(
            self.settings.api_url,
            token,
            timeout=self.settings.api_timeout,
            ca_file=self.settings.ca_file,
            retry=RETRY,
            user_agent=USER_AGENT,
        )

    async def call(self, function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """``function(*args, **kwargs)`` in a worker thread (a blocking client call)."""
        return await anyio.to_thread.run_sync(functools.partial(function, *args, **kwargs))


def describe(error: ForkliftError) -> str:
    """What a tool says when the gateway (or the store) refused or failed a call.

    The client's messages name the method, the API path, the status, the API's code and detail
    and the request id; they never carry the token or presigned URLs.
    """
    if isinstance(error, Unauthorized):
        return (
            f"The gateway refused the API token: it is unknown, revoked or expired ({error}). "
            "Configure the MCP client with a valid forklift API token."
        )
    if isinstance(error, Forbidden):
        hint = _FORBIDDEN_HINTS.get(error.code, "")
        return f"The gateway does not allow this: {error}{hint}"
    if isinstance(error, TooManyRequests):
        wait = f" in {error.retry_after:g} seconds" if error.retry_after is not None else " later"
        return f"The gateway is busy: {error}. Try again{wait}."
    if isinstance(error, (BadRequest, NotFound, Conflict, Gone)):
        return f"The gateway refused this: {error}"
    if isinstance(error, ApiError):
        return f"The gateway could not answer: {error}. Try again later."
    if isinstance(error, TransportError):
        return f"The gateway or the store did not answer: {error}. Try again later."
    return str(error)


_FORBIDDEN_HINTS = {
    "raw_rows_required": (
        " (the rows of sensitive data are shown only to users with the 'view raw rows' "
        "permission; an admin grants it)"
    ),
    "scope_missing": " (use a token that has that scope)",
    "role_insufficient": " (your role does not include it; an admin can change roles)",
}


def tool_errors(method: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Turn the gateway's refusals into tool errors that say what happened."""

    @functools.wraps(method)
    async def call(*args: Any, **kwargs: Any) -> T:
        try:
            return await method(*args, **kwargs)
        except ForkliftError as error:
            raise ToolInputError(describe(error)) from None

    return call
