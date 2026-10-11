"""The gate in front of /mcp: the Host and Origin checks, then the caller's API token.

Every request to ``/mcp`` (and ``/mcp/...``) passes, in this order:

1. **DNS-rebinding guard.** The MCP SDK's transport security (``TransportSecuritySettings``) with
   ``FORKLIFT_MCP_ALLOWED_HOSTS`` and ``FORKLIFT_MCP_ALLOWED_ORIGINS``: a ``Host`` it does not
   list is answered 421, an ``Origin`` it does not list 403 (requests without one, as MCP
   clients send them, pass). The SDK's transport checks again; this gate checks first so that
   nothing reaches the gateway for such a request.
2. **Methods.** The server is stateless (no sessions, no server-initiated stream), so only POST
   is served; GET and DELETE are answered 405, as the MCP specification allows.
3. **The token.** ``Authorization: Bearer <forklift API token>`` is required (401 with
   ``WWW-Authenticate: Bearer`` without one), and checked with the gateway (``GET /api/v1/me``)
   on every request, so a revoked token stops working at once. The token is forwarded to the
   gateway and nowhere else, and never stored or logged.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable, Mapping, Optional

from forklift_client import ApiError, ForkliftError, TransportError, Unauthorized
from mcp.server.transport_security import TransportSecurityMiddleware, TransportSecuritySettings
from starlette.requests import Request

from .gateway import Gateway

log = logging.getLogger("forklift_mcp_server")

MCP_PATH = "/mcp"
REALM = "forklift-mcp"

Scope = Mapping[str, Any]
ASGIApp = Callable[..., Awaitable[None]]


def security_settings(allowed_hosts, allowed_origins) -> TransportSecuritySettings:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(allowed_hosts),
        allowed_origins=list(allowed_origins),
    )


def bearer_token(headers: Optional[Mapping[str, str]]) -> Optional[str]:
    """The token of an ``Authorization: Bearer <token>`` header (None without one)."""
    if not headers:
        return None
    value = next((v for k, v in headers.items() if k.lower() == "authorization"), None)
    if value is None:
        return None
    scheme, _, token = value.strip().partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        return None
    return token


class Gate:
    """ASGI middleware: the checks above for /mcp; everything else passes through."""

    def __init__(self, app: ASGIApp, gateway: Gateway):
        self.app = app
        self.gateway = gateway
        settings = gateway.settings
        self._security = TransportSecurityMiddleware(
            security_settings(settings.allowed_hosts, settings.allowed_origins)
        )

    async def __call__(self, scope, receive, send) -> None:
        path = scope.get("path", "") if scope["type"] == "http" else ""
        if path != MCP_PATH and not path.startswith(MCP_PATH + "/"):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        refused = await self._security.validate_request(request)
        if refused is not None:
            if refused.status_code == 421:
                log.warning("Refused a request with Host %r", request.headers.get("host"))
                await _json(
                    send,
                    421,
                    {
                        "error": "invalid_host",
                        "error_description": "This server does not answer to this Host header: "
                        "add it to FORKLIFT_MCP_ALLOWED_HOSTS (name, name:port, or name:* for "
                        "any port), exactly as clients send it",
                    },
                )
            else:
                log.warning("Refused a request with Origin %r", request.headers.get("origin"))
                await _json(
                    send,
                    403,
                    {
                        "error": "invalid_origin",
                        "error_description": "Requests from this Origin are not accepted: add it "
                        "to FORKLIFT_MCP_ALLOWED_ORIGINS to allow a browser-based client",
                    },
                )
            return
        if scope["method"] != "POST":
            await _json(
                send,
                405,
                {
                    "error": "method_not_allowed",
                    "error_description": "This MCP server is stateless: send JSON-RPC messages "
                    "with POST (it offers no server-sent event stream and no sessions)",
                },
                headers=[(b"allow", b"POST")],
            )
            return
        token = bearer_token(request.headers)
        if token is None:
            await _unauthorized(
                send,
                "This MCP server needs a forklift API token: send Authorization: Bearer fkl_... "
                "(create one in the forklift UI under API tokens, or with POST /api/v1/tokens)",
                error=None,
            )
            return
        try:
            client = self.gateway.client(token)
            me = await self.gateway.call(client.me)
        except ValueError:  # whitespace or control characters: no header could carry it on
            await _unauthorized(send, "The API token is not a valid token")
            return
        except Unauthorized:
            await _unauthorized(
                send,
                "The gateway refused the API token: it is unknown, revoked or expired (or a "
                "worker token)",
            )
            return
        except (TransportError, ApiError) as error:
            log.warning("Could not check a token with the gateway: %s", error)
            transient = isinstance(error, TransportError) or error.transient
            await _json(
                send,
                503 if transient else 502,
                {
                    "error": "gateway_unavailable",
                    "error_description": "The forklift gateway did not answer the token check "
                    f"({_short(error)}); try again later",
                },
                headers=[(b"retry-after", b"5")] if transient else [],
            )
            return
        except ForkliftError as error:  # an answer that is not the API's (a wrong API URL)
            log.error("The gateway's answer to GET /api/v1/me is not usable: %s", error)
            await _json(
                send,
                502,
                {
                    "error": "gateway_unavailable",
                    "error_description": "The forklift gateway's answer to the token check is "
                    "not usable; is FORKLIFT_MCP_API_URL the gateway's public port?",
                },
            )
            return
        scope.setdefault("state", {})["forklift_user"] = me.user.username
        await self.app(scope, receive, send)


def _short(error: ForkliftError) -> str:
    if isinstance(error, ApiError):
        return f"HTTP {error.status}"
    return error.reason if isinstance(error, TransportError) else type(error).__name__


async def _unauthorized(send, description: str, *, error: Optional[str] = "invalid_token") -> None:
    # RFC 6750: no error code when the request carried no credentials at all
    parts = [f'realm="{REALM}"']
    if error is not None:
        parts += [f'error="{error}"', f'error_description="{description}"']
    body = {"error": error or "unauthorized", "error_description": description}
    challenge = ("Bearer " + ", ".join(parts)).encode("latin-1", "replace")
    await _json(send, 401, body, headers=[(b"www-authenticate", challenge)])


async def _json(send, status: int, body: Mapping[str, Any], headers=()) -> None:
    data = json.dumps(body).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(data)).encode()),
                (b"cache-control", b"no-store"),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": data})
