"""The HTTP side: Streamable HTTP at /mcp behind the gate, /healthz, and uvicorn.

The MCP endpoint is **stateless** (the SDK's ``stateless_http``): every POST is handled on its
own, with no session to keep, so replicas behind a Service need no sticky sessions and a
restart loses nothing. Responses are server-sent events where a call takes long (the SDK sends
keep-alive pings on the 2026-07-28 protocol) and JSON otherwise.
"""

from __future__ import annotations

import logging

import uvicorn
from starlette.requests import Request
from starlette.responses import JSONResponse

from .auth import MCP_PATH, Gate, security_settings
from .gateway import Gateway
from .server import create_server
from .settings import Settings
from .tools import Toolbox

log = logging.getLogger("forklift_mcp_server")

#: Seconds uvicorn gives requests in flight to finish after SIGTERM
GRACEFUL_SHUTDOWN_SECONDS = 25


async def healthz(request: Request) -> JSONResponse:
    """Liveness and readiness: the process answers. No authentication, no gateway call."""
    return JSONResponse({"status": "ok"}, headers={"cache-control": "no-store"})


def create_app(settings: Settings, toolbox: Toolbox | None = None):
    """The ASGI application: ``/mcp`` (gated) and ``/healthz``."""
    gateway = Gateway(settings)
    server = create_server(gateway, toolbox)
    server.custom_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)(healthz)
    app = server.streamable_http_app(
        streamable_http_path=MCP_PATH,
        stateless_http=True,
        json_response=False,
        transport_security=security_settings(settings.allowed_hosts, settings.allowed_origins),
        host=settings.bind_address,
    )
    return Gate(app, gateway)


def serve(settings: Settings) -> None:
    """Serve until SIGTERM or SIGINT."""
    log.info(
        "forklift-mcp-server on %s:%s, gateway %s",
        settings.bind_address,
        settings.port,
        settings.api_url,
    )
    uvicorn.run(
        create_app(settings),
        host=settings.bind_address,
        port=settings.port,
        log_config=None,
        log_level=settings.log_level.lower(),
        server_header=False,
        proxy_headers=False,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_SECONDS,
    )
