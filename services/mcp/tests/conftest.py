"""Fixtures: the fake gateway and store, the MCP server on a real uvicorn, and an MCP client.

Every tool test goes the way an agent's call does: an MCP client (the SDK's own, over Streamable
HTTP) to the server on a free port, which calls the fake gateway on another one over HTTP.
"""

from __future__ import annotations

import io
import json
import socket
import threading
import time
from typing import Any, Dict, Optional

import anyio
import httpx2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import uvicorn
from fake_gateway import TOKEN, FakeGateway
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from forklift_mcp_server.app import create_app
from forklift_mcp_server.gateway import Gateway
from forklift_mcp_server.settings import Settings
from forklift_mcp_server.tools import Toolbox


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class RunningServer:
    """The MCP server's ASGI app on uvicorn in a thread."""

    def __init__(self, settings: Settings, toolbox: Optional[Toolbox] = None):
        self.settings = settings
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            create_app(settings, toolbox),
            host="127.0.0.1",
            port=self.port,
            log_config=None,
            lifespan="on",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> "RunningServer":
        self._thread.start()
        deadline = time.monotonic() + 20
        while not self._server.started:
            assert time.monotonic() < deadline, "uvicorn did not start"
            time.sleep(0.01)
        return self

    def __exit__(self, *exc) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=20)


def settings_for(fake: FakeGateway, **changes: Any) -> Settings:
    values: Dict[str, Any] = dict(
        api_url=fake.url,
        allowed_hosts=("127.0.0.1:*", "localhost:*"),
        api_timeout=10.0,
        max_wait_seconds=5.0,
    )
    values.update(changes)
    return Settings(**values)


def fast_toolbox(settings: Settings) -> Toolbox:
    return Toolbox(Gateway(settings), poll_interval=0.01, max_poll_interval=0.02)


class Mcp:
    """A synchronous face of the SDK's MCP client, one connection per call."""

    def __init__(self, url: str, token: Optional[str] = TOKEN, mode: str = "auto"):
        self.url = url + "/mcp"
        self.token = token
        self.mode = mode

    async def _with_client(self, action):
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        async with httpx2.AsyncClient(headers=headers, timeout=30) as http:
            async with Client(
                streamable_http_client(self.url, http_client=http), mode=self.mode
            ) as client:
                return await action(client)

    def run(self, action):
        return anyio.run(self._with_client, action)

    def call(self, name: str, arguments: Optional[Dict[str, Any]] = None):
        return self.run(lambda client: client.call_tool(name, arguments or {}))

    def ok(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = self.call(name, arguments)
        assert not result.is_error, result.content[0].text
        assert json.loads(result.content[0].text) == result.structured_content
        return result.structured_content

    def refused(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> str:
        result = self.call(name, arguments)
        assert result.is_error, result.structured_content
        return result.content[0].text


@pytest.fixture
def fake():
    gateway = FakeGateway().start()
    yield gateway
    gateway.stop()
    assert gateway.violations == [], gateway.violations


@pytest.fixture
def server(fake):
    settings = settings_for(fake)
    with RunningServer(settings, fast_toolbox(settings)) as running:
        yield running


@pytest.fixture
def mcp(server) -> Mcp:
    return Mcp(server.url)


def parquet_bytes(table: pa.Table, row_group_size: Optional[int] = None) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=row_group_size)
    return sink.getvalue()


def bad_rows_table(rows: int, reason=None) -> pa.Table:
    """A bad_rows file as the engine writes it: the input's text and _rejection_reason."""
    reason = reason or (lambda i: "VALIDATION_ERROR:age" if i % 3 else "type_conversion_failed")
    return pa.table(
        {
            "id": [str(i) for i in range(rows)],
            "age": [f"-{i}" for i in range(rows)],
            "_rejection_reason": [reason(i) for i in range(rows)],
        }
    )
