"""An HTTP forward proxy for the worker's tests: it forwards what it allows and records everything.

``ForwardProxy`` answers the two forms an HTTP proxy sees: plain requests in absolute form
(``GET http://host:port/path?query HTTP/1.1``), which it sends on to the server and whose answer
it relays, and ``CONNECT host:port``, after which it copies bytes both ways (TLS stays end to
end). With ``allow`` set, a request for any other ``host:port`` gets 403 with an
``X-Squid-Error: ERR_ACCESS_DENIED 0`` header, as Squid answers one its rules deny. Each request
is recorded in ``requests`` as ``{"method", "target", "headers"}``, the query string included, so
that tests can check exactly what went through it (tests only).
"""

from __future__ import annotations

import http.client
import select
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterable
from urllib.parse import urlsplit

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class ForwardProxy:
    def __init__(self, allow: Iterable[str] | None = None):
        self.allow = None if allow is None else {entry.lower() for entry in allow}
        self.requests: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self))
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )

    def __enter__(self) -> "ForwardProxy":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def record(self, method: str, target: str, headers) -> None:
        with self._lock:
            self.requests.append(
                {
                    "method": method,
                    "target": target,
                    "headers": {key.lower(): value for key, value in headers.items()},
                }
            )

    def allows(self, host: str, port: int) -> bool:
        return self.allow is None or f"{host}:{port}".lower() in self.allow

    def targets(self, method: str | None = None) -> list[str]:
        return [r["target"] for r in self.requests if method is None or r["method"] == method]


def _handler(proxy: ForwardProxy):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            pass

        def _refuse(self) -> None:
            body = b"<html><body>Access Denied</body></html>"
            self.send_response(403)
            self.send_header("X-Squid-Error", "ERR_ACCESS_DENIED 0")
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_CONNECT(self) -> None:
            proxy.record("CONNECT", self.path, self.headers)
            host, _, port = self.path.rpartition(":")
            if not proxy.allows(host, int(port)):
                self._refuse()
                return
            upstream = socket.create_connection((host, int(port)), timeout=10)
            self.send_response(200, "Connection established")
            self.end_headers()
            self.close_connection = True
            sockets = [self.connection, upstream]
            try:
                while True:
                    readable, _, _ = select.select(sockets, [], [], 10)
                    if not readable:
                        return
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        (upstream if source is self.connection else self.connection).sendall(data)
            finally:
                upstream.close()

        def _forward(self) -> None:
            proxy.record(self.command, self.path, self.headers)
            target = urlsplit(self.path)
            port = target.port or 80
            if target.scheme != "http" or not proxy.allows(target.hostname or "", port):
                self._refuse()
                return
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            headers = {
                key: value for key, value in self.headers.items() if key.lower() not in _HOP_BY_HOP
            }
            path = target.path + (f"?{target.query}" if target.query else "")
            upstream = http.client.HTTPConnection(target.hostname, port, timeout=10)
            try:
                upstream.request(self.command, path, body=body, headers=headers)
                answer = upstream.getresponse()
                data = answer.read()
                self.send_response(answer.status, answer.reason)
                for key, value in answer.getheaders():
                    if key.lower() not in _HOP_BY_HOP and key.lower() != "content-length":
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            finally:
                upstream.close()

        do_GET = do_PUT = do_POST = do_HEAD = do_DELETE = _forward

    return Handler
