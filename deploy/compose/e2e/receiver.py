"""A webhook receiver for the end-to-end tests (the webhook-receiver service of webhooks.yml).

It records every POST (path, headers and body, as text) and answers 200; GET /deliveries
returns what it recorded as JSON, GET /health answers 200. Standard library only; it keeps
everything in memory and is not meant for anything but the tests.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RECEIVED = []
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with LOCK:
            RECEIVED.append(
                {
                    "path": self.path,
                    "headers": {name.lower(): value for name, value in self.headers.items()},
                    "body": body.decode("utf-8", "replace"),
                }
            )
        self._answer(200, b"{}")

    def do_GET(self):
        if self.path == "/health":
            self._answer(200, b"{}")
        elif self.path == "/deliveries":
            with LOCK:
                self._answer(200, json.dumps(RECEIVED).encode())
        else:
            self._answer(404, b"{}")

    def _answer(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
