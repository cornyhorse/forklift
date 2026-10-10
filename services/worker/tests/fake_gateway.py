"""An in-process fake of the gateway's internal API, with a minimal object store (tests only).

``FakeGateway`` serves ``/internal/v1`` (leases, heartbeats, presign, complete) from memory, the
way the brief specifies it, and under ``/store/`` an object store that its "presigned" URLs point
at: GET returns an object with Content-Length and ETag, PUT stores a body and checks Content-MD5
the way S3 does. ``fail(endpoint, ...)`` queues faults; every request is recorded.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlsplit

SIGNATURE = "X-Amz-Signature=0123456789abcdef0123456789abcdef"
CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "jobresult.schema.json"


@dataclass
class FakeJob:
    job_id: str
    spec: dict[str, Any]
    attempt: int = 1
    leased: bool = False
    completed: dict[str, Any] | None = None
    heartbeats: list[dict[str, Any]] = field(default_factory=list)
    cancel: bool = False


class FakeGateway:
    def __init__(self, token: str = "worker-token-0123456789"):
        self.token = token
        self.lease_seconds: float = 30
        self.stage_max_bytes = 1024 * 1024
        self.queue: deque[FakeJob] = deque()
        self.jobs: dict[str, FakeJob] = {}
        self.objects: dict[str, bytes] = {}
        self.etags: dict[str, str] = {}
        self.requests: list[dict[str, Any]] = []
        self.faults: dict[str, deque] = defaultdict(deque)
        self.contract_errors: list[str] = []  # completed results that break the JobResult schema
        self.cancel_when: Callable[[FakeJob, dict], bool] | None = None
        self.lost_when: Callable[[FakeJob, dict], bool] | None = None
        self.changed = threading.Condition()
        self._ids = itertools.count(1)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self))
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )

    # ----------------------------------------------------------------------- lifecycle

    def __enter__(self) -> "FakeGateway":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def host(self) -> str:
        return urlsplit(self.url).netloc

    # ----------------------------------------------------------------------- test helpers

    def put_object(self, key: str, data: bytes, etag: str | None = None) -> dict[str, Any]:
        """Store an object; returns the presigned_url location a lease would carry."""
        self.objects[key] = data
        self.etags[key] = etag or hashlib.md5(data).hexdigest()
        return {
            "type": "presigned_url",
            "url": f"{self.url}/store/{key}?{SIGNATURE}",
            "size": len(data),
            "etag": f'"{self.etags[key]}"',
        }

    def upload_target(self, key: str) -> tuple[str, dict[str, str]]:
        """The presigned PUT URL and headers for ``key`` (a real store overrides this)."""
        return f"{self.url}/store/{key}?{SIGNATURE}", {"Content-Type": "application/octet-stream"}

    def enqueue(
        self, spec: dict[str, Any] | None = None, *, attempt: int = 1, **fields
    ) -> FakeJob:
        job_id = fields.pop("job_id", None) or f"job-{next(self._ids)}"
        if spec is None:
            spec = make_spec(
                job_id, self.put_object(f"uploads/{job_id}/people.csv", b"id,name\n1,a\n")
            )
        spec = {**spec, "job_id": job_id, **fields}
        job = FakeJob(job_id, spec, attempt)
        self.queue.append(job)
        self.jobs[job_id] = job
        return job

    def fail(self, endpoint: str, status: int | str | Callable, times: int = 1) -> None:
        """The next ``times`` requests to ``endpoint`` (leases, heartbeat, presign, complete,
        get, put) get ``status``, or the action it names ("drop": close without answering)."""
        for _ in range(times):
            self.faults[endpoint].append(status)

    def wait_for(self, predicate: Callable[[], bool], timeout: float = 20) -> None:
        deadline = time.monotonic() + timeout
        with self.changed:
            while not predicate():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError("timed out waiting for the fake gateway")
                self.changed.wait(remaining)

    def calls(self, endpoint: str) -> list[dict[str, Any]]:
        return [request for request in self.requests if request["endpoint"] == endpoint]

    def notify(self) -> None:
        with self.changed:
            self.changed.notify_all()


def contract_errors(result: dict[str, Any]) -> list[str]:
    """Where ``result`` breaks contracts/jobresult.schema.json (nothing before E publishes it)."""
    if not CONTRACT.exists():
        return []
    import jsonschema

    validator = jsonschema.Draft202012Validator(json.loads(CONTRACT.read_text()))
    return [f"{list(error.path)}: {error.message}" for error in validator.iter_errors(result)]


def make_spec(job_id: str, location: dict[str, Any], **options) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "job_id": job_id,
        "kind": "run",
        "input": {"format": "csv", "location": location, "options": {}},
        "schema": None,
        "output": {"location": {"type": "file", "path": "out/"}, "compression": "snappy"},
        "options": {"fake_engine": options} if options else {},
        "limits": {"max_input_bytes": None, "max_seconds": None, "max_rows": None},
    }


def _handler(gateway: FakeGateway):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            pass

        # ------------------------------------------------------------------- plumbing

        def _body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def _send(self, status: int, body: bytes = b"", headers: dict | None = None) -> None:
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _json(self, status: int, document: Any) -> None:
            self._send(status, json.dumps(document).encode(), {"Content-Type": "application/json"})

        def _fault(self, endpoint: str) -> bool:
            """Answer with a queued fault for ``endpoint``, if any."""
            queue = gateway.faults.get(endpoint)
            if not queue:
                return False
            fault = queue.popleft()
            if callable(fault):
                fault(self)
            elif fault == "drop":
                self.close_connection = True
                self.connection.shutdown(2)
            else:
                self._json(fault, {"detail": f"injected HTTP {fault}"})
            return True

        def _record(self, endpoint: str, body: Any) -> None:
            gateway.requests.append(
                {
                    "endpoint": endpoint,
                    "path": self.path,
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                }
            )

        # ------------------------------------------------------------------- object store

        def do_GET(self) -> None:
            key = urlsplit(self.path).path.removeprefix("/store/")
            self._record("get", None)
            if self._fault("get"):
                return
            if SIGNATURE not in self.path or key not in gateway.objects:
                self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")
                return
            data, etag = gateway.objects[key], f'"{gateway.etags[key]}"'
            if_match = self.headers.get("If-Match")
            if if_match and if_match != etag:
                self._send(412, b"<Error><Code>PreconditionFailed</Code></Error>")
                return
            ranged = re.match(r"^bytes=(\d+)-(\d*)$", self.headers.get("Range", ""))
            if not ranged:
                self._send(200, data, {"ETag": etag})
                return
            start = int(ranged.group(1))
            end = min(int(ranged.group(2) or len(data) - 1), len(data) - 1)
            if start >= len(data):
                self._send(416, b"<Error><Code>InvalidRange</Code></Error>")
                return
            content_range = f"bytes {start}-{end}/{len(data)}"
            self._send(206, data[start : end + 1], {"ETag": etag, "Content-Range": content_range})

        def do_PUT(self) -> None:
            key = urlsplit(self.path).path.removeprefix("/store/")
            body = self._body()
            self._record("put", {"key": key, "bytes": len(body)})
            if self._fault("put"):
                return
            expected = self.headers.get("Content-MD5")
            if expected and base64.b64encode(hashlib.md5(body).digest()).decode() != expected:
                self._send(400, b"<Error><Code>BadDigest</Code></Error>")
                return
            gateway.objects[key] = body
            self._send(200, b"", {"ETag": f'"{hashlib.md5(body).hexdigest()}"'})
            gateway.notify()

        # ------------------------------------------------------------------- internal API

        def do_POST(self) -> None:
            raw = self._body()
            path = urlsplit(self.path).path
            parts = path.removeprefix("/internal/v1/").split("/")
            endpoint = parts[0] if parts[0] == "leases" else parts[-1]
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = None
            self._record(endpoint, body)
            try:
                if self.headers.get("Authorization") != f"Bearer {gateway.token}":
                    self._json(401, {"detail": "invalid worker token"})
                elif not path.startswith("/internal/v1/"):
                    self._json(404, {"detail": "not found"})
                elif self._fault(endpoint):
                    pass
                elif endpoint == "leases":
                    self._lease()
                else:
                    self._job(unquote(parts[1]), endpoint, body)
            finally:
                gateway.notify()

        def _lease(self) -> None:
            if not gateway.queue:
                self._send(204)
                return
            job = gateway.queue.popleft()
            job.leased = True
            self._json(
                200,
                {
                    "job_id": job.job_id,
                    "attempt": job.attempt,
                    "lease_seconds": gateway.lease_seconds,
                    "stage_max_bytes": gateway.stage_max_bytes,
                    "spec": job.spec,
                },
            )

        def _job(self, job_id: str, endpoint: str, body: dict) -> None:
            job = gateway.jobs.get(job_id)
            if job is None:
                self._json(404, {"detail": "no such job"})
                return
            if not job.leased or job.completed is not None or body.get("attempt") != job.attempt:
                self._json(409, {"detail": "the lease is not yours"})
                return
            if endpoint == "heartbeat":
                progress = body["progress"]
                if any(
                    isinstance(v, bool) or not isinstance(v, (int, float))
                    for v in progress.values()
                ):
                    self._json(400, {"detail": "progress values must be numbers"})
                    return
                job.heartbeats.append(progress)
                if gateway.lost_when and gateway.lost_when(job, body["progress"]):
                    self._json(409, {"detail": "the lease expired"})
                    return
                if gateway.cancel_when and gateway.cancel_when(job, body["progress"]):
                    job.cancel = True
                self._json(200, {"lease_seconds": gateway.lease_seconds, "cancel": job.cancel})
            elif endpoint == "presign":
                uploads = []
                for item in body["files"]:
                    key = f"jobs/{job_id}/attempt-{job.attempt}/{item['name']}"
                    url, headers = gateway.upload_target(key)
                    uploads.append(
                        {
                            "name": item["name"],
                            "key": key,
                            "url": url,
                            "method": "PUT",
                            "headers": headers,
                        }
                    )
                self._json(200, {"uploads": uploads})
            elif endpoint == "complete":
                gateway.contract_errors += contract_errors(body["result"])
                job.completed = body
                self._json(200, {})
            else:
                self._json(404, {"detail": "no such endpoint"})

    return Handler
