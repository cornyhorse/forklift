"""A fake of the gateway's /api/v1 and of the object store, on a real HTTP server (tests only).

``FakeGateway`` serves on 127.0.0.1 and a free port:

- ``/api/v1/...``: the endpoints the MCP server calls, from memory: bearer tokens of users with a
  role's scopes (and optionally "view raw rows"), the API's error shape (``{"detail", "code"}``),
  limit/offset pages, ``Idempotency-Key`` replay of job submissions, uploads (one presigned PUT),
  jobs whose statuses follow a plan (each GET of a job takes the next status), artifacts, and
  downloads that are presigned URLs of the store below (refused for raw-row artifacts of
  sensitive jobs without the permission, as the gateway does).
- ``/store/<key>?sig=<n>``: the store. GET serves an object, with ``Range: bytes=a-b`` answered
  206 and ``Content-Range`` (as S3 does); PUT stores a body after checking ``Content-MD5``. Like
  S3 it refuses a request that also carries an ``Authorization`` header, and a signature that is
  no longer current (``expire_urls()``), with 403.

Every API request and every JSON answer is checked against contracts/openapi.json with the
client package's checker (clients/python/tests/openapi_check.py), so the fake cannot drift from
the contract unnoticed. Faults: ``fail(route, status, ...)`` answers the next requests of a route
with an error; ``store_faults`` changes how the store answers range requests.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import itertools
import json
import re
import threading
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

API = "/api/v1"
TOKEN = "fkl_operator-token-0123456789"
ADMIN_TOKEN = "fkl_admin-token-0123456789"
VIEWER_TOKEN = "fkl_viewer-token-0123456789"

_CHECKER = (
    Path(__file__).resolve().parents[3] / "clients" / "python" / "tests" / "openapi_check.py"
)
_spec = importlib.util.spec_from_file_location("forklift_openapi_check", _CHECKER)
openapi_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(openapi_check)

VIEWER_SCOPES = [
    "artifacts:read",
    "connections:read",
    "datasets:read",
    "jobs:read",
    "schemas:read",
    "tokens:read",
    "tokens:write",
    "webhooks:read",
    "webhooks:write",
]
OPERATOR_SCOPES = VIEWER_SCOPES + ["jobs:run", "uploads:read", "uploads:write"]
AUTHOR_SCOPES = OPERATOR_SCOPES + ["datasets:write", "schemas:write"]
ADMIN_SCOPES = AUTHOR_SCOPES + ["admin:read", "admin:write"]


def iso(moment: Optional[datetime] = None) -> str:
    moment = moment or datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def uid() -> str:
    return str(uuid.uuid4())


@dataclass
class User:
    id: int
    username: str
    role: str
    scopes: List[str]
    raw_rows: bool = False


@dataclass
class Plan:
    """How a submitted job goes: the statuses its GETs take, then how it ends."""

    statuses: List[str] = field(default_factory=lambda: ["succeeded"])
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, str]] = None
    #: (kind, name, body bytes, rows) of the artifacts it has once it ended
    artifacts: List[Tuple[str, str, bytes, Optional[int]]] = field(default_factory=list)
    attempt: int = 1


class ApiFailure(Exception):
    def __init__(self, status: int, detail: str, code: str, headers: Optional[dict] = None):
        super().__init__(detail)
        self.status, self.detail, self.code = status, detail, code
        self.headers = headers or {}


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


class FakeGateway:
    def __init__(self) -> None:
        self.contract = openapi_check.Contract()
        self.violations: List[str] = []
        self.requests: List[Dict[str, Any]] = []
        self.store_requests: List[Dict[str, Any]] = []
        self.users = {
            TOKEN: User(2, "olivia", "operator", OPERATOR_SCOPES),
            ADMIN_TOKEN: User(1, "admin", "admin", ADMIN_SCOPES, raw_rows=True),
            VIEWER_TOKEN: User(3, "victor", "viewer", VIEWER_SCOPES),
        }
        self.faults: Dict[str, Deque[ApiFailure]] = defaultdict(deque)
        #: How the store answers range requests: "ignore_range" (200, whole body),
        #: "wrong_range" (another Content-Range), "short" (fewer bytes than the range)
        self.store_faults: Deque[str] = deque()
        self.connections: Dict[str, Dict[str, Any]] = {}
        self.datasets: Dict[str, Dict[str, Any]] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.versions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.uploads: Dict[str, Dict[str, Any]] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.plans: Dict[str, Deque[str]] = {}
        self.endings: Dict[str, Plan] = {}
        self.artifacts: Dict[str, Dict[str, Any]] = {}
        self.objects: Dict[str, bytes] = {}
        self.keys: Dict[Tuple[int, str], Tuple[str, str]] = {}
        #: The plan of the next submitted job: a Plan, or a function of the request body
        self.planner: Callable[[Dict[str, Any]], Plan] = lambda body: Plan()
        self.signature = 1
        self.me_answer: Optional[Tuple[int, bytes]] = None  # a raw answer to GET /me
        self._lock = threading.RLock()
        self._server = _Server(("127.0.0.1", 0), _handler(self))
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> "FakeGateway":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    # ------------------------------------------------------------------ test helpers

    def fail(
        self,
        route: str,
        status: int,
        *,
        times: int = 1,
        code: str = "",
        detail: str = "",
        headers: Optional[dict] = None,
    ) -> None:
        """Answer the next ``times`` requests of ``route`` ("GET /jobs/{id}") with an error."""
        for _ in range(times):
            self.faults[route].append(
                ApiFailure(
                    status, detail or f"injected HTTP {status}", code or "injected", headers
                )
            )

    def expire_urls(self) -> None:
        self.signature += 1

    def add_connection(self, name: str, kind: str = "s3", **config: Any) -> Dict[str, Any]:
        connection = {
            "id": uid(),
            "name": name,
            "kind": kind,
            "description": f"The {name} connection",
            "config": config or {"bucket": "data", "endpoint_url": "https://s3.example.org"},
            "secret_fields": ["secret_access_key"],
            "allowed_roles": ["operator", "author"],
            "created_at": iso(),
            "updated_at": iso(),
            "last_tested_at": None,
            "last_test_ok": None,
            "last_test_message": "",
        }
        self.connections[connection["id"]] = connection
        return connection

    def add_schema(self, name: str, *documents: Dict[str, Any]) -> Dict[str, Any]:
        schema = {
            "id": uid(),
            "name": name,
            "description": f"The {name} schema",
            "latest_version": len(documents) or None,
            "created_at": iso(),
            "updated_at": iso(),
        }
        self.schemas[schema["id"]] = schema
        for number, document in enumerate(documents, start=1):
            self.versions[schema["id"]].append(
                {
                    "id": uid(),
                    "schema_id": schema["id"],
                    "number": number,
                    "document": document,
                    "sha256": hashlib.sha256(json.dumps(document).encode()).hexdigest(),
                    "author_id": 1,
                    "created_at": iso(),
                    "notes": f"version {number}",
                }
            )
        return schema

    def add_dataset(
        self,
        name: str,
        schema: Dict[str, Any],
        *,
        source: Optional[str] = None,
        classification: str = "internal",
        input_format: str = "csv",
    ) -> Dict[str, Any]:
        version = self.versions[schema["id"]][-1]
        dataset = {
            "id": uid(),
            "name": name,
            "description": f"The {name} dataset",
            "classification": classification,
            "input_format": input_format,
            "input_options": {"delimiter": ","},
            "source_connection_id": source,
            "source_path": "incoming/orders.csv" if source else "",
            "schema_version_id": version["id"],
            "schema_id": schema["id"],
            "schema_version": version["number"],
            "destination_connection_id": None,
            "destination_prefix": "",
            "destination_options": {},
            "compression": "snappy",
            "options": {},
            "created_at": iso(),
            "updated_at": iso(),
        }
        self.datasets[dataset["id"]] = dataset
        return dataset

    def add_upload(
        self,
        owner: str = TOKEN,
        *,
        status: str = "complete",
        data: bytes = b"a\n1\n",
        classification: str = "internal",
    ) -> Dict[str, Any]:
        upload = {
            "id": uid(),
            "filename": "input.csv",
            "content_type": "text/csv",
            "size": len(data),
            "declared_sha256": "",
            "classification": classification,
            "status": status,
            "uploaded_by_id": self.users[owner].id,
            "created_at": iso(),
            "completed_at": iso() if status == "complete" else None,
            "deleted_at": None,
            "multipart": False,
        }
        self.uploads[upload["id"]] = upload
        self.objects[f"uploads/{upload['id']}"] = data
        return upload

    def add_job(
        self,
        plan: Plan,
        *,
        kind: str = "run",
        classification: str = "internal",
        owner: str = TOKEN,
    ) -> Dict[str, Any]:
        """A job as if submitted earlier, following ``plan``."""
        with self._lock:
            return self._create_job(
                {"kind": kind, "classification": classification}, plan, self.users[owner]
            )

    def api_requests(self, method: str, prefix: str) -> List[Dict[str, Any]]:
        return [r for r in self.requests if r["method"] == method and r["path"].startswith(prefix)]

    # ------------------------------------------------------------------ jobs

    def _create_job(self, body: Dict[str, Any], plan: Plan, user: User) -> Dict[str, Any]:
        job = {
            "id": uid(),
            "kind": body["kind"],
            "lane": "batch" if body["kind"] == "run" else "interactive",
            "status": "queued",
            "classification": body.get("classification") or "internal",
            "dataset_id": body.get("dataset_id"),
            "upload_id": body.get("upload_id"),
            "schema_version_id": body.get("schema_version_id"),
            "requested_by_id": user.id,
            "schedule_id": None,
            "scheduled_for": None,
            "attempt": plan.attempt,
            "max_attempts": 3,
            "publish_attempts": 0,
            "publish_error": None,
            "cancel_requested": False,
            "progress": {},
            "result": None,
            "error": None,
            "worker": None,
            "created_at": iso(),
            "started_at": None,
            "finished_at": None,
            "spec": {"kind": body["kind"], "input": {"format": body.get("format") or "csv"}},
        }
        self.jobs[job["id"]] = job
        self.plans[job["id"]] = deque(plan.statuses)
        self.endings[job["id"]] = plan
        return job

    def _advance(self, job: Dict[str, Any]) -> None:
        statuses = self.plans[job["id"]]
        if not statuses:
            return
        status = statuses.popleft()
        job["status"] = status
        if status == "running":
            job["started_at"] = iso()
            job["progress"] = {"rows_read": 10}
        if status == "publishing":
            job["publish_error"] = "SlowDown: please reduce your request rate"
        if status in ("succeeded", "failed", "cancelled"):
            plan = self.endings[job["id"]]
            job["finished_at"] = iso()
            job["result"] = plan.result
            job["error"] = plan.error
            job["publish_error"] = None
            for kind, name, data, rows in plan.artifacts:
                artifact_id = uid()
                key = f"jobs/{job['id']}/{name}"
                self.objects[key] = data
                self.artifacts[artifact_id] = {
                    "id": artifact_id,
                    "job_id": job["id"],
                    "attempt": plan.attempt,
                    "kind": kind,
                    "name": name,
                    "bytes": len(data),
                    "rows": rows,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "created_at": iso(),
                    "deleted_at": None,
                    "expires_at": None,
                    "_key": key,
                }

    # ------------------------------------------------------------------ the API

    def handle_api(
        self, method: str, path: str, query: Dict[str, str], headers, body: Any, user: User
    ) -> Tuple[int, Any]:
        segments = path[len(API) :].strip("/").split("/")
        route = (
            method
            + " /"
            + "/".join("{id}" if re.fullmatch(r"[0-9a-f-]{36}|\d+", s) else s for s in segments)
        )
        faults = self.faults.get(route)
        if faults:
            raise faults.popleft()
        handler = _ROUTES.get(route)
        if handler is None:
            raise ApiFailure(404, f"no route {route}", "not_found")
        return handler(self, segments, query, headers, body, user)

    def _page(self, items: List[Dict[str, Any]], query: Dict[str, str]) -> Tuple[int, Any]:
        limit, offset = int(query.get("limit", 50)), int(query.get("offset", 0))
        return 200, {"items": items[offset : offset + limit], "count": len(items)}

    def _get(self, table: Dict[str, Dict[str, Any]], key: str, what: str) -> Dict[str, Any]:
        if key not in table:
            raise ApiFailure(404, f"There is no {what} with id {key}.", "not_found")
        return table[key]

    def _need(self, user: User, scope: str) -> None:
        if scope not in user.scopes:
            raise ApiFailure(
                403,
                f"This needs the {scope} scope, which the {user.role} role does not include.",
                "role_insufficient",
            )


def _me(fake, segments, query, headers, body, user):
    return 200, {
        "user": {
            "id": user.id,
            "username": user.username,
            "email": f"{user.username}@example.org",
            "first_name": "",
            "last_name": "",
            "role": user.role,
            "can_view_raw_rows": user.raw_rows,
            "may_view_raw_rows": user.raw_rows or user.role == "admin",
            "is_service_account": False,
            "is_active": True,
            "date_joined": iso(),
            "last_login": None,
        },
        "scopes": sorted(user.scopes),
        "role_scopes": sorted(user.scopes),
        "token": None,
    }


def _connections(fake, segments, query, headers, body, user):
    fake._need(user, "connections:read")
    items = [c for c in fake.connections.values() if query.get("kind") in (None, c["kind"])]
    return fake._page(sorted(items, key=lambda c: c["name"]), query)


def _datasets(fake, segments, query, headers, body, user):
    fake._need(user, "datasets:read")
    return fake._page(sorted(fake.datasets.values(), key=lambda d: d["name"]), query)


def _dataset(fake, segments, query, headers, body, user):
    fake._need(user, "datasets:read")
    return 200, fake._get(fake.datasets, segments[1], "dataset")


def _schemas(fake, segments, query, headers, body, user):
    fake._need(user, "schemas:read")
    return fake._page(sorted(fake.schemas.values(), key=lambda s: s["name"]), query)


def _schema(fake, segments, query, headers, body, user):
    fake._need(user, "schemas:read")
    return 200, fake._get(fake.schemas, segments[1], "schema")


def _version(fake, segments, query, headers, body, user):
    fake._need(user, "schemas:read")
    schema = fake._get(fake.schemas, segments[1], "schema")
    for version in fake.versions[schema["id"]]:
        if version["number"] == int(segments[3]):
            return 200, version
    raise ApiFailure(404, f"Schema {schema['name']!r} has no version {segments[3]}.", "not_found")


def _create_upload(fake, segments, query, headers, body, user):
    fake._need(user, "uploads:write")
    upload = fake.add_upload(
        next(t for t, u in fake.users.items() if u is user),
        status="pending",
        data=b"",
        classification=body.get("classification") or "internal",
    )
    upload.update(filename=body["filename"], size=body["size"], content_type=body["content_type"])
    url = f"{fake.url}/store/uploads/{upload['id']}?sig={fake.signature}"
    return 201, {
        "upload": upload,
        "method": "PUT",
        "url": url,
        "headers": {"Content-Type": body["content_type"] or "application/octet-stream"},
        "expires_at": iso(),
        "part_size": None,
        "part_count": None,
        "parts": [],
    }


def _upload(fake, segments, query, headers, body, user):
    fake._need(user, "uploads:read")
    return 200, fake._get(fake.uploads, segments[1], "upload")


def _complete_upload(fake, segments, query, headers, body, user):
    fake._need(user, "uploads:write")
    upload = fake._get(fake.uploads, segments[1], "upload")
    data = fake.objects.get(f"uploads/{upload['id']}", b"")
    if len(data) != upload["size"]:
        raise ApiFailure(400, f"The object is {len(data)} bytes, not {upload['size']}.", "size")
    upload.update(status="complete", completed_at=iso())
    return 200, upload


def _delete_upload(fake, segments, query, headers, body, user):
    upload = fake._get(fake.uploads, segments[1], "upload")
    upload.update(status="deleted", deleted_at=iso())
    return 204, None


def _create_job(fake, segments, query, headers, body, user):
    fake._need(user, "jobs:run")
    key = headers.get("Idempotency-Key", "")
    fingerprint = json.dumps(body, sort_keys=True)
    if key and (user.id, key) in fake.keys:
        seen, job_id = fake.keys[(user.id, key)]
        if seen != fingerprint:
            raise ApiFailure(
                409,
                f"The Idempotency-Key {key!r} was already used for a different request (job "
                f"{job_id}); use a new key for a new request.",
                "idempotency_key_reused",
            )
        return 200, fake.jobs[job_id]
    if body.get("upload_id"):
        upload = fake._get(fake.uploads, body["upload_id"], "upload")
        if upload["status"] != "complete":
            raise ApiFailure(400, f"Upload {upload['id']} is {upload['status']}.", "invalid")
    if body.get("dataset_id"):
        fake._get(fake.datasets, body["dataset_id"], "dataset")
    plan = fake.planner(body)
    job = fake._create_job(body, plan, user)
    if key:
        fake.keys[(user.id, key)] = (fingerprint, job["id"])
    return 201, job


def _job(fake, segments, query, headers, body, user):
    fake._need(user, "jobs:read")
    job = fake._get(fake.jobs, segments[1], "job")
    fake._advance(job)
    return 200, job


def _artifacts(fake, segments, query, headers, body, user):
    fake._need(user, "jobs:read")
    job = fake._get(fake.jobs, segments[1], "job")
    found = [a for a in fake.artifacts.values() if a["job_id"] == job["id"]]
    return 200, [_public(a) for a in sorted(found, key=lambda a: (a["attempt"], a["name"]))]


def _download(fake, segments, query, headers, body, user):
    fake._need(user, "artifacts:read")
    artifact = fake._get(fake.artifacts, segments[1], "artifact")
    job = fake.jobs[artifact["job_id"]]
    if (
        artifact["kind"] in ("data", "bad_rows", "preview")
        and job["classification"] == "sensitive"
        and not (user.raw_rows or user.role == "admin")
    ):
        raise ApiFailure(
            403,
            f"The {artifact['kind']} artifact {artifact['name']!r} is sensitive data and needs "
            "the 'view raw rows' permission, which your account does not have; an admin can "
            "grant it.",
            "raw_rows_required",
        )
    if artifact["deleted_at"] is not None:
        raise ApiFailure(410, f"Artifact {artifact['name']!r} was deleted by retention.", "gone")
    return 200, {
        "url": f"{fake.url}/store/{artifact['_key']}?sig={fake.signature}&X-Amz-Signature=s3cr3t",
        "expires_at": iso(),
        "filename": f"{job['id']}-{artifact['name']}",
    }


def _public(artifact: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in artifact.items() if not key.startswith("_")}


_ROUTES = {
    "GET /me": _me,
    "GET /connections": _connections,
    "GET /datasets": _datasets,
    "GET /datasets/{id}": _dataset,
    "GET /schemas": _schemas,
    "GET /schemas/{id}": _schema,
    "GET /schemas/{id}/versions/{id}": _version,
    "POST /uploads": _create_upload,
    "GET /uploads/{id}": _upload,
    "POST /uploads/{id}/complete": _complete_upload,
    "DELETE /uploads/{id}": _delete_upload,
    "POST /jobs": _create_job,
    "GET /jobs/{id}": _job,
    "GET /jobs/{id}/artifacts": _artifacts,
    "GET /artifacts/{id}/download": _download,
}


def _handler(fake: FakeGateway):
    request_ids = itertools.count(1)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # quiet
            pass

        def _send(
            self,
            status: int,
            body: Any = None,
            headers: Optional[dict] = None,
            raw: Optional[bytes] = None,
        ) -> None:
            data = raw if raw is not None else (b"" if body is None else json.dumps(body).encode())
            self.send_response(status)
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            if raw is None and body is not None:
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)

        def _body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def do_GET(self):
            self._route()

        def do_POST(self):
            self._route()

        def do_PUT(self):
            self._route()

        def do_DELETE(self):
            self._route()

        def _route(self):
            parts = urlsplit(self.path)
            if parts.path.startswith("/store/"):
                return self._store(parts)
            raw = self._body()
            query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
            request_id = self.headers.get("X-Request-ID") or f"req-{next(request_ids)}"
            body = json.loads(raw) if raw else None
            with fake._lock:
                fake.requests.append(
                    {
                        "method": self.command,
                        "path": parts.path,
                        "query": query,
                        "body": body,
                        "headers": dict(self.headers.items()),
                    }
                )
                try:
                    fake.contract.check_request(self.command, parts.path, query, body)
                except openapi_check.ContractViolation as violation:
                    fake.violations.append(str(violation))
                if parts.path == API + "/me" and fake.me_answer is not None:
                    status, data = fake.me_answer
                    return self._send(status, raw=data, headers={"X-Request-ID": request_id})
                token = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                user = fake.users.get(token)
                extra = {}
                try:
                    if user is None:
                        raise ApiFailure(
                            401,
                            "Sign in, or send an API token (Authorization: Bearer fkl_...)",
                            "not_authenticated",
                        )
                    status, answer = fake.handle_api(
                        self.command, parts.path, query, self.headers, body, user
                    )
                except ApiFailure as failure:
                    status, answer = failure.status, {
                        "detail": failure.detail,
                        "code": failure.code,
                    }
                    extra = failure.headers
                if answer is not None:
                    try:
                        fake.contract.check_response(self.command, parts.path, status, answer)
                    except openapi_check.ContractViolation as violation:
                        fake.violations.append(str(violation))
            self._send(status, answer, headers={"X-Request-ID": request_id, **extra})

        def _store(self, parts):
            key = parts.path[len("/store/") :]
            query = parse_qs(parts.query)
            with fake._lock:
                fake.store_requests.append(
                    {
                        "method": self.command,
                        "key": key,
                        "range": self.headers.get("Range"),
                        "authorization": self.headers.get("Authorization"),
                    }
                )
                fault = fake.store_faults.popleft() if fake.store_faults else None
            if self.headers.get("Authorization") or query.get("sig") != [str(fake.signature)]:
                if self.command == "PUT":
                    self._body()  # read it, so that the connection stays usable
                return self._send(403, raw=b"<Error><Code>AccessDenied</Code></Error>")
            if fault and fault.isdigit():
                return self._send(int(fault), raw=b"<Error><Code>SlowDown</Code></Error>")
            if self.command == "PUT":
                data = self._body()
                digest = base64.b64encode(hashlib.md5(data).digest()).decode()
                if self.headers.get("Content-MD5") not in (None, digest):
                    return self._send(400, raw=b"<Error><Code>BadDigest</Code></Error>")
                fake.objects[key] = data
                return self._send(
                    200, headers={"ETag": f'"{hashlib.md5(data).hexdigest()}"'}, raw=b""
                )
            data = fake.objects.get(key)
            if data is None:
                return self._send(404, raw=b"<Error><Code>NoSuchKey</Code></Error>")
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range") or "")
            if match is None or fault == "ignore_range":
                return self._send(200, raw=data, headers={"ETag": '"x"'})
            start, end = int(match.group(1)), min(int(match.group(2)), len(data) - 1)
            piece = data[start : end + 1]
            content_range = f"bytes {start}-{end}/{len(data)}"
            if fault == "wrong_range":
                content_range = f"bytes 0-{end - start}/{len(data)}"
            if fault == "short":
                self.send_response(206)
                self.send_header("Content-Range", content_range)
                self.send_header("Content-Length", str(len(piece) - 1))
                self.end_headers()
                self.wfile.write(piece[:-1])
                return
            self._send(206, raw=piece, headers={"Content-Range": content_range})

    return Handler
