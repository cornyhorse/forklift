"""An in-process fake of the gateway's /api/v1 and of an object store (tests only).

``FakeForklift`` serves, on 127.0.0.1 and a free port (or https with ``tls=(cert, key)``):

- ``/api/v1/...``: the endpoints the client uses, from memory: bearer-token authentication, the
  API's error shape (``{"detail", "code"}``), the ``X-Request-ID`` echo, limit/offset pages
  (``{"items", "count"}``), ``Idempotency-Key`` replay for job submissions and dataset runs,
  uploads (one presigned PUT, or multipart with part URLs), jobs whose statuses follow a script
  (each GET of the job takes the next one), events, cancellation, artifacts and downloads.
- ``/store/...``: the object store behind the presigned URLs. PUT stores a body, checking
  Content-Length and Content-MD5 the way S3 does, and answers an ETag; GET returns an object.
  Like S3, it refuses a request that carries an Authorization header as well as a signature,
  and a URL whose signature is no longer current (``expire_urls()``) with 403.

Every request to the API and every JSON answer is checked against contracts/openapi.json
(:mod:`openapi_check`), so the fake cannot drift from the contract and the client cannot send
what the API does not take. Faults: ``fail(route, ...)`` answers the next requests of a route
with an error status, a delay, a dropped connection (before or after the request was handled)
or a broken body; every request is recorded in ``requests``.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import math
import re
import socket
import ssl
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

from openapi_check import Contract, ContractViolation

TOKEN = "fkl_test-token-0123456789abcdef"
API = "/api/v1"


def iso(moment: Optional[datetime] = None) -> str:
    moment = moment or datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@dataclass
class Fault:
    status: Optional[int] = None  # answer with this status (and ``body``)
    body: Any = None  # a dict (sent as JSON), bytes, or None for the API's error shape
    headers: Dict[str, str] = field(default_factory=dict)
    delay: float = 0.0  # seconds before answering
    drop: Optional[str] = None  # "before" / "after": close the connection without an answer
    truncate: bool = False  # store GET: announce the whole body, send half of it
    corrupt: bool = False  # store GET: flip a byte
    stall: float = 0.0  # store GET: send half the body, then wait this long


class ApiFailure(Exception):
    def __init__(self, status: int, detail: str, code: str):
        super().__init__(detail)
        self.status, self.detail, self.code = status, detail, code


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        if isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            return  # the client went away first (a test that drops or breaks a request)
        super().handle_error(request, client_address)


def _uid() -> str:
    return str(uuid.uuid4())


class FakeForklift:
    def __init__(self, *, tls: Optional[Tuple[str, str]] = None, token: str = TOKEN):
        self.token = token
        self.contract = Contract()
        self.requests: List[Dict[str, Any]] = []
        self.violations: List[str] = []  # requests or answers the contract refuses
        self.faults: Dict[str, Deque[Fault]] = defaultdict(deque)
        self.multipart_threshold = 64  # bytes: larger uploads go up in parts
        self.part_size = 32
        self.first_parts = 2  # part URLs in the ticket
        self.ticket_headers: Dict[str, str] = {}
        self.validate_finished = True  # POST /schemas/validate answers 200 (else 202)
        self.url_generation = 0
        self.objects: Dict[str, bytes] = {}
        self.uploads: Dict[str, Dict[str, Any]] = {}
        self.upload_keys: Dict[str, str] = {}  # upload id -> its object's key
        self.upload_parts: Dict[str, Dict[int, Tuple[str, bytes]]] = defaultdict(dict)
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.scripts: Dict[str, Deque[Any]] = defaultdict(deque)
        self.keys: Dict[str, Tuple[str, str]] = {}  # idempotency key -> (fingerprint, job id)
        self.events: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.artifacts: Dict[str, Dict[str, Any]] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.versions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.connections: Dict[str, Dict[str, Any]] = {}
        self.datasets: Dict[str, Dict[str, Any]] = {}
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self.webhooks: Dict[str, Dict[str, Any]] = {}
        self.next_job_status = "queued"  # the status of a job the API creates
        self.new_job_script: List[Any] = []  # the statuses it then takes (see script())
        self._event_ids = itertools.count(1)
        self._lock = threading.RLock()
        self._server = _Server(("127.0.0.1", 0), _handler(self))
        self.scheme = "http"
        if tls is not None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(*tls)
            self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
            self.scheme = "https"
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )

    # ----------------------------------------------------------------------- lifecycle

    def __enter__(self) -> "FakeForklift":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(5)

    @property
    def url(self) -> str:
        return f"{self.scheme}://127.0.0.1:{self._server.server_address[1]}"

    # ----------------------------------------------------------------------- test controls

    def fail(self, route: str, times: int = 1, **fault: Any) -> None:
        """Answer the next ``times`` requests of ``route`` (``"GET /jobs/{id}"``, ``"PUT store"``,
        ``"GET store"``) with ``Fault(**fault)``."""
        for _ in range(times):
            self.faults[route].append(Fault(**fault))

    def expire_urls(self) -> None:
        """Presigned URLs handed out so far answer 403, as expired ones do."""
        self.url_generation += 1

    def uploaded(self, upload_id: str) -> Optional[bytes]:
        """The object of an upload, as the store holds it."""
        return self.objects.get(self.upload_keys[upload_id])

    def sent(self, route: str) -> List[Dict[str, Any]]:
        """The recorded requests of a route."""
        return [entry for entry in self.requests if entry["route"] == route]

    def script(self, job_id: str, *statuses: Any) -> None:
        """The statuses the job takes, one per GET (a status, or a dict of fields)."""
        self.scripts[job_id].extend(statuses)

    def add_schema(self, name: str = "people", document: Optional[dict] = None) -> Dict:
        schema_id = _uid()
        now = iso()
        self.schemas[schema_id] = {
            "id": schema_id,
            "name": name,
            "description": "",
            "latest_version": None,
            "created_at": now,
            "updated_at": now,
        }
        self._add_version(schema_id, document or {"properties": {}}, "")
        return self.schemas[schema_id]

    def _add_version(self, schema_id: str, document: dict, notes: str) -> Dict:
        versions = self.versions[schema_id]
        version = {
            "id": _uid(),
            "schema_id": schema_id,
            "number": len(versions) + 1,
            "document": document,
            "sha256": hashlib.sha256(json.dumps(document).encode()).hexdigest(),
            "author_id": 1,
            "created_at": iso(),
            "notes": notes,
        }
        versions.insert(0, version)
        self.schemas[schema_id]["latest_version"] = version["number"]
        return version

    def add_connection(self, name: str = "lake", kind: str = "s3") -> Dict:
        connection_id = _uid()
        now = iso()
        self.connections[connection_id] = {
            "id": connection_id,
            "name": name,
            "kind": kind,
            "description": "",
            "config": {"bucket": "lake"},
            "secret_fields": ["access_key_id", "secret_access_key"],
            "allowed_roles": ["author", "admin"],
            "created_at": now,
            "updated_at": now,
            "last_tested_at": None,
            "last_test_ok": None,
            "last_test_message": "",
        }
        return self.connections[connection_id]

    def add_dataset(self, name: str = "people") -> Dict:
        schema = self.add_schema(f"{name}-schema")
        version = self.versions[schema["id"]][0]
        dataset_id = _uid()
        now = iso()
        self.datasets[dataset_id] = {
            "id": dataset_id,
            "name": name,
            "description": "",
            "classification": "internal",
            "input_format": "csv",
            "input_options": {},
            "source_connection_id": None,
            "source_path": "",
            "schema_version_id": version["id"],
            "schema_id": schema["id"],
            "schema_version": version["number"],
            "destination_connection_id": None,
            "destination_prefix": "",
            "destination_options": {},
            "compression": "snappy",
            "options": {},
            "created_at": now,
            "updated_at": now,
        }
        return self.datasets[dataset_id]

    def add_schedule(self, dataset: Dict) -> Dict:
        schedule_id = _uid()
        now = iso()
        nxt = iso(datetime.now(timezone.utc) + timedelta(hours=1))
        self.schedules[schedule_id] = {
            "id": schedule_id,
            "dataset_id": dataset["id"],
            "dataset_name": dataset["name"],
            "cron": "0 * * * *",
            "timezone": "Europe/Berlin",
            "enabled": True,
            "next_run_at": nxt,
            "next_runs": [nxt],
            "last_run_at": None,
            "last_outcome": None,
            "last_message": "",
            "last_job_id": None,
            "created_by_id": 1,
            "created_at": now,
            "updated_at": now,
        }
        return self.schedules[schedule_id]

    def add_webhook(self, name: str = "alerts") -> Dict:
        webhook_id = _uid()
        now = iso()
        self.webhooks[webhook_id] = {
            "id": webhook_id,
            "name": name,
            "owner_id": 1,
            "owner_username": "ana",
            "events": ["job.failed"],
            "kinds": ["run"],
            "scope": "own_jobs",
            "dataset_id": None,
            "active": True,
            "disabled_reason": None,
            "consecutive_failures": 0,
            "backoff_until": None,
            "created_at": now,
            "updated_at": now,
            "url": "https://hooks.example.org/forklift",
            "secret_prefix": "fkwh_abc",
        }
        return self.webhooks[webhook_id]

    def add_job(self, status: str = "queued", **fields: Any) -> Dict:
        job = _job_doc(status=status, **fields)
        self.jobs[job["id"]] = job
        self._event(job, "state", {"status": status})
        return job

    def add_artifact(
        self, job: Dict, name: str, content: bytes, kind: str = "data", sha256: Any = True
    ) -> Dict:
        artifact_id = _uid()
        key = f"jobs/{job['id']}/attempt-1/{name}"
        self.objects[key] = content
        digest = hashlib.sha256(content).hexdigest() if sha256 is True else sha256
        self.artifacts[artifact_id] = {
            "id": artifact_id,
            "job_id": job["id"],
            "attempt": 1,
            "kind": kind,
            "name": name,
            "bytes": len(content),
            "rows": None,
            "sha256": digest,
            "created_at": iso(),
            "deleted_at": None,
            "expires_at": None,
            "_key": key,
        }
        return self.artifacts[artifact_id]

    def _event(self, job: Dict, kind: str, payload: Dict) -> None:
        self.events[job["id"]].append(
            {
                "id": next(self._event_ids),
                "created_at": iso(),
                "type": kind,
                "attempt": job["attempt"],
                "payload": payload,
            }
        )

    # ----------------------------------------------------------------------- store URLs

    def _signature(self) -> str:
        return f"X-Amz-Signature=sig{self.url_generation:04d}"

    def presign(self, key: str, **query: Any) -> str:
        extra = "".join(f"&{name}={value}" for name, value in query.items())
        return f"{self.url}/store/{key}?{self._signature()}{extra}"

    # ----------------------------------------------------------------------- the API

    def handle_api(self, method: str, path: str, query: Dict[str, str], body: Any, headers):
        """(status, document or None)"""
        routes: List[Tuple[str, str, Callable]] = [
            ("GET", r"/me", self._me),
            ("GET", r"/schemas", self._list_schemas),
            ("POST", r"/schemas", self._create_schema),
            ("POST", r"/schemas/validate", self._validate),
            ("GET", r"/schemas/(?P<id>[^/]+)", self._get_schema),
            ("GET", r"/schemas/(?P<id>[^/]+)/versions", self._list_versions),
            ("POST", r"/schemas/(?P<id>[^/]+)/versions", self._create_version),
            ("GET", r"/schemas/(?P<id>[^/]+)/versions/(?P<n>\d+)", self._get_version),
            ("GET", r"/connections", self._list_connections),
            ("GET", r"/connections/(?P<id>[^/]+)", self._get("connections", "connection")),
            ("GET", r"/datasets", self._lister("datasets")),
            ("GET", r"/datasets/(?P<id>[^/]+)", self._get("datasets", "dataset")),
            ("POST", r"/datasets/(?P<id>[^/]+)/run", self._run_dataset),
            ("POST", r"/uploads", self._create_upload),
            ("GET", r"/uploads", self._lister("uploads")),
            ("GET", r"/uploads/(?P<id>[^/]+)", self._get("uploads", "upload")),
            ("POST", r"/uploads/(?P<id>[^/]+)/parts", self._part_urls),
            ("POST", r"/uploads/(?P<id>[^/]+)/complete", self._complete_upload),
            ("DELETE", r"/uploads/(?P<id>[^/]+)", self._delete_upload),
            ("POST", r"/jobs", self._create_job),
            ("GET", r"/jobs", self._list_jobs),
            ("GET", r"/jobs/(?P<id>[^/]+)", self._get_job),
            ("GET", r"/jobs/(?P<id>[^/]+)/events", self._list_events),
            ("POST", r"/jobs/(?P<id>[^/]+)/cancel", self._cancel_job),
            ("GET", r"/jobs/(?P<id>[^/]+)/artifacts", self._list_artifacts),
            ("GET", r"/artifacts/(?P<id>[^/]+)", self._get_artifact),
            ("GET", r"/artifacts/(?P<id>[^/]+)/download", self._download),
            ("GET", r"/schedules", self._lister("schedules")),
            ("GET", r"/schedules/(?P<id>[^/]+)", self._get("schedules", "schedule")),
            ("GET", r"/webhooks", self._lister("webhooks")),
            ("GET", r"/webhooks/(?P<id>[^/]+)", self._get("webhooks", "webhook")),
        ]
        for verb, pattern, handler in routes:
            match = re.fullmatch(pattern, path)
            if verb == method and match:
                return handler(query=query, body=body, headers=headers, **match.groupdict())
        raise ApiFailure(404, f"No route {method} {path}", "not_found")

    def _page(self, items: List[Dict], query: Dict[str, str]) -> Tuple[int, Dict]:
        limit, offset = int(query.get("limit", 50)), int(query.get("offset", 0))
        return 200, {"items": items[offset : offset + limit], "count": len(items)}

    def _find(self, table: str, what: str, key: str) -> Dict:
        found = getattr(self, table).get(key)
        if found is None:
            raise ApiFailure(404, f"There is no {what} with id {key}.", "not_found")
        return found

    def _get(self, table: str, what: str):
        return lambda id, **_: (200, self._find(table, what, id))

    def _lister(self, table: str):
        return lambda query, **_: self._page(list(getattr(self, table).values()), query)

    def _me(self, **_):
        user = {
            "id": 1,
            "username": "ana",
            "email": "ana@example.org",
            "first_name": "Ana",
            "last_name": "",
            "role": "author",
            "can_view_raw_rows": False,
            "may_view_raw_rows": False,
            "is_service_account": False,
            "is_active": True,
            "date_joined": iso(),
            "last_login": None,
        }
        token = {
            "id": _uid(),
            "name": "pipeline",
            "prefix": self.token[:8],
            "scopes": ["jobs:read", "jobs:write"],
            "owner_id": 1,
            "owner_username": "ana",
            "created_at": iso(),
            "expires_at": None,
            "last_used_at": iso(),
            "revoked_at": None,
        }
        scopes = ["jobs:read", "jobs:write"]
        return 200, {"user": user, "scopes": scopes, "role_scopes": scopes, "token": token}

    # schemas

    def _list_schemas(self, query, **_):
        return self._page(sorted(self.schemas.values(), key=lambda s: s["name"]), query)

    def _create_schema(self, body, **_):
        if any(schema["name"] == body["name"] for schema in self.schemas.values()):
            raise ApiFailure(409, f"A schema named {body['name']!r} exists.", "conflict")
        schema = self.add_schema(body["name"], body["document"])
        schema["description"] = body.get("description", "")
        self.versions[schema["id"]][0]["notes"] = body.get("notes", "")
        return 201, schema

    def _get_schema(self, id, **_):
        return 200, self._find("schemas", "schema", id)

    def _list_versions(self, id, query, **_):
        self._find("schemas", "schema", id)
        return self._page(self.versions[id], query)

    def _create_version(self, id, body, **_):
        self._find("schemas", "schema", id)
        return 201, self._add_version(id, body["document"], body.get("notes", ""))

    def _get_version(self, id, n, **_):
        for version in self.versions.get(id, []):
            if version["number"] == int(n):
                return 200, version
        raise ApiFailure(404, f"Schema {id} has no version {n}.", "not_found")

    def _validate(self, body, **_):
        status = "succeeded" if self.validate_finished else "queued"
        job = self.add_job(
            status,
            kind="validate_schema",
            lane="interactive",
            upload_id=body.get("upload_id"),
            dataset_id=body.get("dataset_id"),
        )
        return (200 if self.validate_finished else 202), job

    # connections

    def _list_connections(self, query, **_):
        found = [c for c in self.connections.values() if query.get("kind") in (None, c["kind"])]
        return self._page(found, query)

    # uploads

    def _create_upload(self, body, **_):
        size = body["size"]
        if size < 1:
            raise ApiFailure(400, "size must be the file's size in bytes.", "invalid_request")
        upload_id = _uid()
        key = f"uploads/{upload_id}/{body['filename']}"
        upload = {
            "id": upload_id,
            "filename": body["filename"],
            "content_type": body.get("content_type", ""),
            "size": size,
            "declared_sha256": body.get("sha256", ""),
            "classification": body.get("classification") or "internal",
            "status": "pending",
            "uploaded_by_id": 1,
            "created_at": iso(),
            "completed_at": None,
            "deleted_at": None,
            "multipart": size > self.multipart_threshold,
        }
        self.uploads[upload_id] = upload
        expires = iso(datetime.now(timezone.utc) + timedelta(hours=1))
        ticket: Dict[str, Any] = {
            "upload": upload,
            "method": "PUT",
            "url": None,
            "headers": dict(self.ticket_headers),
            "expires_at": expires,
            "part_size": None,
            "part_count": None,
            "parts": [],
        }
        self.upload_keys[upload_id] = key
        if upload["multipart"]:
            count = math.ceil(size / self.part_size)
            ticket["part_size"] = self.part_size
            ticket["part_count"] = count
            ticket["parts"] = self._part_list(upload, range(1, min(count, self.first_parts) + 1))
        else:
            ticket["url"] = self.presign(key)
        return 201, ticket

    def _part_list(self, upload: Dict, numbers) -> List[Dict]:
        return [
            {
                "part_number": number,
                "url": self.presign(
                    self.upload_keys[upload["id"]], partNumber=number, uploadId=upload["id"]
                ),
            }
            for number in numbers
        ]

    def _part_urls(self, id, body, **_):
        upload = self._find("uploads", "upload", id)
        if not upload["multipart"] or upload["status"] != "pending":
            raise ApiFailure(409, f"Upload {id} is not a pending multipart upload.", "conflict")
        return 200, self._part_list(upload, sorted(set(body["part_numbers"])))

    def _complete_upload(self, id, body, **_):
        upload = self._find("uploads", "upload", id)
        if upload["status"] == "complete":
            return 200, upload
        if upload["status"] != "pending":
            raise ApiFailure(410, f"Upload {id} is {upload['status']}.", "gone")
        if upload["multipart"]:
            stored = self.upload_parts[id]
            given = sorted((part["part_number"], part["etag"]) for part in body.get("parts") or [])
            expected = sorted((number, etag) for number, (etag, _) in stored.items())
            if not given or given != expected:
                raise ApiFailure(400, f"Upload {id}: the parts do not match.", "invalid_request")
            content = b"".join(stored[number][1] for number in sorted(stored))
            self.objects[self.upload_keys[id]] = content
        content = self.objects.get(self.upload_keys[id])
        if content is None:
            raise ApiFailure(409, f"Nothing has been uploaded for upload {id} yet.", "conflict")
        if len(content) != upload["size"]:
            raise ApiFailure(
                409,
                f"The uploaded object has {len(content)} bytes but {upload['size']} were "
                "declared.",
                "conflict",
            )
        upload["status"] = "complete"
        upload["completed_at"] = iso()
        return 200, upload

    def _delete_upload(self, id, **_):
        upload = self._find("uploads", "upload", id)
        if upload["status"] in ("deleted", "expired"):
            raise ApiFailure(410, f"Upload {id} is already {upload['status']}.", "gone")
        upload["status"] = "deleted"
        upload["deleted_at"] = iso()
        self.objects.pop(self.upload_keys[id], None)
        return 204, None

    # jobs

    def _submit(self, request: Dict, key: str) -> Tuple[int, Dict]:
        fingerprint = json.dumps(request, sort_keys=True)
        if key and key in self.keys:
            known, job_id = self.keys[key]
            if known != fingerprint:
                raise ApiFailure(
                    409,
                    f"The Idempotency-Key {key!r} was already used for a different request (job "
                    f"{job_id}); use a new key for a new request.",
                    "idempotency_key_reused",
                )
            return 200, self.jobs[job_id]
        if request.get("dataset_id") and request["dataset_id"] not in self.datasets:
            raise ApiFailure(400, f"There is no dataset with id {request['dataset_id']}.", "x")
        job = self.add_job(
            self.next_job_status,
            kind=request["kind"],
            dataset_id=request.get("dataset_id"),
            upload_id=request.get("upload_id"),
            schema_version_id=request.get("schema_version_id"),
            spec={"version": 1, "kind": request["kind"]},
        )
        self.scripts[job["id"]].extend(self.new_job_script)
        if key:
            self.keys[key] = (fingerprint, job["id"])
        return 201, job

    def _create_job(self, body, headers, **_):
        request = {
            "kind": body["kind"],
            "dataset_id": body.get("dataset_id"),
            "upload_id": body.get("upload_id"),
            "format": body.get("format"),
            "input_options": body.get("input_options") or {},
            "schema_version_id": body.get("schema_version_id"),
            "schema": body.get("schema"),
            "compression": body.get("compression"),
            "options": body.get("options") or {},
            "limits": body.get("limits") or {},
            "classification": body.get("classification"),
        }
        return self._submit(request, headers.get("Idempotency-Key", ""))

    def _run_dataset(self, id, body, headers, **_):
        self._find("datasets", "dataset", id)
        request = {
            "kind": "run",
            "dataset_id": id,
            "upload_id": (body or {}).get("upload_id"),
            "format": None,
            "input_options": {},
            "schema_version_id": None,
            "schema": None,
            "compression": None,
            "options": {},
            "limits": {},
            "classification": None,
        }
        return self._submit(request, headers.get("Idempotency-Key", ""))

    def _list_jobs(self, query, **_):
        found = [
            {k: v for k, v in job.items() if k != "spec"}
            for job in reversed(list(self.jobs.values()))
            if query.get("status") in (None, job["status"])
            and query.get("kind") in (None, job["kind"])
            and query.get("dataset_id") in (None, job["dataset_id"])
        ]
        return self._page(found, query)

    def _get_job(self, id, **_):
        job = self._find("jobs", "job", id)
        script = self.scripts.get(id)
        if script:
            step = script.popleft()
            changes = {"status": step} if isinstance(step, str) else dict(step)
            _apply(job, changes)
            self._event(job, "state", {"status": job["status"]})
        return 200, job

    def _list_events(self, id, query, **_):
        self._find("jobs", "job", id)
        after = int(query["after"]) if "after" in query else None
        found = [e for e in self.events[id] if after is None or e["id"] > after]
        return self._page(found, query)

    def _cancel_job(self, id, **_):
        job = self._find("jobs", "job", id)
        if job["status"] in ("succeeded", "failed", "cancelled"):
            raise ApiFailure(409, f"Job {id} already finished ({job['status']}).", "conflict")
        if job["status"] == "publishing":
            raise ApiFailure(409, f"Job {id} is publishing; it cannot be cancelled.", "conflict")
        job["cancel_requested"] = True
        if job["status"] == "queued":
            _apply(job, {"status": "cancelled"})
        return 200, job

    # artifacts

    def _public_artifact(self, artifact: Dict) -> Dict:
        return {k: v for k, v in artifact.items() if not k.startswith("_")}

    def _list_artifacts(self, id, **_):
        self._find("jobs", "job", id)
        found = [a for a in self.artifacts.values() if a["job_id"] == id]
        return 200, [self._public_artifact(a) for a in found]

    def _get_artifact(self, id, **_):
        return 200, self._public_artifact(self._find("artifacts", "artifact", id))

    def _download(self, id, **_):
        artifact = self._find("artifacts", "artifact", id)
        if artifact["deleted_at"]:
            raise ApiFailure(410, f"Artifact {artifact['name']!r} was deleted.", "gone")
        expires = iso(datetime.now(timezone.utc) + timedelta(minutes=5))
        filename = f"{artifact['job_id']}-{artifact['name'].replace('/', '-')}"
        return 200, {
            "url": self.presign(artifact["_key"]),
            "expires_at": expires,
            "filename": filename,
        }

    # ----------------------------------------------------------------------- the store

    def handle_store(self, method: str, key: str, query: Dict[str, str], headers, body: bytes):
        """(status, headers, body bytes)"""
        if "Authorization" in headers:
            return 400, {}, _s3_error("InvalidArgument", "Only one auth mechanism allowed")
        if query.get("X-Amz-Signature") != self._signature().split("=", 1)[1]:
            return 403, {}, _s3_error("AccessDenied", "Request has expired")
        if method == "GET":
            content = self.objects.get(key)
            if content is None:
                return 404, {}, _s3_error("NoSuchKey", "The specified key does not exist.")
            return 200, {"ETag": f'"{hashlib.md5(content).hexdigest()}"'}, content
        if method != "PUT":
            return 405, {}, _s3_error("MethodNotAllowed", method)
        digest = hashlib.md5(body).digest()
        given = headers.get("Content-MD5")
        if given is not None and base64.b64decode(given) != digest:
            return 400, {}, _s3_error("BadDigest", "The Content-MD5 you specified did not match")
        etag = f'"{digest.hex()}"'
        if "partNumber" in query:
            self.upload_parts[query["uploadId"]][int(query["partNumber"])] = (etag, body)
        else:
            self.objects[key] = body
        return 200, {"ETag": etag}, b""


def _s3_error(code: str, message: str) -> bytes:
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code>'
        f"<Message>{message}</Message></Error>"
    ).encode()


def _job_doc(status: str = "queued", **fields: Any) -> Dict[str, Any]:
    job = {
        "id": _uid(),
        "kind": "run",
        "lane": "batch",
        "status": "queued",
        "classification": "internal",
        "dataset_id": None,
        "upload_id": None,
        "schema_version_id": None,
        "requested_by_id": 1,
        "schedule_id": None,
        "scheduled_for": None,
        "attempt": 0,
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
        "spec": {"version": 1},
    }
    job.update(fields)
    _apply(job, {"status": status})
    return job


def _apply(job: Dict[str, Any], changes: Dict[str, Any]) -> None:
    status = changes.get("status", job["status"])
    if status != "queued" and job["started_at"] is None and status != "cancelled":
        job["started_at"] = iso()
        job["attempt"] = max(job["attempt"], 1)
        job["worker"] = "worker-1"
    if status in ("publishing", "succeeded") and job["result"] is None:
        job["result"] = {"counts": {"total_rows": 3, "valid_rows": 2, "invalid_rows": 1}}
    if status in ("succeeded", "failed", "cancelled") and job["finished_at"] is None:
        job["finished_at"] = iso()
    if status == "failed" and "error" not in changes and job["error"] is None:
        job["error"] = {"code": "BAD_ROWS_THRESHOLD_EXCEEDED", "message": "1 of 3 rows rejected"}
    if status == "cancelled" and job["error"] is None:
        job["error"] = {"code": "CANCELLED", "message": "Cancelled by ana."}
    job.update(changes)


def _handler(fake: FakeForklift):
    class Handler(BaseHTTPRequestHandler):
        def _route(self, method: str, path: str) -> str:
            if path.startswith("/store/"):
                return f"{method} store"
            template = re.sub(r"/[0-9a-f]{8}-[0-9a-f-]{27}(?=/|$)", "/{id}", path[len(API) :])
            return f"{method} {template}"

        def _respond(self, status: int, headers: Dict[str, str], body: bytes, fault=None):
            if fault is not None and fault.truncate:
                headers = {**headers, "Content-Length": str(len(body))}
                body = body[: len(body) // 2]
            elif fault is not None and fault.corrupt and body:
                body = bytes([body[0] ^ 1]) + body[1:]
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            if "Content-Length" not in headers:
                self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if fault is not None and fault.stall:
                self.wfile.write(body[: len(body) // 2])
                self.wfile.flush()
                time.sleep(fault.stall)
                return
            self.wfile.write(body)

        def _drop(self) -> None:
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        def _handle(self, method: str) -> None:
            parts = urlsplit(self.path)
            query = {key: values[-1] for key, values in parse_qs(parts.query).items()}
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            route = self._route(method, parts.path)
            with fake._lock:
                fake.requests.append(
                    {
                        "route": route,
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": raw,
                    }
                )
                fault = fake.faults[route].popleft() if fake.faults[route] else None
            if fault is not None and fault.delay:
                time.sleep(fault.delay)
            if fault is not None and fault.drop == "before":
                return self._drop()
            if parts.path.startswith("/store/"):
                return self._store(method, parts.path[len("/store/") :], query, raw, fault)
            try:
                self._api(method, parts.path, query, raw, fault)
            except ContractViolation as violation:
                fake.violations.append(str(violation))
                data = json.dumps({"detail": str(violation), "code": "contract"}).encode()
                self._respond(500, {"Content-Type": "application/json"}, data)

        def _store(self, method, key, query, raw, fault) -> None:
            if fault is not None and fault.status is not None:
                body = fault.body if fault.body is not None else _s3_error("InternalError", "x")
                return self._respond(fault.status, fault.headers, body)
            with fake._lock:
                answer = fake.handle_store(method, key, query, self.headers, raw)
            self._respond(*answer, fault=fault)

        def _api(self, method, path, query, raw, fault) -> None:
            request_id = self.headers.get("X-Request-ID") or uuid.uuid4().hex
            out = {"Content-Type": "application/json", "X-Request-ID": request_id}
            body = json.loads(raw) if raw else None
            fake.contract.check_request(method, path, query, body)
            if fault is not None and fault.status is not None:
                document = fault.body
                if document is None:
                    document = {"detail": f"Injected HTTP {fault.status}.", "code": "injected"}
                data = document if isinstance(document, bytes) else json.dumps(document).encode()
                return self._respond(fault.status, {**out, **fault.headers}, data)
            if self.headers.get("Authorization") != f"Bearer {fake.token}":
                status, document = 401, {
                    "detail": "Sign in, or send an API token (Authorization: Bearer fkl_...).",
                    "code": "not_authenticated",
                }
            else:
                try:
                    with fake._lock:
                        status, document = fake.handle_api(
                            method, path[len(API) :], query, body, self.headers
                        )
                except ApiFailure as failure:
                    status, document = failure.status, {
                        "detail": failure.detail,
                        "code": failure.code,
                    }
            if fault is not None and fault.drop == "after":  # handled; the answer is lost
                return self._drop()
            if document is None:
                return self._respond(status, {"X-Request-ID": request_id}, b"")
            fake.contract.check_response(method, path, status, document)
            self._respond(status, out, json.dumps(document).encode())

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def do_PUT(self):
            self._handle("PUT")

        def do_DELETE(self):
            self._handle("DELETE")

        def log_message(self, format, *args):
            pass

    return Handler
