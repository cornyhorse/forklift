"""Every public endpoint x every kind of caller -> the expected status.

Callers: anonymous; a session of each role (Viewer, Operator, Author, Admin, none of them with
"view raw rows"); an Admin's API token narrowed to ``jobs:read``; and a Viewer's token whose
stored scopes claim ``admin:read`` / ``admin:write`` (a token cannot widen its owner's role, so
it can do nothing but ask who it is). Each case runs against a fresh ``World``. A test checks
that the table names every operation of the OpenAPI document, so a new endpoint cannot be added
without its row.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pytest
from django.test import Client
from world import SCHEMA_DOCUMENT, World, api_token

from forklift_web.api import api

pytestmark = [pytest.mark.django_db, pytest.mark.matrix]

CALLERS = ("anonymous", "viewer", "operator", "author", "admin", "narrow_token", "widened_token")
PASSWORD = "correct horse battery staple"


@dataclass
class Row:
    method: str
    path: str  # an OpenAPI path; {placeholders} are filled from the world
    expected: str  # one status per caller, in CALLERS order
    body: Optional[Callable] = None
    ids: Optional[Callable] = None  # world -> values for the path's placeholders
    needs: str = ""  # a World method building store-backed objects into world.extra

    def statuses(self) -> dict:
        codes = [int(code) for code in self.expected.split()]
        assert len(codes) == len(CALLERS)
        return dict(zip(CALLERS, codes))


ADMIN_ONLY = "401 403 403 403 {} 403 403"
READ_ALL = "401 200 200 200 200 403 403"
JOBS_READ = "401 200 200 200 200 200 403"


def admin(code: int) -> str:
    return ADMIN_ONLY.format(code)


ROWS = [
    # me and own tokens
    Row("GET", "/me", "401 200 200 200 200 200 200"),
    Row("GET", "/tokens", "401 200 200 200 200 403 403"),
    Row(
        "POST",
        "/tokens",
        "401 201 201 201 201 403 403",
        body=lambda w: {"name": "ci", "scopes": ["jobs:read"]},
    ),
    Row(
        "DELETE",
        "/tokens/{token_id}",
        "401 403 204 403 403 403 403",
        ids=lambda w: {"token_id": w.tokens["operator"].id},
    ),
    # uploads
    Row(
        "POST",
        "/uploads",
        "401 403 201 201 201 403 403",
        body=lambda w: {"filename": "people.csv", "size": 24},
    ),
    Row("GET", "/uploads", "401 403 200 200 200 403 403"),
    Row(
        "GET",
        "/uploads/{upload_id}",
        "401 403 200 403 200 403 403",
        ids=lambda w: {"upload_id": w.upload.id},
    ),
    Row(
        "POST",
        "/uploads/{upload_id}/complete",
        "401 403 200 403 200 403 403",
        ids=lambda w: {"upload_id": w.extra["pending_upload"].id},
        needs="pending_upload",
    ),
    Row(
        "POST",
        "/uploads/{upload_id}/parts",
        "401 403 200 403 200 403 403",
        body=lambda w: {"part_numbers": [1, 2]},
        ids=lambda w: {"upload_id": w.extra["multipart_upload"].id},
        needs="multipart_upload",
    ),
    Row(
        "DELETE",
        "/uploads/{upload_id}",
        "401 403 204 403 204 403 403",
        ids=lambda w: {"upload_id": w.spare_upload.id},
    ),
    # schemas
    Row("GET", "/schemas", READ_ALL),
    Row(
        "POST",
        "/schemas",
        "401 403 403 201 201 403 403",
        body=lambda w: {"name": "new schema", "document": SCHEMA_DOCUMENT},
    ),
    Row(
        "POST",
        "/schemas/validate",
        "401 403 202 403 202 403 403",
        body=lambda w: {
            "upload_id": str(w.upload.id),
            "schema": SCHEMA_DOCUMENT,
            "wait_seconds": 0,
        },
    ),
    Row("GET", "/schemas/{schema_id}", READ_ALL, ids=lambda w: {"schema_id": w.version.schema_id}),
    Row(
        "PATCH",
        "/schemas/{schema_id}",
        "401 403 403 200 200 403 403",
        body=lambda w: {"description": "people"},
        ids=lambda w: {"schema_id": w.version.schema_id},
    ),
    Row(
        "GET",
        "/schemas/{schema_id}/versions",
        READ_ALL,
        ids=lambda w: {"schema_id": w.version.schema_id},
    ),
    Row(
        "POST",
        "/schemas/{schema_id}/versions",
        "401 403 403 201 201 403 403",
        body=lambda w: {"document": {"type": "object"}},
        ids=lambda w: {"schema_id": w.version.schema_id},
    ),
    Row(
        "GET",
        "/schemas/{schema_id}/versions/{number}",
        READ_ALL,
        ids=lambda w: {"schema_id": w.version.schema_id, "number": 1},
    ),
    # datasets
    Row("GET", "/datasets", READ_ALL),
    Row(
        "POST",
        "/datasets",
        "401 403 403 201 201 403 403",
        body=lambda w: {"name": "new dataset", "schema_version_id": str(w.version.id)},
    ),
    Row("GET", "/datasets/{dataset_id}", READ_ALL, ids=lambda w: {"dataset_id": w.dataset.id}),
    Row(
        "PATCH",
        "/datasets/{dataset_id}",
        "401 403 403 200 200 403 403",
        body=lambda w: {"description": "monthly export"},
        ids=lambda w: {"dataset_id": w.dataset.id},
    ),
    Row(
        "DELETE",
        "/datasets/{dataset_id}",
        "401 403 403 204 204 403 403",
        ids=lambda w: {"dataset_id": w.spare_dataset.id},
    ),
    Row(
        "POST",
        "/datasets/{dataset_id}/run",
        "401 403 201 403 201 403 403",
        body=lambda w: {"upload_id": str(w.upload.id)},
        ids=lambda w: {"dataset_id": w.dataset.id},
    ),
    # jobs and artifacts
    Row(
        "POST",
        "/jobs",
        "401 403 201 403 201 403 403",
        body=lambda w: {"kind": "run", "upload_id": str(w.upload.id), "schema": SCHEMA_DOCUMENT},
    ),
    Row("GET", "/jobs", JOBS_READ),
    Row("GET", "/jobs/{job_id}", JOBS_READ, ids=lambda w: {"job_id": w.queued_job.id}),
    Row("GET", "/jobs/{job_id}/events", JOBS_READ, ids=lambda w: {"job_id": w.queued_job.id}),
    Row(
        "POST",
        "/jobs/{job_id}/cancel",
        "401 403 200 403 200 403 403",
        ids=lambda w: {"job_id": w.queued_job.id},
    ),
    Row("GET", "/jobs/{job_id}/artifacts", JOBS_READ, ids=lambda w: {"job_id": w.finished_job.id}),
    Row(
        "GET", "/artifacts/{artifact_id}", JOBS_READ, ids=lambda w: {"artifact_id": w.artifact.id}
    ),
    Row(
        "GET",
        "/artifacts/{artifact_id}/download",
        READ_ALL,
        ids=lambda w: {"artifact_id": w.artifact.id},
    ),
    # connections
    Row("GET", "/connections", READ_ALL),
    Row(
        "POST",
        "/connections",
        admin(201),
        body=lambda w: {"name": "exports", "kind": "localfs", "config": {"root_path": "/data"}},
    ),
    Row(
        "GET",
        "/connections/{connection_id}",
        "401 403 403 200 200 403 403",
        ids=lambda w: {"connection_id": w.connection.id},
    ),
    Row(
        "PATCH",
        "/connections/{connection_id}",
        admin(200),
        body=lambda w: {"description": "the main store"},
        ids=lambda w: {"connection_id": w.connection.id},
    ),
    Row(
        "DELETE",
        "/connections/{connection_id}",
        admin(204),
        ids=lambda w: {"connection_id": w.spare_connection.id},
    ),
    Row(
        "POST",
        "/connections/{connection_id}/test",
        admin(200),
        ids=lambda w: {"connection_id": w.connection.id},
    ),
    # administration
    Row("GET", "/admin/roles", admin(200)),
    Row("GET", "/admin/users", admin(200)),
    Row(
        "POST",
        "/admin/users",
        admin(201),
        body=lambda w: {"username": "new-user", "role": "viewer", "password": PASSWORD},
    ),
    Row("GET", "/admin/users/{user_id}", admin(200), ids=lambda w: {"user_id": w.viewer.id}),
    Row(
        "PATCH",
        "/admin/users/{user_id}",
        admin(200),
        body=lambda w: {"role": "operator"},
        ids=lambda w: {"user_id": w.viewer.id},
    ),
    Row(
        "POST",
        "/admin/users/{user_id}/password",
        admin(200),
        body=lambda w: {"password": "another correct horse"},
        ids=lambda w: {"user_id": w.viewer.id},
    ),
    Row("GET", "/admin/tokens", admin(200)),
    Row(
        "POST",
        "/admin/tokens",
        admin(201),
        body=lambda w: {"owner_id": w.viewer.id, "name": "svc", "scopes": ["jobs:read"]},
    ),
    Row(
        "POST",
        "/admin/tokens/{token_id}/revoke",
        admin(200),
        ids=lambda w: {"token_id": w.tokens["viewer"].id},
    ),
    Row("GET", "/admin/worker-tokens", admin(200)),
    Row("POST", "/admin/worker-tokens", admin(201), body=lambda w: {"name": "batch pool"}),
    Row(
        "POST",
        "/admin/worker-tokens/{token_id}/revoke",
        admin(200),
        ids=lambda w: {"token_id": w.worker_token.id},
    ),
    Row("GET", "/admin/workers", admin(200)),
    Row("GET", "/admin/workers/{worker_pk}", admin(200), ids=lambda w: {"worker_pk": w.worker.id}),
    Row("GET", "/admin/retention", admin(200)),
    Row("PUT", "/admin/retention/installation", admin(200), body=lambda w: {"days": {"data": 30}}),
    Row("DELETE", "/admin/retention/installation", admin(204)),
    Row(
        "PUT",
        "/admin/retention/classifications/{classification}",
        admin(200),
        body=lambda w: {"days": {"bad_rows": 7}},
        ids=lambda w: {"classification": "internal"},
    ),
    Row(
        "DELETE",
        "/admin/retention/classifications/{classification}",
        admin(204),
        ids=lambda w: {"classification": "public"},
    ),
    Row(
        "PUT",
        "/admin/retention/datasets/{dataset_id}",
        admin(200),
        body=lambda w: {"days": {"data": None}},
        ids=lambda w: {"dataset_id": w.dataset.id},
    ),
    Row(
        "DELETE",
        "/admin/retention/datasets/{dataset_id}",
        admin(204),
        ids=lambda w: {"dataset_id": w.spare_dataset.id},
    ),
    Row("POST", "/admin/retention/sweep", admin(200), body=lambda w: {"dry_run": True}),
    Row("GET", "/admin/audit", admin(200)),
    Row("GET", "/admin/settings", admin(200)),
    Row("PATCH", "/admin/settings", admin(200), body=lambda w: {"lease_seconds": 30}),
]

CASES = [(row, caller) for row in ROWS for caller in CALLERS]


def _client(world: World, caller: str) -> tuple:
    client, headers = Client(), {}
    if caller in {"viewer", "operator", "author", "admin"}:
        client.force_login(getattr(world, caller))
    elif caller == "narrow_token":
        _, raw = api_token(world.admin, ["jobs:read"])
        headers["HTTP_AUTHORIZATION"] = f"Bearer {raw}"
    elif caller == "widened_token":
        _, raw = api_token(world.viewer, ["admin:read", "admin:write"])
        headers["HTTP_AUTHORIZATION"] = f"Bearer {raw}"
    return client, headers


@pytest.mark.parametrize(
    "row,caller", CASES, ids=[f"{row.method} {row.path} as {caller}" for row, caller in CASES]
)
def test_status(row: Row, caller: str):
    world = World.build()
    if row.needs:
        world.extra[row.needs] = getattr(world, row.needs)()
    client, headers = _client(world, caller)
    path = "/api/v1" + row.path.format(**(row.ids(world) if row.ids else {}))
    kwargs = dict(headers)
    if row.body is not None or row.method in {"POST", "PUT", "PATCH"}:
        kwargs.update(data=(row.body(world) if row.body else {}), content_type="application/json")
    response = getattr(client, row.method.lower())(path, **kwargs)
    expected = row.statuses()[caller]
    assert response.status_code == expected, (response.status_code, response.content[:500])
    if expected in {401, 403}:
        body = response.json()
        assert body["code"] in {
            "not_authenticated",
            "role_insufficient",
            "scope_missing",
            "not_owner",
            "connection_not_allowed",
        }
        assert body["detail"]


def test_every_operation_has_a_row():
    schema = api.get_openapi_schema()
    operations = {
        (method.upper(), path.removeprefix("/api/v1"))
        for path, methods in schema["paths"].items()
        for method in methods
    }
    assert operations == {(row.method, row.path) for row in ROWS}


VIEW_CALLERS = ("anonymous", "viewer", "operator", "author", "admin")
VIEWS = [
    ("GET", "/", "302 200 200 200 200"),  # the home page needs a session
    ("GET", "/accounts/login/", "200 302 302 302 302"),  # signed in: straight on
    ("POST", "/accounts/logout/", "302 302 302 302 302"),
    ("GET", "/healthz", "200 200 200 200 200"),
    ("GET", "/readyz", "200 200 200 200 200"),
    ("GET", "/api/v1/openapi.json", "200 200 200 200 200"),  # the published contract
]


@pytest.mark.parametrize(
    "method,path,caller,expected",
    [
        (method, path, caller, int(code))
        for method, path, codes in VIEWS
        for caller, code in zip(VIEW_CALLERS, codes.split())
    ],
)
def test_views(method, path, caller, expected):
    world = World.build()
    client = Client()
    if caller != "anonymous":
        client.force_login(getattr(world, caller))
    response = getattr(client, method.lower())(path)
    assert response.status_code == expected
