"""Every HTML page and every form action x anonymous / Viewer / Operator / Author / Admin.

Each case runs against a fresh ``World`` (none of its users has "view raw rows"), signed in
with a session, and posts a valid form, so that the status shows the permission decision:
anonymous callers go to the sign-in page (302), refusals are 403 pages that show the service
layer's message, successful form posts redirect (302) or show what they created (200). A test
checks that the table names every URL of the UI, so a new page cannot be added without its
row; tests/test_ui_security.py replays every POST row without the CSRF token.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Optional

import pytest
from django.test import Client
from django.urls import get_resolver, reverse
from world import PASSWORD, SCHEMA_DOCUMENT, World, make_job, make_upload

from forklift_web.core.choices import JobKind, RetentionKind

pytestmark = [pytest.mark.django_db, pytest.mark.matrix]

CALLERS = ("anonymous", "viewer", "operator", "author", "admin")
EVERYONE = "302 200 200 200 200"
OPERATORS = "302 403 200 200 200"
AUTHORS = "302 403 403 200 200"
NEW_PASSWORD = "a much better passphrase 7"


def admin_only(code: int = 200) -> str:
    return f"302 403 403 403 {code}"


@dataclass
class Row:
    method: str
    name: str  # a URL name of the ui namespace
    expected: str  # one status per caller, in CALLERS order
    kwargs: Optional[Callable] = None  # world -> the URL's arguments
    data: Optional[Callable] = None  # (world, user) -> the form's fields
    needs: str = ""  # a builder below that puts more objects into world.extra
    query: str = ""

    def statuses(self) -> dict:
        codes = [int(code) for code in self.expected.split()]
        assert len(codes) == len(CALLERS)
        return dict(zip(CALLERS, codes))

    def url(self, world: World) -> str:
        path = reverse(f"ui:{self.name}", kwargs=self.kwargs(world) if self.kwargs else None)
        return path + self.query


def own_upload(world: World, user) -> str:
    return str(make_upload(user or world.viewer).pk)


def retention_data(prefix: str, scope: str, target: str = "", **days) -> dict:
    data = {"scope": scope, "target": target, "prefix": prefix}
    for kind in RetentionKind.values:
        data[f"{prefix}-{kind}_mode"] = "days" if kind in days else "inherit"
        data[f"{prefix}-{kind}_days"] = str(days.get(kind, ""))
    return data


def dataset_data(world: World, name: str) -> dict:
    return {
        "name": name,
        "classification": "internal",
        "schema_version": str(world.version.pk),
        "input_format": "csv",
        "compression": "snappy",
    }


def s3_data(connection) -> dict:
    return {
        "name": connection.name,
        "description": "the main store",
        "allowed_roles": ["author", "admin"],
        **{key: connection.config.get(key, "") for key in ("bucket", "endpoint_url", "prefix")},
        "region": "us-east-1",
        "addressing_style": "path",
    }


def _validation_job(world: World) -> None:
    world.extra["validation"] = make_job(
        world.operator, world.upload, kind=JobKind.VALIDATE_SCHEMA
    )


BUILDERS = {"validation_job": _validation_job}


def schema_id(world: World) -> dict:
    return {"schema_id": world.version.schema_id}


ROWS = [
    # the signed-in user's own pages
    Row("GET", "home", EVERYONE),
    Row("GET", "password", EVERYONE),
    Row(
        "POST",
        "password",
        "302 302 302 302 302",
        data=lambda w, u: {
            "old_password": PASSWORD,
            "new_password1": NEW_PASSWORD,
            "new_password2": NEW_PASSWORD,
        },
    ),
    Row("GET", "tokens", EVERYONE),
    Row(
        "POST",
        "tokens",
        EVERYONE,
        data=lambda w, u: {"name": "ci", "scopes": ["jobs:read"], "expires_in": "30"},
    ),
    Row(
        "POST",
        "token-revoke",
        "302 403 302 403 403",
        kwargs=lambda w: {"token_id": w.tokens["operator"].pk},
    ),
    # schemas
    Row("GET", "schemas", EVERYONE),
    Row("GET", "schema-new", AUTHORS),
    Row(
        "POST",
        "schema-new",
        "302 403 403 302 302",
        data=lambda w, u: {"name": "people", "document": json.dumps(SCHEMA_DOCUMENT)},
    ),
    Row("GET", "schema", EVERYONE, kwargs=schema_id),
    Row(
        "POST",
        "schema-edit",
        "302 403 403 302 302",
        kwargs=schema_id,
        data=lambda w, u: {"name": w.version.schema.name, "description": "people"},
    ),
    Row("GET", "schema-diff", EVERYONE, kwargs=schema_id),
    Row("GET", "schema-version", EVERYONE, kwargs=lambda w: {**schema_id(w), "number": 1}),
    Row("GET", "version-new", AUTHORS, kwargs=schema_id),
    Row(
        "POST",
        "version-new",
        "302 403 403 302 302",
        kwargs=schema_id,
        data=lambda w, u: {"document": '{"type": "object"}', "notes": "looser"},
    ),
    Row(
        "POST",
        "schema-validate",
        "302 403 302 302 302",
        data=lambda w, u: {
            "source": f"upload:{own_upload(w, u)}",
            "document": json.dumps(SCHEMA_DOCUMENT),
        },
    ),
    Row(
        "GET",
        "job-validation",
        EVERYONE,
        kwargs=lambda w: {"job_id": w.extra["validation"].pk},
        needs="validation_job",
    ),
    # datasets
    Row("GET", "datasets", EVERYONE),
    Row("GET", "dataset-new", AUTHORS),
    Row("POST", "dataset-new", "302 403 403 302 302", data=lambda w, u: dataset_data(w, "new-ds")),
    Row("GET", "dataset", EVERYONE, kwargs=lambda w: {"dataset_id": w.dataset.pk}),
    Row("GET", "dataset-edit", AUTHORS, kwargs=lambda w: {"dataset_id": w.dataset.pk}),
    Row(
        "POST",
        "dataset-edit",
        "302 403 403 302 302",
        kwargs=lambda w: {"dataset_id": w.dataset.pk},
        data=lambda w, u: {**dataset_data(w, w.dataset.name), "description": "monthly"},
    ),
    Row(
        "POST",
        "dataset-delete",
        "302 403 403 302 302",
        kwargs=lambda w: {"dataset_id": w.spare_dataset.pk},
    ),
    Row(
        "POST",
        "dataset-run",
        "302 403 302 302 302",
        kwargs=lambda w: {"dataset_id": w.dataset.pk},
        data=lambda w, u: {"upload": own_upload(w, u), "idempotency_key": "run-1"},
    ),
    # uploads, jobs and downloads
    Row("GET", "upload", OPERATORS),
    Row("GET", "uploads", OPERATORS),
    Row(
        "GET", "upload-detail", "302 403 200 403 200", kwargs=lambda w: {"upload_id": w.upload.pk}
    ),
    Row(
        "POST",
        "upload-run",
        "302 403 302 403 302",
        kwargs=lambda w: {"upload_id": w.upload.pk},
        data=lambda w, u: {
            "action": "run",
            "schema_version": str(w.version.pk),
            "format": "csv",
            "idempotency_key": "upload-run-1",
        },
    ),
    Row(
        "POST",
        "upload-delete",
        "302 403 302 403 302",
        kwargs=lambda w: {"upload_id": w.spare_upload.pk},
    ),
    Row("GET", "jobs", EVERYONE),
    Row("GET", "job", EVERYONE, kwargs=lambda w: {"job_id": w.finished_job.pk}),
    Row("GET", "job-live", EVERYONE, kwargs=lambda w: {"job_id": w.queued_job.pk}),
    Row("POST", "job-cancel", "302 403 302 403 302", kwargs=lambda w: {"job_id": w.queued_job.pk}),
    Row(
        "GET",
        "artifact-download",
        "302 302 302 302 302",
        kwargs=lambda w: {"artifact_id": w.artifact.pk},
    ),
    # administration
    Row("GET", "admin", admin_only()),
    Row("GET", "admin-store", admin_only()),
    Row("GET", "admin-users", admin_only()),
    Row("GET", "admin-user-new", admin_only()),
    Row(
        "POST",
        "admin-user-new",
        admin_only(302),
        data=lambda w, u: {
            "username": "new-user",
            "role": "viewer",
            "password1": NEW_PASSWORD,
            "password2": NEW_PASSWORD,
        },
    ),
    Row("GET", "admin-user", admin_only(), kwargs=lambda w: {"user_id": w.viewer.pk}),
    Row(
        "POST",
        "admin-user",
        admin_only(302),
        kwargs=lambda w: {"user_id": w.viewer.pk},
        data=lambda w, u: {"role": "operator", "is_active": "on"},
    ),
    Row(
        "POST",
        "admin-user-password",
        admin_only(302),
        kwargs=lambda w: {"user_id": w.viewer.pk},
        data=lambda w, u: {"password1": NEW_PASSWORD, "password2": NEW_PASSWORD},
    ),
    Row("GET", "admin-tokens", admin_only()),
    Row(
        "POST",
        "admin-tokens",
        admin_only(200),
        data=lambda w, u: {
            "owner": str(w.viewer.pk),
            "name": "svc",
            "scopes": ["jobs:read"],
            "expires_in": "30",
        },
    ),
    Row(
        "POST",
        "admin-token-revoke",
        admin_only(302),
        kwargs=lambda w: {"token_id": w.tokens["viewer"].pk},
    ),
    Row("GET", "admin-workers", admin_only()),
    Row(
        "POST",
        "admin-workers",
        admin_only(200),
        data=lambda w, u: {"name": "batch pool", "expires_in": ""},
    ),
    Row(
        "POST",
        "admin-worker-token-revoke",
        admin_only(302),
        kwargs=lambda w: {"token_id": w.worker_token.pk},
    ),
    Row("GET", "admin-connections", admin_only()),
    Row("GET", "admin-connection-new", admin_only(), kwargs=lambda w: {"kind": "s3"}),
    Row(
        "POST",
        "admin-connection-new",
        admin_only(302),
        kwargs=lambda w: {"kind": "localfs"},
        data=lambda w, u: {
            "name": "exports",
            "root_path": "/data",
            "allowed_roles": ["author", "admin"],
        },
    ),
    Row(
        "GET",
        "admin-connection",
        admin_only(),
        kwargs=lambda w: {"connection_id": w.connection.pk},
    ),
    Row(
        "POST",
        "admin-connection",
        admin_only(302),
        kwargs=lambda w: {"connection_id": w.connection.pk},
        data=lambda w, u: s3_data(w.connection),
    ),
    Row(
        "POST",
        "admin-connection-test",
        admin_only(302),
        kwargs=lambda w: {"connection_id": w.connection.pk},
    ),
    Row(
        "POST",
        "admin-connection-delete",
        admin_only(302),
        kwargs=lambda w: {"connection_id": w.spare_connection.pk},
    ),
    Row("GET", "admin-retention", admin_only()),
    Row(
        "POST",
        "admin-retention-save",
        admin_only(302),
        data=lambda w, u: retention_data("installation", "installation", uploads=30),
    ),
    Row(
        "POST",
        "admin-retention-delete",
        admin_only(302),
        data=lambda w, u: {"scope": "installation", "target": ""},
    ),
    Row(
        "POST",
        "admin-retention-sweep",
        admin_only(200),
        data=lambda w, u: {"dry_run": "1"},
    ),
    Row("GET", "admin-audit", admin_only()),
    Row("GET", "admin-audit-export", admin_only()),
    Row("GET", "admin-settings", admin_only()),
    Row(
        "POST",
        "admin-setting",
        admin_only(302),
        kwargs=lambda w: {"key": "lease_seconds"},
        data=lambda w, u: {"lease_seconds-value": "30"},
    ),
    Row(
        "POST",
        "admin-setting-reset",
        admin_only(302),
        kwargs=lambda w: {"key": "lease_seconds"},
    ),
    Row("GET", "admin-jobs", admin_only()),
]

CASES = [(row, caller) for row in ROWS for caller in CALLERS]


def build(row: Row) -> World:
    world = World.build()
    if row.needs:
        BUILDERS[row.needs](world)
    return world


def call(row: Row, world: World, client: Client, user):
    url = row.url(world)
    if row.method == "GET":
        return client.get(url)
    return client.post(url, row.data(world, user) if row.data else {})


@pytest.mark.parametrize(
    "row,caller", CASES, ids=[f"{row.method} {row.name} as {caller}" for row, caller in CASES]
)
def test_status(row: Row, caller: str):
    world = build(row)
    client = Client()
    user = None if caller == "anonymous" else getattr(world, caller)
    if user is not None:
        client.force_login(user)
    response = call(row, world, client, user)
    expected = row.statuses()[caller]
    assert response.status_code == expected, (response.status_code, response.content[:800])
    if caller == "anonymous":
        assert response["Location"].startswith(reverse("forklift-login") + "?next=")
    elif expected == 403:
        assert b'role="alert"' in response.content  # the service layer's message, on a page
        assert b"Not allowed" in response.content


def test_every_url_of_the_ui_has_a_row():
    resolver = get_resolver("forklift_web.ui.urls")
    names = {pattern.name for pattern in resolver.url_patterns}
    assert names == {row.name for row in ROWS}


def test_signing_in_and_out(make_user):
    """The two pages outside the ui namespace, for every role."""
    for role in CALLERS[1:]:
        user = make_user(role)
        client = Client()
        assert client.get(reverse("forklift-login")).status_code == 200
        signed_in = client.post(
            reverse("forklift-login"), {"username": user.username, "password": PASSWORD}
        )
        assert (signed_in.status_code, signed_in["Location"]) == (302, "/")
        assert client.get(reverse("forklift-login")).status_code == 302  # already signed in
        assert client.post(reverse("forklift-logout")).status_code == 302
        assert client.get("/").status_code == 302
