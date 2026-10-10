"""End-to-end tests against a running Compose stack (deploy/compose/docker-compose.yml).

They use the stack the way a person or a pipeline does, from outside: sign in, create an API
token, upload a file straight to the store through a presigned URL, run jobs, wait for the
worker, and download the results through presigned URLs. Nothing is mocked.

    python deploy/compose/generate_env.py > deploy/compose/.env
    docker compose -f deploy/compose/docker-compose.yml up -d --build --wait
    FORKLIFT_E2E_ENV_FILE=deploy/compose/.env python -m pytest deploy/compose/e2e --no-cov

Settings come from the .env file the stack was started with (FORKLIFT_E2E_ENV_FILE):
FORKLIFT_PUBLIC_URL, FORKLIFT_ADMIN_USERNAME and FORKLIFT_ADMIN_PASSWORD. Standard library plus
pyarrow (to read the Parquet results).
"""

from __future__ import annotations

import http.cookiejar
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pyarrow.parquet as pq
import pytest

JOB_TIMEOUT_SECONDS = 180


def _settings() -> Dict[str, str]:
    path = os.environ.get("FORKLIFT_E2E_ENV_FILE")
    if not path:
        pytest.skip("set FORKLIFT_E2E_ENV_FILE to the .env file of a running Compose stack")
    values = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    values.setdefault("FORKLIFT_PUBLIC_URL", "http://localhost:8080")
    values.setdefault("FORKLIFT_ADMIN_USERNAME", "admin")
    return values


class Client:
    """A JSON client of /api/v1 with a bearer token (or a signed-in session)."""

    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))

    def _csrf(self) -> Optional[str]:
        return next((c.value for c in self.cookies if c.name == "csrftoken"), None)

    def request(self, method: str, path: str, body: Any = None, expect=(200, 201, 202, 204)):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base_url + path, data=data, method=method)
        request.add_header("Accept", "application/json")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        elif self._csrf():
            request.add_header("X-CSRFToken", self._csrf())
        try:
            with self.opener.open(request, timeout=30) as response:
                status, text = response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            status, text = error.code, error.read().decode()
        assert status in expect, f"{method} {path} -> {status}: {text[:500]}"
        try:
            return status, (json.loads(text) if text else None)
        except ValueError:  # an HTML error page (a path that is not routed at all)
            return status, text

    def sign_in(self, username: str, password: str) -> None:
        with self.opener.open(self.base_url + "/accounts/login/", timeout=30) as response:
            page = response.read().decode()
        csrf = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', page).group(1)
        form = urllib.parse.urlencode(
            {"username": username, "password": password, "csrfmiddlewaretoken": csrf}
        ).encode()
        request = urllib.request.Request(self.base_url + "/accounts/login/", data=form)
        request.add_header("Referer", self.base_url + "/accounts/login/")
        with self.opener.open(request, timeout=30) as response:
            assert response.status == 200
        assert any(c.name == "sessionid" for c in self.cookies), "sign-in failed"


def _put(url: str, body: bytes, headers: Dict[str, str]) -> None:
    request = urllib.request.Request(url, data=body, method="PUT")
    for name, value in headers.items():
        request.add_header(name, value)
    with urllib.request.urlopen(request, timeout=60) as response:
        assert response.status in (200, 204)


def _get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


@pytest.fixture(scope="session")
def settings():
    return _settings()


@pytest.fixture(scope="session")
def admin(settings) -> Client:
    """An admin API token, created through a signed-in session as a person would."""
    session = Client(settings["FORKLIFT_PUBLIC_URL"])
    session.sign_in(settings["FORKLIFT_ADMIN_USERNAME"], settings["FORKLIFT_ADMIN_PASSWORD"])
    _, me = session.request("GET", "/api/v1/me")
    _, created = session.request(
        "POST",
        "/api/v1/tokens",
        {"name": f"e2e-{uuid.uuid4().hex[:8]}", "scopes": me["role_scopes"]},
    )
    return Client(settings["FORKLIFT_PUBLIC_URL"], token=created["token"])


def upload(client: Client, name: str, body: bytes, classification: str = "internal") -> str:
    _, ticket = client.request(
        "POST",
        "/api/v1/uploads",
        {
            "filename": name,
            "size": len(body),
            "content_type": "text/csv",
            "classification": classification,
        },
    )
    assert ticket["url"], "expected a single-part upload for a small file"
    _put(ticket["url"], body, ticket["headers"])
    client.request("POST", f"/api/v1/uploads/{ticket['upload']['id']}/complete", {})
    return ticket["upload"]["id"]


def run_job(client: Client, **job) -> Dict[str, Any]:
    _, created = client.request("POST", "/api/v1/jobs", job)
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    while True:
        _, current = client.request("GET", f"/api/v1/jobs/{created['id']}")
        if current["status"] in ("succeeded", "failed", "cancelled"):
            return current
        assert time.monotonic() < deadline, f"job still {current['status']}: {current}"
        time.sleep(1)


def artifact(client: Client, job: Dict[str, Any], kind: str) -> bytes:
    _, artifacts = client.request("GET", f"/api/v1/jobs/{job['id']}/artifacts")
    (found,) = [a for a in artifacts if a["kind"] == kind]
    _, download = client.request("GET", f"/api/v1/artifacts/{found['id']}/download")
    return _get(download["url"])


PEOPLE = b"id,name,age\n1,Ana,34\n2,Bo,41\n3,Cy,\n"
SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "age": {"type": "integer"},
    },
    "required": ["id", "age"],
}


class TestUploadRunDownload:
    def test_csv_upload_runs_on_a_worker_and_downloads_as_parquet(self, admin):
        upload_id = upload(admin, "people.csv", PEOPLE)

        job = run_job(admin, kind="run", upload_id=upload_id, format="csv", schema=SCHEMA)

        assert job["status"] == "succeeded", job["error"]
        assert job["result"]["counts"]["valid_rows"] == 2
        data = pq.read_table(io.BytesIO(artifact(admin, job, "data"))).to_pylist()
        assert data == [{"id": 1, "name": "Ana", "age": 34}, {"id": 2, "name": "Bo", "age": 41}]
        bad = pq.read_table(io.BytesIO(artifact(admin, job, "bad_rows"))).to_pylist()
        assert [row["id"] for row in bad] == ["3"]  # rejected rows keep the file's text

    def test_a_schema_is_generated_from_an_upload(self, admin):
        upload_id = upload(admin, "people.csv", PEOPLE)

        job = run_job(admin, kind="generate_schema", upload_id=upload_id, format="csv")

        assert job["status"] == "succeeded", job["error"]
        schema = json.loads(artifact(admin, job, "schema"))
        assert set(schema["properties"]) == {"id", "name", "age"}

    def test_threshold_failure_keeps_bad_rows(self, admin):
        rows = b"id,age\n" + b"".join(f"{i},{999 if i % 2 else 30}\n".encode() for i in range(20))
        schema = {
            "properties": {"id": {"type": "integer"}, "age": {"type": "integer"}},
            "x-validation": {"fieldValidations": {"age": {"range": {"min": 0, "max": 150}}}},
        }

        job = run_job(
            admin,
            kind="run",
            upload_id=upload(admin, "ages.csv", rows),
            format="csv",
            schema=schema,
        )

        assert job["status"] == "failed"
        assert job["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"
        bad = pq.read_table(io.BytesIO(artifact(admin, job, "bad_rows")))
        assert bad.num_rows == 10

    def test_a_file_without_a_header_takes_the_schemas_column_order(self, admin):
        # Stored as jsonb, the schema's properties would come back sorted ("a" before
        # "zeta_name") and the values would land under the wrong names.
        schema = {"properties": {"zeta_name": {"type": "string"}, "a": {"type": "integer"}}}
        body = b"Ana,1\nBo,2\n"

        job = run_job(
            admin,
            kind="run",
            upload_id=upload(admin, "no-header.csv", body),
            format="csv",
            schema=schema,
            input_options={"header_mode": "absent"},
        )

        assert job["status"] == "succeeded", job["error"]
        rows = pq.read_table(io.BytesIO(artifact(admin, job, "data"))).to_pylist()
        assert rows == [{"zeta_name": "Ana", "a": 1}, {"zeta_name": "Bo", "a": 2}]

    def test_an_input_above_stage_max_bytes_is_streamed(self, admin):
        _, before = admin.request("GET", "/api/v1/admin/settings")
        admin.request("PATCH", "/api/v1/admin/settings", {"stage_max_bytes": 1024})
        try:
            body = b"id,name,age\n" + b"".join(
                f"{i},name-{i},{20 + i % 50}\n".encode() for i in range(5000)
            )
            assert len(body) > 1024
            job = run_job(
                admin,
                kind="run",
                upload_id=upload(admin, "large.csv", body),
                format="csv",
                schema=SCHEMA,
            )
        finally:
            admin.request("PATCH", "/api/v1/admin/settings", {"stage_max_bytes": None})

        assert job["status"] == "succeeded", job["error"]
        assert job["result"]["counts"]["valid_rows"] == 5000
        assert before is not None


class TestRoles:
    def test_a_viewer_sees_jobs_but_cannot_upload_or_run(self, admin, settings):
        username = f"viewer-{uuid.uuid4().hex[:6]}"
        _, user = admin.request(
            "POST",
            "/api/v1/admin/users",
            {"username": username, "role": "viewer", "password": uuid.uuid4().hex + "Aa1!"},
        )
        _, token = admin.request(
            "POST",
            "/api/v1/admin/tokens",
            {"owner_id": user["id"], "name": "e2e-viewer", "scopes": ["jobs:read"]},
        )
        viewer = Client(settings["FORKLIFT_PUBLIC_URL"], token=token["token"])

        viewer.request("GET", "/api/v1/jobs")
        viewer.request("POST", "/api/v1/uploads", {"filename": "x.csv", "size": 3}, expect=(403,))
        viewer.request("POST", "/api/v1/jobs", {"kind": "run", "format": "csv"}, expect=(403,))

    def test_the_worker_api_is_not_on_the_public_port(self, admin):
        admin.request("POST", "/internal/v1/leases", {}, expect=(404,))


class TestPages:
    def test_signed_in_pages_admin_screens_and_static_files_are_served(self, settings):
        session = Client(settings["FORKLIFT_PUBLIC_URL"])
        session.sign_in(settings["FORKLIFT_ADMIN_USERNAME"], settings["FORKLIFT_ADMIN_PASSWORD"])

        for path in ("/", "/schemas/", "/jobs/", "/admin/", "/admin/users/", "/admin/audit/"):
            with session.opener.open(session.base_url + path, timeout=30) as response:
                page = response.read().decode()
            assert response.status == 200, path
            assert "<html" in page.lower(), path
        scripts = re.findall(r'<script[^>]+src="([^"]+)"', page)
        assert scripts, "pages load their scripts from the gateway"
        with urllib.request.urlopen(session.base_url + scripts[0], timeout=30) as response:
            assert response.status == 200

    def test_signed_out_visitors_are_sent_to_sign_in(self, settings):
        with urllib.request.urlopen(settings["FORKLIFT_PUBLIC_URL"] + "/admin/", timeout=30) as r:
            assert "/accounts/login/" in r.url
