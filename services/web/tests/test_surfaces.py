"""The internal API is served only on the internal port, and the internal port serves nothing
else; worker tokens work only there, API tokens and sessions only on the public port.

``test_real_server_routes_by_listener_port`` starts the gateway the way the Docker image does
(one gunicorn, two --bind addresses) and calls both ports over TCP.
"""

from __future__ import annotations

import io
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path
from wsgiref.util import setup_testing_defaults

import pytest
from django.conf import settings
from django.db import connection
from django.utils import timezone

from forklift_web import wsgi
from forklift_web.core.choices import Role
from forklift_web.middleware import INTERNAL, PUBLIC, SURFACE_KEY

LEASE = {"worker_id": "w-1", "lanes": ["batch"], "spec_versions": [1]}
WEB = Path(__file__).resolve().parents[1]


@pytest.mark.django_db
def test_public_surface_does_not_route_the_internal_api(as_token, worker_token, client):
    _, raw = worker_token
    assert (
        client.post("/internal/v1/leases", LEASE, content_type="application/json").status_code
        == 404
    )
    response = as_token(raw).post("/internal/v1/leases", LEASE)
    assert response.status_code == 404
    assert as_token(raw).get("/api/v1/me").status_code == 401  # worker tokens are not API tokens


@pytest.mark.django_db
def test_internal_surface_routes_only_the_internal_api(
    internal, worker_token, make_user, api_token
):
    _, raw = worker_token
    worker = internal(raw)
    assert worker.post("/internal/v1/leases", LEASE).status_code == 204
    assert worker.get("/healthz").status_code == 200
    assert worker.get("/api/v1/me").status_code == 404
    assert worker.get("/accounts/login/").status_code == 404
    _, api_raw = api_token(make_user(Role.ADMIN), ["jobs:read"])
    refused = internal(api_raw).post("/internal/v1/leases", LEASE)
    assert refused.status_code == 401
    assert refused.json()["code"] == "not_authenticated"
    assert internal().post("/internal/v1/leases", LEASE).status_code == 401


@pytest.mark.django_db
def test_sessions_do_not_reach_the_internal_api(internal, make_user):
    caller = internal()
    caller.client.force_login(make_user(Role.ADMIN))
    assert caller.post("/internal/v1/leases", LEASE).status_code == 401


@pytest.mark.django_db
def test_revoked_and_expired_worker_tokens_are_refused(internal, worker_token):
    token, raw = worker_token
    token.expires_at = timezone.now() - timedelta(seconds=1)
    token.save()
    assert internal(raw).post("/internal/v1/leases", LEASE).status_code == 401
    token.expires_at = None
    token.revoked_at = timezone.now()
    token.save()
    assert internal(raw).post("/internal/v1/leases", LEASE).status_code == 401
    assert internal("fkw_" + "x" * 43).post("/internal/v1/leases", LEASE).status_code == 401


class _Socket:
    def __init__(self, name):
        self.name = name

    def getsockname(self):
        return self.name


def test_surface_is_chosen_by_the_local_port_of_the_connection():
    internal_port = settings.FORKLIFT_INTERNAL_PORT
    assert wsgi.surface_of({"gunicorn.socket": _Socket(("0.0.0.0", internal_port))}) == INTERNAL
    assert wsgi.surface_of({"gunicorn.socket": _Socket(("0.0.0.0", internal_port + 1))}) == PUBLIC
    assert wsgi.surface_of({"gunicorn.socket": _Socket("/run/forklift.sock")}) == PUBLIC
    assert wsgi.surface_of({}) == PUBLIC
    # Headers cannot choose the surface: they arrive as HTTP_* keys, never as SURFACE_KEY
    assert wsgi.surface_of({"HTTP_HOST": f"gateway:{internal_port}"}) == PUBLIC


def _call(app, path: str, extra=None) -> str:
    environ = {"PATH_INFO": path, "REQUEST_METHOD": "GET", "wsgi.input": io.BytesIO()}
    environ.update(extra or {})
    setup_testing_defaults(environ)
    environ["SERVER_NAME"] = "localhost"
    statuses = []
    body = b"".join(app(environ, lambda status, headers, exc_info=None: statuses.append(status)))
    assert body is not None
    return statuses[0]


def test_wsgi_entry_points():
    assert _call(wsgi.public_application, "/internal/v1/leases").startswith("404")
    assert _call(wsgi.internal_application, "/api/v1/me").startswith("404")
    assert _call(wsgi.internal_application, "/healthz").startswith("200")
    # Even a forged surface key in the environ is overwritten by the entry point
    assert _call(
        wsgi.public_application, "/internal/v1/leases", {SURFACE_KEY: INTERNAL}
    ).startswith("404")
    assert _call(
        wsgi.application, "/healthz", {"gunicorn.socket": _Socket(("::1", 1))}
    ).startswith("200")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _request(port: int, path: str, *, token=None, body=None, host=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if host:
        headers["Host"] = host
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method="POST" if body is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


@pytest.mark.django_db(transaction=True)
def test_real_server_routes_by_listener_port(worker_token, make_user, api_token, tmp_path):
    _, worker_raw = worker_token
    _, api_raw = api_token(make_user(Role.VIEWER), ["jobs:read"])
    public_port, internal_port = _free_port(), _free_port()
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "django_settings",
        "PYTHONPATH": os.pathsep.join([str(WEB / "src"), str(WEB / "tests")]),
        "FORKLIFT_DB_NAME": connection.settings_dict["NAME"],
        "FORKLIFT_S3_BUCKET": settings.FORKLIFT_STORE["bucket"],
        "FORKLIFT_INTERNAL_PORT": str(internal_port),
        "FORKLIFT_PUBLIC_PORT": str(public_port),
    }
    log = (tmp_path / "gunicorn.log").open("w")
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "gunicorn",
            "forklift_web.wsgi:application",
            "--bind",
            f"127.0.0.1:{public_port}",
            "--bind",
            f"127.0.0.1:{internal_port}",
            "--workers",
            "1",
            "--threads",
            "2",
        ],
        env=env,
        cwd=tmp_path,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        for _ in range(100):  # wait until both ports answer (not a timing assertion)
            if server.poll() is not None:
                break
            try:
                if _request(public_port, "/healthz") == 200 == _request(internal_port, "/healthz"):
                    break
            except OSError:
                pass
            time.sleep(0.2)
        assert server.poll() is None, (tmp_path / "gunicorn.log").read_text()

        assert _request(internal_port, "/internal/v1/leases", token=worker_raw, body=LEASE) == 204
        assert _request(public_port, "/internal/v1/leases", token=worker_raw, body=LEASE) == 404
        # A Host header naming the internal port does not change what the public port routes
        assert (
            _request(
                public_port,
                "/internal/v1/leases",
                token=worker_raw,
                body=LEASE,
                host=f"127.0.0.1:{internal_port}",
            )
            == 404
        )
        assert _request(public_port, "/api/v1/me", token=api_raw) == 200
        assert _request(internal_port, "/api/v1/me", token=api_raw) == 404
    finally:
        server.terminate()
        server.wait(timeout=30)
        log.close()
        connection.close()
