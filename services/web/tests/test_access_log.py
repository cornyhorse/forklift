"""The gateway's logs keep the single sign-on callback's code and state out: gunicorn's access
log (the logger class on its own, and a real gunicorn started as the image starts it) and
Django's warning about the answer (the JSON log)."""

from __future__ import annotations

import datetime
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest
from django.conf import settings
from django.db import connection
from gunicorn.config import Config

from forklift_web.access_log import AccessLog

WEB = Path(__file__).resolve().parents[1]
DOCKERFILE = (WEB / "Dockerfile").read_text()


def atoms(path: str, query: str = "", referer: str = "") -> dict:
    environ = {
        "REQUEST_METHOD": "GET",
        "RAW_URI": path + (f"?{query}" if query else ""),
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "SERVER_PROTOCOL": "HTTP/1.1",
        "REMOTE_ADDR": "192.0.2.1",
    }
    if referer:
        environ["HTTP_REFERER"] = referer
    response = SimpleNamespace(status="302 Found", headers=[], sent=0)
    return AccessLog(Config()).atoms(
        response, SimpleNamespace(headers=[]), environ, datetime.timedelta(0)
    )


def test_the_callbacks_query_is_redacted():
    found = atoms("/accounts/oidc/callback/", "code=the-code&state=the-state")
    assert found["r"] == "GET /accounts/oidc/callback/?[redacted] HTTP/1.1"
    assert found["q"] == "[redacted]" and found["U"] == "/accounts/oidc/callback/"
    found = atoms("/accounts/oidc/callback", "code=the-code")  # redirected to the one with /
    assert found["r"] == "GET /accounts/oidc/callback?[redacted] HTTP/1.1"


def test_a_referer_naming_the_callback_is_redacted():
    found = atoms(
        "/static/ui/forklift.css", referer="https://f.example/accounts/oidc/callback/?code=c"
    )
    assert found["f"] == "https://f.example/accounts/oidc/callback/?[redacted]"
    assert (
        atoms("/", referer="https://f.example/jobs/?page=2")["f"]
        == "https://f.example/jobs/?page=2"
    )
    assert atoms("/", referer="https://f.example/accounts/oidc/callback/")["f"].endswith(
        "callback/"
    )
    assert atoms("/")["f"] == "-"


def test_other_requests_are_logged_as_they_are():
    assert atoms("/jobs/", "page=2")["r"] == "GET /jobs/?page=2 HTTP/1.1"
    assert atoms("/accounts/oidc/callback/")["r"] == "GET /accounts/oidc/callback/ HTTP/1.1"
    assert atoms("/accounts/oidc/callbackx", "code=1")["q"] == "code=1"


def test_the_image_logs_through_it():
    assert "--logger-class forklift_web.access_log.AccessLog" in DOCKERFILE


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(port: int, path: str) -> int:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


@pytest.mark.django_db(transaction=True)
def test_a_real_gunicorn_redacts_the_callback(tmp_path):
    port = _free_port()
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "django_settings",
        "PYTHONPATH": os.pathsep.join([str(WEB / "src"), str(WEB / "tests")]),
        "FORKLIFT_DB_NAME": connection.settings_dict["NAME"],
        "FORKLIFT_S3_BUCKET": settings.FORKLIFT_STORE["bucket"],
        "FORKLIFT_PUBLIC_PORT": str(port),
        "FORKLIFT_INTERNAL_PORT": str(_free_port()),
    }
    log_path = tmp_path / "gunicorn.log"
    log = log_path.open("w")
    server = subprocess.Popen(
        [sys.executable, "-m", "gunicorn", "forklift_web.wsgi:application"]
        + ["--bind", f"127.0.0.1:{port}", "--workers", "1", "--access-logfile", "-"]
        + ["--logger-class", "forklift_web.access_log.AccessLog"],
        env=env,
        cwd=tmp_path,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        for _ in range(100):  # wait until it answers (not a timing assertion)
            if server.poll() is not None:
                break
            try:
                if _get(port, "/healthz") == 200:
                    break
            except OSError:
                pass
            time.sleep(0.2)
        assert server.poll() is None, log_path.read_text()
        # single sign-on is not configured here: 404, but the line is what matters
        assert _get(port, "/accounts/oidc/callback/?code=SECRET-CODE&state=SECRET-STATE") == 404
        assert _get(port, "/healthz?probe=1") == 200
    finally:
        server.terminate()
        server.wait(timeout=30)
        log.close()
        connection.close()
    text = log_path.read_text()
    assert '"GET /accounts/oidc/callback/?[redacted] HTTP/1.1" 404' in text
    assert '"GET /healthz?probe=1 HTTP/1.1" 200' in text
    assert '"request": "GET /accounts/oidc/callback/"' in text  # Django's warning, JSON
    assert "SECRET" not in text
