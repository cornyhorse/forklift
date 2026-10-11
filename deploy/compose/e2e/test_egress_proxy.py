"""End-to-end tests of the egress-proxy profile: workers reach an external store through Squid.

    python deploy/compose/generate_env.py --store-url http://store-external:9000 \
        --public-store-url http://localhost:48290 > deploy/compose/.env
    # fill in FORKLIFT_STORE_ACCESS_KEY and FORKLIFT_STORE_SECRET_KEY (e2e/egress-proxy.yml)
    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/external-store.yml \
        -f deploy/compose/e2e/egress-proxy.yml up -d --build --wait
    FORKLIFT_E2E_ENV_FILE=deploy/compose/.env FORKLIFT_E2E_COMPOSE_PROJECT=forklift \
        python -m pytest deploy/compose/e2e/test_egress_proxy.py deploy/compose/e2e/test_stack.py \
        --no-cov

The store is a second RustFS on the public network only (e2e/egress-proxy.yml); the worker is
on the internal network, which has no route out, and reaches the store through the proxy
(FORKLIFT_STORE_PROXY), which allows that store's URL and nothing else. test_stack.py runs
every kind of job against this stack too. These tests check that staged and streamed inputs
went through the proxy (its log names the supervisor and the engine by their User-Agent, and
has no query strings), that the worker cannot reach the store without it, and that the proxy
refuses every other destination. They need the docker CLI (to look inside the worker and at the
proxy's log) and are skipped for a stack whose .env has no FORKLIFT_STORE_URL.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.parse
from typing import Any, Dict

import pytest
from test_stack import (  # noqa: F401  (admin and settings are the session fixtures)
    PEOPLE,
    SCHEMA,
    admin,
    artifact,
    run_job,
    settings,
    upload,
)

PROXY = "http://egress-proxy:3128"


@pytest.fixture(scope="module")
def egress(settings):  # noqa: F811
    if not settings.get("FORKLIFT_STORE_URL"):
        pytest.skip("not an external-store stack (its .env has no FORKLIFT_STORE_URL)")
    if shutil.which("docker") is None:
        pytest.skip("these tests need the docker CLI")
    return settings


@pytest.fixture(scope="module")
def containers(egress) -> Dict[str, str]:
    """The stack's containers by service name."""
    project = os.environ.get("FORKLIFT_E2E_COMPOSE_PROJECT", "forklift")
    listed = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            '{{.Label "com.docker.compose.service"}} {{.Names}}',
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split("\n")
    found = dict(line.split(" ", 1) for line in listed if line)
    assert {"worker", "egress-proxy", "store-external"} <= set(found), (
        f"the compose project {project!r} (FORKLIFT_E2E_COMPOSE_PROJECT) has no worker, "
        f"egress-proxy and store-external: {sorted(found)}"
    )
    return found


def in_worker(containers: Dict[str, str], code: str) -> Any:
    """Run ``code`` with the worker's Python inside the worker; it prints one JSON value."""
    done = subprocess.run(
        ["docker", "exec", "-i", containers["worker"], "python", "-I", "-"],
        input=code,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


# Requests from inside the worker: without a proxy, or through the egress proxy, never following
# redirects; each answers its HTTP status, the X-Squid-Error header, or the error it raised.
PROBE = """
import json, socket, ssl, sys, urllib.error, urllib.request

class Proxy(urllib.request.ProxyHandler):
    def proxy_open(self, req, proxy, type):
        req.set_proxy(PROXY.split("://")[1], "http")

def fetch(url, proxy):
    handler = Proxy({"http": PROXY, "https": PROXY}) if proxy else urllib.request.ProxyHandler({})
    # tls-stand-in's certificate is from a CA of its own: only the tunnel is under test
    tls = urllib.request.HTTPSHandler(context=ssl._create_unverified_context())
    try:
        with urllib.request.build_opener(handler, tls).open(url, timeout=10) as response:
            return {"status": response.status, "body": response.read(16).decode()}
    except urllib.error.HTTPError as error:
        return {"status": error.code, "squid": error.headers.get("X-Squid-Error", "")}
    except OSError as error:
        return {"error": f"{type(error).__name__}: {error}"}

def connect(host, port):
    try:
        socket.create_connection((host, port), timeout=5).close()
        return "connected"
    except OSError as error:
        return type(error).__name__
"""


def probe(containers, calls: str) -> Any:
    return in_worker(containers, f"PROXY = {PROXY!r}\n{PROBE}\nprint(json.dumps({calls}))")


def proxy_log(containers) -> str:
    done = subprocess.run(
        ["docker", "logs", containers["egress-proxy"]], capture_output=True, text=True, check=True
    )
    return done.stdout


def address_of(containers, service: str) -> str:
    return subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
            containers[service],
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()[0]


def test_the_proxy_allows_exactly_the_store(egress, containers):
    store = urllib.parse.urlsplit(egress["FORKLIFT_STORE_URL"])
    assert store.scheme == "http", "this test's external store is an http:// one"
    host, address = store.netloc, address_of(containers, "store-external")
    answers = probe(
        containers,
        "{"
        f"'store': fetch('http://{host}/health', True),"
        f"'store by its address': fetch('http://{address}:{store.port}/health', True),"
        f"'store, another port': fetch('http://{store.hostname}:9001/', True),"
        f"'store, a tunnel': fetch('https://{host}/health', True),"
        "'https destination': fetch('https://tls-stand-in/', True),"
        "'https destination, another port': fetch('https://tls-stand-in:8443/', True),"
        "'https destination, plain': fetch('http://tls-stand-in/', True),"
        "'the gateway': fetch('http://gateway:8080/readyz', True),"
        "'elsewhere': fetch('http://example.org/', True),"
        "'elsewhere, a tunnel': fetch('https://example.org/', True)"
        "}",
    )

    assert answers.pop("store")["status"] == 200
    assert answers.pop("https destination") == {"status": 200, "body": "ok"}, "a tunnel"
    denied = (
        "store by its address",  # the allowed name, not its address (nor its reverse DNS)
        "store, another port",
        "https destination, plain",
        "the gateway",
        "elsewhere",
    )
    for what in denied:
        assert answers.pop(what) == {"status": 403, "squid": "ERR_ACCESS_DENIED 0"}, what
    tunnels = ("store, a tunnel", "https destination, another port", "elsewhere, a tunnel")
    for what in tunnels:  # Squid refuses these CONNECTs with 403
        assert "Tunnel connection failed: 403" in answers.pop(what)["error"], what
    assert answers == {}


def test_the_worker_cannot_reach_the_store_without_the_proxy(egress, containers):
    address = address_of(containers, "store-external")
    store = urllib.parse.urlsplit(egress["FORKLIFT_STORE_URL"])
    answers = probe(
        containers,
        "{"
        f"'by name': fetch('{egress['FORKLIFT_STORE_URL']}/health', False),"
        f"'by address': connect({address!r}, {store.port or 80}),"
        "'the internet': connect('1.1.1.1', 443)"
        "}",
    )

    assert "Name or service not known" in answers["by name"]["error"] or (
        "Temporary failure in name resolution" in answers["by name"]["error"]
    ), answers["by name"]
    assert answers["by address"] != "connected", "the internal network has no route to it"
    assert answers["the internet"] != "connected"


def test_staged_and_streamed_inputs_and_the_uploads_go_through_the_proxy(
    egress, containers, admin  # noqa: F811
):
    staged_upload = upload(admin, "people.csv", PEOPLE)
    staged = run_job(admin, kind="run", upload_id=staged_upload, format="csv", schema=SCHEMA)
    larger = b"id,name,age\n" + b"".join(
        b"%d,name-%d,%d\n" % (n, n, 20 + n % 50) for n in range(2000)
    )
    admin.request("PATCH", "/api/v1/admin/settings", {"stage_max_bytes": 1024})
    try:
        streamed_upload = upload(admin, "larger.csv", larger)
        streamed = run_job(
            admin, kind="run", upload_id=streamed_upload, format="csv", schema=SCHEMA
        )
    finally:
        admin.request("PATCH", "/api/v1/admin/settings", {"stage_max_bytes": None})

    for job, rows in ((staged, 2), (streamed, 2000)):
        assert job["status"] == "succeeded", job["error"]
        assert job["result"]["counts"]["valid_rows"] == rows
        assert artifact(admin, job, "data"), "the artifacts are in the external store"
    log = proxy_log(containers).splitlines()
    bucket = egress.get("FORKLIFT_STORE_BUCKET", "forklift")

    def agents(method: str, path: str) -> set:
        return {
            line.rsplit('"', 2)[-2].split("/")[0]
            for line in log
            if f" {method} " in line and f"/{bucket}/{path}" in line
        }

    assert agents("GET", f"uploads/{staged_upload}/") == {"forklift-worker"}, "the supervisor"
    assert agents("GET", f"uploads/{streamed_upload}/") == {"Python-urllib"}, "the engine"
    for job in (staged, streamed):
        assert agents("PUT", f"jobs/{job['id']}/") == {"forklift-worker"}
    assert not [line for line in log if "X-Amz-" in line], "no query strings in the log"
