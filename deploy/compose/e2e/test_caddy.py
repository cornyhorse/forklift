"""End-to-end tests of the caddy profile: TLS in front of the gateway and the object store.

    python deploy/compose/generate_env.py --domain forklift-edge.test --internal-ca \
        --https-port 48443 --http-port 48480 > deploy/compose/.env
    echo "127.0.0.1 forklift-edge.test store.forklift-edge.test" | sudo tee -a /etc/hosts
    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/e2e/caddy.yml \
        up -d --build --wait
    docker compose -f deploy/compose/docker-compose.yml exec caddy \
        cat /data/caddy/pki/authorities/local/root.crt > caddy-root.crt
    SSL_CERT_FILE=caddy-root.crt FORKLIFT_E2E_ENV_FILE=deploy/compose/.env python -m pytest \
        deploy/compose/e2e/test_caddy.py deploy/compose/e2e/test_stack.py --no-cov

SSL_CERT_FILE makes Python trust Caddy's CA (and nothing else), so test_stack.py and the other
end-to-end tests run over https against the same stack: sign-in, tokens, uploads and downloads
through presigned URLs on the store's domain, streamed inputs, multipart outputs. These tests
add what only the TLS setup has: the redirect to https, HSTS, secure cookies, a multipart upload
by the client to the store's domain (SigV4 signs the Host header, so it must reach the store as
the client sent it), a large single PUT, client addresses through the proxy, the /mcp route and
the gateway's and store's ports published on localhost only. They are skipped for a stack
without FORKLIFT_DOMAIN.
"""

from __future__ import annotations

import io
import ipaddress
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from email.message import Message
from typing import Dict, Optional, Tuple

import pyarrow.parquet as pq
import pytest
from test_stack import (  # noqa: F401  (admin and settings are the session fixtures)
    SCHEMA,
    Client,
    admin,
    artifact,
    run_job,
    settings,
)

MIB = 1024 * 1024
HSTS = "max-age=31536000"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def call(
    method: str,
    url: str,
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Message, bytes]:
    """One request (redirects not followed); returns the status, the headers (their names in
    any case: Caddy sends X-Request-Id for the gateway's X-Request-ID) and the body."""
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=120) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.headers, error.read()


@pytest.fixture(scope="module")
def tls(settings):  # noqa: F811
    if not settings.get("FORKLIFT_DOMAIN"):
        pytest.skip("not a caddy-profile stack (its .env has no FORKLIFT_DOMAIN)")
    return settings


@pytest.fixture(scope="module")
def store_url(tls) -> str:
    return tls["FORKLIFT_PUBLIC_STORE_URL"]


def test_the_public_urls_are_https_on_the_two_domains(tls, store_url):
    ui, store = urllib.parse.urlsplit(tls["FORKLIFT_PUBLIC_URL"]), urllib.parse.urlsplit(store_url)
    assert (ui.scheme, ui.hostname) == ("https", tls["FORKLIFT_DOMAIN"])
    assert (store.scheme, store.hostname) == ("https", tls["FORKLIFT_STORE_DOMAIN"])


def test_plain_http_is_redirected_to_https(tls):
    port = tls.get("FORKLIFT_HTTP_HOST_PORT", "80")
    status, headers, _ = call("GET", f"http://{tls['FORKLIFT_DOMAIN']}:{port}/jobs/?page=2")
    assert status == 308
    location = urllib.parse.urlsplit(headers["Location"])
    assert (location.scheme, location.hostname) == ("https", tls["FORKLIFT_DOMAIN"])
    assert (location.path, location.query) == ("/jobs/", "page=2")


def test_both_sites_send_hsts(tls, store_url):
    status, ui, _ = call("GET", tls["FORKLIFT_PUBLIC_URL"] + "/accounts/login/")
    assert status == 200 and ui["Strict-Transport-Security"] == HSTS
    status, store, _ = call("GET", store_url + "/health")
    assert status == 200 and store["Strict-Transport-Security"] == HSTS


def test_sign_in_cookies_are_secure(tls):
    session = Client(tls["FORKLIFT_PUBLIC_URL"])
    session.sign_in(tls["FORKLIFT_ADMIN_USERNAME"], tls["FORKLIFT_ADMIN_PASSWORD"])

    cookies = {cookie.name: cookie for cookie in session.cookies}
    assert cookies["sessionid"].secure and cookies["csrftoken"].secure
    # The gateway took the request for https (X-Forwarded-Proto): the API works in the session
    _, me = session.request("GET", "/api/v1/me")
    assert me["user"]["username"] == tls["FORKLIFT_ADMIN_USERNAME"]


def test_a_multipart_upload_to_the_store_domain_runs_and_downloads(
    tls, admin, store_url  # noqa: F811
):
    body = b"id,name,age\n" + b"".join(
        b"%d,name-%07d,%d\n" % (n, n, 20 + n % 50) for n in range(500_000)
    )
    assert 2 * 5 * MIB < len(body) < 3 * 5 * MIB, "three parts of 5 MiB"
    five_mib = {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB}
    admin.request("PATCH", "/api/v1/admin/settings", five_mib)
    try:
        _, ticket = admin.request(
            "POST",
            "/api/v1/uploads",
            {"filename": "people.csv", "size": len(body), "content_type": "text/csv"},
        )
    finally:
        admin.request("PATCH", "/api/v1/admin/settings", dict.fromkeys(five_mib))
    upload_id, size = ticket["upload"]["id"], ticket["part_size"]
    assert ticket["url"] is None and ticket["part_count"] == 3
    numbers = list(range(1, ticket["part_count"] + 1))
    _, fresh = admin.request(
        "POST", f"/api/v1/uploads/{upload_id}/parts", {"part_numbers": numbers}
    )
    parts = []
    for part in sorted(fresh, key=lambda part: part["part_number"]):
        number = part["part_number"]
        assert part["url"].startswith(store_url + "/"), "signed for the store's https domain"
        chunk = body[(number - 1) * size : number * size]
        status, headers, _ = call("PUT", part["url"], chunk, {"Content-Length": str(len(chunk))})
        assert status == 200, f"part {number}: HTTP {status}"
        parts.append({"part_number": number, "etag": headers["ETag"]})
    admin.request("POST", f"/api/v1/uploads/{upload_id}/complete", {"parts": parts})

    job = run_job(admin, kind="run", upload_id=upload_id, format="csv", schema=SCHEMA)

    assert job["status"] == "succeeded", job["error"]
    _, artifacts = admin.request("GET", f"/api/v1/jobs/{job['id']}/artifacts")
    (data,) = [a for a in artifacts if a["kind"] == "data"]
    _, download = admin.request("GET", f"/api/v1/artifacts/{data['id']}/download")
    assert download["url"].startswith(store_url + "/")
    table = pq.read_table(io.BytesIO(artifact(admin, job, "data")))
    assert table.num_rows == 500_000


def test_a_large_single_put_goes_through_unbuffered_and_unlimited(admin, store_url):  # noqa: F811
    body = bytes(64 * MIB)  # Caddy has no body limit; RustFS keeps all of it
    _, ticket = admin.request(
        "POST",
        "/api/v1/uploads",
        {"filename": "zeros.csv", "size": len(body), "content_type": "text/csv"},
    )
    assert ticket["url"].startswith(store_url + "/")
    status, _, _ = call("PUT", ticket["url"], body, ticket["headers"])
    assert status == 200
    _, done = admin.request("POST", f"/api/v1/uploads/{ticket['upload']['id']}/complete", {})
    assert done["size"] == len(body)


def test_the_gateway_records_the_clients_address_not_caddys_or_a_forged_one(
    tls, admin  # noqa: F811
):
    def audited_address(base_url: str, **headers: str) -> str:
        request_id = uuid.uuid4().hex
        status, _, _ = call(
            "PATCH",
            base_url + "/api/v1/admin/settings",
            json.dumps({"stage_max_bytes": None}).encode(),
            {
                "Authorization": f"Bearer {admin.token}",
                "Content-Type": "application/json",
                "X-Request-ID": request_id,
                **headers,
            },
        )
        assert status == 200
        _, page = admin.request("GET", "/api/v1/admin/audit?action=settings.update&limit=50")
        (entry,) = [e for e in page["items"] if e["request_id"] == request_id]
        return entry["ip"]

    # Caddy replaces what the client claims with the address it sees
    through_caddy = audited_address(
        tls["FORKLIFT_PUBLIC_URL"], **{"X-Forwarded-For": "203.0.113.9"}
    )
    # ... which is the one the gateway sees when this client connects to it directly (its port
    # is published on localhost only: a client that reached it could claim any address)
    port = tls.get("FORKLIFT_PUBLIC_HOST_PORT", "8080")
    direct = audited_address(f"http://127.0.0.1:{port}")

    assert through_caddy == direct != "203.0.113.9"
    assert ipaddress.ip_address(through_caddy)


def test_mcp_is_routed_to_the_mcp_server_and_nothing_else(tls):
    base = tls["FORKLIFT_PUBLIC_URL"]
    for path in ("/mcp", "/mcp/", "/mcp/messages"):
        status, headers, body = call(
            "POST", base + path, b"{}", {"Content-Type": "application/json"}
        )
        assert (status, body) == (200, b"{}"), path  # the stand-in's answer
        assert headers.get("X-Request-ID") is None, f"{path} reached the gateway"
    status, headers, _ = call("POST", base + "/mcpx", b"{}", {"Content-Type": "application/json"})
    assert headers.get("X-Request-ID"), "/mcpx is the gateway's"


def test_the_gateway_and_the_store_are_published_on_localhost_only(tls):
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("192.0.2.1", 9))  # no packet is sent: it picks the outgoing address
        address = probe.getsockname()[0]
        probe.close()
    except OSError:
        pytest.skip("this host has no outgoing IPv4 address to test with")
    if ipaddress.ip_address(address).is_loopback:
        pytest.skip("this host has no address but loopback")
    for name, default in (
        ("FORKLIFT_PUBLIC_HOST_PORT", "8080"),
        ("FORKLIFT_STORE_HOST_PORT", "9000"),
    ):
        port = int(tls.get(name, default))
        socket.create_connection(("127.0.0.1", port), timeout=10).close()
        with pytest.raises(OSError):
            socket.create_connection((address, port), timeout=10).close()
