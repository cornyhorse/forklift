"""The k3s smoke test's checks against a running release (k3s_smoke.sh runs it; standard library).

    python3 k3s_e2e.py all    # the API end to end, then the NetworkPolicies
    python3 k3s_e2e.py proxy  # the API end to end, then the proxy mode's policies and Squid

From outside, as a person or a pipeline: sign in, create an API token, upload a CSV straight to
the store through its presigned URL, run it on the batch worker group, download the Parquet,
and generate a schema on the no-network interactive group. Then, from inside the cluster, that
the NetworkPolicies bite: the workers reach the gateway's internal port, the store and DNS and
nothing else they try; a pod outside the release reaches neither port of the gateway nor the
store; a pod like the ingress controller reaches the public port but not the internal one.

Settings (environment): FORKLIFT_K3S_GATEWAY_URL, FORKLIFT_K3S_ADMIN_PASSWORD, FORKLIFT_K3S_NAME
(the k3s container), FORKLIFT_K3S_NAMESPACE, FORKLIFT_K3S_RELEASE, FORKLIFT_K3S_WORKER_IMAGE.
"""

from __future__ import annotations

import http.cookiejar
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

GATEWAY = os.environ.get("FORKLIFT_K3S_GATEWAY_URL", "http://127.0.0.1:18400")
CLUSTER = os.environ.get("FORKLIFT_K3S_NAME", "forklift-helm-smoke")
NAMESPACE = os.environ.get("FORKLIFT_K3S_NAMESPACE", "forklift")
RELEASE = os.environ.get("FORKLIFT_K3S_RELEASE", "forklift")
WORKER_IMAGE = os.environ.get("FORKLIFT_K3S_WORKER_IMAGE", "forklift-worker:local")
JOB_TIMEOUT_SECONDS = 300
PEOPLE = b"id,name,age\n1,Ana,34\n2,Bo,41\n3,Cy,\n"
SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "age": {"type": "integer"},
    },
    "required": ["id", "age"],
}


# --------------------------------------------------------------------------- the public API


class Client:
    def __init__(self, token=None):
        self.token = token
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))

    def request(self, method, path, body=None, expect=(200, 201, 202, 204)):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(GATEWAY + path, data=data, method=method)
        request.add_header("Accept", "application/json")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        else:
            csrf = next((c.value for c in self.cookies if c.name == "csrftoken"), None)
            if csrf:
                request.add_header("X-CSRFToken", csrf)
        try:
            with self.opener.open(request, timeout=30) as response:
                status, text = response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            status, text = error.code, error.read().decode()
        if status not in expect:
            raise AssertionError(f"{method} {path} -> {status}: {text[:500]}")
        return json.loads(text) if text else None

    def sign_in(self, username, password):
        with self.opener.open(GATEWAY + "/accounts/login/", timeout=30) as response:
            page = response.read().decode()
        csrf = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', page).group(1)
        form = urllib.parse.urlencode(
            {"username": username, "password": password, "csrfmiddlewaretoken": csrf}
        ).encode()
        request = urllib.request.Request(GATEWAY + "/accounts/login/", data=form)
        request.add_header("Referer", GATEWAY + "/accounts/login/")
        with self.opener.open(request, timeout=30):
            pass
        if not any(c.name == "sessionid" for c in self.cookies):
            raise AssertionError("signing in as the bootstrap admin failed")


def admin_client():
    session = Client()
    session.sign_in("admin", os.environ["FORKLIFT_K3S_ADMIN_PASSWORD"])
    me = session.request("GET", "/api/v1/me")
    created = session.request(
        "POST",
        "/api/v1/tokens",
        {"name": f"k3s-{uuid.uuid4().hex[:8]}", "scopes": me["role_scopes"]},
    )
    return Client(created["token"])


def upload(client, name, body):
    ticket = client.request(
        "POST",
        "/api/v1/uploads",
        {
            "filename": name,
            "size": len(body),
            "content_type": "text/csv",
            "classification": "internal",
        },
    )
    put = urllib.request.Request(ticket["url"], data=body, method="PUT")
    for header, value in ticket["headers"].items():
        put.add_header(header, value)
    with urllib.request.urlopen(put, timeout=60) as response:
        assert response.status in (200, 204), response.status
    client.request("POST", f"/api/v1/uploads/{ticket['upload']['id']}/complete", {})
    return ticket["upload"]["id"]


def run_job(client, **job):
    created = client.request("POST", "/api/v1/jobs", job)
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    while True:
        current = client.request("GET", f"/api/v1/jobs/{created['id']}")
        if current["status"] in ("succeeded", "failed", "cancelled"):
            return current
        if time.monotonic() > deadline:
            raise AssertionError(f"job still {current['status']} after {JOB_TIMEOUT_SECONDS} s")
        time.sleep(1)


def artifact(client, job, kind):
    artifacts = client.request("GET", f"/api/v1/jobs/{job['id']}/artifacts")
    (found,) = [a for a in artifacts if a["kind"] == kind]
    download = client.request("GET", f"/api/v1/artifacts/{found['id']}/download")
    with urllib.request.urlopen(download["url"], timeout=60) as response:
        return response.read()


def check_jobs():
    client = admin_client()
    upload_id = upload(client, "people.csv", PEOPLE)
    job = run_job(client, kind="run", upload_id=upload_id, format="csv", schema=SCHEMA)
    assert job["status"] == "succeeded", job.get("error")
    assert job["result"]["counts"]["valid_rows"] == 2, job["result"]["counts"]
    data = artifact(client, job, "data")
    assert data[:4] == b"PAR1" and data[-4:] == b"PAR1", "the data artifact is not Parquet"
    print(f"run {job['id']}: succeeded on the batch group, {len(data)} bytes of Parquet")
    generated = run_job(client, kind="generate_schema", upload_id=upload_id, format="csv")
    assert generated["status"] == "succeeded", generated.get("error")
    schema = json.loads(artifact(client, generated, "schema"))
    assert set(schema["properties"]) == {"id", "name", "age"}, schema
    print(f"generate_schema {generated['id']}: succeeded on the no-network interactive group")
    seen = client.request("GET", "/api/v1/admin/workers", expect=(200,))
    seen = seen["items"] if isinstance(seen, dict) else seen
    print(f"workers the gateway has seen: {sorted(w['worker_id'] for w in seen)}")


# --------------------------------------------------------------------------- NetworkPolicies

PROBE = """
import json, socket, sys
results = {}
for target in sys.argv[1:]:
    host, port = target.rsplit(":", 1)
    try:
        socket.create_connection((host, int(port)), timeout=4).close()
        results[target] = "open"
    except OSError as error:
        results[target] = type(error).__name__
print(json.dumps(results))
"""


def kubectl(*args, input_text=None, check=True):
    done = subprocess.run(
        ["docker", "exec", "-i", CLUSTER, "kubectl", *args],
        input=input_text,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if check and done.returncode != 0:
        raise AssertionError(f"kubectl {' '.join(args[:4])} failed: {done.stderr.strip()[:500]}")
    return done.stdout


def probe(namespace, pod, targets, container=None):
    args = ["-n", namespace, "exec", pod]
    if container:
        args += ["-c", container]
    return json.loads(kubectl(*args, "--", "python", "-c", PROBE, *targets))


def expect(namespace, pod, open_targets, blocked_targets, attempts=5):
    """Open targets must connect (retried: a new pod's rules take a moment); blocked ones must
    fail every time."""
    for attempt in range(attempts):
        results = probe(namespace, pod, open_targets + blocked_targets)
        closed = [t for t in open_targets if results[t] != "open"]
        if not closed:
            break
        time.sleep(3)
    else:
        raise AssertionError(f"{pod}: expected to reach {closed}: {results}")
    leaked = [t for t in blocked_targets if results[t] == "open"]
    assert not leaked, f"{pod} reached {leaked}, which its NetworkPolicy should block"
    print(f"{namespace}/{pod}: reaches {open_targets}; blocked: {blocked_targets}")


def pod_of(selector):
    names = kubectl(
        "-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "jsonpath={.items[*].metadata.name}"
    )
    return names.split()[0]


def start_probe(namespace, name, labels):
    """A pod outside the release (the worker image, which has Python) that just waits."""
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True, "runAsUser": 10001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [{
                "name": "probe", "image": WORKER_IMAGE, "imagePullPolicy": "Never",
                "command": ["python", "-c", "import time; time.sleep(3600)"],
                "securityContext": {
                    "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
                },
            }],
        },
    }  # fmt: skip
    kubectl("create", "namespace", namespace, check=False)
    kubectl("apply", "-f", "-", input_text=json.dumps(manifest))
    kubectl("-n", namespace, "wait", "--for=condition=Ready", f"pod/{name}", "--timeout=120s")


def check_policies():
    svc = f"{NAMESPACE}.svc.cluster.local"
    public = f"{RELEASE}-gateway.{svc}:8080"
    internal = f"{RELEASE}-gateway-internal.{svc}:8081"
    store = f"{RELEASE}-rustfs.{svc}:9000"
    postgres = "postgres.forklift-test-db.svc.cluster.local:5432"
    for group in ("batch", "interactive"):
        worker = pod_of(
            f"app.kubernetes.io/component=worker,forklift.cornyhorse.dev/worker-group={group}"
        )
        expect(
            NAMESPACE,
            worker,
            [internal, store],
            [postgres, public, "kubernetes.default.svc.cluster.local:443"],
        )
    gateway = pod_of("app.kubernetes.io/component=gateway")
    expect(NAMESPACE, gateway, [postgres, store], [])
    start_probe("forklift-probe", "outsider", {"app": "probe"})
    expect("forklift-probe", "outsider", [], [public, internal, store])
    # The ingress controller's pods (the chart's default selector) reach the public port only
    start_probe("ingress-nginx", "controller", {"app.kubernetes.io/name": "ingress-nginx"})
    expect("ingress-nginx", "controller", [public], [internal, store])


PROXY_PROBE = """
import json, socket, sys
proxy_host, proxy_port = sys.argv[1].rsplit(":", 1)
results = {}
for request in sys.argv[2:]:
    with socket.create_connection((proxy_host, int(proxy_port)), timeout=10) as sock:
        sock.sendall(request.encode() + b"\\r\\n\\r\\n")
        results[request.split("\\r\\n")[0]] = sock.recv(64).split(b"\\r\\n")[0].decode()
print(json.dumps(results))
"""


def check_proxy_mode():
    """The workers reach the store only through Squid, which allows only the store."""
    svc = f"{NAMESPACE}.svc.cluster.local"
    proxy = f"{RELEASE}-egress-proxy.{svc}:3128"
    store = f"{RELEASE}-rustfs.{svc}:9000"
    for group in ("batch", "interactive"):
        worker = pod_of(
            f"app.kubernetes.io/component=worker,forklift.cornyhorse.dev/worker-group={group}"
        )
        env = kubectl(
            "-n", NAMESPACE, "exec", worker, "--", "printenv", "FORKLIFT_STORE_PROXY"
        ).strip()
        assert env == f"http://{RELEASE}-egress-proxy:3128", env
        expect(
            NAMESPACE,
            worker,
            [proxy, f"{RELEASE}-gateway-internal.{svc}:8081"],
            [f"{RELEASE}-rustfs.{svc}:9000"],
        )
    worker = pod_of(
        "app.kubernetes.io/component=worker,forklift.cornyhorse.dev/worker-group=batch"
    )
    requests = [
        f"GET http://{store}/health HTTP/1.1\r\nHost: {store}",
        f"CONNECT {store} HTTP/1.1\r\nHost: {store}",
        f"GET http://{RELEASE}-gateway:8080/healthz HTTP/1.1\r\nHost: {RELEASE}-gateway:8080",
        f"CONNECT {RELEASE}-gateway-internal:8081 HTTP/1.1\r\nHost: {RELEASE}-gateway-internal",
        "GET http://postgres.forklift-test-db.svc.cluster.local:5432/ HTTP/1.1\r\nHost: postgres",
        "CONNECT example.org:443 HTTP/1.1\r\nHost: example.org:443",
    ]
    answers = json.loads(
        kubectl(
            "-n", NAMESPACE, "exec", worker, "--", "python", "-c", PROXY_PROBE, proxy, *requests
        )
    )
    statuses = [answers[r.split("\r\n")[0]].split()[1] for r in requests]
    assert statuses == ["200", "403", "403", "403", "403", "403"], answers
    print(f"squid: the store's own requests pass, everything else is refused: {statuses}")


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    check_jobs()
    if what == "all":
        check_policies()
    if what == "proxy":
        check_proxy_mode()
    print("k3s e2e: passed")
