"""--store-proxy end to end: store requests through the proxy, gateway requests never, the
streaming engine through it too (and, under Landlock, to its port only); proxies in the worker's
environment change nothing."""

from __future__ import annotations

import json
from urllib.parse import urlsplit

import pytest
from forward_proxy import ForwardProxy

from forklift_worker import linux

LANDLOCK = linux.landlock_abi()
PROXY_NAMES = ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY")


@pytest.fixture
def store_proxy(gateway):
    """A proxy that lets requests through to the fake gateway's store only (its host and port
    are the gateway's: the test tells them apart by path)."""
    with ForwardProxy(allow=[gateway.host]) as proxy:
        yield proxy


def paths(proxy: ForwardProxy, method: str) -> list[str]:
    return [urlsplit(target).path for target in proxy.targets(method)]


def report_of(gateway, job) -> dict:
    assert job.completed["result"]["status"] == "succeeded", job.completed["result"]
    return json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])


def test_staging_and_uploads_go_through_the_proxy_and_gateway_calls_never(
    gateway, run_worker, store_proxy
):
    gateway.multipart_threshold, gateway.part_size = 11, 4  # data.parquet goes up in parts
    job = gateway.enqueue()
    run_worker(store_proxy=store_proxy.url)

    assert job.completed["result"]["status"] == "succeeded"
    assert paths(store_proxy, "GET") == [f"/store/uploads/{job.job_id}/people.csv"]
    puts = paths(store_proxy, "PUT")
    prefix = f"/store/jobs/{job.job_id}/attempt-1/"
    assert puts.count(prefix + "data.parquet") == 3, "three parts of four bytes or fewer"
    names = ("data.parquet", "bad_rows.parquet", "manifest.json")
    assert set(puts) == {prefix + name for name in names}
    agents = {r["headers"]["user-agent"] for r in store_proxy.requests}
    assert agents == {"forklift-worker/0.1.0"}, "the supervisor's requests, all of them"
    assert not [t for t in store_proxy.targets() if "/internal/" in t], "no gateway call"
    assert gateway.calls("leases") and gateway.calls("complete")
    assert gateway.objects[f"jobs/{job.job_id}/attempt-1/data.parquet"] == b"ID,NAME\n1,A\n"


def test_a_streaming_engine_reads_through_the_proxy(gateway, run_worker, store_proxy):
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    run_worker(store_proxy=store_proxy.url)

    env = report_of(gateway, job)["env"]
    assert {name: env.get(name) for name in PROXY_NAMES} == dict.fromkeys(
        PROXY_NAMES, store_proxy.url
    )
    assert "NO_PROXY" not in env and "no_proxy" not in env
    (engine_get,) = [r for r in store_proxy.requests if r["method"] == "GET"]
    assert urlsplit(engine_get["target"]).path == f"/store/uploads/{job.job_id}/people.csv"
    assert engine_get["headers"]["user-agent"].startswith("Python-urllib/"), "the engine's"
    assert gateway.objects[f"jobs/{job.job_id}/attempt-1/data.parquet"] == b"ID,NAME\n1,A\n"


@pytest.mark.skipif(LANDLOCK < 4, reason="Landlock TCP rules need ABI 4 (Linux 6.7)")
def test_under_landlock_the_streaming_engine_reaches_the_proxys_port_only(
    gateway, run_worker, store_proxy
):
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    store_port = int(gateway.host.rpartition(":")[2])
    job.spec["options"] = {
        "fake_engine": {
            "mode": "probe",
            "connect": [["127.0.0.1", store_port], ["127.0.0.1", store_proxy.port]],
        }
    }
    run_worker(store_proxy=store_proxy.url)

    connect = report_of(gateway, job)["connect"]
    assert connect[f"127.0.0.1:{store_port}"] == "PermissionError", "not the store's port"
    assert connect[f"127.0.0.1:{store_proxy.port}"] == "ok"


def test_a_staged_engine_gets_no_proxy(gateway, run_worker, store_proxy):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    run_worker(store_proxy=store_proxy.url)

    env = report_of(gateway, job)["env"]
    assert not [name for name in env if name.lower().endswith("_proxy")]


def test_proxies_in_the_worker_environment_change_nothing(gateway, run_worker, monkeypatch):
    with ForwardProxy(allow=[]) as ambient:  # it would refuse everything
        for name in (*PROXY_NAMES, "ALL_PROXY"):
            monkeypatch.setenv(name, ambient.url)
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)
        staged = gateway.enqueue()
        run_worker()
        gateway.stage_max_bytes = 4
        streamed = gateway.enqueue()
        streamed.spec["options"] = {"fake_engine": {"mode": "probe"}}
        run_worker()

    assert staged.completed["result"]["status"] == "succeeded"
    env = report_of(gateway, streamed)["env"]
    assert not [name for name in env if name.lower().endswith("_proxy")]
    assert ambient.requests == []


def test_a_proxy_that_refuses_the_store_fails_the_job_with_a_clear_message(gateway, run_worker):
    with ForwardProxy(allow=["store.example.org:443"]) as proxy:
        job = gateway.enqueue()
        run_worker(store_proxy=proxy.url)

    error = job.completed["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert f"refused by the store proxy 127.0.0.1:{proxy.port} (ERR_ACCESS_DENIED)" in (
        error["message"]
    )
    assert "X-Amz-Signature" not in error["message"]
