"""The worker and the real engine reaching RustFS through a forward proxy (--store-proxy): staged
and streamed inputs and multipart outputs, with presigned URLs signed the way the gateway signs
them (SigV4 signs the Host header, which must reach the store unchanged through the proxy)."""

from __future__ import annotations

import hashlib
import io
import random
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow.parquet as pq
import pytest
from forward_proxy import ForwardProxy
from it_helpers import CSV, csv_spec, engine_has_run_job

from forklift_worker.supervisor import Supervisor
from forklift_worker.transport import host_of

pytestmark = [
    pytest.mark.services,
    pytest.mark.skipif(not engine_has_run_job(), reason="this engine has no `forklift run-job`"),
]

MIB = 1024 * 1024
FAKE_ENGINE = Path(__file__).resolve().parents[1] / "fake_engine.py"


@pytest.fixture
def proxy(store_gateway):
    """A proxy that allows the store and nothing else (the fake gateway is not behind it)."""
    with ForwardProxy(allow=[host_of(store_gateway.store.endpoint)]) as forward:
        yield forward


def run(store_gateway, make_store_settings, spec, **overrides):
    job = store_gateway.enqueue(spec)
    Supervisor(make_store_settings(**overrides)).run()
    assert job.completed["result"]["status"] == "succeeded", job.completed["result"]
    return job.completed


def through(proxy: ForwardProxy, method: str) -> list[str]:
    return [urlsplit(target).path for target in proxy.targets(method)]


def names(store_gateway, report) -> list[str]:
    keys = {artifact["name"]: artifact["key"] for artifact in report["artifacts"]}
    table = pq.read_table(io.BytesIO(store_gateway.read(keys["data.parquet"])))
    return [row["name"] for row in table.to_pylist()]


def test_a_staged_input_and_its_artifacts_go_through_the_proxy(
    store_gateway, make_store_settings, proxy
):
    location = store_gateway.put_object("uploads/p1/people.csv", CSV)
    report = run(
        store_gateway, make_store_settings, csv_spec("p1", location), store_proxy=proxy.url
    )

    bucket = store_gateway.bucket
    assert through(proxy, "GET") == [f"/{bucket}/uploads/p1/people.csv"]
    assert sorted(through(proxy, "PUT")) == sorted(
        f"/{bucket}/{a['key']}" for a in report["artifacts"]
    )
    assert names(store_gateway, report) == ["alice", "bob"]
    assert proxy.targets("CONNECT") == []


def test_a_streamed_input_is_read_by_the_engine_through_the_proxy(
    store_gateway, make_store_settings, proxy
):
    store_gateway.stage_max_bytes = 10
    location = store_gateway.put_object("uploads/p2/people.csv", CSV)
    # Under Landlock (where the kernel has it) the engine may connect to the proxy's port only.
    report = run(
        store_gateway, make_store_settings, csv_spec("p2", location), store_proxy=proxy.url
    )

    engine = [r for r in proxy.requests if r["method"] == "GET"]
    assert engine, "the engine read the input through the proxy"
    assert all(r["headers"]["user-agent"].startswith("Python-urllib/") for r in engine)
    assert {urlsplit(r["target"]).path for r in engine} == {
        f"/{store_gateway.bucket}/uploads/p2/people.csv"
    }
    assert names(store_gateway, report) == ["alice", "bob"]


def test_a_large_output_goes_up_in_parts_through_the_proxy(
    store_gateway, make_store_settings, proxy
):
    store_gateway.multipart_threshold, store_gateway.part_size = 5 * MIB, 5 * MIB
    store_gateway.first_parts, store_gateway.stage_max_bytes = 1, 64 * MIB
    text = bytes(random.Random(5).choices(b"abcdefghij,\n", k=11 * MIB))
    location = store_gateway.put_object("uploads/p3/big.csv", text)
    report = run(
        store_gateway,
        make_store_settings,
        csv_spec("p3", location),
        store_proxy=proxy.url,
        engine_command=[sys.executable, str(FAKE_ENGINE)],  # data.parquet: the input upper-cased
        engine_read_path=[FAKE_ENGINE.parent],
    )

    data = next(a for a in report["artifacts"] if a["name"] == "data.parquet")
    assert data["part_count"] == 3
    parts = [t for t in proxy.targets("PUT") if "partNumber=" in t and data["key"] in t]
    assert len(parts) == 3, "every part through the proxy"
    stored = store_gateway.read(data["key"])
    assert stored == text.upper() and hashlib.sha256(stored).hexdigest() == data["sha256"]


def test_a_proxy_that_does_not_allow_the_store_fails_the_job(store_gateway, make_store_settings):
    location = store_gateway.put_object("uploads/p4/people.csv", CSV)
    job = store_gateway.enqueue(csv_spec("p4", location))
    with ForwardProxy(allow=["store.example.org:443"]) as refusing:
        Supervisor(make_store_settings(store_proxy=refusing.url)).run()

    error = job.completed["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert "refused by the store proxy" in error["message"]
    assert store_gateway.store.keys(store_gateway.bucket, "jobs/") == []
