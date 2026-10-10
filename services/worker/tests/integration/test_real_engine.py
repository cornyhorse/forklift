"""The worker running the real engine (`python -I -m forklift run-job`) with the fake gateway."""

from __future__ import annotations

import io
import json

import pyarrow.parquet as pq
import pytest
from it_helpers import CSV, REAL_ENGINE, csv_spec, engine_has_run_job

from forklift_worker import linux
from forklift_worker.isolation import probe_network_namespace

pytestmark = pytest.mark.skipif(
    not engine_has_run_job(), reason="this engine has no `forklift run-job` yet"
)


def parquet_rows(data: bytes) -> list[dict]:
    return pq.read_table(io.BytesIO(data)).to_pylist()


def run_real(gateway, run_worker, spec, **overrides):
    job = gateway.enqueue(spec)
    supervisor = run_worker(engine_command=REAL_ENGINE, engine_read_path=[], **overrides)
    assert job.completed is not None, supervisor.outcomes
    return job, job.completed


def test_a_staged_csv_run(gateway, run_worker):
    location = gateway.put_object("uploads/u1/people.csv", CSV)
    job, report = run_real(gateway, run_worker, csv_spec("x", location))

    result = report["result"]
    assert result["status"] == "succeeded", result
    assert result["counts"]["valid_rows"] == 2 and result["counts"]["invalid_rows"] == 1
    names = [artifact["name"] for artifact in report["artifacts"]]
    assert names[-1] == "manifest.json"
    assert {"data.parquet", "bad_rows.parquet", "metadata.json"} <= set(names)
    keys = {artifact["name"]: artifact["key"] for artifact in report["artifacts"]}
    rows = parquet_rows(gateway.objects[keys["data.parquet"]])
    assert [row["name"] for row in rows] == ["alice", "bob"]
    assert len(parquet_rows(gateway.objects[keys["bad_rows.parquet"]])) == 1
    assert all(
        set(beat) >= {"rows_read", "rows_rejected", "bytes_read"} for beat in job.heartbeats
    )


def test_a_streamed_csv_run_gives_the_same_rows(gateway, run_worker):
    gateway.stage_max_bytes = 8
    location = gateway.put_object("uploads/u2/people.csv", CSV)
    _, report = run_real(gateway, run_worker, csv_spec("y", location))

    assert report["result"]["status"] == "succeeded", report["result"]
    keys = {artifact["name"]: artifact["key"] for artifact in report["artifacts"]}
    assert [row["name"] for row in parquet_rows(gateway.objects[keys["data.parquet"]])] == [
        "alice",
        "bob",
    ]
    ranged = [call for call in gateway.calls("get") if "range" in call["headers"]]
    assert ranged, "the engine read the object itself, in range requests"


def test_an_invalid_spec_is_reported_by_the_engine(gateway, run_worker):
    location = gateway.put_object("uploads/u3/people.csv", CSV)
    spec = csv_spec("z", location)
    spec["input"]["options"] = {"no_such_option": 1}
    _, report = run_real(gateway, run_worker, spec)

    error = report["result"]["error"]
    assert error["code"] == "SPEC_INVALID"
    assert "no_such_option" in error["message"]


@pytest.mark.skipif(not linux.landlock_abi(), reason="this kernel has no Landlock")
def test_the_real_engine_runs_under_landlock_required(gateway, run_worker):
    location = gateway.put_object("uploads/u4/people.csv", CSV)
    _, report = run_real(gateway, run_worker, csv_spec("w", location), landlock="required")

    assert report["result"]["status"] == "succeeded", report["result"]


@pytest.mark.skipif(not probe_network_namespace()[0], reason="no network namespaces here")
def test_the_real_engine_runs_without_a_network(gateway, run_worker):
    location = gateway.put_object("uploads/u5/people.csv", CSV)
    _, report = run_real(gateway, run_worker, csv_spec("v", location), isolation="no-network")

    assert report["result"]["status"] == "succeeded", report["result"]
    assert json.dumps(report).count("alice") == 0, "results carry no cell values"
