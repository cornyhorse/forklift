"""The internal API over HTTP, exactly as workers call it (see the platform brief)."""

from __future__ import annotations

import pytest
from conftest import put_url
from world import World, job_result

pytestmark = pytest.mark.django_db

LEASE = {
    "worker_id": "w-1",
    "lanes": ["batch"],
    "spec_versions": [1],
    "engine_version": "0.2",
    "worker_version": "0.1",
}


@pytest.fixture
def worker(worker_token, internal):
    _, raw = worker_token
    return internal(raw)


def test_lease_heartbeat_presign_input_url_complete(worker):
    World.build()
    lease = worker.post("/internal/v1/leases", LEASE)
    assert lease.status_code == 200
    body = lease.json()
    assert set(body) == {"job_id", "attempt", "lease_seconds", "stage_max_bytes", "spec"}
    job_id = body["job_id"]
    assert worker.post("/internal/v1/leases", LEASE).status_code == 204
    beat = worker.post(
        f"/internal/v1/jobs/{job_id}/heartbeat",
        {"attempt": 1, "progress": {"rows_read": 5, "rows_rejected": 0}},
    )
    assert beat.json() == {"lease_seconds": 60, "cancel": False}
    lost = worker.post(f"/internal/v1/jobs/{job_id}/heartbeat", {"attempt": 2})
    assert lost.status_code == 409 and lost.json()["code"] == "lease_lost"
    refreshed = worker.post(f"/internal/v1/jobs/{job_id}/input-url", {"attempt": 1})
    assert refreshed.json()["location"]["type"] == "presigned_url"
    signed = worker.post(
        f"/internal/v1/jobs/{job_id}/presign",
        {"attempt": 1, "files": [{"name": "data.parquet", "bytes": 4}]},
    )
    [upload] = signed.json()["uploads"]
    assert set(upload) == {"name", "key", "url", "method", "headers"}
    put_url(upload["url"], b"PAR1")
    artifact = {
        "kind": "data",
        "name": "data.parquet",
        "key": upload["key"],
        "bytes": 4,
        "rows": 1,
    }
    done = worker.post(
        f"/internal/v1/jobs/{job_id}/complete",
        {"attempt": 1, "result": job_result(job_id, [artifact]), "artifacts": [artifact]},
    )
    assert done.json() == {"job_id": job_id, "status": "succeeded"}


def test_bad_requests(worker):
    World.build()
    assert worker.post("/internal/v1/leases", {"worker_id": "w"}).status_code == 422
    bad = worker.post("/internal/v1/leases", {**LEASE, "lanes": ["gpu"]})
    assert bad.status_code == 400 and "lanes must name" in bad.json()["detail"]
    missing = "00000000-0000-0000-0000-000000000000"
    gone = worker.post(f"/internal/v1/jobs/{missing}/heartbeat", {"attempt": 1})
    assert gone.status_code == 404 and gone.json()["code"] == "not_found"
    job_id = worker.post("/internal/v1/leases", LEASE).json()["job_id"]
    invalid = worker.post(
        f"/internal/v1/jobs/{job_id}/complete", {"attempt": 1, "result": {"status": "succeeded"}}
    )
    assert invalid.status_code == 400 and invalid.json()["code"] == "result_invalid"
    assert "job_id" in invalid.json()["detail"]
