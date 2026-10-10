"""The service MVP's core loop through both APIs, with real presigned URLs against RustFS:
upload a file, run it with a schema, let a (simulated) worker lease, upload outputs and
complete, then download the Parquet and bad_rows."""

from __future__ import annotations

import hashlib

import pytest
from conftest import get_url, put_url
from world import job_result

from forklift_web.core.choices import Role

pytestmark = pytest.mark.django_db

CSV = b"id,name\n1,alice\n2,bob\n"
SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}


def upload_file(caller, data: bytes = CSV, filename: str = "people.csv") -> str:
    response = caller.post("/api/v1/uploads", {"filename": filename, "size": len(data)})
    assert response.status_code == 201, response.json()
    ticket = response.json()
    put_url(ticket["url"], data)
    completed = caller.post(f"/api/v1/uploads/{ticket['upload']['id']}/complete")
    assert completed.status_code == 200, completed.json()
    assert completed.json()["status"] == "complete"
    return ticket["upload"]["id"]


def test_upload_run_lease_complete_download(make_user, as_user, worker_token, internal):
    operator = as_user(make_user(Role.OPERATOR))
    upload_id = upload_file(operator)

    created = operator.post(
        "/api/v1/jobs",
        {"kind": "run", "upload_id": upload_id, "schema": SCHEMA},
        HTTP_IDEMPOTENCY_KEY="run-1",
    )
    assert created.status_code == 201, created.json()
    job = created.json()
    assert (job["status"], job["lane"], job["classification"]) == ("queued", "batch", "internal")
    replay = operator.post(
        "/api/v1/jobs",
        {"kind": "run", "upload_id": upload_id, "schema": SCHEMA},
        HTTP_IDEMPOTENCY_KEY="run-1",
    )
    assert (replay.status_code, replay.json()["id"]) == (200, job["id"])

    _, raw = worker_token
    worker = internal(raw)
    leased = worker.post(
        "/internal/v1/leases",
        {"worker_id": "w-1", "lanes": ["batch"], "spec_versions": [1], "engine_version": "0.1"},
    )
    assert leased.status_code == 200, leased.content
    lease = leased.json()
    assert (lease["job_id"], lease["attempt"]) == (job["id"], 1)
    location = lease["spec"]["input"]["location"]
    assert location["type"] == "presigned_url" and location["size"] == len(CSV)
    assert get_url(location["url"]) == CSV  # the worker can read the input with it
    assert lease["spec"]["output"]["location"] == {"type": "file", "path": "out/"}

    beat = worker.post(
        f"/internal/v1/jobs/{job['id']}/heartbeat", {"attempt": 1, "progress": {"rows_read": 2}}
    )
    assert beat.json() == {"lease_seconds": 60, "cancel": False}

    outputs = {"data.parquet": b"PAR1-data", "bad_rows.parquet": b"PAR1-bad"}
    signed = worker.post(
        f"/internal/v1/jobs/{job['id']}/presign",
        {"attempt": 1, "files": [{"name": n, "bytes": len(d)} for n, d in outputs.items()]},
    )
    assert signed.status_code == 200, signed.content
    reported = []
    for entry in signed.json()["uploads"]:
        assert entry["key"] == f"jobs/{job['id']}/attempt-1/{entry['name']}"
        put_url(entry["url"], outputs[entry["name"]])
        data = outputs[entry["name"]]
        reported.append(
            {
                "kind": "data" if entry["name"] == "data.parquet" else "bad_rows",
                "name": entry["name"],
                "key": entry["key"],
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "rows": 1,
            }
        )
    done = worker.post(
        f"/internal/v1/jobs/{job['id']}/complete",
        {"attempt": 1, "result": job_result(job["id"], reported), "artifacts": reported},
    )
    assert done.status_code == 200, done.content
    assert done.json()["status"] == "succeeded"

    finished = operator.get(f"/api/v1/jobs/{job['id']}").json()
    assert finished["status"] == "succeeded"
    assert finished["result"]["counts"]["valid_rows"] == 1
    artifacts = operator.get(f"/api/v1/jobs/{job['id']}/artifacts").json()
    assert sorted(a["name"] for a in artifacts) == ["bad_rows.parquet", "data.parquet"]
    for artifact in artifacts:
        download = operator.get(f"/api/v1/artifacts/{artifact['id']}/download")
        assert download.status_code == 200, download.content
        assert get_url(download.json()["url"]) == outputs[artifact["name"]]
