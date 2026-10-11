"""Jobs whose artifacts go up in parts (above the gateway's multipart threshold), end to end
against the fake gateway: progress, cancellation and lost leases between parts, refusals."""

from __future__ import annotations

import hashlib

import pytest

from forklift_worker import job as job_module
from forklift_worker import uploads
from forklift_worker.supervisor import EXIT_GATEWAY

DATA = b"ID,NAME\n1,A\n"  # what the fake engine writes to data.parquet: three parts of 4 bytes


@pytest.fixture
def in_parts(gateway):
    """data.parquet and manifest.json above the threshold, bad_rows.parquet below it."""
    gateway.multipart_threshold, gateway.part_size, gateway.first_parts = 11, 4, 1
    return gateway


def test_large_artifacts_go_up_in_parts(in_parts, run_worker):
    job = in_parts.enqueue()
    run_worker()

    report = job.completed
    assert report["result"]["status"] == "succeeded"
    by_name = {artifact["name"]: artifact for artifact in report["artifacts"]}
    data = by_name["data.parquet"]
    assert data["upload_id"] and data["part_count"] == 3 and "parts" not in data
    etags = "".join(f"{hashlib.md5(DATA[i : i + 4]).hexdigest()}\n" for i in (0, 4, 8))
    assert data["parts_sha256"] == hashlib.sha256(etags.encode()).hexdigest()
    assert in_parts.objects[data["key"]] == DATA
    assert data["sha256"] == hashlib.sha256(DATA).hexdigest() and data["bytes"] == len(DATA)
    assert "upload_id" not in by_name["bad_rows.parquet"]
    assert in_parts.objects[by_name["manifest.json"]["key"]] == b'{"files": ["data.parquet"]}'
    assert [call["body"]["multipart"] for call in in_parts.calls("presign")] == [True]
    asked = [call["body"]["part_numbers"] for call in in_parts.calls("parts")]
    assert asked == [[2, 3], [2, 3, 4, 5, 6, 7]]
    assert in_parts.multipart == {}, "every multipart upload was completed"


def test_heartbeats_carry_the_bytes_uploaded(in_parts, run_worker):
    in_parts.part_delay = 0.1
    job = in_parts.enqueue()
    run_worker(heartbeat_seconds=0.02)

    uploaded = [beat["bytes_uploaded"] for beat in job.heartbeats if "bytes_uploaded" in beat]
    assert uploaded and uploaded == sorted(uploaded) and 0 < uploaded[0] < len(DATA)


def test_cancel_between_parts(in_parts, run_worker):
    in_parts.part_delay = 0.1
    job = in_parts.enqueue()
    in_parts.cancel_when = lambda job, progress: progress.get("bytes_uploaded", 0) > 0
    run_worker(heartbeat_seconds=0.02)

    result = job.completed["result"]
    assert result["status"] == "cancelled" and job.completed["artifacts"] == []
    assert "while its artifacts were being uploaded" in result["error"]["message"]
    assert len(in_parts.calls("part")) < 3 + 7


def test_a_lease_lost_between_parts(in_parts, run_worker):
    in_parts.part_delay = 0.1
    job = in_parts.enqueue()
    in_parts.lost_when = lambda job, progress: progress.get("bytes_uploaded", 0) > 0
    supervisor = run_worker(heartbeat_seconds=0.02)

    assert job.completed is None and supervisor.outcomes == ["abandoned"]


def test_a_part_the_store_refuses_fails_the_job(in_parts, run_worker):
    job = in_parts.enqueue()
    in_parts.fail("part", 400)
    run_worker()

    error = job.completed["result"]["error"]
    assert (error["code"], error["retryable"]) == ("INTERNAL", False)
    assert "refused the upload of part 1 of 3 of the artifact data.parquet (HTTP 400" in (
        error["message"]
    )
    assert job.completed["artifacts"] == []


@pytest.mark.parametrize(
    "fault, outcome, exit_code",
    [(400, "failed", 0), (409, "abandoned", 0), (401, "abandoned", EXIT_GATEWAY)],
)
def test_part_urls_the_gateway_refuses(in_parts, run_worker, fault, outcome, exit_code):
    job = in_parts.enqueue()
    in_parts.fail("parts", fault)
    supervisor = run_worker()

    assert supervisor.outcomes == [outcome] and supervisor.exit_code == exit_code
    if outcome == "failed":
        message = job.completed["result"]["error"]["message"]
        assert message.startswith("The gateway would not presign the job's artifacts")


def test_part_urls_while_the_gateway_is_down(in_parts, run_worker, monkeypatch):
    monkeypatch.setattr(job_module, "PRESIGN_ATTEMPTS", 1)
    job = in_parts.enqueue()
    in_parts.fail("parts", 503)
    supervisor = run_worker()

    assert job.completed is None and supervisor.outcomes == ["abandoned"]


@pytest.mark.parametrize("threshold, presigned", [(None, 1 + 3), (11, 1 + 1)])
def test_urls_that_grew_old_are_signed_again(
    gateway, run_worker, monkeypatch, threshold, presigned
):
    gateway.multipart_threshold, gateway.part_size, gateway.url_seconds = threshold, 4, 60
    monkeypatch.setattr(uploads, "URL_USE_FRACTION", 0.0)  # every URL is old when it is used
    job = gateway.enqueue()
    run_worker()

    assert job.completed["result"]["status"] == "succeeded"
    assert len(gateway.calls("presign")) == presigned  # once, then each single PUT again
    if threshold:  # and every part's URL is fetched again
        assert len(gateway.calls("parts")) == len(gateway.calls("part")) == 3 + 7
    data = next(a for a in job.completed["artifacts"] if a["name"] == "data.parquet")
    assert gateway.objects[data["key"]] == DATA
