"""The worker against RustFS presigned URLs, signed the way the gateway signs them (and signed
anew when the engine of a streamed input asks for a fresh one)."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import random
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from it_helpers import CSV, csv_spec, engine_has_run_job

from forklift_worker.supervisor import Supervisor

pytestmark = [
    pytest.mark.services,
    pytest.mark.skipif(not engine_has_run_job(), reason="this engine has no `forklift run-job`"),
]


def tamper(url: str) -> str:
    """The URL with one character of its signature changed."""
    head, tail = url.split("X-Amz-Signature=")
    signature, _, rest = tail.partition("&")
    flipped = ("1" if signature[0] == "0" else "0") + signature[1:]
    return f"{head}X-Amz-Signature={flipped}" + (f"&{rest}" if rest else "")


def run(store_gateway, make_store_settings, spec, **overrides):
    job = store_gateway.enqueue(spec)
    supervisor = Supervisor(make_store_settings(**overrides))
    supervisor.run()
    return job, supervisor


def data_rows(store_gateway, report) -> list[str]:
    keys = {artifact["name"]: artifact["key"] for artifact in report["artifacts"]}
    table = pq.read_table(io.BytesIO(store_gateway.read(keys["data.parquet"])))
    return [row["name"] for row in table.to_pylist()]


def test_a_staged_input_and_its_artifacts_go_through_presigned_urls(
    store_gateway, make_store_settings
):
    location = store_gateway.put_object("uploads/a/people.csv", CSV)
    job, _ = run(store_gateway, make_store_settings, csv_spec("a", location))

    report = job.completed
    assert report["result"]["status"] == "succeeded", report["result"]
    for artifact in report["artifacts"]:
        stored = store_gateway.read(artifact["key"])
        assert artifact["key"].startswith(f"jobs/{job.job_id}/attempt-1/")
        assert hashlib.sha256(stored).hexdigest() == artifact["sha256"]
        assert len(stored) == artifact["bytes"]
    assert data_rows(store_gateway, report) == ["alice", "bob"]
    head = store_gateway.s3.head_object(
        Bucket=store_gateway.bucket, Key=report["artifacts"][0]["key"]
    )
    assert head["ContentType"] == "application/octet-stream"


def test_a_streamed_input_gives_the_same_rows(store_gateway, make_store_settings):
    store_gateway.stage_max_bytes = 10
    location = store_gateway.put_object("uploads/b/people.csv", CSV)
    job, _ = run(store_gateway, make_store_settings, csv_spec("b", location))

    assert job.completed["result"]["status"] == "succeeded", job.completed["result"]
    assert data_rows(store_gateway, job.completed) == ["alice", "bob"]


def test_a_streamed_input_whose_url_expired_gets_a_fresh_one(store_gateway, make_store_settings):
    store_gateway.stage_max_bytes = 10
    staged = store_gateway.put_object("uploads/h/people.csv", CSV)
    location = store_gateway.location("uploads/h/people.csv", staged["size"], staged["etag"], 1)
    time.sleep(2)  # expired before the engine starts: it asks for a fresh URL over the pipe
    job, _ = run(store_gateway, make_store_settings, csv_spec("h", location))

    assert job.completed["result"]["status"] == "succeeded", job.completed["result"]
    assert data_rows(store_gateway, job.completed) == ["alice", "bob"]
    assert len(store_gateway.calls("input-url")) == 1


def test_a_multipart_object_is_staged(store_gateway, make_store_settings):
    s3, bucket, key = store_gateway.s3, store_gateway.bucket, "uploads/c/people.csv"
    part = b"id,name\n" + b"".join(b"%d,name%d\n" % (n, n) for n in range(600_000))
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key)
    parts = []
    for number, body in enumerate([part[: 5 * 1024 * 1024], part[5 * 1024 * 1024 :]], start=1):
        answer = s3.upload_part(
            Bucket=bucket, Key=key, PartNumber=number, UploadId=upload["UploadId"], Body=body
        )
        parts.append({"PartNumber": number, "ETag": answer["ETag"]})
    s3.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload["UploadId"], MultipartUpload={"Parts": parts}
    )
    head = s3.head_object(Bucket=bucket, Key=key)
    assert "-" in head["ETag"], "RustFS gives multipart objects a multipart ETag"
    store_gateway.stage_max_bytes = 64 * 1024 * 1024
    location = store_gateway.location(key, head["ContentLength"], head["ETag"])
    job, _ = run(store_gateway, make_store_settings, csv_spec("c", location))

    result = job.completed["result"]
    assert result["status"] == "succeeded", result
    assert result["counts"]["valid_rows"] == 600_000


def test_an_object_replaced_after_the_lease_is_refused(store_gateway, make_store_settings):
    location = store_gateway.put_object("uploads/d/people.csv", CSV)
    store_gateway.s3.put_object(
        Bucket=store_gateway.bucket, Key="uploads/d/people.csv", Body=CSV.replace(b"bob", b"eve")
    )
    job, _ = run(store_gateway, make_store_settings, csv_spec("d", location))

    error = job.completed["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert "changed after the job was queued" in error["message"]


def test_a_tampered_download_url_is_refused_by_the_store(
    store_gateway, make_store_settings, caplog
):
    location = store_gateway.put_object("uploads/e/people.csv", CSV)
    location["url"] = tamper(location["url"])
    caplog.set_level(logging.DEBUG, logger="forklift_worker")
    job, _ = run(store_gateway, make_store_settings, csv_spec("e", location))

    error = job.completed["result"]["error"]
    assert error["code"] == "INPUT_UNREADABLE"
    assert "refused the input's presigned URL (HTTP 403" in error["message"]
    signature = location["url"].split("X-Amz-Signature=")[1].split("&")[0]
    assert signature not in caplog.text and signature not in json.dumps(job.completed)


def test_an_upload_url_the_store_refuses_fails_the_job(store_gateway, make_store_settings):
    original = store_gateway.upload_target

    def tampered(key):
        url, headers = original(key)
        return tamper(url), headers

    store_gateway.upload_target = tampered
    location = store_gateway.put_object("uploads/f/people.csv", CSV)
    job, _ = run(store_gateway, make_store_settings, csv_spec("f", location))

    error = job.completed["result"]["error"]
    assert "refused the upload of the artifact" in error["message"]
    assert "HTTP 403" in error["message"]
    assert store_gateway.store.keys(store_gateway.bucket, "jobs/") == []


def test_a_presigned_url_for_another_store_is_refused(store_gateway, make_store_settings):
    location = store_gateway.put_object("uploads/g/people.csv", CSV)
    job, _ = run(
        store_gateway, make_store_settings, csv_spec("g", location), store_host=["other:9000"]
    )

    error = job.completed["result"]["error"]
    assert error["code"] == "SPEC_INVALID"
    assert "not an object store this worker may read" in error["message"]


MIB = 1024 * 1024
FAKE_ENGINE = Path(__file__).resolve().parents[1] / "fake_engine.py"


def test_a_large_output_goes_up_in_parts_byte_for_byte(store_gateway, make_store_settings):
    store_gateway.multipart_threshold, store_gateway.part_size = 5 * MIB, 5 * MIB
    store_gateway.first_parts, store_gateway.stage_max_bytes = 1, 64 * MIB
    text = bytes(random.Random(7).choices(b"abcdefghij,\n", k=11 * MIB))
    location = store_gateway.put_object("uploads/j/big.csv", text)
    job, _ = run(
        store_gateway,
        make_store_settings,
        csv_spec("j", location),
        engine_command=[
            sys.executable,
            str(FAKE_ENGINE),
        ],  # its data.parquet: the input upper-cased
        engine_read_path=[FAKE_ENGINE.parent],
    )

    report = job.completed
    assert report["result"]["status"] == "succeeded", report["result"]
    data = next(a for a in report["artifacts"] if a["name"] == "data.parquet")
    assert data["part_count"] == 3 and len(store_gateway.calls("parts")) == 1
    assert store_gateway.read(data["key"]) == text.upper()
    head = store_gateway.s3.head_object(Bucket=store_gateway.bucket, Key=data["key"])
    assert head["ETag"].strip('"').endswith("-3"), "RustFS assembled it from three parts"
    assert store_gateway.store.unfinished_uploads(store_gateway.bucket) == []


def test_a_parquet_output_of_the_real_engine_goes_up_in_parts(store_gateway, make_store_settings):
    store_gateway.multipart_threshold, store_gateway.part_size = 5 * MIB, 5 * MIB
    store_gateway.stage_max_bytes = 64 * MIB
    rng = random.Random(11)
    rows = [b"%d,%032x" % (n, rng.getrandbits(128)) for n in range(250_000)]
    location = store_gateway.put_object(
        "uploads/k/big.csv", b"id,name\n" + b"\n".join(rows) + b"\n"
    )
    job, _ = run(store_gateway, make_store_settings, csv_spec("k", location))

    report = job.completed
    assert report["result"]["status"] == "succeeded", report["result"]
    data = next(a for a in report["artifacts"] if a["name"] == "data.parquet")
    assert data["part_count"] >= 2, f"data.parquet has only {data['bytes']} bytes"
    stored = store_gateway.read(data["key"])
    assert hashlib.sha256(stored).hexdigest() == data["sha256"] and len(stored) == data["bytes"]
    assert pq.read_table(io.BytesIO(stored)).num_rows == 250_000
