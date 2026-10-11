"""Outputs above multipart_threshold_bytes go up in parts: presign starts a multipart upload,
POST /parts signs more part URLs, complete checks the parts against the store's before it
completes the upload, and uploads an attempt leaves pending are aborted (when the attempt ends,
and by the sweeper). Real presigned URLs against RustFS throughout."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from datetime import timedelta

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from conftest import get_url, put_url, root_client
from django.utils import timezone
from world import World, job_result

from forklift_web import storage
from forklift_web.core.choices import JobStatus
from forklift_web.core.models import Artifact, AuditLog, Job
from forklift_web.errors import Conflict, InvalidRequest, StoreUnavailable
from forklift_web.policy import Actor
from forklift_web.services import installation, queue, retention
from forklift_web.services.workers import WorkerPrincipal

pytestmark = pytest.mark.django_db

MIB = 1024**2
SYSTEM = Actor.for_system("sweep_retention")


def abort_pending_outputs() -> None:
    """With the tests' own client: a test may have patched storage.Bucket until its end."""
    client, bucket = root_client(), storage.store().bucket
    pages = client.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix="jobs/")
    for upload in (item for page in pages for item in page.get("Uploads", [])):
        client.abort_multipart_upload(
            Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
        )


@pytest.fixture(autouse=True)
def leave_no_pending_outputs():
    """The session's bucket is shared: other tests' sweeps should not find these uploads."""
    yield
    abort_pending_outputs()


@pytest.fixture
def principal(worker_token):
    token, _ = worker_token
    return WorkerPrincipal(token=token)


@pytest.fixture
def small_parts(admin_actor):
    """The smallest threshold and part size the settings (and S3) allow."""
    installation.update(
        admin_actor, {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB}
    )


@pytest.fixture
def job(principal):
    World.build()
    return queue.lease(principal, worker_id="w-1", lanes=["batch"], spec_versions=[1]).job


def md5_base64(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data).digest()).decode()


def put_part(url: str, data: bytes) -> str:
    """PUT one part as the worker does (with Content-MD5); returns the ETag it was answered."""
    return put_url(url, data, {"Content-MD5": md5_base64(data)})["ETag"]


def start_upload(job: Job, name: str) -> tuple:
    key = job.attempt_prefix + name
    return key, storage.store().create_multipart(key)


def upload_parts(key: str, upload_id: str, pieces: list) -> list:
    bucket = storage.store()
    return [
        put_part(
            bucket.presign_part(
                key, upload_id, number, expires=60, audience=storage.Audience.GATEWAY
            ),
            piece,
        )
        for number, piece in enumerate(pieces, start=1)
    ]


def reported(job: Job, name: str, data: bytes, upload_id: str, etags: list, **fields) -> dict:
    return {
        "kind": "data",
        "name": name,
        "key": job.attempt_prefix + name,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "rows": 1,
        "upload_id": upload_id,
        "part_count": len(etags),
        "parts_sha256": queue.parts_sha256(etags),
        **fields,
    }


def complete(principal, job, artifacts, **result_fields):
    result = job_result(str(job.pk), artifacts, **result_fields)
    return queue.complete(
        principal, job.pk, attempt=job.attempt, result=result, artifacts=artifacts
    )


def pending(prefix: str) -> list:
    return sorted(upload.key for upload in storage.store().list_multipart_uploads(prefix))


def stored(key: str) -> bytes:
    bucket = storage.store()
    return get_url(bucket.presign_get(key, expires=60, audience=storage.Audience.GATEWAY))


# --------------------------------------------------------------------------- presign


def test_presign_answers_large_outputs_with_a_multipart_upload(principal, job, small_parts):
    small, large = queue.presign_outputs(
        principal,
        job.pk,
        attempt=1,
        files=[
            {"name": "manifest.json", "bytes": 10},
            {"name": "data.parquet", "bytes": 12 * MIB},
        ],
        multipart=True,
    )
    assert small["url"] and "upload_id" not in small and small["expires_in"] == 3600
    assert large["url"] is None and large["key"] == job.attempt_prefix + "data.parquet"
    assert (large["part_size"], large["part_count"]) == (5 * MIB, 3)
    assert [part["part_number"] for part in large["parts"]] == [1, 2, 3]
    assert (large["method"], large["headers"], large["expires_in"]) == ("PUT", {}, 3600)
    assert pending(job.attempt_prefix) == [large["key"]]


def test_part_urls_are_signed_for_the_worker_endpoint(principal, job, small_parts, settings):
    settings.FORKLIFT_STORE = {
        **settings.FORKLIFT_STORE,
        "worker_endpoint_url": "http://rustfs.internal:9000",
    }
    storage.reset_store()
    try:
        [upload] = queue.presign_outputs(
            principal, job.pk, attempt=1, files=[{"name": "d", "bytes": 6 * MIB}], multipart=True
        )
        fresh = queue.part_urls(
            principal, job.pk, attempt=1, name="d", upload_id=upload["upload_id"], part_numbers=[2]
        )
        urls = [part["url"] for part in upload["parts"] + fresh["parts"]]
        assert len(urls) == 3 and all(
            url.startswith("http://rustfs.internal:9000/") for url in urls
        )
    finally:
        storage.reset_store()


def test_part_size_and_the_first_batch_of_a_huge_output(principal, job):
    [huge] = queue.presign_outputs(
        principal, job.pk, attempt=1, files=[{"name": "d", "bytes": 1024**4}], multipart=True
    )
    assert huge["part_size"] % MIB == 0 and huge["part_size"] > 64 * MIB
    assert huge["part_count"] <= 10_000
    assert huge["part_count"] * huge["part_size"] >= 1024**4
    assert len(huge["parts"]) == queue.FIRST_PARTS


def test_workers_that_do_not_upload_in_parts_get_single_puts(principal, job, small_parts):
    [upload] = queue.presign_outputs(
        principal, job.pk, attempt=1, files=[{"name": "data.parquet", "bytes": 12 * MIB}]
    )
    assert upload["url"] and "upload_id" not in upload
    assert pending(job.attempt_prefix) == []
    with pytest.raises(InvalidRequest, match="one PUT holds at most 5368709120 bytes"):
        queue.presign_outputs(
            principal, job.pk, attempt=1, files=[{"name": "d", "bytes": 5 * 1024**3 + 1}]
        )


def test_output_max_bytes_goes_up_to_the_largest_object(principal, job, admin_actor):
    assert installation.get("output_max_bytes") == 1024**4
    with pytest.raises(InvalidRequest, match="outputs may be at most 1099511627776 bytes"):
        queue.presign_outputs(
            principal,
            job.pk,
            attempt=1,
            files=[{"name": "d", "bytes": 1024**4 + 1}],
            multipart=True,
        )
    installation.update(admin_actor, {"output_max_bytes": 5 * 1024**4})
    with pytest.raises(InvalidRequest, match="output_max_bytes must be at least 1 and at most"):
        installation.update(admin_actor, {"output_max_bytes": 5 * 1024**4 + 1})


# --------------------------------------------------------------------------- part URLs


def test_part_urls(principal, job):
    key, upload_id = start_upload(job, "data.parquet")
    answer = queue.part_urls(
        principal,
        job.pk,
        attempt=1,
        name="data.parquet",
        upload_id=upload_id,
        part_numbers=[3, 1, 3],
    )
    assert answer["expires_in"] == 3600
    assert [part["part_number"] for part in answer["parts"]] == [1, 3]
    etag = put_part(answer["parts"][0]["url"], b"part one")
    [held] = storage.store().list_parts(key, upload_id)
    assert (held.number, held.size, held.etag) == (1, 8, etag.strip('"'))


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"part_numbers": []}, "part_numbers must name 1 to 1000 parts between 1 and 10000"),
        ({"part_numbers": [0]}, "part_numbers must name"),
        ({"part_numbers": [10_001]}, "part_numbers must name"),
        ({"part_numbers": list(range(1, 1002))}, "part_numbers must name"),
        ({"name": "../data.parquet"}, "must be a relative path"),
        ({"upload_id": ""}, "upload_id must be the id presign answered with"),
        ({"upload_id": "u" * 1025}, "upload_id must be"),
    ],
)
def test_part_url_requests_are_checked(principal, job, fields, message):
    request = {"name": "data.parquet", "upload_id": "u", "part_numbers": [1], **fields}
    with pytest.raises(InvalidRequest, match=message):
        queue.part_urls(principal, job.pk, attempt=1, **request)


def test_part_urls_need_the_lease(principal, job):
    with pytest.raises(Conflict) as raised:
        queue.part_urls(
            principal, job.pk, attempt=2, name="data.parquet", upload_id="u", part_numbers=[1]
        )
    assert raised.value.code == "lease_lost"


# --------------------------------------------------------------------------- completion


def test_a_multipart_output_is_completed_byte_for_byte(principal, job, small_parts):
    data = os.urandom(12 * MIB)
    [upload] = queue.presign_outputs(
        principal,
        job.pk,
        attempt=1,
        files=[{"name": "data.parquet", "bytes": len(data)}],
        multipart=True,
    )
    size = upload["part_size"]
    pieces = [data[i : i + size] for i in range(0, len(data), size)]
    fresh = queue.part_urls(
        principal,
        job.pk,
        attempt=1,
        name="data.parquet",
        upload_id=upload["upload_id"],
        part_numbers=[3],
    )["parts"]
    urls = [part["url"] for part in upload["parts"][:2]] + [fresh[0]["url"]]
    etags = [put_part(url, piece) for url, piece in zip(urls, pieces)]
    artifact = reported(job, "data.parquet", data, upload["upload_id"], etags)
    done = complete(principal, job, [artifact])
    assert done.status == JobStatus.SUCCEEDED
    recorded = Artifact.objects.get(job=job)
    assert (recorded.bytes, recorded.sha256) == (len(data), artifact["sha256"])
    assert stored(artifact["key"]) == data
    assert pending(job.attempt_prefix) == []


def test_the_store_refuses_a_corrupted_part(principal, job):
    key, upload_id = start_upload(job, "data.parquet")
    url = storage.store().presign_part(
        key, upload_id, 1, expires=60, audience=storage.Audience.GATEWAY
    )
    with pytest.raises(Exception) as raised:
        put_url(url, b"the bytes that arrived", {"Content-MD5": md5_base64(b"the bytes sent")})
    assert raised.value.code == 400 and b"BadDigest" in raised.value.read()


def _digest_of(*numbers):
    """A change that reports the parts ``numbers`` (1-based) of the three uploaded."""
    return lambda e, etags: e.update(
        part_count=len(numbers), parts_sha256=queue.parts_sha256(etags[n - 1] for n in numbers)
    )


@pytest.mark.parametrize(
    "change,message",
    [
        (_digest_of(1, 2), "The 2 parts of artifact 'data.parquet' hold 8 bytes in all, but it"),
        (
            lambda e, etags: e.update(part_count=4),
            "Part 4 of artifact 'data.parquet' is not in its multipart upload",
        ),
        (_digest_of(3, 2, 1), "The parts the store holds for artifact 'data.parquet' are not the"),
        (lambda e, etags: e.update(parts_sha256="0" * 64), "are not the ones reported"),
        (lambda e, etags: e.update(bytes=11), "hold 10 bytes in all, but it was reported with 11"),
        (lambda e, etags: e.update(upload_id=None), "was uploaded in parts: report its upload_id"),
        (lambda e, etags: e.update(upload_id="u" * 1025), "was uploaded in parts"),
        (lambda e, etags: e.update(part_count=None), r"its part_count \(1 to 10000\)"),
        (lambda e, etags: e.update(part_count=0), "was uploaded in parts"),
        (lambda e, etags: e.update(part_count=True), "was uploaded in parts"),
        (lambda e, etags: e.update(part_count=10_001), "was uploaded in parts"),
        (lambda e, etags: e.update(parts_sha256=None), "and parts_sha256"),
        (lambda e, etags: e.update(parts_sha256="ABC"), "was uploaded in parts"),
    ],
)
def test_completion_failures_record_nothing(principal, job, change, message):
    data = b"0123456789"
    key, upload_id = start_upload(job, "data.parquet")
    etags = upload_parts(key, upload_id, [data[:4], data[4:8], data[8:]])
    entry = reported(job, "data.parquet", data, upload_id, etags)
    change(entry, etags)
    with pytest.raises(InvalidRequest, match=message):
        complete(principal, job, [entry])
    assert Job.objects.get(pk=job.pk).status == JobStatus.RUNNING
    assert not Artifact.objects.filter(job=job).exists()
    assert pending(job.attempt_prefix) == [key]


def test_parts_that_are_not_cut_evenly_are_refused(principal, job):
    data = b"0123456789"
    key, upload_id = start_upload(job, "data.parquet")
    etags = upload_parts(key, upload_id, [data[:4], data[4:9], data[9:]])
    with pytest.raises(InvalidRequest, match="are not cut evenly"):
        complete(principal, job, [reported(job, "data.parquet", data, upload_id, etags)])
    key, upload_id = start_upload(job, "bad_rows.parquet")
    etags = upload_parts(key, upload_id, [data[:4], data[4:]])
    with pytest.raises(InvalidRequest, match="are not cut evenly"):
        complete(principal, job, [reported(job, "bad_rows.parquet", data, upload_id, etags)])


def test_an_upload_that_is_no_longer_pending_needs_its_object(principal, job):
    data = b"PAR1"
    key, upload_id = start_upload(job, "data.parquet")
    etags = upload_parts(key, upload_id, [data])
    storage.store().abort_multipart(key, upload_id)
    with pytest.raises(InvalidRequest, match="the store has nothing at"):
        complete(principal, job, [reported(job, "data.parquet", data, upload_id, etags)])
    with pytest.raises(InvalidRequest, match="the store has nothing at"):
        complete(principal, job, [reported(job, "data.parquet", data, "not-an-upload-id", etags)])


def test_one_invalid_artifact_completes_no_upload(principal, job):
    data = b"PAR1"
    key, upload_id = start_upload(job, "data.parquet")
    good = reported(job, "data.parquet", data, upload_id, upload_parts(key, upload_id, [data]))
    other_key, other_id = start_upload(job, "bad_rows.parquet")
    upload_parts(other_key, other_id, [data])
    bad = reported(job, "bad_rows.parquet", data, other_id, ['"' + "0" * 32 + '"'])
    with pytest.raises(InvalidRequest, match="are not the ones reported"):
        complete(principal, job, [good, bad])
    assert pending(job.attempt_prefix) == sorted([key, other_key])
    assert storage.store().head(key) is None


def test_every_object_is_checked_before_any_upload_is_completed(principal, job):
    data = b"PAR1"
    key, upload_id = start_upload(job, "data.parquet")
    entry = reported(job, "data.parquet", data, upload_id, upload_parts(key, upload_id, [data]))
    missing = {
        "kind": "manifest",
        "name": "manifest.json",
        "key": job.attempt_prefix + "manifest.json",
        "bytes": 2,
    }
    with pytest.raises(InvalidRequest, match="the store has nothing at"):
        complete(principal, job, [entry, missing])
    assert pending(job.attempt_prefix) == [key] and storage.store().head(key) is None


def _wrong_size_once_completed(monkeypatch, key):
    real_head = storage.Bucket.head
    monkeypatch.setattr(
        storage.Bucket,
        "head",
        lambda self, k: storage.ObjectInfo(size=1, etag="e") if k == key else real_head(self, k),
    )


def _artifact_records_fail(monkeypatch, key):
    def refuse(**fields):
        raise RuntimeError("the database went away")

    monkeypatch.setattr(Artifact.objects, "create", refuse)


@pytest.mark.parametrize(
    "failure, error",
    [(_wrong_size_once_completed, InvalidRequest), (_artifact_records_fail, RuntimeError)],
)
def test_a_completion_refused_after_completing_uploads_deletes_their_objects(
    principal, job, monkeypatch, failure, error
):
    data = b"PAR1"
    key, upload_id = start_upload(job, "data.parquet")
    entry = reported(job, "data.parquet", data, upload_id, upload_parts(key, upload_id, [data]))
    failure(monkeypatch, key)
    with pytest.raises(error):
        complete(principal, job, [entry])
    monkeypatch.undo()
    assert storage.store().head(key) is None, "nothing is left that no artifact records"
    assert pending(job.attempt_prefix) == [] and not Artifact.objects.filter(job=job).exists()


def test_objects_of_a_refused_completion_that_cannot_be_deleted_are_logged(
    principal, job, monkeypatch, caplog
):
    data = b"PAR1"
    key, upload_id = start_upload(job, "data.parquet")
    entry = reported(job, "data.parquet", data, upload_id, upload_parts(key, upload_id, [data]))
    _wrong_size_once_completed(monkeypatch, key)

    def refuse_delete(self, key):
        raise StoreUnavailable(
            f"The object store refused or failed DELETE of {key} (AccessDenied)."
        )

    monkeypatch.setattr(storage.Bucket, "delete", refuse_delete)
    caplog.set_level(logging.WARNING, logger="forklift_web.services.queue")
    with pytest.raises(InvalidRequest, match="the store has 1 bytes"):
        complete(principal, job, [entry])
    assert "An object of a refused completion was not deleted" in caplog.text


def test_a_store_error_while_completing_records_nothing_and_a_retry_completes(
    principal, job, monkeypatch
):
    first, second = b"PAR1 data", b"PAR1 bad rows"
    entries = []
    for name, data in (("data.parquet", first), ("bad_rows.parquet", second)):
        key, upload_id = start_upload(job, name)
        entries.append(reported(job, name, data, upload_id, upload_parts(key, upload_id, [data])))
    real_complete = storage.Bucket.complete_multipart
    calls = []

    def fail_the_second(self, key, upload_id, parts):
        calls.append(key)
        if len(calls) == 2:
            raise StoreUnavailable("The object store refused or failed completing (timeout).")
        real_complete(self, key, upload_id, parts)

    monkeypatch.setattr(storage.Bucket, "complete_multipart", fail_the_second)
    with pytest.raises(StoreUnavailable):
        complete(principal, job, entries)
    assert not Artifact.objects.filter(job=job).exists()
    assert pending(job.attempt_prefix) == [entries[1]["key"]]  # the first one was completed
    monkeypatch.setattr(storage.Bucket, "complete_multipart", real_complete)
    done = complete(principal, job, entries)
    assert done.status == JobStatus.SUCCEEDED and Artifact.objects.filter(job=job).count() == 2
    assert stored(entries[0]["key"]) == first and stored(entries[1]["key"]) == second


def test_a_store_error_while_listing_the_parts_surfaces(principal, job, monkeypatch):
    def unavailable(self, key, upload_id):
        raise StoreUnavailable("The object store refused or failed listing the parts (timeout).")

    monkeypatch.setattr(storage.Bucket, "list_parts", unavailable)
    entry = reported(job, "data.parquet", b"x", "u", ['"e"'])
    with pytest.raises(StoreUnavailable):
        complete(principal, job, [entry])
    assert Job.objects.get(pk=job.pk).status == JobStatus.RUNNING


# --------------------------------------------------------------------------- aborting


@pytest.mark.parametrize(
    "status,error",
    [
        ("failed", {"code": "INTERNAL", "message": "An upload failed.", "retryable": True}),
        ("cancelled", {"code": "CANCELLED", "message": "Cancelled.", "retryable": False}),
    ],
)
def test_an_attempt_that_fails_or_is_cancelled_aborts_its_uploads(principal, job, status, error):
    key, upload_id = start_upload(job, "data.parquet")
    upload_parts(key, upload_id, [b"half of it"])
    done = complete(principal, job, [], status=status, error=error)
    assert done.status == status and pending(job.attempt_prefix) == []


def test_a_completed_attempt_aborts_the_uploads_it_left_behind(principal, job):
    data = b"PAR1"
    start_upload(job, "data.parquet")  # a presign whose answer was lost, say
    key, upload_id = start_upload(job, "data.parquet")
    entry = reported(job, "data.parquet", data, upload_id, upload_parts(key, upload_id, [data]))
    assert complete(principal, job, [entry]).status == JobStatus.SUCCEEDED
    assert pending(job.attempt_prefix) == [] and stored(key) == data


@pytest.mark.parametrize("max_attempts,status", [(3, JobStatus.QUEUED), (1, JobStatus.FAILED)])
def test_a_lost_lease_aborts_the_attempts_uploads(principal, job, max_attempts, status):
    start_upload(job, "data.parquet")
    Job.objects.filter(pk=job.pk).update(
        lease_expires_at=timezone.now() - timedelta(seconds=1), max_attempts=max_attempts
    )
    queue.requeue_expired_leases()
    assert Job.objects.get(pk=job.pk).status == status
    assert pending(job.attempt_prefix) == []


def test_aborting_is_best_effort(principal, job, monkeypatch, caplog):
    start_upload(job, "data.parquet")

    def unavailable(self, prefix):
        raise StoreUnavailable(f"The object store refused or failed listing under {prefix}.")

    monkeypatch.setattr(storage.Bucket, "list_multipart_uploads", unavailable)
    caplog.set_level(logging.WARNING, logger="forklift_web.services.queue")
    assert complete(principal, job, []).status == JobStatus.SUCCEEDED
    assert "Pending output uploads were not aborted" in caplog.text


# --------------------------------------------------------------------------- the sweeper


@pytest.fixture
def no_pending_outputs():
    abort_pending_outputs()


def test_the_sweeper_aborts_stale_uploads_no_running_attempt_will_complete(
    principal, job, no_pending_outputs
):
    running_key, _ = start_upload(job, "data.parquet")
    finished = Job.objects.get(status=JobStatus.SUCCEEDED)  # the world's finished job
    stale = {
        f"jobs/{finished.pk}/attempt-1/data.parquet",  # its attempt ended
        f"jobs/{job.pk}/attempt-0/data.parquet",  # an attempt before the running one
        "jobs/not-a-job/data.parquet",
        "jobs/00000000-0000-0000-0000-000000000000/attempt-1/data.parquet",  # no such job
    }
    for key in stale:
        storage.store().create_multipart(key)
    later = timezone.now() + timedelta(hours=2)

    young = retention.sweep(SYSTEM, dry_run=True)
    assert young.output_uploads == 0
    counted = retention.sweep(SYSTEM, dry_run=True, now=later)
    assert counted.output_uploads == 4 and len(pending("jobs/")) == 5
    report = retention.sweep(SYSTEM, now=later)
    assert report.output_uploads == 4 and report.errors == []
    assert pending("jobs/") == [running_key]
    purged = AuditLog.objects.filter(
        action="retention.purge", details__kind="pending output upload"
    )
    assert {entry.details["key"] for entry in purged} == stale
    assert {entry.object_id for entry in purged} == {str(finished.pk), str(job.pk), ""}


def test_sweeper_store_errors_are_reported(principal, job, no_pending_outputs, monkeypatch):
    start_upload(job, "data.parquet")
    Job.objects.filter(pk=job.pk).update(status=JobStatus.FAILED)
    later = timezone.now() + timedelta(hours=2)

    def refuse_abort(self, key, upload_id):
        raise StoreUnavailable("The object store refused or failed aborting (AccessDenied).")

    monkeypatch.setattr(storage.Bucket, "abort_multipart", refuse_abort)
    report = retention.sweep(SYSTEM, now=later)
    assert report.output_uploads == 1
    assert report.errors == [
        f"pending output upload {job.attempt_prefix}data.parquet: The object store refused or "
        "failed aborting (AccessDenied)."
    ]

    def refuse_list(self, prefix):
        raise StoreUnavailable("The object store refused or failed listing (AccessDenied).")

    monkeypatch.setattr(storage.Bucket, "list_multipart_uploads", refuse_list)
    report = retention.sweep(SYSTEM, now=later)
    assert report.errors == [
        "pending output uploads: The object store refused or failed listing (AccessDenied)."
    ]


# --------------------------------------------------------------------------- the store client


class _Failing:
    def __init__(self, error):
        self.error = error

    def get_paginator(self, name):
        raise self.error


def _bucket() -> storage.Bucket:
    return storage.Bucket(
        bucket="b",
        region="us-east-1",
        addressing_style="path",
        endpoints={},
        credentials={purpose: ("k", "s") for purpose in storage.Purpose},
    )


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "nope"}}, "Op")


@pytest.mark.parametrize(
    "call,operation",
    [
        (lambda b: b.list_parts("k", "u"), "listing the parts of k"),
        (lambda b: b.list_multipart_uploads("jobs/"), "listing the multipart uploads of jobs/"),
    ],
)
@pytest.mark.parametrize(
    "error,reason",
    [
        (_client_error("AccessDenied"), "AccessDenied: nope"),
        (EndpointConnectionError(endpoint_url="http://x"), "EndpointConnectionError"),
    ],
)
def test_listing_errors_name_the_operation_and_reason(monkeypatch, call, operation, error, reason):
    bucket = _bucket()
    monkeypatch.setattr(bucket, "client", lambda purpose, audience=None: _Failing(error))
    with pytest.raises(StoreUnavailable) as raised:
        call(bucket)
    assert operation in raised.value.message and reason in raised.value.message


@pytest.mark.parametrize("code", ["NoSuchUpload", "InvalidArgument"])
def test_parts_of_an_upload_that_is_not_pending(monkeypatch, code):
    bucket = _bucket()
    monkeypatch.setattr(bucket, "client", lambda p, a=None: _Failing(_client_error(code)))
    assert bucket.list_parts("k", "u") is None


# --------------------------------------------------------------------------- over HTTP


def test_the_internal_api_uploads_an_output_in_parts(worker_token, internal, small_parts):
    _, raw = worker_token
    worker = internal(raw)
    World.build()
    lease = {"worker_id": "w-1", "lanes": ["batch"], "spec_versions": [1]}
    job_id = worker.post("/internal/v1/leases", lease).json()["job_id"]
    data = os.urandom(5 * MIB) + b"the last part"
    signed = worker.post(
        f"/internal/v1/jobs/{job_id}/presign",
        {"attempt": 1, "files": [{"name": "data.parquet", "bytes": len(data)}], "multipart": True},
    )
    [upload] = signed.json()["uploads"]
    assert set(upload) == {
        "name",
        "key",
        "url",
        "method",
        "headers",
        "expires_in",
        "upload_id",
        "part_size",
        "part_count",
        "parts",
    }
    assert upload["url"] is None and upload["part_count"] == 2
    parts = worker.post(
        f"/internal/v1/jobs/{job_id}/parts",
        {
            "attempt": 1,
            "name": "data.parquet",
            "upload_id": upload["upload_id"],
            "part_numbers": [2],
        },
    )
    assert parts.status_code == 200 and parts.json()["expires_in"] == 3600
    etags = [
        put_part(upload["parts"][0]["url"], data[: 5 * MIB]),
        put_part(parts.json()["parts"][0]["url"], data[5 * MIB :]),
    ]
    artifact = {
        "kind": "data",
        "name": "data.parquet",
        "key": upload["key"],
        "bytes": len(data),
        "upload_id": upload["upload_id"],
        "part_count": 2,
        "parts_sha256": queue.parts_sha256(etags),
    }
    done = worker.post(
        f"/internal/v1/jobs/{job_id}/complete",
        {"attempt": 1, "result": job_result(job_id, [artifact]), "artifacts": [artifact]},
    )
    assert done.json() == {"job_id": job_id, "status": "succeeded"}
    assert stored(upload["key"]) == data
    lost = worker.post(
        f"/internal/v1/jobs/{job_id}/parts",
        {"attempt": 1, "name": "data.parquet", "upload_id": "u", "part_numbers": [1]},
    )
    assert lost.status_code == 409 and lost.json()["code"] == "lease_lost"


def test_a_completion_of_many_parts_is_accepted(worker_token, internal, monkeypatch):
    """Four artifacts of 10,000 parts each: listed part by part, the report would be 2.8 MB,
    above the 2.5 MiB Django reads; reported by digest, it stays small."""
    _, raw = worker_token
    worker = internal(raw)
    World.build()
    lease = {"worker_id": "w-1", "lanes": ["batch"], "spec_versions": [1]}
    job_id = worker.post("/internal/v1/leases", lease).json()["job_id"]
    count, size = 10_000, 5 * MIB
    etags = [f"{number:032x}" for number in range(1, count + 1)]
    held = [storage.PartInfo(number=n, size=size, etag=e) for n, e in enumerate(etags, start=1)]
    completed = []
    monkeypatch.setattr(storage.Bucket, "list_parts", lambda self, key, upload_id: held)
    monkeypatch.setattr(
        storage.Bucket,
        "complete_multipart",
        lambda self, key, upload_id, parts: completed.append((key, len(parts))),
    )
    monkeypatch.setattr(
        storage.Bucket, "head", lambda self, key: storage.ObjectInfo(size=count * size, etag="e")
    )
    artifacts = [
        {
            "kind": "data",
            "name": f"part-{i}.parquet",
            "key": f"jobs/{job_id}/attempt-1/part-{i}.parquet",
            "bytes": count * size,
            "upload_id": f"upload-{i}",
            "part_count": count,
            "parts_sha256": queue.parts_sha256(etags),
        }
        for i in range(4)
    ]
    report = {"attempt": 1, "result": job_result(job_id, artifacts), "artifacts": artifacts}
    done = worker.post(f"/internal/v1/jobs/{job_id}/complete", report)
    assert done.status_code == 200 and done.json()["status"] == "succeeded"
    assert completed == [(artifact["key"], count) for artifact in artifacts]


def test_the_largest_completion_report_fits_in_a_request(settings):
    artifact = {
        "kind": "metadata",
        "name": "n" * 512,
        "key": f"jobs/{'0' * 36}/attempt-999999999/{'n' * 512}",
        "bytes": 5 * 1024**4,
        "sha256": "f" * 64,
        "rows": 10**18,
        "upload_id": "u" * 1024,
        "part_count": 10_000,
        "parts_sha256": "f" * 64,
    }
    report = {
        "attempt": 10**9,
        "result": {"warnings": ["w" * (queue.MAX_RESULT_BYTES - 20)]},
        "artifacts": [artifact] * queue.MAX_FILES,
    }
    assert len(json.dumps(report)) < settings.DATA_UPLOAD_MAX_MEMORY_SIZE
