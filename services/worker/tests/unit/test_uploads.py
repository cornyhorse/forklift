"""Uploading artifacts through presigned PUTs, in one piece or part by part."""

from __future__ import annotations

import base64
import hashlib
import os
import time

import pytest
from fake_gateway import SIGNATURE

from forklift_worker import uploads
from forklift_worker.gateway import MultipartTarget, PartUrls, UploadTarget
from forklift_worker.results import collect_artifacts
from forklift_worker.retry import Backoff, Interrupted
from forklift_worker.transport import HttpClient
from forklift_worker.uploads import UploadError, upload_artifact, upload_multipart, usable


@pytest.fixture
def artifact(tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "data.parquet").write_bytes(b"PAR1")
    result = {"artifacts": [{"kind": "data", "path": "out/data.parquet", "rows": 1}]}
    (item,) = collect_artifacts(result, tmp_path, ["out"])
    return item


def upload(url, item, **kwargs):
    target = UploadTarget("data.parquet", "k", url, "PUT", {})
    defaults = {
        "stopped": lambda: False,
        "wait": lambda s: False,
        "backoff": Backoff(0.001, 0.001),
    }
    defaults.update(kwargs)
    upload_artifact(HttpClient(timeout=5, user_agent="t"), target, item, **defaults)


def test_an_upload(gateway, artifact):
    upload(f"{gateway.url}/store/k?{SIGNATURE}", artifact)
    assert gateway.objects["k"] == b"PAR1"


def test_an_upload_that_keeps_failing(gateway, artifact):
    gateway.fail("put", 503, times=5)
    with pytest.raises(UploadError) as caught:
        upload(f"{gateway.url}/store/k", artifact)
    assert "(HTTP 503: injected HTTP 503) after 5 attempts" in caught.value.message
    assert caught.value.retryable


def test_an_upload_the_store_cannot_be_reached_for(artifact):
    with pytest.raises(UploadError, match="failed after 2 attempts") as caught:
        upload("http://127.0.0.1:9/k", artifact, attempts=2)
    assert caught.value.retryable


def test_a_corrupted_upload_is_refused_by_the_store(gateway, artifact):
    (artifact.root / artifact.relative).write_bytes(b"PAR2")  # same size, other bytes
    with pytest.raises(UploadError, match="BadDigest") as caught:
        upload(f"{gateway.url}/store/k", artifact)
    assert not caught.value.retryable


@pytest.mark.parametrize(
    "change, phrase",
    [
        (lambda path: path.unlink(), "disappeared after it was hashed"),
        (lambda path: path.write_bytes(b"longer"), "changed after it was hashed"),
    ],
)
def test_an_artifact_that_changed_after_hashing(gateway, artifact, change, phrase):
    change(artifact.root / artifact.relative)
    with pytest.raises(UploadError, match=phrase) as caught:
        upload(f"{gateway.url}/store/k", artifact)
    assert not caught.value.retryable


def test_an_upload_stops_with_its_job(gateway, artifact):
    with pytest.raises(Interrupted):
        upload(f"{gateway.url}/store/k", artifact, stopped=lambda: True)


# --------------------------------------------------------------------------- in parts

CONTENT = b"0123456789"  # three parts of 4, 4 and 2 bytes


@pytest.fixture
def large(tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "data.parquet").write_bytes(CONTENT)
    result = {"artifacts": [{"kind": "data", "path": "out/data.parquet", "rows": 1}]}
    (item,) = collect_artifacts(result, tmp_path, ["out"])
    return item


class Upload:
    """A multipart upload in the fake store, and the part URLs asked for."""

    def __init__(self, gateway, first=(1,), part_count=3, **batch):
        self.gateway = gateway
        self.upload_id = gateway.start_multipart("k")
        self.asked: list[list[int]] = []
        self.target = MultipartTarget(
            "data.parquet", "k", self.upload_id, 4, part_count, self.urls(first, **batch)
        )

    def urls(self, numbers, **batch) -> PartUrls:
        return PartUrls(
            {n: self.gateway.part_url("k", self.upload_id, n) for n in numbers}, **batch
        )

    def fresh(self, numbers):
        self.asked.append(list(numbers))
        return self.urls(numbers)

    def stored(self) -> bytes:
        held = self.gateway.multipart[self.upload_id]["parts"]
        return b"".join(held[number] for number in sorted(held))


def upload_in_parts(upload: Upload, item, **kwargs):
    defaults = {
        "fresh_urls": upload.fresh,
        "stopped": lambda: False,
        "wait": lambda s: False,
        "backoff": Backoff(0.001, 0.001),
    }
    defaults.update(kwargs)
    return upload_multipart(HttpClient(timeout=5, user_agent="t"), upload.target, item, **defaults)


def test_an_upload_in_parts(gateway, large):
    upload, progress = Upload(gateway), []
    parts = upload_in_parts(upload, large, on_progress=progress.append)

    assert [part["part_number"] for part in parts] == [1, 2, 3]
    pieces = [CONTENT[:4], CONTENT[4:8], CONTENT[8:]]
    assert [part["etag"] for part in parts] == [f'"{hashlib.md5(p).hexdigest()}"' for p in pieces]
    assert upload.stored() == CONTENT and progress == [4, 8, 10]
    assert upload.asked == [[2, 3]], "the URLs presign did not give are asked for"
    for put, piece in zip(gateway.calls("part"), pieces):
        assert put["headers"]["content-length"] == str(len(piece))
        assert (
            put["headers"]["content-md5"] == base64.b64encode(hashlib.md5(piece).digest()).decode()
        )


def test_a_failed_part_is_tried_again_with_a_fresh_url(gateway, large):
    upload = Upload(gateway, first=(1, 2, 3))
    gateway.fail("part", 503)
    upload_in_parts(upload, large)

    assert upload.stored() == CONTENT and upload.asked == [[1, 2, 3]]


def test_an_expired_part_url_is_replaced(gateway, large):
    upload = Upload(gateway, first=(1, 2, 3))
    gateway.expire_part_urls()
    upload_in_parts(upload, large)

    assert upload.stored() == CONTENT and upload.asked == [[1, 2, 3]]
    assert len(gateway.calls("part")) == 4  # one 403, then three parts


def test_part_urls_are_replaced_before_they_grow_old(gateway, large):
    upload = Upload(gateway, first=(1, 2, 3), expires_in=100, received_at=time.monotonic() - 80)
    upload_in_parts(upload, large)

    assert upload.asked == [[1, 2, 3]] and len(gateway.calls("part")) == 3


def test_a_url_is_used_in_the_first_three_quarters_of_its_life():
    assert usable(100.0, None, 10**9)
    assert usable(100.0, 40, 129.9) and not usable(100.0, 40, 130.0)


@pytest.mark.parametrize(
    "fault, phrase, retryable",
    [
        (503, "refused the upload of part 1 of 3 of the artifact data.parquet (HTTP 503", True),
        (403, "(HTTP 403: injected HTTP 403) after 5 attempts", False),
        (400, "(HTTP 400: injected HTTP 400).", False),
    ],
)
def test_a_part_that_keeps_failing(gateway, large, fault, phrase, retryable):
    upload = Upload(gateway)
    gateway.fail("part", fault, times=5)
    with pytest.raises(UploadError) as caught:
        upload_in_parts(upload, large)
    assert phrase in caught.value.message and caught.value.retryable is retryable


def test_a_part_the_store_cannot_be_reached_for(gateway, large):
    upload = Upload(gateway)
    upload.urls = lambda numbers, **batch: PartUrls({n: "http://127.0.0.1:9/k" for n in numbers})
    upload.target = MultipartTarget("data.parquet", "k", "u", 4, 3, upload.urls([1]))
    with pytest.raises(UploadError) as caught:
        upload_in_parts(upload, large, attempts=2)
    assert caught.value.message.startswith(
        "Uploading part 1 of 3 of the artifact data.parquet to 127.0.0.1:9 failed after 2 attempts"
    )
    assert caught.value.retryable


def test_a_part_corrupted_on_the_way_is_refused_by_the_store(gateway, large, monkeypatch):
    real_read = uploads._PartBody.read

    def corrupting_read(self, size=-1):
        chunk = real_read(self, size)
        return b"X" + chunk[1:] if chunk[:1] == b"4" else chunk

    monkeypatch.setattr(uploads._PartBody, "read", corrupting_read)
    with pytest.raises(UploadError, match="part 2 of 3 .*BadDigest") as caught:
        upload_in_parts(Upload(gateway), large)
    assert not caught.value.retryable


def test_a_part_answered_without_an_etag(gateway, large):
    gateway.fail("part", lambda handler: handler._send(200))
    with pytest.raises(UploadError) as caught:
        upload_in_parts(Upload(gateway), large)
    assert caught.value.message == (
        "The artifact data.parquet cannot be uploaded: the store answered part 1 without an ETag."
    )


def test_an_upload_in_parts_stops_between_parts(gateway, large):
    with pytest.raises(Interrupted):
        upload_in_parts(Upload(gateway), large, stopped=lambda: bool(gateway.calls("part")))
    assert len(gateway.calls("part")) == 1


def test_parts_are_read_in_bounded_chunks(gateway, tmp_path, monkeypatch):
    content = os.urandom(64 * 1024 + 5)
    (tmp_path / "data.parquet").write_bytes(content)
    (item,) = collect_artifacts(
        {"artifacts": [{"kind": "data", "path": "data.parquet"}]}, tmp_path, []
    )
    reads, real_pread = [], os.pread

    def recording_pread(descriptor, size, offset):
        reads.append(size)
        return real_pread(descriptor, size, offset)

    monkeypatch.setattr(uploads, "READ_CHUNK", 1024)
    monkeypatch.setattr(uploads.os, "pread", recording_pread)
    upload = Upload(gateway, part_count=3)
    upload.target = MultipartTarget(
        "data.parquet", "k", upload.upload_id, 32 * 1024, 3, upload.urls([1])
    )
    upload_in_parts(upload, item)

    assert upload.stored() == content
    assert max(reads) <= 1024 and sum(reads) >= 2 * len(content)  # hashed, then sent


def test_a_multipart_artifact_that_changed_after_hashing(gateway, large):
    (large.root / large.relative).write_bytes(b"longer than it was")
    with pytest.raises(UploadError, match="changed after it was hashed") as caught:
        upload_in_parts(Upload(gateway), large)
    assert not caught.value.retryable
    (large.root / large.relative).unlink()
    with pytest.raises(UploadError, match="disappeared after it was hashed"):
        upload_in_parts(Upload(gateway), large)


def test_an_artifact_that_shrank_while_its_parts_went_up(gateway, large, monkeypatch):
    monkeypatch.setattr(uploads.os, "pread", lambda descriptor, size, offset: b"")
    with pytest.raises(UploadError) as caught:
        upload_in_parts(Upload(gateway), large)
    assert caught.value.message == (
        "The artifact data.parquet cannot be uploaded: the artifact 'data.parquet' changed "
        "after it was hashed."
    )


def test_a_multipart_upload_that_does_not_fit_the_artifact(gateway, large):
    with pytest.raises(UploadError, match="has 2 parts of 4 bytes, which does not fit its 10"):
        upload_in_parts(Upload(gateway, part_count=2), large)


def test_the_digest_complete_reports_for_the_parts():
    expected = hashlib.sha256(b"etag-1\netag-2\n").hexdigest()
    assert uploads.parts_sha256(['"etag-1"', "etag-2"]) == expected
