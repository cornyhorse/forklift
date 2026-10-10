"""Uploading artifacts through presigned PUTs."""

from __future__ import annotations

import pytest
from fake_gateway import SIGNATURE

from forklift_worker.gateway import UploadTarget
from forklift_worker.results import collect_artifacts
from forklift_worker.retry import Backoff, Interrupted
from forklift_worker.transport import HttpClient
from forklift_worker.uploads import UploadError, upload_artifact


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
