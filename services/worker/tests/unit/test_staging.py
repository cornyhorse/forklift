"""Staging inputs through presigned GETs, with size and checksum checks."""

from __future__ import annotations

import hashlib
from collections import namedtuple

import pytest

from forklift_worker import staging
from forklift_worker.retry import Backoff, Interrupted
from forklift_worker.spec import StagedInput
from forklift_worker.staging import StagingError, stage_input
from forklift_worker.transport import HttpClient

DATA = b"id,name\n1,a\n2,b\n"


def item_for(gateway, key: str = "uploads/u/people.csv", data: bytes = DATA, **changes):
    location = gateway.put_object(key, data, changes.pop("store_etag", None))
    values = {
        "url": location["url"],
        "host": gateway.host,
        "size": location["size"],
        "etag": location["etag"],
        "path": "in/people.csv",
    }
    values.update(changes)
    return StagedInput(**values)


def stage(item, workdir, **kwargs):
    defaults = {
        "stopped": lambda: False,
        "wait": lambda seconds: False,
        "backoff": Backoff(0.001, 0.001),
    }
    defaults.update(kwargs)
    return stage_input(HttpClient(timeout=5, user_agent="test"), item, workdir, **defaults)


def failure(item, workdir, **kwargs) -> StagingError:
    with pytest.raises(StagingError) as caught:
        stage(item, workdir, **kwargs)
    return caught.value


def respond(status: int, body: bytes, headers: dict):
    def fault(handler) -> None:
        handler._send(status, body, headers)

    return fault


def test_an_input_is_staged_and_its_progress_reported(gateway, tmp_path):
    seen = []
    path = stage(item_for(gateway), tmp_path, on_progress=seen.append)
    assert path.read_bytes() == DATA
    assert path.stat().st_mode & 0o777 == 0o600
    assert seen[-1] == len(DATA)


def test_a_multipart_etag_is_compared_but_not_hashed(gateway, tmp_path):
    item = item_for(gateway, store_etag="0123456789abcdef0123456789abcdef-3")
    assert stage(item, tmp_path).read_bytes() == DATA
    weak = item_for(
        gateway,
        etag='W/"0123456789abcdef0123456789abcdef-3"',
        store_etag="0123456789abcdef0123456789abcdef-3",
    )
    assert stage(weak, tmp_path).read_bytes() == DATA
    assert stage(item_for(gateway, etag=None), tmp_path).read_bytes() == DATA


def test_a_checksum_mismatch_is_retried_then_reported(gateway, tmp_path):
    wrong = hashlib.md5(b"other").hexdigest()
    item = item_for(gateway, store_etag=wrong)
    error = failure(item, tmp_path)
    assert "does not match its checksum" in error.message and error.retryable
    assert len(gateway.calls("get")) == 3


def chunked(body: bytes):
    def fault(handler) -> None:
        handler.send_response(200)
        handler.send_header("Transfer-Encoding", "chunked")
        handler.end_headers()
        handler.wfile.write(b"%x\r\n%s\r\n0\r\n\r\n" % (len(body), body))

    return fault


def test_a_body_longer_than_expected_is_refused(gateway, tmp_path):
    item = item_for(gateway)
    gateway.fail("get", chunked(DATA + b"more"))
    error = failure(item, tmp_path)
    assert error.code == "INPUT_UNREADABLE"
    assert "more than the" in error.message and not error.retryable


def test_a_short_body_without_a_length_is_retried(gateway, tmp_path):
    item = item_for(gateway)
    gateway.fail("get", chunked(b"id,n"), times=3)
    error = failure(item, tmp_path)
    assert f"ended after 4 of {len(DATA)} bytes" in error.message and error.retryable


@pytest.mark.parametrize(
    "status, phrase, retryable",
    [
        (403, "refused the input's presigned URL (HTTP 403: injected", False),
        (500, "failed with HTTP 500: injected HTTP 500", True),
        (400, "failed with HTTP 400", False),
    ],
)
def test_status_errors(gateway, tmp_path, status, phrase, retryable):
    gateway.fail("get", status, times=3)
    error = failure(item_for(gateway), tmp_path)
    assert phrase in error.message
    assert error.retryable is retryable


def test_the_wrong_length_header_is_refused(gateway, tmp_path):
    gateway.fail("get", respond(200, DATA[:-1], {}))
    error = failure(item_for(gateway), tmp_path)
    assert f"reports the input as {len(DATA) - 1} bytes" in error.message


def test_staging_stops_when_the_job_does(gateway, tmp_path):
    with pytest.raises(Interrupted):
        stage(item_for(gateway), tmp_path, stopped=lambda: True)
    gateway.fail("get", 503)
    with pytest.raises(Interrupted):
        stage(item_for(gateway), tmp_path, wait=lambda seconds: True)


def test_scratch_without_room_for_the_input(gateway, tmp_path, monkeypatch):
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(staging.shutil, "disk_usage", lambda path: usage(100, 95, 5))
    error = failure(item_for(gateway), tmp_path)
    assert error.code == "LIMIT_EXCEEDED"
    assert "more than the 5 bytes free" in error.message


def test_a_scratch_write_failure(gateway, tmp_path):
    (tmp_path / "in" / "people.csv").mkdir(parents=True)
    error = failure(item_for(gateway), tmp_path)
    assert error.code == "INTERNAL"
    assert "Writing the staged input into scratch failed" in error.message


def test_an_etag_header_that_differs_is_refused(gateway, tmp_path):
    item = item_for(gateway, etag='"' + hashlib.md5(b"other").hexdigest() + '"')
    error = failure(item, tmp_path)
    assert "ETag in the object store differs" in error.message and not error.retryable
