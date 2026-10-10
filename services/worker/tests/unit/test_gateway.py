"""The internal API client: requests, status codes and malformed answers."""

from __future__ import annotations

import json

import pytest

from forklift_worker.gateway import (
    GatewayAuthError,
    GatewayClient,
    GatewayRejected,
    GatewayUnavailable,
    LeaseLost,
)
from forklift_worker.results import synthesized
from forklift_worker.transport import HttpClient


@pytest.fixture
def client(gateway, tmp_path):
    token = tmp_path / "token"
    token.write_text(f"  {gateway.token}\n")
    return GatewayClient(
        HttpClient(timeout=5, user_agent="test"),
        gateway.url + "/internal/v1",
        token,
        worker_id="w1",
        lanes=["batch"],
        engine_version="0.2.0",
    )


def answer(body, status: int = 200):
    """A fault that answers with ``body`` (JSON unless bytes)."""

    def respond(handler) -> None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        handler._send(status, data, {"Content-Type": "application/json"})

    return respond


def test_a_lease_and_its_job_calls(gateway, client):
    job = gateway.enqueue(job_id="a/b c")
    lease = client.lease()
    assert lease.job_id == "a/b c" and lease.attempt == 1 and lease.spec["job_id"] == "a/b c"
    assert lease.lease_seconds == 30.0 and lease.stage_max_bytes == gateway.stage_max_bytes
    reply = client.heartbeat("a/b c", 1, {"rows_read": 1})
    assert reply.lease_seconds == 30.0 and reply.cancel is False
    assert job.heartbeats == [{"rows_read": 1}]
    assert gateway.calls("heartbeat")[0]["path"] == "/internal/v1/jobs/a%2Fb%20c/heartbeat"
    targets = client.presign("a/b c", 1, [{"name": "data.parquet", "bytes": 3}])
    assert targets["data.parquet"].key == "jobs/a/b c/attempt-1/data.parquet"
    assert targets["data.parquet"].headers == {"Content-Type": "application/octet-stream"}
    result = synthesized("a/b c", "succeeded", None)
    client.complete("a/b c", 1, result, [{"name": "data.parquet"}])
    assert job.completed["result"] == result
    assert client.lease() is None


@pytest.mark.parametrize(
    "status, error, phrase",
    [
        (401, GatewayAuthError, "refused the worker token (HTTP 401: injected HTTP 401)"),
        (403, GatewayAuthError, "HTTP 403"),
        (404, GatewayRejected, "--gateway must point at the gateway's internal port"),
        (409, GatewayRejected, "HTTP 409"),
        (422, GatewayRejected, "refused /leases (HTTP 422"),
        (503, GatewayUnavailable, "answered HTTP 503"),
        (429, GatewayUnavailable, "answered HTTP 429"),
        ("drop", GatewayUnavailable, "could not be reached"),
    ],
)
def test_lease_failures(gateway, client, status, error, phrase):
    gateway.fail("leases", status)
    with pytest.raises(error) as caught:
        client.lease()
    assert phrase in str(caught.value)
    assert gateway.token not in str(caught.value)


@pytest.mark.parametrize("status, phrase", [(409, "no longer leased"), (404, "no longer knows")])
def test_a_job_the_gateway_took_back(gateway, client, status, phrase):
    gateway.enqueue(job_id="j")
    client.lease()
    gateway.fail("heartbeat", status)
    with pytest.raises(LeaseLost, match=phrase):
        client.heartbeat("j", 1, {})


def test_the_token_file_is_read_for_every_request(gateway, client):
    assert client.lease() is None
    client.token_file.write_text("rotated")
    with pytest.raises(GatewayAuthError):
        client.lease()
    gateway.token = "rotated"
    assert client.lease() is None
    client.token_file.write_text("\n")
    with pytest.raises(GatewayAuthError, match="is empty"):
        client.lease()
    client.token_file.unlink()
    with pytest.raises(GatewayAuthError, match="cannot be read"):
        client.lease()


@pytest.mark.parametrize(
    "body, phrase",
    [
        (b"not json", "not JSON"),
        ([1], "not a JSON object"),
        ({"job_id": "", "attempt": 1}, "job_id"),
        ({"job_id": "j", "attempt": True}, "attempt"),
        ({"job_id": "j", "attempt": 1, "lease_seconds": 0}, "lease_seconds"),
        ({"job_id": "j", "attempt": 1, "lease_seconds": 5, "stage_max_bytes": -1}, "stage_max"),
        (
            {"job_id": "j", "attempt": 1, "lease_seconds": 5, "stage_max_bytes": 1, "spec": []},
            "spec",
        ),
    ],
)
def test_a_malformed_lease(gateway, client, body, phrase):
    gateway.fail("leases", answer(body))
    with pytest.raises(GatewayRejected, match=phrase) as caught:
        client.lease()
    assert "internal API version" in str(caught.value)


@pytest.mark.parametrize(
    "body, phrase",
    [({"lease_seconds": "60"}, "lease_seconds"), ({"cancel": "yes"}, "cancel")],
)
def test_a_malformed_heartbeat_reply(gateway, client, body, phrase):
    gateway.fail("heartbeat", answer(body))
    with pytest.raises(GatewayRejected, match=phrase):
        client.heartbeat("j", 1, {})


def test_a_heartbeat_reply_may_leave_the_lease_unchanged(gateway, client):
    gateway.fail("heartbeat", answer({}))
    reply = client.heartbeat("j", 1, {})
    assert reply.lease_seconds is None and reply.cancel is False


def _upload(**overrides):
    upload = {"name": "a", "key": "k", "url": "http://store/k", "method": "PUT", "headers": {}}
    upload.update(overrides)
    return upload


@pytest.mark.parametrize(
    "body, phrase",
    [
        ({"uploads": {}}, "uploads is not a list"),
        ({"uploads": ["x"]}, "not a JSON object"),
        ({"uploads": [_upload(url="")]}, "lacks name, key or url"),
        ({"uploads": [_upload(method="POST")]}, "is not PUT"),
        ({"uploads": [_upload(headers={"a": 1})]}, "headers are not strings"),
        ({"uploads": [_upload(headers="x")]}, "headers are not strings"),
        ({"uploads": [_upload(name="b")]}, "no upload URL for a"),
    ],
)
def test_a_malformed_presign_reply(gateway, client, body, phrase):
    gateway.fail("presign", answer(body))
    with pytest.raises(GatewayRejected, match=phrase):
        client.presign("j", 1, [{"name": "a", "bytes": 1}])


def test_presign_defaults(gateway, client):
    upload = _upload()
    del upload["method"], upload["headers"]
    gateway.fail("presign", answer({"uploads": [upload]}))
    target = client.presign("j", 1, [{"name": "a", "bytes": 1}])["a"]
    assert target.method == "PUT" and target.headers == {}
