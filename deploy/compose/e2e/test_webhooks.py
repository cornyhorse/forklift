"""End-to-end: a webhook registered through the API hears about a job a worker finished, and the
delivery the dispatcher sends to a receiver on the Compose network is signed with its secret.

It needs the receiver of webhooks.yml (next to this file) and its published port:

    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/e2e/webhooks.yml \
      up -d --build --wait
    FORKLIFT_E2E_ENV_FILE=deploy/compose/.env FORKLIFT_E2E_RECEIVER_URL=http://localhost:18099 \
      python -m pytest deploy/compose/e2e/test_webhooks.py --no-cov
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.request
import uuid

import pytest
from test_stack import PEOPLE, SCHEMA, Client, _settings, run_job, upload

DELIVERY_TIMEOUT_SECONDS = 90  # the dispatcher passes every 10 seconds
RECEIVER_IN_COMPOSE = "http://webhook-receiver:8000"


@pytest.fixture(scope="module")
def receiver_url() -> str:
    url = os.environ.get("FORKLIFT_E2E_RECEIVER_URL")
    if not url:
        pytest.skip(
            "start the stack with deploy/compose/e2e/webhooks.yml and set "
            "FORKLIFT_E2E_RECEIVER_URL to the receiver's published address"
        )
    return url.rstrip("/")


@pytest.fixture(scope="module")
def admin() -> Client:
    settings = _settings()
    session = Client(settings["FORKLIFT_PUBLIC_URL"])
    session.sign_in(settings["FORKLIFT_ADMIN_USERNAME"], settings["FORKLIFT_ADMIN_PASSWORD"])
    _, me = session.request("GET", "/api/v1/me")
    _, created = session.request(
        "POST",
        "/api/v1/tokens",
        {"name": f"e2e-webhooks-{uuid.uuid4().hex[:8]}", "scopes": me["role_scopes"]},
    )
    return Client(settings["FORKLIFT_PUBLIC_URL"], token=created["token"])


def received(receiver_url: str, path: str) -> list:
    with urllib.request.urlopen(f"{receiver_url}/deliveries", timeout=10) as response:
        return [item for item in json.loads(response.read()) if item["path"] == path]


def sent_test_event(admin: Client, webhook_id: str, delivery_id: str) -> dict:
    """The test event's delivery once the dispatcher has sent it."""
    deadline = time.monotonic() + DELIVERY_TIMEOUT_SECONDS
    while True:
        _, log = admin.request("GET", f"/api/v1/webhooks/{webhook_id}/deliveries")
        [delivery] = [item for item in log["items"] if item["id"] == delivery_id]
        if delivery["status"] != "pending":
            return delivery
        assert time.monotonic() < deadline, "the dispatcher did not send the test event"
        time.sleep(2)


def verify(secret: str, header: str, body: bytes) -> bool:
    """A receiver's check, as docs/platform/webhooks.md describes it."""
    fields = dict(part.split("=", 1) for part in header.split(","))
    timestamp = int(fields["t"])
    expected = hmac.new(
        secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return abs(time.time() - timestamp) <= 300 and hmac.compare_digest(expected, fields["v1"])


def test_a_finished_job_is_delivered_signed(admin, receiver_url):
    path = f"/forklift/{uuid.uuid4().hex}?token=e2e"
    _, webhook = admin.request(
        "POST",
        "/api/v1/webhooks",
        {
            "name": "e2e receiver",
            "url": RECEIVER_IN_COMPOSE + path,
            "events": ["job.succeeded", "job.failed"],
        },
    )
    secret = webhook["secret"]
    try:
        status, tested = admin.request("POST", f"/api/v1/webhooks/{webhook['id']}/test")
        assert (status, tested["status"]) == (202, "pending")  # queued for the dispatcher
        sent = sent_test_event(admin, webhook["id"], tested["id"])
        assert sent["status"] == "delivered", sent["last_error"]

        job = run_job(
            admin, kind="run", upload_id=upload(admin, "people.csv", PEOPLE), schema=SCHEMA
        )
        assert job["status"] == "succeeded", job["error"]

        deadline = time.monotonic() + DELIVERY_TIMEOUT_SECONDS
        while True:
            found = [
                item
                for item in received(receiver_url, path)
                if (json.loads(item["body"])["job"] or {}).get("id") == job["id"]
            ]
            if found:
                break
            assert time.monotonic() < deadline, "no job.succeeded delivery reached the receiver"
            time.sleep(2)

        [delivery] = found
        body = delivery["body"].encode()
        assert verify(secret, delivery["headers"]["forklift-signature"], body)
        assert delivery["headers"]["forklift-event"] == "job.succeeded"
        payload = json.loads(body)
        assert payload["id"] == delivery["headers"]["forklift-delivery"]
        assert payload["job"]["id"] == job["id"] and payload["job"]["status"] == "succeeded"
        assert payload["job"]["counts"]["valid_rows"] == 2
        assert payload["job"]["url"].endswith(f"/api/v1/jobs/{job['id']}")
        assert "Ana" not in delivery["body"]  # outcomes only, never rows
        assert delivery["headers"]["user-agent"].startswith("forklift-webhooks/")

        _, log = admin.request("GET", f"/api/v1/webhooks/{webhook['id']}/deliveries")
        statuses = {item["event"]: item["status"] for item in log["items"]}
        assert statuses == {"job.succeeded": "delivered", "webhook.test": "delivered"}
    finally:
        admin.request("DELETE", f"/api/v1/webhooks/{webhook['id']}")
