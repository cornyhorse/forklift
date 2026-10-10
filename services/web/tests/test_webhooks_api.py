"""/api/v1/webhooks and /api/v1/admin/webhooks: shapes, the secret shown once, errors, tokens'
scopes, and a test event sent to a real receiver through the real resolver."""

from __future__ import annotations

import pytest
from webhook_support import Receiver
from world import World, api_token

from forklift_web.core.models import Webhook
from forklift_web.services import webhooks

pytestmark = pytest.mark.django_db

BODY = {"name": "ci", "url": "https://hooks.example.org/in?key=private", "events": ["job.failed"]}


@pytest.fixture
def world():
    return World.build()


def test_creating_reading_and_listing(world, as_user):
    viewer = as_user(world.viewer)
    response = viewer.post("/api/v1/webhooks", BODY)
    assert response.status_code == 201, response.content
    created = response.json()
    secret = created.pop("secret")
    assert secret.startswith("fkwh_") and created["secret_prefix"] == secret[:12]
    assert {key: created[key] for key in ("name", "url", "events", "scope", "kinds")} == {
        "name": "ci",
        "url": BODY["url"],
        "events": ["job.failed"],
        "scope": "own_jobs",
        "kinds": ["run"],
    }
    assert (created["active"], created["disabled_reason"], created["dataset_id"]) == (
        True,
        None,
        None,
    )
    assert (created["owner_id"], created["owner_username"]) == (
        world.viewer.pk,
        world.viewer.username,
    )
    shown = viewer.get(f"/api/v1/webhooks/{created['id']}").json()
    assert shown == created and "secret" not in shown
    listed = viewer.get("/api/v1/webhooks").json()
    assert listed["count"] == 1 and listed["items"] == [created]
    assert secret not in viewer.get("/api/v1/webhooks").content.decode()


def test_errors_are_explained(world, as_user):
    viewer = as_user(world.viewer)
    refused = viewer.post("/api/v1/webhooks", {**BODY, "url": "https://10.0.0.1/"})
    assert refused.status_code == 400 and refused.json()["code"] == "url_refused"
    assert "not a publicly routable address" in refused.json()["detail"]
    everything = viewer.post("/api/v1/webhooks", {**BODY, "scope": "all_jobs"})
    assert (everything.status_code, everything.json()["code"]) == (403, "role_insufficient")
    unknown = viewer.post(
        "/api/v1/webhooks",
        {**BODY, "scope": "dataset", "dataset_id": str(world.version.pk)},
    )
    assert unknown.status_code == 404
    assert viewer.post("/api/v1/webhooks", {**BODY, "events": ["job.started"]}).status_code == 422


def test_changing_disabling_rotating_and_deleting(world, as_user):
    viewer = as_user(world.viewer)
    created = viewer.post("/api/v1/webhooks", BODY).json()
    path = f"/api/v1/webhooks/{created['id']}"
    dataset = {"scope": "dataset", "dataset_id": str(world.dataset.pk)}
    changed = viewer.patch(path, {"name": "alerts", **dataset}).json()
    assert (changed["name"], changed["scope"], changed["dataset_id"]) == (
        "alerts",
        "dataset",
        str(world.dataset.pk),
    )
    assert viewer.patch(path, {"scope": "own_jobs"}).json()["dataset_id"] is None
    off = viewer.patch(path, {"active": False}).json()
    assert (off["active"], off["disabled_reason"]) == (False, "owner")
    bad = viewer.patch(path, {"url": "http://hooks.example.org/"})
    assert (bad.status_code, bad.json()["code"]) == (400, "url_refused")
    rotated = viewer.post(f"{path}/rotate-secret")
    assert rotated.status_code == 200
    assert rotated.json()["secret"].startswith("fkwh_")
    assert rotated.json()["secret_prefix"] != created["secret_prefix"]
    assert viewer.delete(path).status_code == 204
    gone = viewer.get(path)
    assert (gone.status_code, gone.json()["code"]) == (404, "not_found")


def test_a_test_event_is_queued_and_the_log_shows_how_it_went(world, as_user, settings):
    receiver = Receiver(204)
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["127.0.0.1"]
    try:
        operator = as_user(world.operator)
        url = f"http://127.0.0.1:{receiver.port}/forklift"
        created = operator.post("/api/v1/webhooks", {**BODY, "url": url}).json()
        path = f"/api/v1/webhooks/{created['id']}"
        tested = operator.post(f"{path}/test")
        assert tested.status_code == 202, tested.content
        delivery = tested.json()
        assert (delivery["status"], delivery["attempts"], delivery["event"]) == (
            "pending",
            0,
            "webhook.test",
        )
        assert delivery["payload"]["webhook"] == {"id": created["id"], "name": "ci"}
        assert receiver.requests == []  # the request thread sent nothing
        waiting = operator.post(f"{path}/test")
        assert (waiting.status_code, waiting.json()["code"]) == (409, "test_pending")
        assert webhooks.deliver_due()["delivered"] == 1  # the dispatcher's pass
        assert receiver.requests[0].headers["forklift-delivery"] == delivery["id"]
    finally:
        receiver.stop()
    assert operator.post(f"{path}/test").status_code == 202
    webhooks.deliver_due()  # nobody listens any more
    log = operator.get(f"{path}/deliveries").json()
    assert log["count"] == 2 and [d["status"] for d in log["items"]] == ["failed", "delivered"]
    assert "refused the connection" in log["items"][0]["last_error"]
    assert log["items"][1]["last_status_code"] == 204
    assert operator.get(f"{path}/deliveries?status=delivered").json()["count"] == 1
    assert operator.get(path).json()["backoff_until"] is None  # tests do not back off
    again = operator.post(f"{path}/deliveries/{delivery['id']}/redeliver")
    assert (again.status_code, again.json()["code"]) == (400, "invalid_request")
    job_delivery = world.webhook().deliveries.get()
    redelivered = operator.post(
        f"/api/v1/webhooks/{world.webhook().id}/deliveries/{job_delivery.id}/redeliver"
    )
    assert (redelivered.json()["status"], redelivered.json()["attempts"]) == ("pending", 0)


def test_tokens_need_the_webhook_scopes(world, as_token):
    _, raw = api_token(world.operator, ["webhooks:read"])
    reader = as_token(raw)
    assert reader.get("/api/v1/webhooks").status_code == 200
    assert reader.get(f"/api/v1/webhooks/{world.webhook().id}").status_code == 200
    refused = reader.post("/api/v1/webhooks", BODY)
    assert (refused.status_code, refused.json()["code"]) == (403, "scope_missing")
    _, raw = api_token(world.operator, ["webhooks:read", "webhooks:write"])
    assert as_token(raw).post("/api/v1/webhooks", BODY).status_code == 201


def test_admins_see_everyones_webhooks_without_their_query_strings(world, as_user):
    as_user(world.viewer).post("/api/v1/webhooks", BODY)
    admin = as_user(world.admin)
    listed = admin.get("/api/v1/admin/webhooks").json()
    [item] = listed["items"]
    assert item["endpoint"] == "https://hooks.example.org/in"
    assert "url" not in item and "secret_prefix" not in item and "private" not in str(listed)
    assert admin.get("/api/v1/admin/webhooks?active=false").json()["count"] == 0
    mine = admin.get(f"/api/v1/admin/webhooks?owner_id={world.admin.pk}").json()
    assert mine["count"] == 0
    other = admin.get(f"/api/v1/webhooks/{item['id']}")
    assert (other.status_code, other.json()["code"]) == (403, "not_owner")
    disabled = admin.post(f"/api/v1/admin/webhooks/{item['id']}/disable")
    assert (disabled.json()["active"], disabled.json()["disabled_reason"]) == (False, "admin")
    assert admin.post(f"/api/v1/admin/webhooks/{item['id']}/disable").status_code == 409
    assert Webhook.objects.get(pk=item["id"]).disabled_reason == "admin"
