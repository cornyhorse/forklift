"""Installation settings, the audit log API and the request plumbing."""

from __future__ import annotations

from datetime import timedelta

import pytest
from django.test import Client, RequestFactory, override_settings
from django.utils import timezone

from forklift_web.core.choices import Role
from forklift_web.core.models import AuditLog, ImmutableError, InstallationSetting
from forklift_web.errors import InvalidRequest
from forklift_web.middleware import client_ip
from forklift_web.policy import Actor
from forklift_web.services import audit, installation

pytestmark = pytest.mark.django_db


def test_settings_have_defaults_until_set(admin_actor):
    described = installation.describe(admin_actor)
    assert described["stage_max_bytes"] == {
        "value": 2 * 1024**3,
        "default": 2 * 1024**3,
        "description": described["stage_max_bytes"]["description"],
    }
    assert "streamed" in described["stage_max_bytes"]["description"]
    changed = installation.update(admin_actor, {"stage_max_bytes": 1024, "lease_seconds": 30})
    assert changed["stage_max_bytes"]["value"] == 1024 and installation.get("lease_seconds") == 30
    entry = AuditLog.objects.get(action="settings.update")
    assert entry.details["stage_max_bytes"] == {"from": 2 * 1024**3, "to": 1024}
    installation.update(admin_actor, {"stage_max_bytes": None})  # back to the default
    assert installation.get("stage_max_bytes") == 2 * 1024**3
    assert not InstallationSetting.objects.filter(key="stage_max_bytes").exists()
    assert InstallationSetting.objects.get(key="lease_seconds").updated_by == admin_actor.user


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"colour": "blue"}, "Unknown installation settings: colour"),
        ({"lease_seconds": "60"}, "lease_seconds must be an integer"),
        ({"lease_seconds": True}, "lease_seconds must be an integer"),
        ({"lease_seconds": 5}, "at least 10 and at most 3600"),
        ({"stage_max_bytes": -1}, "at least 0"),
        ({"default_classification": "secret"}, "must be one of: public, internal, sensitive"),
        ({"lane_limits": []}, "must map lanes"),
        ({"lane_limits": {"gpu": {}}}, "must map lanes"),
        ({"lane_limits": {"batch": {"max_cpu": 1}}}, "may only set max_input_bytes"),
        ({"lane_limits": {"batch": []}}, "may only set max_input_bytes"),
        ({"lane_limits": {"batch": {"max_rows": 0}}}, "lane_limits.batch.max_rows must be at"),
        ({"lane_limits": {"batch": {"max_rows": "x"}}}, "must be an integer or null"),
    ],
)
def test_settings_validation(admin_actor, changes, message):
    with pytest.raises(InvalidRequest, match=message):
        installation.update(admin_actor, changes)


def test_lane_limits_merge_over_the_defaults(admin_actor):
    installation.update(
        admin_actor, {"lane_limits": {"batch": {"max_rows": 1000, "max_seconds": None}}}
    )
    limits = installation.get("lane_limits")
    assert limits["batch"] == {"max_input_bytes": None, "max_seconds": None, "max_rows": 1000}
    assert limits["interactive"]["max_seconds"] == 120


def test_settings_api(as_user, make_user):
    admin = as_user(make_user(Role.ADMIN))
    body = admin.get("/api/v1/admin/settings").json()
    assert set(body) == set(installation.SETTINGS)
    patched = admin.patch("/api/v1/admin/settings", {"max_attempts": 5})
    assert patched.status_code == 200 and patched.json()["max_attempts"]["value"] == 5
    bad = admin.patch("/api/v1/admin/settings", {"max_attempts": 0})
    assert bad.status_code == 400 and bad.json()["code"] == "invalid_request"


def test_audit_log_filters_and_immutability(admin_actor, make_user, as_user):
    other = make_user(Role.VIEWER)
    first = audit.record(admin_actor, "a.one", other, {"x": 1})
    audit.record(Actor.for_user(other), "a.two")
    assert [e.action for e in audit.list_entries(admin_actor, action="a.one")] == ["a.one"]
    assert [e.action for e in audit.list_entries(admin_actor, actor_id=other.pk)] == ["a.two"]
    assert [e.object_id for e in audit.list_entries(admin_actor, object_type="user")] == [
        str(other.pk)
    ]
    assert audit.list_entries(admin_actor, object_id=str(other.pk)).count() == 1
    now = timezone.now()
    assert audit.list_entries(admin_actor, since=now - timedelta(minutes=1)).count() == 2
    assert audit.list_entries(admin_actor, until=now - timedelta(minutes=1)).count() == 0
    with pytest.raises(ImmutableError):
        first.action = "changed"
        first.save()
    with pytest.raises(ImmutableError):
        first.delete()
    listed = as_user(admin_actor.user).get("/api/v1/admin/audit?action=a.one").json()
    assert listed["count"] == 1 and listed["items"][0]["object_repr"] == other.username


def test_request_ids_and_client_addresses(make_user):
    client = Client()
    response = client.get("/healthz", HTTP_X_REQUEST_ID="trace-123")
    assert response["X-Request-ID"] == "trace-123"
    generated = client.get("/healthz", HTTP_X_REQUEST_ID="not ok!")["X-Request-ID"]
    assert generated != "not ok!" and len(generated) == 32
    user = make_user(Role.VIEWER)
    client.force_login(user)
    client.post(
        "/api/v1/tokens",
        {"name": "t", "scopes": ["jobs:read"]},
        content_type="application/json",
        HTTP_X_REQUEST_ID="req-7",
        REMOTE_ADDR="192.0.2.10",
    )
    entry = AuditLog.objects.get(action="token.create")
    assert (entry.request_id, entry.ip) == ("req-7", "192.0.2.10")


def test_client_ip_trusts_only_the_configured_proxies():
    factory = RequestFactory()
    request = factory.get("/", HTTP_X_FORWARDED_FOR="6.6.6.6, 203.0.113.5", REMOTE_ADDR="10.0.0.2")
    assert client_ip(request) == "10.0.0.2"
    with override_settings(FORKLIFT_TRUSTED_PROXIES=1):
        assert client_ip(request) == "203.0.113.5"
    with override_settings(FORKLIFT_TRUSTED_PROXIES=2):
        assert client_ip(request) == "6.6.6.6"
    with override_settings(FORKLIFT_TRUSTED_PROXIES=3):
        assert client_ip(request) == "10.0.0.2"  # fewer entries than proxies: not trusted
    assert client_ip(factory.get("/", REMOTE_ADDR="")) is None


@override_settings(FORKLIFT_TRUSTED_PROXIES=1)
def test_client_ip_drops_the_port_a_proxy_writes():
    factory = RequestFactory()
    for forwarded, expected in (
        ("203.0.113.5:5555", "203.0.113.5"),
        ("[2001:db8::1]:443", "2001:db8::1"),
        ("2001:DB8::1", "2001:db8::1"),
        ("unknown", "unknown"),  # no address: passed on as it is
    ):
        request = factory.get("/", HTTP_X_FORWARDED_FOR=forwarded, REMOTE_ADDR="10.0.0.2")
        assert client_ip(request) == expected
