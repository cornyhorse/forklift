"""The UI's building blocks: template filters and tags, the version diff, forms, helpers."""

from __future__ import annotations

import io

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from django.core.management import CommandError, call_command
from django.template import Context, Template
from django.test import RequestFactory
from django.urls import reverse
from test_ui_matrix import retention_data
from world import World

from forklift_web import storage
from forklift_web.policy import Actor
from forklift_web.services import audit
from forklift_web.ui import diff
from forklift_web.ui.forms import expiry_choices
from forklift_web.ui.templatetags.forklift_ui import event_text, filesize
from forklift_web.ui.views.account import recent


@pytest.mark.parametrize(
    "value,text",
    [
        (None, "—"),
        ("", "—"),
        (1, "1 byte"),
        (1023, "1023 bytes"),
        (1536, "1.5 KiB"),
        (5 * 1024**4, "5.0 TiB"),
        (3 * 1024**6, "3072.0 PiB"),
    ],
)
def test_filesize(value, text):
    assert filesize(value) == text


def test_event_text_leaves_out_empty_values():
    class Event:
        payload = {"status": "failed", "error_code": None, "rows_read": 3}

    assert event_text(Event()) == "status: failed, rows read: 3"


def test_the_query_tag_keeps_the_filters():
    request = RequestFactory().get("/admin/audit/", {"action": "user.login", "page": "1"})
    rendered = Template("{% load forklift_ui %}{% query page=3 %}").render(
        Context({"request": request})
    )
    assert rendered == "?action=user.login&amp;page=3"


def test_long_lists_are_paginated(db, client):
    world = World.build()
    actor = Actor.for_user(world.admin)
    for number in range(60):
        audit.record(actor, "test.entry", details={"n": number})
    client.force_login(world.admin)
    first = client.get(reverse("ui:admin-audit"), {"action": "test.entry"})
    assert first.context["page"].paginator.num_pages == 2
    assert b"?action=test.entry&amp;page=2" in first.content and b"Page 1 of 2" in first.content
    second = client.get(reverse("ui:admin-audit"), {"action": "test.entry", "page": "2"})
    assert len(second.context["page"]) == 10 and b"Previous" in second.content


def test_a_diff_shortens_long_unchanged_stretches():
    old = {"properties": {f"c{n:02}": {"type": "string"} for n in range(20)}}
    new = {"properties": dict(old["properties"])}
    new["properties"]["c00"] = {"type": "integer"}
    new["properties"]["c19"] = {"type": "integer"}
    lines = diff.line_diff(old, new)
    assert [line.kind for line in lines].count("gap") == 1
    gap = next(line for line in lines if line.kind == "gap")
    assert gap.sign == "…" and Line_signs(lines) == {"+", "-", " ", "…"}
    assert diff.summary(old, new).changed == ["c00", "c19"]


def Line_signs(lines):  # noqa: N802 - reads as a phrase in the assertion
    return {line.sign for line in lines}


def test_a_summary_of_documents_without_columns():
    summary = diff.summary({"properties": [], "required": "id"}, {"type": "object"})
    assert summary.empty is False and summary.other_keys == ["type"]
    assert diff.summary({}, {}).empty


def test_expiry_choices():
    assert expiry_choices(None) == [
        ("7", "7 days"),
        ("30", "30 days"),
        ("90", "90 days"),
        ("365", "1 year"),
        ("", "Never"),
    ]
    assert expiry_choices(None, allow_never=False)[-1] == ("365", "1 year")
    assert expiry_choices(30) == [("7", "7 days"), ("30", "30 days")]


def test_the_home_page_of_an_actor_that_sees_neither_jobs_nor_files(db, api_token):
    world = World.build()
    token, _ = api_token(world.operator, ["tokens:read"])
    found = recent(Actor.for_user(world.operator, token=token))
    assert found == {"recent_jobs": [], "recent_uploads": [], "running": []}


def test_a_refusal_inside_a_form_is_a_page_of_its_own(db, client):
    world = World.build()
    client.force_login(world.admin)
    missing = "00000000-0000-0000-0000-000000000000"
    response = client.post(
        reverse("ui:admin-retention-save"),
        retention_data("dataset-new", "dataset", missing, data=1),
    )
    assert response.status_code == 404 and b"<h1>Not found</h1>" in response.content


def test_configure_cors_sets_the_rule_the_browser_needs(test_bucket, s3):
    out = io.StringIO()
    call_command(
        "configure_cors",
        "--origin",
        "https://forklift.example.org/",
        "--origin",
        "http://localhost:8080",
        stdout=out,
    )
    try:
        (rule,) = s3.get_bucket_cors(Bucket=test_bucket)["CORSRules"]
    finally:
        s3.delete_bucket_cors(Bucket=test_bucket)
    assert rule["AllowedOrigins"] == ["https://forklift.example.org", "http://localhost:8080"]
    assert sorted(rule["AllowedMethods"]) == ["GET", "HEAD", "PUT"]
    assert rule["ExposeHeaders"] == ["ETag"] and rule["MaxAgeSeconds"] == 3600
    assert "now allows GET, PUT, HEAD from https://forklift.example.org" in out.getvalue()


@pytest.mark.parametrize("origin", ["forklift.example.org", "https://x.org/ui", "ftp://x.org"])
def test_configure_cors_needs_origins(origin):
    with pytest.raises(CommandError, match="is not an origin"):
        call_command("configure_cors", "--origin", origin)


@pytest.mark.parametrize(
    "error,reason",
    [
        (ClientError({"Error": {"Code": "AccessDenied"}}, "PutBucketCors"), "AccessDenied"),
        (EndpointConnectionError(endpoint_url="http://nowhere"), "EndpointConnectionError"),
    ],
)
def test_configure_cors_explains_a_refusal(monkeypatch, error, reason):
    class Refusing:
        def put_bucket_cors(self, **kwargs):
            raise error

    monkeypatch.setattr(storage.Bucket, "client", lambda self, purpose, audience=None: Refusing())
    with pytest.raises(CommandError, match=f"refused to set the CORS rules .*\\({reason}\\)"):
        call_command("configure_cors", "--origin", "http://localhost:8080")
