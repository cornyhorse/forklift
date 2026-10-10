"""The home page, changing one's own password, and one's own API tokens."""

from __future__ import annotations

import pytest
from django.contrib.auth import authenticate
from django.urls import reverse
from world import PASSWORD, World, make_job

from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.models import ApiToken, AuditLog
from forklift_web.policy import Actor
from forklift_web.services import installation, tokens

pytestmark = pytest.mark.django_db


@pytest.fixture
def world():
    return World.build()


def signed_in(client, user):
    client.force_login(user)
    return client


@pytest.mark.parametrize(
    "role,shown,hidden",
    [
        (Role.VIEWER, ["Browse", "Automate"], ["Upload a file", "New schema", "Admin overview"]),
        (Role.OPERATOR, ["Upload a file", "Browse"], ["New schema", "New dataset", "Admin"]),
        (Role.AUTHOR, ["Upload a file", "New schema", "New dataset"], ["Admin overview"]),
        (Role.ADMIN, ["Upload a file", "New schema", "Admin overview"], []),
    ],
)
def test_the_home_page_offers_what_the_role_can_do(world, client, role, shown, hidden):
    page = signed_in(client, getattr(world, role)).get("/").content.decode()
    for text in shown:
        assert text in page
    for text in hidden:
        assert text not in page


def test_the_home_page_lists_the_users_recent_jobs_and_files(world, client):
    make_job(world.operator, world.upload, status=JobStatus.RUNNING)
    page = signed_in(client, world.operator).get("/").content.decode()
    assert str(world.queued_job.pk)[:8] in page
    assert "still queued or running" in page
    assert "Your recent files" in page and "people.csv" in page
    other = signed_in(client, world.author).get("/").content.decode()
    assert "You have not started any jobs yet." in other
    assert "Your recent files" not in other  # authors' own uploads only, and they have none


def test_the_raw_rows_permission_is_explained_on_the_home_page(client, make_user):
    user = make_user(Role.VIEWER, raw_rows=True)
    assert b"view raw rows of sensitive data" in signed_in(client, user).get("/").content


# --------------------------------------------------------------------------- password


def test_changing_ones_own_password(world, client):
    signed_in(client, world.viewer)
    wrong = client.post(
        reverse("ui:password"),
        {"old_password": "nope", "new_password1": "x", "new_password2": "x"},
    )
    assert wrong.status_code == 200 and b"errorlist" in wrong.content
    weak = client.post(
        reverse("ui:password"),
        {"old_password": PASSWORD, "new_password1": "12345678", "new_password2": "12345678"},
    )
    assert b"too short" in weak.content or b"too common" in weak.content
    changed = client.post(
        reverse("ui:password"),
        {
            "old_password": PASSWORD,
            "new_password1": "a much better passphrase 7",
            "new_password2": "a much better passphrase 7",
        },
        follow=True,
    )
    assert b"Your password was changed." in changed.content
    assert client.get("/").status_code == 200  # still signed in
    assert authenticate(username=world.viewer.username, password="a much better passphrase 7")
    entry = AuditLog.objects.get(action="user.change_own_password")
    assert entry.actor == world.viewer and entry.details == {}


def test_service_accounts_get_no_password_link(client, make_user):
    service = make_user(Role.VIEWER, is_service_account=True)
    page = signed_in(client, service).get("/").content.decode()
    assert reverse("ui:password") not in page


# --------------------------------------------------------------------------- API tokens


def test_a_new_token_is_shown_once_and_never_again(world, client):
    signed_in(client, world.operator)
    created = client.post(
        reverse("ui:tokens"),
        {"name": "airflow", "scopes": ["jobs:read", "jobs:run"], "expires_in": "30"},
    )
    assert created.status_code == 200
    assert "no-store" in created["Cache-Control"]
    token = ApiToken.objects.get(owner=world.operator, name="airflow")
    raw = created.context["raw"]
    assert raw.startswith("fkl_") and raw in created.content.decode()
    assert tokens.find(ApiToken.objects, raw, tokens.API_TOKEN_PREFIX) == token
    assert token.scopes == ["jobs:read", "jobs:run"] and token.expires_at is not None
    listed = client.get(reverse("ui:tokens")).content.decode()
    assert "airflow" in listed and token.prefix in listed and raw not in listed


def test_the_scopes_offered_are_the_roles(world, client):
    page = signed_in(client, world.viewer).get(reverse("ui:tokens"))
    offered = {value for value, _ in page.context["form"].fields["scopes"].choices}
    assert "jobs:read" in offered and "jobs:run" not in offered and "admin:read" not in offered
    refused = client.post(reverse("ui:tokens"), {"name": "x", "scopes": ["jobs:run"]})
    assert refused.status_code == 400  # not a choice the form offers
    assert not ApiToken.objects.filter(owner=world.viewer, name="x").exists()


def test_a_token_needs_a_name(world, client):
    signed_in(client, world.viewer)
    blank = client.post(reverse("ui:tokens"), {"name": "  ", "scopes": ["jobs:read"]})
    assert blank.status_code == 400 and b"This field is required." in blank.content


def test_the_installation_can_require_tokens_to_expire(world, client):
    installation.update(Actor.for_system("tests"), {"token_max_days": 45})
    page = signed_in(client, world.viewer).get(reverse("ui:tokens"))
    choices = page.context["form"].fields["expires_in"].choices
    assert [value for value, _ in choices] == ["7", "30", "45"]  # no "never"
    assert page.context["form"].fields["expires_in"].initial == "45"
    installation.update(Actor.for_system("tests"), {"token_max_days": 3})
    form = client.get(reverse("ui:tokens")).context["form"]
    assert [value for value, _ in form.fields["expires_in"].choices] == ["3"]


def test_the_service_layer_refusal_is_shown_on_the_form(world, client):
    installation.update(Actor.for_system("tests"), {"token_max_days": 1})
    signed_in(client, world.viewer)
    # "Never" is not offered any more, but a stale form can still send it
    refused = client.post(
        reverse("ui:tokens"), {"name": "forever", "scopes": ["jobs:read"], "expires_in": ""}
    )
    assert refused.status_code == 400
    assert "must expire within 1 days" in refused.content.decode()
    assert not ApiToken.objects.filter(name="forever").exists()


def test_revoking_ones_own_token(world, client):
    token = world.tokens["viewer"]
    signed_in(client, world.viewer)
    revoked = client.post(reverse("ui:token-revoke", kwargs={"token_id": token.pk}), follow=True)
    assert f"({token.prefix}...) was revoked" in revoked.content.decode()
    token.refresh_from_db()
    assert token.revoked_at is not None
    page = client.get(reverse("ui:tokens")).content.decode()
    assert "badge revoked" in page
    again = client.post(reverse("ui:token-revoke", kwargs={"token_id": token.pk}))
    assert again.status_code == 409 and b"already revoked" in again.content


def test_expired_tokens_are_marked(world, client, api_token):
    from datetime import timedelta

    from django.utils import timezone

    api_token(world.viewer, ["jobs:read"], expires_at=timezone.now() - timedelta(days=1))
    page = signed_in(client, world.viewer).get(reverse("ui:tokens")).content.decode()
    assert "badge expired" in page


def test_someone_elses_token_cannot_be_revoked(world, client):
    token = world.tokens["admin"]
    response = signed_in(client, world.author).post(
        reverse("ui:token-revoke", kwargs={"token_id": token.pk})
    )
    assert response.status_code == 403
    assert b"belongs to another user" in response.content
    token.refresh_from_db()
    assert token.revoked_at is None
