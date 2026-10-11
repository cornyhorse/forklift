"""What admins see of single sign-on: a user's linked sign-ins on the user page and through
/api/v1/admin, unlinking one (audited), and `forklift-web check_oidc`."""

from __future__ import annotations

import io

import pytest
from django.core.management import CommandError, call_command
from django.test import Client
from oidc_support import REMOVE, idp, sign_in, sso  # noqa: F401

from forklift_web.core.choices import Role
from forklift_web.core.models import AuditLog, OidcIdentity, User

pytestmark = pytest.mark.django_db


@pytest.fixture
def linked(idp, sso):  # noqa: F811
    """alice, who signed in through the provider once."""
    sign_in(Client(), idp, idp.person("alice"))
    return User.objects.get(username="user-alice")


def admin_client(make_user) -> Client:
    client = Client()
    client.force_login(make_user(Role.ADMIN))
    return client


def test_the_user_page_lists_linked_sign_ins(linked, make_user, idp):  # noqa: F811
    page = admin_client(make_user).get(f"/admin/users/{linked.pk}/").content.decode()
    assert "Single sign-on" in page and "<code>alice</code>" in page and idp.issuer in page
    assert "account created at the first sign-in" in page
    assert "<dt>Password</dt><dd>none</dd>" in page


def test_without_single_sign_on_the_section_shows_only_links(make_user, settings):
    settings.FORKLIFT_OIDC = None
    person = make_user(Role.VIEWER)
    page = admin_client(make_user).get(f"/admin/users/{person.pk}/").content.decode()
    assert "Single sign-on" not in page and "<dt>Password</dt><dd>set</dd>" in page


def test_a_user_without_links(make_user, sso):  # noqa: F811
    person = make_user(Role.VIEWER)
    page = admin_client(make_user).get(f"/admin/users/{person.pk}/").content.decode()
    assert f"No sign-in at the provider is linked to {person.username}." in page


def test_unlinking_on_the_user_page(linked, make_user, idp):  # noqa: F811
    client = admin_client(make_user)
    identity = linked.oidc_identities.get()
    response = client.post(
        f"/admin/users/{linked.pk}/identities/{identity.pk}/unlink/", follow=True
    )
    assert f"The sign-in alice at {idp.issuer} is no longer linked to user-alice." in (
        response.content.decode()
    )
    assert not OidcIdentity.objects.exists()
    entry = AuditLog.objects.get(action="user.oidc_unlink")
    assert entry.object_id == str(linked.pk)
    assert entry.details == {"issuer": idp.issuer, "subject": "alice"}
    # the next sign-in starts over: the username is taken, so it is refused
    refused = sign_in(Client(), idp, idp.people["alice"])
    assert refused.status_code == 403 and b"already exists" in refused.content


def test_unlinking_someone_elses_or_an_unknown_identity(linked, make_user):  # noqa: F811
    client = admin_client(make_user)
    other = make_user(Role.VIEWER)
    identity = linked.oidc_identities.get()
    response = client.post(f"/admin/users/{other.pk}/identities/{identity.pk}/unlink/")
    assert response.status_code == 404
    assert f"{other.username} has no linked sign-in with id {identity.pk}" in (
        response.content.decode()
    )
    assert OidcIdentity.objects.exists()


def test_the_admin_api(linked, make_user, idp, as_user):  # noqa: F811
    admin = as_user(make_user(Role.ADMIN))
    listed = admin.get(f"/api/v1/admin/users/{linked.pk}/identities")
    assert listed.status_code == 200
    (item,) = listed.json()["items"]
    identity = linked.oidc_identities.get()
    assert item["id"] == identity.pk
    assert (item["issuer"], item["subject"], item["linked_by"]) == (idp.issuer, "alice", "created")
    assert item["created_at"] and item["last_used_at"]
    assert admin.get("/api/v1/admin/users/99999/identities").status_code == 404
    gone = admin.delete(f"/api/v1/admin/users/{linked.pk}/identities/{identity.pk}")
    assert gone.status_code == 204
    again = admin.delete(f"/api/v1/admin/users/{linked.pk}/identities/{identity.pk}")
    assert again.status_code == 404 and again.json()["code"] == "not_found"
    assert AuditLog.objects.filter(action="user.oidc_unlink").count() == 1


# --------------------------------------------------------------------------- check_oidc


def run_check() -> str:
    out = io.StringIO()
    call_command("check_oidc", stdout=out)
    return out.getvalue()


def test_check_oidc_prints_what_a_sign_in_uses(idp, sso, settings):  # noqa: F811
    sso(PROVIDER_LOGOUT="true", USERINFO="true", DEFAULT_ROLE="viewer")
    settings.FORKLIFT_PUBLIC_URL = "https://forklift.example.org"
    text = run_check()
    assert f"Issuer:                 {idp.issuer}" in text
    assert f"Token endpoint:         {idp.url('/token')} (client_secret_basic)" in text
    assert "UserInfo endpoint:      " + idp.url("/userinfo") + " (used)" in text
    assert "End-session endpoint:   " + idp.url("/logout") + " (used)" in text
    assert "ID token algorithms:    RS256, PS256, ES256" in text
    assert "Signing keys:           rsa-1 (RSA, RS256), ec-1 (EC, ES256)" in text
    assert "forklift-admins -> admin; forklift-authors -> author" in text
    assert "otherwise viewer)" in text
    assert "Redirect URI:           https://forklift.example.org/accounts/oidc/callback/" in text
    assert "Post-logout URI:        https://forklift.example.org/accounts/login/" in text
    assert "FORKLIFT_PUBLIC_URL is not set" not in text


def test_check_oidc_without_a_public_url(idp, sso):  # noqa: F811
    idp.discovery_overrides.update(userinfo_endpoint=REMOVE, end_session_endpoint=REMOVE)
    sso(VIEWER_GROUPS=None, ADMIN_GROUPS=None, AUTHOR_GROUPS=None, OPERATOR_GROUPS="ops")
    text = run_check()
    assert "UserInfo endpoint:      (none) (not used)" in text
    assert "End-session endpoint:   (none) (not used)" in text
    assert "Role claim:             groups (ops -> operator; otherwise refused)" in text
    assert "https://<the gateway's address>/accounts/oidc/callback/" in text
    assert "FORKLIFT_PUBLIC_URL is not set" in text and "Post-logout" not in text


def test_check_oidc_failures(idp, sso, settings):  # noqa: F811
    idp.raw["/certs"] = (200, {}, b'{"keys": [{"kid": "x", "use": "enc"}]}')
    with pytest.raises(CommandError, match="has no signing keys"):
        run_check()
    idp.discovery_overrides["issuer"] = "https://elsewhere.example.org"
    sso()
    with pytest.raises(CommandError, match="names the issuer 'https://elsewhere.example.org'"):
        run_check()
    settings.FORKLIFT_OIDC = None
    with pytest.raises(CommandError, match="Single sign-on is not configured"):
        run_check()
