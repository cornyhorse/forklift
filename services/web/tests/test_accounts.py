"""Users, roles, sign-in and API tokens."""

from __future__ import annotations

from datetime import timedelta

import pytest
from django.test import Client
from django.utils import timezone
from world import PASSWORD

from forklift_web.core.choices import Role
from forklift_web.core.models import ApiToken, AuditLog, User
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Actor
from forklift_web.services import accounts, installation, tokens

pytestmark = pytest.mark.django_db


# --------------------------------------------------------------------------- users


def test_create_user_audits_without_the_password(admin_actor):
    user = accounts.create_user(
        admin_actor, username="ana", role=Role.OPERATOR, password=PASSWORD, email="a@x.org"
    )
    assert user.check_password(PASSWORD) and user.role == Role.OPERATOR
    entry = AuditLog.objects.get(action="user.create")
    assert entry.details["password_set"] is True
    assert PASSWORD not in str(entry.details) and entry.actor == admin_actor.user


def test_create_user_validation(admin_actor):
    with pytest.raises(InvalidRequest, match="Unknown role 'root'"):
        accounts.create_user(admin_actor, username="x", role="root")
    with pytest.raises(InvalidRequest, match="must not be empty"):
        accounts.create_user(admin_actor, username=" ", role=Role.VIEWER)
    with pytest.raises(InvalidRequest, match="password is not accepted"):
        accounts.create_user(admin_actor, username="weak", role=Role.VIEWER, password="123")
    with pytest.raises(InvalidRequest, match="Service accounts sign in with API tokens"):
        accounts.create_user(
            admin_actor,
            username="svc",
            role=Role.VIEWER,
            is_service_account=True,
            password=PASSWORD,
        )
    accounts.create_user(admin_actor, username="dup", role=Role.VIEWER)
    with pytest.raises(Conflict, match="already exists"):
        accounts.create_user(admin_actor, username="dup", role=Role.VIEWER)


def test_service_accounts_and_passwordless_users_cannot_sign_in(admin_actor):
    service = accounts.create_user(
        admin_actor, username="airflow", role=Role.OPERATOR, is_service_account=True
    )
    pending = accounts.create_user(admin_actor, username="new", role=Role.VIEWER)
    assert not service.has_usable_password() and not pending.has_usable_password()
    with pytest.raises(InvalidRequest, match="service account and has no password"):
        accounts.set_password(admin_actor, service.pk, PASSWORD)
    accounts.set_password(admin_actor, pending.pk, PASSWORD)
    assert User.objects.get(pk=pending.pk).check_password(PASSWORD)
    assert AuditLog.objects.filter(action="user.set_password", object_id=str(pending.pk)).exists()


def test_update_user(admin_actor, make_user):
    user = make_user(Role.VIEWER)
    updated = accounts.update_user(
        admin_actor, user.pk, role=Role.AUTHOR, can_view_raw_rows=True, email="new@x.org"
    )
    assert (updated.role, updated.can_view_raw_rows) == (Role.AUTHOR, True)
    entry = AuditLog.objects.get(action="user.update")
    assert entry.details["changed"]["role"] == {"from": "viewer", "to": "author"}
    accounts.update_user(admin_actor, user.pk, role=Role.AUTHOR)  # no change, no audit entry
    assert AuditLog.objects.filter(action="user.update").count() == 1
    with pytest.raises(InvalidRequest, match="cannot be changed here: password"):
        accounts.update_user(admin_actor, user.pk, password="x")
    with pytest.raises(InvalidRequest, match="Unknown role"):
        accounts.update_user(admin_actor, user.pk, role="owner")
    with pytest.raises(NotFound):
        accounts.update_user(admin_actor, 999999, role=Role.VIEWER)
    with pytest.raises(NotFound):
        accounts.get_user(admin_actor, 999999)


def test_the_last_active_admin_stays(admin_actor, make_user):
    me = admin_actor.user
    with pytest.raises(Conflict, match="last active admin"):
        accounts.update_user(admin_actor, me.pk, role=Role.VIEWER)
    with pytest.raises(Conflict, match="last active admin"):
        accounts.update_user(admin_actor, me.pk, is_active=False)
    accounts.update_user(admin_actor, me.pk, email="still-admin@x.org")
    other = make_user(Role.ADMIN)
    accounts.update_user(admin_actor, me.pk, role=Role.OPERATOR)
    assert User.objects.get(pk=me.pk).role == Role.OPERATOR
    assert other.role == Role.ADMIN


def test_admin_api_for_users(as_user, admin, make_user):
    caller = as_user(admin)
    roles = caller.get("/api/v1/admin/roles").json()
    assert [r["role"] for r in roles] == ["viewer", "operator", "author", "admin"]
    assert "admin:write" in roles[3]["scopes"] and "admin:write" not in roles[2]["scopes"]
    created = caller.post(
        "/api/v1/admin/users", {"username": "bo", "role": "author", "password": PASSWORD}
    )
    assert created.status_code == 201 and "password" not in created.json()
    user_id = created.json()["id"]
    patched = caller.patch(f"/api/v1/admin/users/{user_id}", {"can_view_raw_rows": True})
    assert patched.json()["can_view_raw_rows"] is True
    listed = caller.get("/api/v1/admin/users").json()
    assert {u["username"] for u in listed["items"]} >= {"bo", admin.username}
    bad = caller.post("/api/v1/admin/users", {"username": "x", "role": "root"})
    assert bad.status_code == 422  # not one of the four roles


# --------------------------------------------------------------------------- sign-in


def test_sign_in_and_out_are_audited(make_user):
    user = make_user(Role.VIEWER)
    client = Client()
    page = client.get("/accounts/login/")
    assert page.status_code == 200 and b"Sign in" in page.content
    failed = client.post("/accounts/login/", {"username": user.username, "password": "wrong"})
    assert failed.status_code == 200 and b"did not match" in failed.content
    entry = AuditLog.objects.get(action="user.login_failed")
    assert entry.details == {"username": user.username} and entry.actor is None
    signed_in = client.post("/accounts/login/", {"username": user.username, "password": PASSWORD})
    assert signed_in.status_code == 302
    assert AuditLog.objects.filter(action="user.login", actor=user).exists()
    home = client.get("/")
    assert home.status_code == 200 and user.username.encode() in home.content
    assert client.post("/accounts/logout/").status_code == 302
    assert AuditLog.objects.filter(action="user.logout", actor=user).exists()
    assert client.get("/").status_code == 302  # back to the sign-in page
    assert client.post("/accounts/logout/").status_code == 302  # signing out twice is harmless


def test_session_requests_that_change_something_need_the_csrf_token(make_user):
    client = Client(enforce_csrf_checks=True)
    client.get("/accounts/login/")  # sets the csrftoken cookie, as any page with a form does
    token = client.cookies["csrftoken"].value
    client.force_login(make_user(Role.VIEWER))
    assert client.get("/api/v1/me").status_code == 200
    body = {"name": "x", "scopes": ["jobs:read"]}
    refused = client.post("/api/v1/tokens", body, content_type="application/json")
    assert refused.status_code == 403 and b"CSRF" in refused.content
    accepted = client.post(
        "/api/v1/tokens", body, content_type="application/json", HTTP_X_CSRFTOKEN=token
    )
    assert accepted.status_code == 201


def test_a_bad_bearer_token_is_401_even_with_a_session(make_user):
    client = Client()
    client.force_login(make_user(Role.ADMIN))
    assert client.get("/api/v1/me", HTTP_AUTHORIZATION="Bearer fkl_nonsense").status_code == 401
    assert client.get("/api/v1/me", HTTP_AUTHORIZATION="Basic dXNlcjpwYXNz").status_code == 401
    anonymous = Client().get("/api/v1/me")
    assert anonymous.status_code == 401 and anonymous.json()["code"] == "not_authenticated"


# --------------------------------------------------------------------------- tokens


def test_token_lifecycle_through_the_api(as_user, as_token, make_user):
    owner = make_user(Role.OPERATOR)
    caller = as_user(owner)
    created = caller.post(
        "/api/v1/tokens", {"name": "airflow", "scopes": ["jobs:run", "jobs:read"]}
    )
    assert created.status_code == 201
    body = created.json()
    raw = body["token"]
    assert raw.startswith("fkl_") and raw[:12] == body["prefix"]
    stored = ApiToken.objects.get(pk=body["id"])
    assert raw not in stored.token_hash and stored.token_hash == tokens.hash_token(raw)
    assert "token" not in caller.get("/api/v1/tokens").json()["items"][0]

    me = as_token(raw).get("/api/v1/me").json()
    assert me["scopes"] == ["jobs:read", "jobs:run"]
    assert me["token"]["prefix"] == body["prefix"] and me["user"]["username"] == owner.username
    assert "uploads:write" in me["role_scopes"]

    assert caller.delete(f"/api/v1/tokens/{body['id']}").status_code == 204
    assert as_token(raw).get("/api/v1/me").status_code == 401
    again = caller.delete(f"/api/v1/tokens/{body['id']}")
    assert again.status_code == 409 and "already revoked" in again.json()["detail"]
    missing = caller.delete("/api/v1/tokens/00000000-0000-0000-0000-000000000000")
    assert missing.status_code == 404


def test_token_scopes_must_lie_within_the_role(make_user):
    viewer = Actor.for_user(make_user(Role.VIEWER))
    with pytest.raises(InvalidRequest, match="go beyond the viewer role"):
        accounts.create_token(viewer, name="x", scopes=["jobs:run"])
    with pytest.raises(InvalidRequest, match="Unknown scopes: jobs:delete"):
        accounts.create_token(viewer, name="x", scopes=["jobs:delete"])
    with pytest.raises(InvalidRequest, match="at least one scope"):
        accounts.create_token(viewer, name="x", scopes=[])
    with pytest.raises(InvalidRequest, match="needs a name"):
        accounts.create_token(viewer, name=" ", scopes=["jobs:read"])


def test_a_token_cannot_create_a_broader_token(make_user, api_token):
    owner = make_user(Role.ADMIN)
    token, raw = api_token(owner, ["tokens:write", "jobs:read"])
    actor = accounts.authenticate_api_token(raw)
    with pytest.raises(InvalidRequest, match="go beyond your token's scopes"):
        accounts.create_token(actor, name="wider", scopes=["admin:write"])
    narrower, _ = accounts.create_token(actor, name="narrower", scopes=["jobs:read"])
    assert narrower.scopes == ["jobs:read"] and narrower.created_by == owner
    assert AuditLog.objects.get(action="token.create").token_prefix == token.prefix


def test_token_expiry_rules(make_user, admin_actor):
    actor = Actor.for_user(make_user(Role.VIEWER))
    with pytest.raises(InvalidRequest, match="must be in the future"):
        accounts.create_token(
            actor, name="x", scopes=["jobs:read"], expires_at=timezone.now() - timedelta(days=1)
        )
    installation.update(admin_actor, {"token_max_days": 30})
    with pytest.raises(InvalidRequest, match="must expire within 30 days"):
        accounts.create_token(actor, name="x", scopes=["jobs:read"])
    with pytest.raises(InvalidRequest, match="must expire within 30 days"):
        accounts.create_token(
            actor, name="x", scopes=["jobs:read"], expires_at=timezone.now() + timedelta(days=31)
        )
    token, _ = accounts.create_token(
        actor, name="x", scopes=["jobs:read"], expires_at=timezone.now() + timedelta(days=29)
    )
    assert token.expires_at is not None


def test_token_authentication(make_user, api_token):
    owner = make_user(Role.OPERATOR)
    token, raw = api_token(owner, ["jobs:read"])
    actor = accounts.authenticate_api_token(raw, request_id="r-1", ip="10.1.2.3")
    assert actor.user == owner and actor.token == token and actor.ip == "10.1.2.3"
    first_use = ApiToken.objects.get(pk=token.pk).last_used_at
    assert first_use is not None
    accounts.authenticate_api_token(raw)  # used again within a minute: not written again
    assert ApiToken.objects.get(pk=token.pk).last_used_at == first_use
    ApiToken.objects.filter(pk=token.pk).update(last_used_at=first_use - timedelta(minutes=5))
    accounts.authenticate_api_token(raw)
    assert ApiToken.objects.get(pk=token.pk).last_used_at > first_use - timedelta(minutes=5)

    assert accounts.authenticate_api_token(raw[:-1] + ("A" if raw[-1] != "A" else "B")) is None
    assert accounts.authenticate_api_token("fkw_" + raw[4:]) is None  # a worker-token prefix
    assert accounts.authenticate_api_token("fkl_") is None
    owner.is_active = False
    owner.save()
    assert accounts.authenticate_api_token(raw) is None
    owner.is_active = True
    owner.save()
    ApiToken.objects.filter(pk=token.pk).update(expires_at=timezone.now() - timedelta(seconds=1))
    assert accounts.authenticate_api_token(raw) is None


def test_admins_manage_everyone_s_tokens(admin_actor, make_user):
    service = make_user(Role.OPERATOR, is_service_account=True)
    token, raw = accounts.create_token_for(
        admin_actor, owner_id=service.pk, name="airflow", scopes=["jobs:run"]
    )
    assert token.owner == service and raw.startswith("fkl_")
    with pytest.raises(InvalidRequest, match="go beyond the operator role"):
        accounts.create_token_for(
            admin_actor, owner_id=service.pk, name="x", scopes=["schemas:write"]
        )
    assert list(accounts.list_all_tokens(admin_actor, owner_id=service.pk)) == [token]
    assert token in accounts.list_all_tokens(admin_actor)
    revoked = accounts.revoke_any_token(admin_actor, token.pk)
    assert revoked.revoked_by == admin_actor.user
    assert AuditLog.objects.filter(action="admin.token.revoke").exists()
    with pytest.raises(NotFound):
        accounts.revoke_any_token(admin_actor, "00000000-0000-0000-0000-000000000000")


def test_token_prefix_collisions_are_retried_then_reported(monkeypatch, make_user):
    owner = make_user(Role.VIEWER)
    first, _ = tokens.create(ApiToken, tokens.API_TOKEN_PREFIX, owner=owner, name="a", scopes=[])
    real = tokens.generate
    calls = iter([("fkl_same", first.prefix, "x")] + [None] * 10)

    def colliding(prefix):
        value = next(calls)
        return value if value is not None else real(prefix)

    monkeypatch.setattr(tokens, "generate", colliding)
    second, raw = tokens.create(
        ApiToken, tokens.API_TOKEN_PREFIX, owner=owner, name="b", scopes=[]
    )
    assert second.prefix != first.prefix and raw.startswith("fkl_")
    monkeypatch.setattr(tokens, "generate", lambda prefix: ("fkl_same", first.prefix, "x"))
    with pytest.raises(RuntimeError, match="unused prefix after 5 attempts"):
        tokens.create(ApiToken, tokens.API_TOKEN_PREFIX, owner=owner, name="c", scopes=[])


def test_actor_for_a_session_request(rf, make_user):
    from django.contrib.auth.models import AnonymousUser

    from forklift_web.api.auth import actor_for_request

    request = rf.get("/")
    request.user = AnonymousUser()
    request.request_id, request.client_ip = "r-9", "10.0.0.9"
    anonymous = actor_for_request(request)
    assert not anonymous.is_authenticated and (anonymous.request_id, anonymous.ip) == (
        "r-9",
        "10.0.0.9",
    )
    request.user = make_user(Role.AUTHOR)
    actor = actor_for_request(request)
    assert actor.user == request.user and "schemas:write" in actor.scopes and actor.token is None
