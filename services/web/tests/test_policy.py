"""The authorization policy as a unit: roles, token narrowing, object rules and messages."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from forklift_web.core.choices import Classification, Role
from forklift_web.core.models import User
from forklift_web.errors import NotAuthenticated, PermissionDenied
from forklift_web.policy import (
    ACTION_SCOPES,
    ALL_SCOPES,
    ROLE_SCOPES,
    Action,
    Actor,
    allowed,
    check,
    scopes_for_role,
)


def user(role, *, raw_rows=False, pk=1):
    return User(pk=pk, role=role, can_view_raw_rows=raw_rows, username=f"{role}-{pk}")


def token(scopes, prefix="fkl_abcdefgh"):
    return SimpleNamespace(scopes=scopes, prefix=prefix)


def test_each_role_includes_the_one_before_it():
    order = [Role.VIEWER, Role.OPERATOR, Role.AUTHOR, Role.ADMIN]
    for lower, higher in zip(order, order[1:]):
        assert ROLE_SCOPES[lower] < ROLE_SCOPES[higher]
    assert ROLE_SCOPES[Role.ADMIN] == ALL_SCOPES
    assert scopes_for_role(Role.OPERATOR) == ROLE_SCOPES[Role.OPERATOR]


def test_every_action_needs_a_known_scope():
    assert set(ACTION_SCOPES) == set(Action)
    assert set(ACTION_SCOPES.values()) <= ALL_SCOPES


@pytest.mark.parametrize(
    "role,action,expected",
    [
        (Role.VIEWER, Action.DATASET_VIEW, True),
        (Role.VIEWER, Action.JOB_RUN, False),
        (Role.OPERATOR, Action.JOB_RUN, True),
        (Role.OPERATOR, Action.UPLOAD_CREATE, True),
        (Role.OPERATOR, Action.SCHEMA_EDIT, False),
        (Role.AUTHOR, Action.SCHEMA_EDIT, True),
        (Role.AUTHOR, Action.DATASET_EDIT, True),
        (Role.AUTHOR, Action.CONNECTION_MANAGE, False),
        (Role.ADMIN, Action.CONNECTION_MANAGE, True),
        (Role.ADMIN, Action.AUDIT_VIEW, True),
    ],
)
def test_roles(role, action, expected):
    assert allowed(Actor.for_user(user(role)), action) is expected


def test_a_token_only_narrows_its_owners_role():
    actor = Actor.for_user(user(Role.VIEWER), token=token(["jobs:read", "admin:write"]))
    assert actor.scopes == frozenset({"jobs:read"})
    assert allowed(actor, Action.JOB_VIEW)
    with pytest.raises(PermissionDenied) as raised:
        check(actor, Action.USER_MANAGE)
    assert raised.value.code == "role_insufficient"
    assert "the viewer role does not include" in raised.value.message


def test_a_missing_token_scope_names_the_token_and_its_scopes():
    actor = Actor.for_user(user(Role.ADMIN), token=token(["jobs:read"]))
    with pytest.raises(PermissionDenied) as raised:
        check(actor, Action.DATASET_EDIT)
    assert raised.value.code == "scope_missing"
    assert "datasets:write" in raised.value.message
    assert "fkl_abcdefgh..." in raised.value.message and "jobs:read" in raised.value.message
    empty = Actor.for_user(user(Role.ADMIN), token=token([]))
    with pytest.raises(PermissionDenied, match=r"its scopes: none"):
        check(empty, Action.JOB_VIEW)


def test_anonymous_and_system_actors():
    anonymous = Actor.anonymous(request_id="r1", ip="10.0.0.1")
    with pytest.raises(NotAuthenticated):
        check(anonymous, Action.JOB_VIEW)
    assert not allowed(anonymous, Action.JOB_VIEW)
    assert (anonymous.label, anonymous.role, anonymous.is_admin) == ("anonymous", None, False)
    assert not anonymous.can_view_raw_rows and not anonymous.owns(1)
    system = Actor.for_system("sweep_retention")
    check(system, Action.RETENTION_MANAGE, object())
    assert system.is_admin and system.can_view_raw_rows and system.is_authenticated
    assert system.label == "system:sweep_retention"
    assert Actor.for_user(user(Role.VIEWER, pk=7)).label == "viewer-7"


def test_uploads_are_used_by_their_uploader_or_an_admin():
    upload = SimpleNamespace(id="u1", uploaded_by_id=1)
    assert allowed(Actor.for_user(user(Role.OPERATOR, pk=1)), Action.UPLOAD_USE, upload)
    assert allowed(Actor.for_user(user(Role.ADMIN, pk=2)), Action.UPLOAD_USE, upload)
    with pytest.raises(PermissionDenied) as raised:
        check(Actor.for_user(user(Role.AUTHOR, pk=3)), Action.UPLOAD_USE, upload)
    assert raised.value.code == "not_owner" and "u1" in raised.value.message


def test_jobs_are_cancelled_by_their_requester_or_an_admin():
    job = SimpleNamespace(id="j1", requested_by_id=1)
    assert allowed(Actor.for_user(user(Role.OPERATOR, pk=1)), Action.JOB_CANCEL, job)
    assert allowed(Actor.for_user(user(Role.ADMIN, pk=2)), Action.JOB_CANCEL, job)
    assert not allowed(Actor.for_user(user(Role.OPERATOR, pk=3)), Action.JOB_CANCEL, job)


def test_tokens_are_managed_by_their_owner():
    owned = SimpleNamespace(owner_id=1, prefix="fkl_12345678")
    assert allowed(Actor.for_user(user(Role.VIEWER, pk=1)), Action.TOKEN_MANAGE, owned)
    with pytest.raises(PermissionDenied, match="belongs to another user"):
        check(Actor.for_user(user(Role.ADMIN, pk=2)), Action.TOKEN_MANAGE, owned)


def test_connections_are_limited_to_their_allowed_roles():
    connection = SimpleNamespace(name="exports", allowed_roles=["author"])
    assert allowed(Actor.for_user(user(Role.AUTHOR)), Action.CONNECTION_USE, connection)
    assert allowed(Actor.for_user(user(Role.ADMIN)), Action.CONNECTION_VIEW, connection)
    with pytest.raises(PermissionDenied, match="limited to: author"):
        check(Actor.for_user(user(Role.OPERATOR)), Action.CONNECTION_VIEW, connection)
    admins_only = SimpleNamespace(name="vault", allowed_roles=[])
    with pytest.raises(PermissionDenied, match="limited to: admins"):
        check(Actor.for_user(user(Role.AUTHOR)), Action.CONNECTION_USE, admins_only)


@pytest.mark.parametrize("role", [Role.OPERATOR, Role.AUTHOR])
def test_previewing_sensitive_data_needs_raw_rows_below_admin(role):
    without = Actor.for_user(user(role))
    with_raw = Actor.for_user(user(role, raw_rows=True))
    assert allowed(without, Action.JOB_PREVIEW, Classification.INTERNAL)
    assert not allowed(without, Action.JOB_PREVIEW, Classification.SENSITIVE)
    assert allowed(with_raw, Action.JOB_PREVIEW, Classification.SENSITIVE)


def test_admins_view_raw_rows_without_the_grant():
    admin = Actor.for_user(user(Role.ADMIN))
    assert admin.can_view_raw_rows
    assert allowed(admin, Action.JOB_PREVIEW, Classification.SENSITIVE)
    sensitive_rows = SimpleNamespace(
        kind="data", name="data.parquet", job=SimpleNamespace(classification="sensitive")
    )
    assert allowed(admin, Action.ARTIFACT_DOWNLOAD, sensitive_rows)
    # An admin's token carries it too, within the token's scopes
    narrowed = Actor.for_user(user(Role.ADMIN), token=token(["artifacts:read"]))
    assert allowed(narrowed, Action.ARTIFACT_DOWNLOAD, sensitive_rows)


@pytest.mark.parametrize(
    "kind,classification,raw_rows,expected",
    [
        ("data", Classification.SENSITIVE, False, False),
        ("bad_rows", Classification.SENSITIVE, False, False),
        ("preview", Classification.SENSITIVE, False, False),
        ("manifest", Classification.SENSITIVE, False, True),
        ("metadata", Classification.SENSITIVE, False, True),
        ("data", Classification.SENSITIVE, True, True),
        ("data", Classification.INTERNAL, False, True),
        ("bad_rows", Classification.PUBLIC, False, True),
    ],
)
def test_downloading_rows_of_sensitive_jobs_needs_raw_rows(
    kind, classification, raw_rows, expected
):
    artifact = SimpleNamespace(
        kind=kind, name=f"{kind}.parquet", job=SimpleNamespace(classification=classification)
    )
    actor = Actor.for_user(user(Role.VIEWER, raw_rows=raw_rows))
    assert allowed(actor, Action.ARTIFACT_DOWNLOAD, artifact) is expected
    if not expected:
        with pytest.raises(PermissionDenied) as raised:
            check(actor, Action.ARTIFACT_DOWNLOAD, artifact)
        assert raised.value.code == "raw_rows_required"
        assert "view raw rows" in raised.value.message


def test_without_an_object_only_role_and_scopes_are_checked():
    actor = Actor.for_user(user(Role.OPERATOR, pk=5))
    check(actor, Action.UPLOAD_USE)  # may use uploads at all
    check(actor, Action.JOB_PREVIEW)
