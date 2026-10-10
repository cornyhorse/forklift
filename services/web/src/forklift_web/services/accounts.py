"""Users, roles and API tokens.

Admins manage users (role, "view raw rows", active) and everyone's tokens; every user manages
their own API tokens. Token scopes must lie within the owner's role (and, when a token creates a
token, within the creating token's scopes), and stay narrowed by the role afterwards.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, Optional

from django.contrib.auth import password_validation
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from forklift_web.core.choices import ROLE_ORDER, Role
from forklift_web.core.models import ApiToken, User
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import ALL_SCOPES, ROLE_SCOPES, Action, Actor, check
from forklift_web.services import audit, installation, tokens

USER_FIELDS = ("email", "first_name", "last_name", "role", "can_view_raw_rows", "is_active")


# --------------------------------------------------------------------------- authentication


def authenticate_api_token(
    raw: str, *, request_id: str = "", ip: Optional[str] = None
) -> Optional[Actor]:
    """The actor an API token stands for, or None if it is unknown, revoked or expired or its
    owner is inactive. Worker tokens are never accepted here."""
    token = tokens.find(ApiToken.objects.select_related("owner"), raw, tokens.API_TOKEN_PREFIX)
    now = timezone.now()
    if token is None or not token.is_usable(now) or not token.owner.is_active:
        return None
    if token.last_used_at is None or now - token.last_used_at > timedelta(minutes=1):
        ApiToken.objects.filter(pk=token.pk).update(last_used_at=now)
    return Actor.for_user(token.owner, token=token, request_id=request_id, ip=ip)


# --------------------------------------------------------------------------- users


def list_roles(actor: Actor) -> list:
    """The four roles, each with the scopes it grants (for role pickers and token forms)."""
    check(actor, Action.USER_VIEW)
    return [
        {"role": role.value, "label": role.label, "scopes": sorted(ROLE_SCOPES[role])}
        for role in ROLE_ORDER
    ]


def list_users(actor: Actor):
    check(actor, Action.USER_VIEW)
    return User.objects.all()


def get_user(actor: Actor, user_id: int) -> User:
    check(actor, Action.USER_VIEW)
    user = User.objects.filter(pk=user_id).first()
    if user is None:
        raise NotFound(f"There is no user with id {user_id}.")
    return user


def _validate_role(role: str) -> str:
    if role not in Role.values:
        raise InvalidRequest(f"Unknown role {role!r}; roles are: {', '.join(Role.values)}.")
    return role


def _validate_password(password: str, user: User) -> None:
    try:
        password_validation.validate_password(password, user)
    except ValidationError as error:
        raise InvalidRequest("The password is not accepted: " + " ".join(error.messages))


def create_user(
    actor: Actor,
    *,
    username: str,
    role: str,
    email: str = "",
    first_name: str = "",
    last_name: str = "",
    can_view_raw_rows: bool = False,
    is_service_account: bool = False,
    password: Optional[str] = None,
) -> User:
    """A new account. Service accounts get no password (they use API tokens only)."""
    check(actor, Action.USER_MANAGE)
    _validate_role(role)
    if not username.strip():
        raise InvalidRequest("A username must not be empty.")
    user = User(
        username=username,
        email=email,
        first_name=first_name,
        last_name=last_name,
        role=role,
        can_view_raw_rows=can_view_raw_rows,
        is_service_account=is_service_account,
    )
    if is_service_account:
        if password:
            raise InvalidRequest("Service accounts sign in with API tokens; give no password.")
        user.set_unusable_password()
    elif password:
        _validate_password(password, user)
        user.set_password(password)
    else:
        user.set_unusable_password()
    try:
        with transaction.atomic():
            user.save()
            audit.record(
                actor,
                "user.create",
                user,
                {
                    "role": role,
                    "can_view_raw_rows": can_view_raw_rows,
                    "is_service_account": is_service_account,
                    "password_set": bool(password),
                },
            )
    except IntegrityError:
        raise Conflict(f"A user named {username!r} already exists.") from None
    return user


def _other_active_admins(user: User) -> bool:
    return User.objects.filter(role=Role.ADMIN, is_active=True).exclude(pk=user.pk).exists()


def update_user(actor: Actor, user_id: int, **changes) -> User:
    """Change any of ``USER_FIELDS``. The last active admin cannot be demoted or deactivated."""
    check(actor, Action.USER_MANAGE)
    unknown = sorted(set(changes) - set(USER_FIELDS))
    if unknown:
        raise InvalidRequest(
            f"These user fields cannot be changed here: {', '.join(unknown)} (changeable: "
            f"{', '.join(USER_FIELDS)})."
        )
    if "role" in changes:
        _validate_role(changes["role"])
    with transaction.atomic():
        user = User.objects.select_for_update().filter(pk=user_id).first()
        if user is None:
            raise NotFound(f"There is no user with id {user_id}.")
        loses_admin = (
            user.role == Role.ADMIN
            and user.is_active
            and (
                changes.get("role", Role.ADMIN) != Role.ADMIN or changes.get("is_active") is False
            )
        )
        if loses_admin and not _other_active_admins(user):
            raise Conflict(
                f"{user.username} is the last active admin; make another user an admin first."
            )
        changed = {}
        for field, value in changes.items():
            if getattr(user, field) != value:
                changed[field] = {"from": getattr(user, field), "to": value}
                setattr(user, field, value)
        if changed:
            user.save(update_fields=list(changed))
            audit.record(actor, "user.update", user, {"changed": changed})
    return user


def set_password(actor: Actor, user_id: int, password: str) -> User:
    check(actor, Action.USER_MANAGE)
    user = get_user(actor, user_id)
    if user.is_service_account:
        raise InvalidRequest(f"{user.username} is a service account and has no password.")
    _validate_password(password, user)
    user.set_password(password)
    user.save(update_fields=["password"])
    audit.record(actor, "user.set_password", user)
    return user


# --------------------------------------------------------------------------- API tokens


def _validate_scopes(scopes: Iterable[str], allowed: frozenset, whose: str) -> list:
    scopes = sorted(set(scopes))
    if not scopes:
        raise InvalidRequest(
            "A token needs at least one scope (available: " + ", ".join(sorted(allowed)) + ")."
        )
    unknown = [scope for scope in scopes if scope not in ALL_SCOPES]
    if unknown:
        raise InvalidRequest(
            f"Unknown scopes: {', '.join(unknown)} (known: {', '.join(sorted(ALL_SCOPES))})."
        )
    beyond = [scope for scope in scopes if scope not in allowed]
    if beyond:
        raise InvalidRequest(
            f"Scopes {', '.join(beyond)} go beyond {whose}; a token can only narrow its "
            "owner's role."
        )
    return scopes


def _validate_expiry(expires_at: Optional[datetime]) -> Optional[datetime]:
    now = timezone.now()
    max_days = installation.get("token_max_days")
    if expires_at is not None and expires_at <= now:
        raise InvalidRequest("expires_at must be in the future.")
    if max_days is not None:
        latest = now + timedelta(days=max_days)
        if expires_at is None or expires_at > latest:
            raise InvalidRequest(
                f"Tokens on this installation must expire within {max_days} days; give an "
                "expires_at before " + latest.isoformat() + "."
            )
    return expires_at


def _create_token(actor: Actor, owner: User, name: str, scopes, expires_at, *, action: str):
    if not name.strip():
        raise InvalidRequest("A token needs a name, so that it can be recognised later.")
    token, raw = tokens.create(
        ApiToken,
        tokens.API_TOKEN_PREFIX,
        owner=owner,
        name=name,
        scopes=scopes,
        expires_at=_validate_expiry(expires_at),
        created_by=actor.user,
    )
    audit.record(
        actor,
        action,
        token,
        {"owner": owner.username, "scopes": scopes, "expires_at": expires_at},
    )
    return token, raw


def list_tokens(actor: Actor):
    """The caller's own API tokens."""
    check(actor, Action.TOKEN_VIEW)
    return ApiToken.objects.filter(owner=actor.user)


def create_token(
    actor: Actor, *, name: str, scopes: Iterable[str], expires_at: Optional[datetime] = None
):
    """A token for the caller; returns (token, raw value). The raw value is shown only once."""
    check(actor, Action.TOKEN_MANAGE)
    whose = "your token's scopes" if actor.token is not None else f"the {actor.role} role"
    scopes = _validate_scopes(scopes, actor.scopes, whose)
    return _create_token(actor, actor.user, name, scopes, expires_at, action="token.create")


def _revoke(actor: Actor, token: ApiToken, action: str) -> ApiToken:
    if token.revoked_at is not None:
        raise Conflict(f"API token {token.prefix}... was already revoked.")
    token.revoked_at = timezone.now()
    token.revoked_by = actor.user
    token.save(update_fields=["revoked_at", "revoked_by"])
    audit.record(actor, action, token, {"owner_id": token.owner_id})
    return token


def revoke_token(actor: Actor, token_id) -> ApiToken:
    """Revoke one of the caller's own tokens."""
    check(actor, Action.TOKEN_MANAGE)
    token = ApiToken.objects.filter(pk=token_id).first()
    if token is None:
        raise NotFound(f"There is no API token with id {token_id}.")
    check(actor, Action.TOKEN_MANAGE, token)
    return _revoke(actor, token, "token.revoke")


def list_all_tokens(actor: Actor, *, owner_id: Optional[int] = None):
    check(actor, Action.ANY_TOKEN_VIEW)
    found = ApiToken.objects.select_related("owner")
    return found.filter(owner_id=owner_id) if owner_id is not None else found


def create_token_for(
    actor: Actor,
    *,
    owner_id: int,
    name: str,
    scopes: Iterable[str],
    expires_at: Optional[datetime] = None,
):
    """An admin creates a token for any user, typically a service account."""
    check(actor, Action.ANY_TOKEN_MANAGE)
    owner = get_user(actor, owner_id)
    scopes = _validate_scopes(scopes, ROLE_SCOPES[owner.role], f"the {owner.role} role")
    return _create_token(actor, owner, name, scopes, expires_at, action="admin.token.create")


def revoke_any_token(actor: Actor, token_id) -> ApiToken:
    check(actor, Action.ANY_TOKEN_MANAGE)
    token = ApiToken.objects.filter(pk=token_id).first()
    if token is None:
        raise NotFound(f"There is no API token with id {token_id}.")
    return _revoke(actor, token, "admin.token.revoke")
