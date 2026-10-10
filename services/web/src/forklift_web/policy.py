"""The one authorization policy of the gateway: role x action x object.

Every permission decision, in the API and in the HTML views, goes through :func:`check` (raise)
or :func:`allowed` (bool). Three rules combine:

1. **Role and scopes.** Each action needs one scope (``Action`` -> ``ACTION_SCOPES``). A role
   grants a fixed set of scopes (``ROLE_SCOPES``; each role includes the one before it). A
   session has all of its user's role scopes; an API token has the intersection of its own
   scopes and its owner's role, so a token can only narrow its owner's role, never widen it,
   and a demotion narrows the owner's tokens at once.
2. **Raw rows.** Previewing ``sensitive`` data and downloading the rows of a ``sensitive`` job
   (``data``, ``bad_rows``, ``preview`` artifacts) also need the user's "view raw rows"
   permission, whatever the role (admins included: they can grant it to themselves, audited).
3. **Objects.** Uploads are used and seen by their uploader (and admins); a job is cancelled by
   whoever requested it (or an admin); a connection is seen and used by the roles it allows
   (admins always); an API token is managed by its owner (and admins, through admin actions).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Optional

from forklift_web.core.choices import RAW_ROW_KINDS, Classification, Role
from forklift_web.errors import NotAuthenticated, PermissionDenied


class Scope(StrEnum):
    SCHEMAS_READ = "schemas:read"
    SCHEMAS_WRITE = "schemas:write"
    DATASETS_READ = "datasets:read"
    DATASETS_WRITE = "datasets:write"
    CONNECTIONS_READ = "connections:read"
    UPLOADS_READ = "uploads:read"
    UPLOADS_WRITE = "uploads:write"
    JOBS_READ = "jobs:read"
    JOBS_RUN = "jobs:run"
    ARTIFACTS_READ = "artifacts:read"
    TOKENS_READ = "tokens:read"
    TOKENS_WRITE = "tokens:write"
    ADMIN_READ = "admin:read"
    ADMIN_WRITE = "admin:write"


_VIEWER = frozenset(
    {
        Scope.SCHEMAS_READ,
        Scope.DATASETS_READ,
        Scope.CONNECTIONS_READ,
        Scope.JOBS_READ,
        Scope.ARTIFACTS_READ,
        Scope.TOKENS_READ,
        Scope.TOKENS_WRITE,
    }
)
_OPERATOR = _VIEWER | {Scope.UPLOADS_READ, Scope.UPLOADS_WRITE, Scope.JOBS_RUN}
_AUTHOR = _OPERATOR | {Scope.SCHEMAS_WRITE, Scope.DATASETS_WRITE}
_ADMIN = _AUTHOR | {Scope.ADMIN_READ, Scope.ADMIN_WRITE}

ROLE_SCOPES: dict[str, frozenset] = {
    Role.VIEWER: _VIEWER,
    Role.OPERATOR: _OPERATOR,
    Role.AUTHOR: _AUTHOR,
    Role.ADMIN: _ADMIN,
}
ALL_SCOPES = frozenset(Scope)


class Action(StrEnum):
    # Schemas
    SCHEMA_VIEW = "schema.view"
    SCHEMA_EDIT = "schema.edit"  # create a schema, change its description, add a version
    SCHEMA_VALIDATE = "schema.validate"  # enqueue a validate_schema job
    # Connections
    CONNECTION_VIEW = "connection.view"
    CONNECTION_USE = "connection.use"  # reference it from a dataset
    CONNECTION_MANAGE = "connection.manage"  # create, change, delete, test
    # Datasets
    DATASET_VIEW = "dataset.view"
    DATASET_EDIT = "dataset.edit"
    DATASET_RUN = "dataset.run"
    # Uploads
    UPLOAD_CREATE = "upload.create"
    UPLOAD_VIEW = "upload.view"
    UPLOAD_CHANGE = "upload.change"  # complete, refresh part URLs, delete
    UPLOAD_USE = "upload.use"  # as the input of a job
    # Jobs and artifacts
    JOB_VIEW = "job.view"
    JOB_RUN = "job.run"  # enqueue run / validate_schema / generate_schema jobs
    JOB_PREVIEW = "job.preview"
    JOB_CANCEL = "job.cancel"
    ARTIFACT_VIEW = "artifact.view"
    ARTIFACT_DOWNLOAD = "artifact.download"
    # The caller's own API tokens
    TOKEN_VIEW = "token.view"
    TOKEN_MANAGE = "token.manage"
    # Administration
    USER_VIEW = "user.view"
    USER_MANAGE = "user.manage"
    ANY_TOKEN_VIEW = "any_token.view"
    ANY_TOKEN_MANAGE = "any_token.manage"
    WORKER_VIEW = "worker.view"
    WORKER_MANAGE = "worker.manage"
    RETENTION_VIEW = "retention.view"
    RETENTION_MANAGE = "retention.manage"
    AUDIT_VIEW = "audit.view"
    SETTINGS_VIEW = "settings.view"
    SETTINGS_MANAGE = "settings.manage"


ACTION_SCOPES: dict[Action, Scope] = {
    Action.SCHEMA_VIEW: Scope.SCHEMAS_READ,
    Action.SCHEMA_EDIT: Scope.SCHEMAS_WRITE,
    Action.SCHEMA_VALIDATE: Scope.JOBS_RUN,
    Action.CONNECTION_VIEW: Scope.CONNECTIONS_READ,
    Action.CONNECTION_USE: Scope.DATASETS_WRITE,
    Action.CONNECTION_MANAGE: Scope.ADMIN_WRITE,
    Action.DATASET_VIEW: Scope.DATASETS_READ,
    Action.DATASET_EDIT: Scope.DATASETS_WRITE,
    Action.DATASET_RUN: Scope.JOBS_RUN,
    Action.UPLOAD_CREATE: Scope.UPLOADS_WRITE,
    Action.UPLOAD_VIEW: Scope.UPLOADS_READ,
    Action.UPLOAD_CHANGE: Scope.UPLOADS_WRITE,
    Action.UPLOAD_USE: Scope.JOBS_RUN,
    Action.JOB_VIEW: Scope.JOBS_READ,
    Action.JOB_RUN: Scope.JOBS_RUN,
    Action.JOB_PREVIEW: Scope.JOBS_RUN,
    Action.JOB_CANCEL: Scope.JOBS_RUN,
    Action.ARTIFACT_VIEW: Scope.JOBS_READ,
    Action.ARTIFACT_DOWNLOAD: Scope.ARTIFACTS_READ,
    Action.TOKEN_VIEW: Scope.TOKENS_READ,
    Action.TOKEN_MANAGE: Scope.TOKENS_WRITE,
    Action.USER_VIEW: Scope.ADMIN_READ,
    Action.USER_MANAGE: Scope.ADMIN_WRITE,
    Action.ANY_TOKEN_VIEW: Scope.ADMIN_READ,
    Action.ANY_TOKEN_MANAGE: Scope.ADMIN_WRITE,
    Action.WORKER_VIEW: Scope.ADMIN_READ,
    Action.WORKER_MANAGE: Scope.ADMIN_WRITE,
    Action.RETENTION_VIEW: Scope.ADMIN_READ,
    Action.RETENTION_MANAGE: Scope.ADMIN_WRITE,
    Action.AUDIT_VIEW: Scope.ADMIN_READ,
    Action.SETTINGS_VIEW: Scope.ADMIN_READ,
    Action.SETTINGS_MANAGE: Scope.ADMIN_WRITE,
}


@dataclass(frozen=True)
class Actor:
    """Who is asking: a user (by session or API token), or the system (management commands).

    ``scopes`` are the effective scopes: the role's for a session, role and token intersected
    for a token. ``request_id`` and ``ip`` go into the audit log.
    """

    user: Any = None
    scopes: frozenset = frozenset()
    token: Any = None
    request_id: str = ""
    ip: Optional[str] = None
    system: str = ""

    @classmethod
    def for_user(cls, user, *, token=None, request_id: str = "", ip: Optional[str] = None):
        scopes = ROLE_SCOPES[user.role]
        if token is not None:
            scopes = scopes & frozenset(token.scopes)
        return cls(user=user, scopes=scopes, token=token, request_id=request_id, ip=ip)

    @classmethod
    def for_system(cls, name: str) -> "Actor":
        """The gateway itself (a management command or the sweeper) acting as ``name``."""
        return cls(scopes=ALL_SCOPES, system=name)

    @classmethod
    def anonymous(cls, *, request_id: str = "", ip: Optional[str] = None) -> "Actor":
        return cls(request_id=request_id, ip=ip)

    @property
    def is_system(self) -> bool:
        return bool(self.system)

    @property
    def is_authenticated(self) -> bool:
        return self.is_system or self.user is not None

    @property
    def role(self) -> Optional[str]:
        return self.user.role if self.user is not None else None

    @property
    def is_admin(self) -> bool:
        return self.is_system or self.role == Role.ADMIN

    @property
    def can_view_raw_rows(self) -> bool:
        return self.is_system or bool(self.user is not None and self.user.can_view_raw_rows)

    @property
    def label(self) -> str:
        if self.is_system:
            return f"system:{self.system}"
        if self.user is None:
            return "anonymous"
        return self.user.username

    def owns(self, user_id) -> bool:
        return self.user is not None and user_id == self.user.pk


# --------------------------------------------------------------------------- object rules


def _raw_rows_needed(what: str) -> PermissionDenied:
    return PermissionDenied(
        f"{what} is sensitive data and needs the 'view raw rows' permission, which your account "
        "does not have; an admin can grant it.",
        code="raw_rows_required",
    )


def _upload_owner(actor: Actor, upload) -> None:
    if not (actor.is_admin or actor.owns(upload.uploaded_by_id)):
        raise PermissionDenied(
            f"Upload {upload.id} belongs to another user; only its uploader or an admin can "
            "use it.",
            code="not_owner",
        )


def _job_owner(actor: Actor, job) -> None:
    if not (actor.is_admin or actor.owns(job.requested_by_id)):
        raise PermissionDenied(
            f"Job {job.id} was requested by another user; only its requester or an admin can "
            "cancel it.",
            code="not_owner",
        )


def _token_owner(actor: Actor, token) -> None:
    if not actor.owns(token.owner_id):
        raise PermissionDenied(
            f"API token {token.prefix}... belongs to another user; admins manage other users' "
            "tokens through /api/v1/admin/tokens.",
            code="not_owner",
        )


def _connection_role(actor: Actor, connection) -> None:
    if not (actor.is_admin or actor.role in connection.allowed_roles):
        raise PermissionDenied(
            f"Connection {connection.name!r} is not available to the {actor.role} role (it is "
            f"limited to: {', '.join(connection.allowed_roles) or 'admins'}).",
            code="connection_not_allowed",
        )


def _preview_classification(actor: Actor, classification: str) -> None:
    if classification == Classification.SENSITIVE and not actor.can_view_raw_rows:
        raise _raw_rows_needed("Previewing this input")


def _artifact_download(actor: Actor, artifact) -> None:
    if (
        artifact.kind in RAW_ROW_KINDS
        and artifact.job.classification == Classification.SENSITIVE
        and not actor.can_view_raw_rows
    ):
        raise _raw_rows_needed(f"The {artifact.kind} artifact {artifact.name!r}")


OBJECT_RULES: dict[Action, Callable[[Actor, Any], None]] = {
    Action.UPLOAD_VIEW: _upload_owner,
    Action.UPLOAD_CHANGE: _upload_owner,
    Action.UPLOAD_USE: _upload_owner,
    Action.JOB_CANCEL: _job_owner,
    Action.JOB_PREVIEW: _preview_classification,
    Action.ARTIFACT_DOWNLOAD: _artifact_download,
    Action.TOKEN_MANAGE: _token_owner,
    Action.CONNECTION_VIEW: _connection_role,
    Action.CONNECTION_USE: _connection_role,
}


# --------------------------------------------------------------------------- decisions


def check(actor: Actor, action: Action, obj: Any = None) -> None:
    """Raise unless ``actor`` may do ``action`` (to ``obj``, when the action has object rules).

    ``obj`` is the model instance the action is about (for ``JOB_PREVIEW``: the input's
    classification). Without ``obj``, only the role and scopes are checked: that is the
    question "may this actor do this kind of thing at all?", for example to show a button.
    """
    if not actor.is_authenticated:
        raise NotAuthenticated("Sign in or send an API token (Authorization: Bearer ...).")
    if actor.is_system:
        return
    scope = ACTION_SCOPES[action]
    if scope not in actor.scopes:
        if actor.token is not None and scope in ROLE_SCOPES[actor.role]:
            raise PermissionDenied(
                f"This needs the {scope} scope, which API token {actor.token.prefix}... does "
                f"not have (its scopes: {', '.join(sorted(actor.token.scopes)) or 'none'}).",
                code="scope_missing",
            )
        raise PermissionDenied(
            f"This needs the {scope} scope, which the {actor.role} role does not include.",
            code="role_insufficient",
        )
    rule = OBJECT_RULES.get(action)
    if rule is not None and obj is not None:
        rule(actor, obj)


def allowed(actor: Actor, action: Action, obj: Any = None) -> bool:
    try:
        check(actor, action, obj)
    except (NotAuthenticated, PermissionDenied):
        return False
    return True


def visible_connections(actor: Actor, queryset):
    """The connections ``actor`` may see (all of them for admins)."""
    check(actor, Action.CONNECTION_VIEW)
    if actor.is_admin:
        return queryset
    return queryset.filter(allowed_roles__contains=[actor.role])


def visible_uploads(actor: Actor, queryset):
    """The uploads ``actor`` may see: their own (all of them for admins)."""
    check(actor, Action.UPLOAD_VIEW)
    if actor.is_admin:
        return queryset
    return queryset.filter(uploaded_by=actor.user)


def scopes_for_role(role: str) -> frozenset:
    return ROLE_SCOPES[role]
