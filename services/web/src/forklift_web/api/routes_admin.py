"""/api/v1/admin: users and roles, sign-in locks, everyone's API tokens, worker tokens and
workers, retention, the audit log, installation settings and everyone's webhooks (connections
are under /api/v1/connections)."""

import uuid
from datetime import datetime
from typing import Any, Optional

from ninja import Body, Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import (
    AdminTokenIn,
    AdminWebhookOut,
    AuditOut,
    PasswordIn,
    RetentionIn,
    RetentionOut,
    RetentionPolicyOut,
    RoleOut,
    SettingOut,
    SignInLockOut,
    SweepIn,
    SweepOut,
    TokenCreatedOut,
    TokenOut,
    UserIn,
    UserOut,
    UserPatch,
    WorkerOut,
    WorkerTokenCreatedOut,
    WorkerTokenIn,
    WorkerTokenOut,
)
from forklift_web.core.choices import RetentionScope
from forklift_web.services import (
    accounts,
    audit,
    installation,
    retention,
    sign_in,
    webhooks,
    workers,
)

router = Router(tags=["admin"])


# --------------------------------------------------------------------------- users


@router.get(
    "/roles", response=responses({200: list[RoleOut]}), summary="Roles and the scopes they grant"
)
def list_roles(request):
    return accounts.list_roles(request.auth)


@router.get("/users", response=responses({200: list[UserOut]}), summary="Users")
@paginate
def list_users(request):
    return accounts.list_users(request.auth)


@router.post("/users", response=responses({201: UserOut}), summary="Create a user")
def create_user(request, payload: UserIn):
    return Status(201, accounts.create_user(request.auth, **payload.model_dump()))


@router.get("/users/{user_id}", response=responses({200: UserOut}), summary="A user")
def get_user(request, user_id: int):
    return accounts.get_user(request.auth, user_id)


@router.patch(
    "/users/{user_id}",
    response=responses({200: UserOut}),
    summary="Change a user's role, raw-rows permission, details or active state",
)
def update_user(request, user_id: int, payload: UserPatch):
    changes = {key: value for key, value in payload.model_dump().items() if value is not None}
    return accounts.update_user(request.auth, user_id, **changes)


@router.post(
    "/users/{user_id}/password", response=responses({200: UserOut}), summary="Set a password"
)
def set_password(request, user_id: int, payload: PasswordIn):
    return accounts.set_password(request.auth, user_id, payload.password)


@router.get(
    "/sign-in-locks",
    response=responses({200: list[SignInLockOut]}),
    summary="Usernames and client addresses that may not sign in now (too many failures)",
)
@paginate
def list_sign_in_locks(request):
    return sign_in.active_locks(request.auth)


@router.delete(
    "/sign-in-locks/{lock_id}",
    response=responses({204: None}),
    summary="Clear a sign-in lock and its failure count",
)
def clear_sign_in_lock(request, lock_id: int):
    sign_in.clear(request.auth, lock_id)
    return Status(204, None)


# --------------------------------------------------------------------------- API tokens


@router.get("/tokens", response=responses({200: list[TokenOut]}), summary="Every API token")
@paginate
def list_tokens(request, owner_id: Optional[int] = None):
    return accounts.list_all_tokens(request.auth, owner_id=owner_id)


@router.post(
    "/tokens",
    response=responses({201: TokenCreatedOut}),
    summary="Create a token for any user, e.g. a service account",
)
def create_token(request, payload: AdminTokenIn):
    token, raw = accounts.create_token_for(request.auth, **payload.model_dump())
    token.token = raw
    return Status(201, token)


@router.post(
    "/tokens/{token_id}/revoke", response=responses({200: TokenOut}), summary="Revoke a token"
)
def revoke_token(request, token_id: uuid.UUID):
    return accounts.revoke_any_token(request.auth, token_id)


# --------------------------------------------------------------------------- workers


@router.get(
    "/worker-tokens", response=responses({200: list[WorkerTokenOut]}), summary="Worker tokens"
)
@paginate
def list_worker_tokens(request):
    return workers.list_worker_tokens(request.auth)


@router.post(
    "/worker-tokens",
    response=responses({201: WorkerTokenCreatedOut}),
    summary="Create a worker token (its value is returned only now)",
)
def create_worker_token(request, payload: WorkerTokenIn):
    token, raw = workers.create_worker_token(request.auth, **payload.model_dump())
    token.token = raw
    return Status(201, token)


@router.post(
    "/worker-tokens/{token_id}/revoke",
    response=responses({200: WorkerTokenOut}),
    summary="Revoke a worker token",
)
def revoke_worker_token(request, token_id: uuid.UUID):
    return workers.revoke_worker_token(request.auth, token_id)


@router.get("/workers", response=responses({200: list[WorkerOut]}), summary="Workers seen")
@paginate
def list_workers(request):
    return workers.list_workers(request.auth)


@router.get("/workers/{worker_pk}", response=responses({200: WorkerOut}), summary="A worker")
def get_worker(request, worker_pk: uuid.UUID):
    return workers.get_worker(request.auth, worker_pk)


# --------------------------------------------------------------------------- webhooks


@router.get(
    "/webhooks",
    response=responses({200: list[AdminWebhookOut]}),
    summary="Every user's webhooks, with their failure counts",
)
@paginate
def list_webhooks(request, active: Optional[bool] = None, owner_id: Optional[int] = None):
    return webhooks.list_all_webhooks(request.auth, active=active, owner_id=owner_id)


@router.post(
    "/webhooks/{webhook_id}/disable",
    response=responses({200: AdminWebhookOut}),
    summary="Disable any user's webhook (its owner can enable it again)",
)
def disable_webhook(request, webhook_id: uuid.UUID):
    return webhooks.disable_webhook(request.auth, webhook_id)


# --------------------------------------------------------------------------- retention


@router.get(
    "/retention",
    response=responses({200: RetentionOut}),
    summary="Retention policies, and warnings while sensitive data never expires",
)
def get_retention(request):
    return retention.overview(request.auth)


@router.put(
    "/retention/installation",
    response=responses({200: RetentionPolicyOut}),
    summary="Set the installation's retention policy",
)
def set_installation_retention(request, payload: RetentionIn):
    return retention.set_policy(request.auth, RetentionScope.INSTALLATION, days=payload.days)


@router.delete(
    "/retention/installation",
    response=responses({204: None}),
    summary="Remove the installation's retention policy",
)
def delete_installation_retention(request):
    retention.delete_policy(request.auth, RetentionScope.INSTALLATION)
    return Status(204, None)


@router.put(
    "/retention/classifications/{classification}",
    response=responses({200: RetentionPolicyOut}),
    summary="Set a classification's retention policy",
)
def set_classification_retention(request, classification: str, payload: RetentionIn):
    return retention.set_policy(
        request.auth,
        RetentionScope.CLASSIFICATION,
        classification=classification,
        days=payload.days,
    )


@router.delete(
    "/retention/classifications/{classification}",
    response=responses({204: None}),
    summary="Remove a classification's retention policy",
)
def delete_classification_retention(request, classification: str):
    retention.delete_policy(
        request.auth, RetentionScope.CLASSIFICATION, classification=classification
    )
    return Status(204, None)


@router.put(
    "/retention/datasets/{dataset_id}",
    response=responses({200: RetentionPolicyOut}),
    summary="Set a dataset's retention policy",
)
def set_dataset_retention(request, dataset_id: uuid.UUID, payload: RetentionIn):
    return retention.set_policy(
        request.auth, RetentionScope.DATASET, dataset_id=dataset_id, days=payload.days
    )


@router.delete(
    "/retention/datasets/{dataset_id}",
    response=responses({204: None}),
    summary="Remove a dataset's retention policy",
)
def delete_dataset_retention(request, dataset_id: uuid.UUID):
    retention.delete_policy(request.auth, RetentionScope.DATASET, dataset_id=dataset_id)
    return Status(204, None)


@router.post(
    "/retention/sweep",
    response=responses({200: SweepOut}),
    summary="Run the retention sweeper now (dry_run, the default, only counts)",
)
def sweep(request, payload: SweepIn):
    return retention.sweep(request.auth, dry_run=payload.dry_run)


# --------------------------------------------------------------------------- audit, settings


@router.get("/audit", response=responses({200: list[AuditOut]}), summary="The audit log")
@paginate
def list_audit(
    request,
    action: Optional[str] = None,
    actor_id: Optional[int] = None,
    object_type: Optional[str] = None,
    object_id: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
):
    return audit.list_entries(
        request.auth,
        action=action,
        actor_id=actor_id,
        object_type=object_type,
        object_id=object_id,
        since=since,
        until=until,
    )


@router.get(
    "/settings",
    response=responses({200: dict[str, SettingOut]}),
    summary="Installation settings with defaults and descriptions",
)
def get_settings(request):
    return installation.describe(request.auth)


@router.patch(
    "/settings",
    response=responses({200: dict[str, SettingOut]}),
    summary="Change installation settings (null resets one to its default)",
)
def update_settings(request, payload: dict[str, Any] = Body(...)):
    return installation.update(request.auth, payload)
