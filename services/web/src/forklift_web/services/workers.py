"""Worker tokens (created by admins, used only on /internal/v1) and the workers seen leasing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from django.utils import timezone

from forklift_web.core.models import Worker, WorkerToken
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, tokens


@dataclass(frozen=True)
class WorkerPrincipal:
    """A caller of the internal API, authenticated by its worker token."""

    token: WorkerToken
    request_id: str = ""
    ip: Optional[str] = None


def authenticate_worker_token(
    raw: str, *, request_id: str = "", ip: Optional[str] = None
) -> Optional[WorkerPrincipal]:
    """The principal for a worker token, or None if it is unknown, revoked or expired. API
    tokens are never accepted here."""
    token = tokens.find(WorkerToken.objects, raw, tokens.WORKER_TOKEN_PREFIX)
    if token is None or not token.is_usable():
        return None
    return WorkerPrincipal(token=token, request_id=request_id, ip=ip)


def list_worker_tokens(actor: Actor):
    check(actor, Action.WORKER_VIEW)
    return WorkerToken.objects.all()


def create_worker_token(actor: Actor, *, name: str, expires_at: Optional[datetime] = None):
    """A new worker token; returns (token, raw value). The raw value is shown only once."""
    check(actor, Action.WORKER_MANAGE)
    if not name.strip():
        raise InvalidRequest("A worker token needs a name (for example the worker pool's).")
    if expires_at is not None and expires_at <= timezone.now():
        raise InvalidRequest("expires_at must be in the future.")
    token, raw = tokens.create(
        WorkerToken,
        tokens.WORKER_TOKEN_PREFIX,
        name=name,
        expires_at=expires_at,
        created_by=actor.user,
    )
    audit.record(actor, "worker_token.create", token, {"expires_at": expires_at})
    return token, raw


def revoke_worker_token(actor: Actor, token_id) -> WorkerToken:
    """Revoke a worker token: its workers' next call fails, so their leases expire and the
    jobs return to the queue."""
    check(actor, Action.WORKER_MANAGE)
    token = WorkerToken.objects.filter(pk=token_id).first()
    if token is None:
        raise NotFound(f"There is no worker token with id {token_id}.")
    if token.revoked_at is not None:
        raise Conflict(f"Worker token {token.prefix}... was already revoked.")
    token.revoked_at = timezone.now()
    token.revoked_by = actor.user
    token.save(update_fields=["revoked_at", "revoked_by"])
    audit.record(actor, "worker_token.revoke", token)
    return token


def list_workers(actor: Actor):
    check(actor, Action.WORKER_VIEW)
    return Worker.objects.select_related("token")


def get_worker(actor: Actor, worker_pk) -> Worker:
    check(actor, Action.WORKER_VIEW)
    worker = Worker.objects.select_related("token").filter(pk=worker_pk).first()
    if worker is None:
        raise NotFound(f"There is no worker with id {worker_pk}.")
    return worker


def seen(principal: WorkerPrincipal, *, worker_id: str, **description) -> Worker:
    """Record that ``worker_id`` (using ``principal``'s token) called the internal API."""
    now = timezone.now()
    worker, created = Worker.objects.get_or_create(
        worker_id=worker_id,
        defaults={
            "token": principal.token,
            "first_seen_at": now,
            "last_seen_at": now,
            **description,
        },
    )
    if not created:
        Worker.objects.filter(pk=worker.pk).update(
            token=principal.token, last_seen_at=now, **description
        )
        worker.refresh_from_db()
    WorkerToken.objects.filter(pk=principal.token.pk).update(last_used_at=now)
    return worker
