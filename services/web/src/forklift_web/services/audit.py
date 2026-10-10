"""The audit log: who changed what, and who downloaded what.

Every state-changing admin action, every change to schemas, datasets and tokens, every
cancellation, every artifact download, sign-ins and every retention deletion is recorded with
:func:`record`. Details name fields and counts, never secrets, tokens or cell values.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from forklift_web.core.models import AuditLog
from forklift_web.policy import Action, Actor, check


def _describe(obj: Any) -> tuple:
    if obj is None:
        return "", "", ""
    return type(obj).__name__.lower(), str(obj.pk), str(obj)[:300]


def record(actor: Actor, action: str, obj: Any = None, details: Optional[dict] = None) -> AuditLog:
    object_type, object_id, object_repr = _describe(obj)
    return AuditLog.objects.create(
        actor=actor.user,
        actor_label=actor.label,
        token_prefix=actor.token.prefix if actor.token is not None else "",
        action=action,
        object_type=object_type,
        object_id=object_id,
        object_repr=object_repr,
        details=details or {},
        request_id=actor.request_id,
        ip=actor.ip,
    )


def list_entries(
    actor: Actor,
    *,
    action: Optional[str] = None,
    actor_id: Optional[int] = None,
    object_type: Optional[str] = None,
    object_id: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
):
    """Audit entries, newest first, filtered by any of the arguments (admins only)."""
    check(actor, Action.AUDIT_VIEW)
    entries = AuditLog.objects.select_related("actor")
    if action:
        entries = entries.filter(action=action)
    if actor_id is not None:
        entries = entries.filter(actor_id=actor_id)
    if object_type:
        entries = entries.filter(object_type=object_type)
    if object_id:
        entries = entries.filter(object_id=object_id)
    if since is not None:
        entries = entries.filter(created_at__gte=since)
    if until is not None:
        entries = entries.filter(created_at__lt=until)
    return entries
