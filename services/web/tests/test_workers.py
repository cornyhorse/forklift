"""Worker tokens and the workers seen leasing (admin side)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from django.utils import timezone
from world import World

from forklift_web.core.models import AuditLog
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.services import workers

pytestmark = pytest.mark.django_db


def test_worker_tokens(admin_actor, as_user):
    token, raw = workers.create_worker_token(admin_actor, name="batch pool")
    assert raw.startswith("fkw_") and token in workers.list_worker_tokens(admin_actor)
    with pytest.raises(InvalidRequest, match="needs a name"):
        workers.create_worker_token(admin_actor, name=" ")
    with pytest.raises(InvalidRequest, match="must be in the future"):
        workers.create_worker_token(
            admin_actor, name="x", expires_at=timezone.now() - timedelta(days=1)
        )
    revoked = workers.revoke_worker_token(admin_actor, token.pk)
    assert revoked.revoked_at and revoked.revoked_by == admin_actor.user
    assert AuditLog.objects.filter(action="worker_token.revoke").count() == 1
    with pytest.raises(Conflict, match="already revoked"):
        workers.revoke_worker_token(admin_actor, token.pk)
    with pytest.raises(NotFound):
        workers.revoke_worker_token(admin_actor, "00000000-0000-0000-0000-000000000000")
    created = as_user(admin_actor.user).post("/api/v1/admin/worker-tokens", {"name": "pool"})
    assert created.status_code == 201 and created.json()["token"].startswith("fkw_")
    listed = as_user(admin_actor.user).get("/api/v1/admin/worker-tokens").json()
    assert all("token" not in item for item in listed["items"])


def test_workers(admin_actor):
    world = World.build()
    assert list(workers.list_workers(admin_actor)) == [world.worker]
    assert workers.get_worker(admin_actor, world.worker.pk) == world.worker
    with pytest.raises(NotFound):
        workers.get_worker(admin_actor, "00000000-0000-0000-0000-000000000000")
