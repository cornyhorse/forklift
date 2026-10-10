"""Management commands, the forklift-web entry point, health checks and the OpenAPI document."""

from __future__ import annotations

import json
import runpy
import sys
from datetime import timedelta
from io import StringIO
from pathlib import Path

import pytest
from django.core.management import CommandError, call_command
from django.db import DatabaseError
from django.utils import timezone
from world import World, make_job, make_upload, make_user

from forklift_web import secret_backend
from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.management.commands import export_openapi, sweep_retention
from forklift_web.core.models import AuditLog, Connection, Job, User, WorkerToken
from forklift_web.services import accounts, connections, tokens

REPOSITORY = Path(__file__).resolve().parents[3]


def run(*args, **options) -> tuple:
    out, err = StringIO(), StringIO()
    call_command(*args, stdout=out, stderr=err, **options)
    return out.getvalue(), err.getvalue()


@pytest.mark.django_db
def test_bootstrap_admin(monkeypatch):
    monkeypatch.delenv("FORKLIFT_ADMIN_PASSWORD", raising=False)
    with pytest.raises(CommandError, match="Set the environment variable FORKLIFT_ADMIN_PASSWORD"):
        run("bootstrap_admin", "--username", "root")
    monkeypatch.setenv("FORKLIFT_ADMIN_PASSWORD", "correct horse battery staple")
    out, _ = run("bootstrap_admin", "--username", "root", "--email", "root@example.org")
    assert "Created the admin 'root'" in out
    admin = User.objects.get(username="root")
    assert admin.role == Role.ADMIN and admin.check_password("correct horse battery staple")
    entry = AuditLog.objects.get(action="user.create")
    assert entry.actor_label == "system:bootstrap_admin"
    out, _ = run("bootstrap_admin", "--username", "root")
    assert "already exists; nothing to do" in out
    make_user(Role.VIEWER, name="ann")
    with pytest.raises(CommandError, match="exists with the role viewer"):
        run("bootstrap_admin", "--username", "ann")
    monkeypatch.setenv("WEAK", "123")
    with pytest.raises(CommandError, match="password is not accepted"):
        run("bootstrap_admin", "--username", "bob", "--password-env", "WEAK")


@pytest.mark.django_db
def test_create_worker_token():
    out, err = run("create_worker_token", "--name", "batch pool", "--expires-days", "30")
    raw = out.strip()
    token = WorkerToken.objects.get(name="batch pool")
    assert raw.startswith("fkw_") and token.prefix == raw[:12] and token.prefix in err
    assert token.expires_at > timezone.now() + timedelta(days=29)
    assert tokens.hash_token(raw) == token.token_hash
    assert AuditLog.objects.get(action="worker_token.create").actor_label.startswith("system:")
    out, _ = run("create_worker_token", "--name", "forever")
    assert WorkerToken.objects.get(name="forever").expires_at is None


@pytest.mark.django_db
def test_requeue_expired_leases(worker_token):
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    Job.objects.filter(pk=job.pk).update(
        status=JobStatus.RUNNING, attempt=1, lease_expires_at=timezone.now() - timedelta(seconds=5)
    )
    out, _ = run("requeue_expired_leases")
    assert "Requeued 1, failed 0 and cancelled 0" in out
    assert Job.objects.get(pk=job.pk).status == JobStatus.QUEUED


@pytest.mark.django_db
def test_sweep_retention_once_and_in_a_loop(monkeypatch):
    World.build()
    out, _ = run("sweep_retention", "--dry-run")
    assert out.startswith("Would delete 0 expired pending uploads")
    sleeps = []
    monkeypatch.setattr(sweep_retention, "_sleep", sleeps.append)
    out, _ = run("sweep_retention", "--every", "60", "--iterations", "3")
    assert out.count("Deleted 0 expired") == 3 and sleeps == [60.0, 60.0]


@pytest.mark.django_db
def test_sweep_retention_reports_errors(monkeypatch):
    from forklift_web.services import retention

    monkeypatch.setattr(
        retention,
        "sweep",
        lambda actor, dry_run: retention.SweepReport(
            dry_run=dry_run, errors=["artifact 1: refused"]
        ),
    )
    _, err = run("sweep_retention", "--every", "0.001", "--iterations", "2")
    assert err.count("artifact 1: refused") == 2


@pytest.mark.django_db
def test_rotate_secrets(settings, admin_actor):
    from cryptography.fernet import Fernet

    connection = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        secrets={"password": "pw"},
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
    )
    connections.create_connection(
        admin_actor, name="files", kind="localfs", config={"root_path": "/data"}
    )
    new_key = Fernet.generate_key().decode()
    settings.FORKLIFT_SECRETS_KEYS = [new_key, *settings.FORKLIFT_SECRETS_KEYS]
    secret_backend.backend.cache_clear()
    try:
        out, _ = run("rotate_secrets")
        assert "Re-encrypted the secrets of 1 connections" in out
        rotated = Connection.objects.get(pk=connection.pk).secret_ciphertext
        assert secret_backend.EnvSecretBackend([new_key]).decrypt(rotated) == {"password": "pw"}
        Connection.objects.filter(pk=connection.pk).update(secret_ciphertext="damaged")
        with pytest.raises(CommandError, match="Connection 'db'"):
            run("rotate_secrets")
    finally:
        secret_backend.backend.cache_clear()


def test_export_openapi(tmp_path):
    target = tmp_path / "out" / "openapi.json"
    out, _ = run("export_openapi", str(target))
    assert "Wrote" in out and json.loads(target.read_text())["info"]["title"] == "Forklift API"
    out, _ = run("export_openapi", str(target), "--check")
    assert "is up to date" in out
    target.write_text("{}")
    with pytest.raises(CommandError, match="out of date; regenerate it with"):
        run("export_openapi", str(target), "--check")
    internal = tmp_path / "internal.json"
    run("export_openapi", str(internal), "--internal")
    paths = json.loads(internal.read_text())["paths"]
    assert set(paths) == {
        "/internal/v1/leases",
        "/internal/v1/jobs/{job_id}/heartbeat",
        "/internal/v1/jobs/{job_id}/presign",
        "/internal/v1/jobs/{job_id}/complete",
        "/internal/v1/jobs/{job_id}/input-url",
    }
    with pytest.raises(CommandError, match="--internal"):
        run("export_openapi", str(tmp_path / "missing.json"), "--internal", "--check")


def test_the_checked_in_openapi_document_is_current():
    checked_in = REPOSITORY / "contracts" / "openapi.json"
    assert (
        checked_in.read_text(encoding="utf-8") == export_openapi.render()
    ), "contracts/openapi.json is out of date: forklift-web export_openapi contracts/openapi.json"


def test_forklift_web_entry_points(monkeypatch, capsys):
    from forklift_web import manage

    manage.main(["forklift-web", "check"])
    assert "no issues" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["forklift-web", "check"])
    manage.main()
    monkeypatch.setattr(sys, "argv", ["python -m forklift_web", "check"])
    runpy.run_module("forklift_web", run_name="__main__")
    assert "no issues" in capsys.readouterr().out


@pytest.mark.django_db
def test_health_checks(client, monkeypatch):
    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ok"
    assert client.post("/healthz").status_code == 405

    def broken_cursor(*args, **kwargs):
        raise DatabaseError("connection refused")

    from forklift_web import views

    monkeypatch.setattr(views.connection, "cursor", broken_cursor)
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "reason": "database: DatabaseError"}


@pytest.mark.django_db
def test_service_accounts_cannot_use_bootstrap_for_existing_names(admin_actor):
    accounts.create_user(admin_actor, username="svc", role=Role.ADMIN, is_service_account=True)
    out, _ = run("bootstrap_admin", "--username", "svc")
    assert "already exists" in out


def test_importing_the_main_module_runs_nothing():
    import importlib

    module = importlib.import_module("forklift_web.__main__")
    assert module.main is not None
