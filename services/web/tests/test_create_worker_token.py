"""`forklift-web create_worker_token --from-file`: registering a token made elsewhere (the Helm
chart keeps one in a Secret), idempotently, in the generated tokens' format and hashing."""

from __future__ import annotations

from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.utils import timezone

from forklift_web.core.models import AuditLog, WorkerToken
from forklift_web.services import tokens

pytestmark = pytest.mark.django_db

LEASE = {
    "worker_id": "helm-worker",
    "lanes": ["batch"],
    "spec_versions": [1],
    "engine_version": "0.2",
    "worker_version": "0.1",
}


def run(*args) -> tuple:
    out, err = StringIO(), StringIO()
    call_command("create_worker_token", *args, stdout=out, stderr=err)
    return out.getvalue(), err.getvalue()


@pytest.fixture
def token_file(tmp_path):
    """``token_file(content)``: a file holding ``content`` (str or bytes), and its path."""

    def make(content) -> str:
        path = tmp_path / "worker-token"
        path.write_bytes(content if isinstance(content, bytes) else content.encode())
        return str(path)

    return make


def test_a_token_from_a_file_is_registered_like_a_generated_one(token_file, internal):
    raw, prefix, digest = tokens.generate(tokens.WORKER_TOKEN_PREFIX)
    path = token_file(raw + "\n")

    out, err = run("--name", "helm", "--from-file", path)

    token = WorkerToken.objects.get()
    assert (token.name, token.prefix, token.token_hash) == ("helm", prefix, digest)
    assert token.expires_at is None and token.created_by is None
    assert out == f"Registered worker token {prefix}... (helm) from {path}.\n" and err == ""
    assert raw not in out + err
    entry = AuditLog.objects.get(action="worker_token.create")
    assert entry.actor_label == "system:create_worker_token"
    assert entry.details == {"expires_at": None, "from_file": True}
    assert entry.object_id == str(token.pk)
    # The workers can use it: the internal API answers its lease (nothing queued: 204)
    assert internal(raw).post("/internal/v1/leases", LEASE).status_code == 204
    assert internal(raw[:-1] + "x").post("/internal/v1/leases", LEASE).status_code == 401


def test_registering_the_same_token_again_does_nothing(token_file):
    raw, prefix, _ = tokens.generate(tokens.WORKER_TOKEN_PREFIX)
    path = token_file(f"  {raw}\r\n")
    run("--name", "helm", "--from-file", path, "--expires-days", "30")

    out, _ = run("--name", "renamed", "--from-file", path, "--expires-days", "1")

    assert out == f"Worker token {prefix}... is already registered (helm); nothing to do.\n"
    token = WorkerToken.objects.get()
    assert token.name == "helm" and token.expires_at > timezone.now() + timedelta(days=29)
    assert AuditLog.objects.filter(action="worker_token.create").count() == 1


def test_a_generated_token_can_be_registered_from_a_file_too(token_file):
    out, _ = run("--name", "generated")
    path = token_file(out)

    again, _ = run("--name", "generated", "--from-file", path)

    assert again.endswith("is already registered (generated); nothing to do.\n")
    assert WorkerToken.objects.count() == 1


@pytest.mark.parametrize(
    "content",
    [
        "",
        "fkw_short",
        "fkl_" + "a" * 43,  # an API token's prefix
        "fkw_" + "a" * 42 + "!",  # not URL-safe
        "fkw_" + "a" * 44,
        "fkw_" + "a" * 43 + "\nfkw_" + "b" * 43,  # two tokens
        b"fkw_" + "é".encode() * 22,  # not ASCII
        b"fkw_" + b"a" * 43 + b"\n" + b" " * 4096,  # longer than a token file can be
    ],
)
def test_a_file_that_does_not_hold_a_worker_token_is_refused(token_file, content):
    path = token_file(content)

    with pytest.raises(CommandError) as raised:
        run("--name", "helm", "--from-file", path)

    message = str(raised.value)
    assert message.startswith(f"The file {path} does not hold a worker token: expected fkw_")
    assert "a" * 20 not in message and "b" * 20 not in message  # never the file's contents
    assert not WorkerToken.objects.exists()


def test_a_file_that_cannot_be_read_is_refused(tmp_path):
    missing = tmp_path / "nothing-here"

    with pytest.raises(CommandError, match=r"Cannot read the worker token file .*nothing-here"):
        run("--name", "helm", "--from-file", str(missing))
    with pytest.raises(CommandError, match=r"\(IsADirectoryError\)"):
        run("--name", "helm", "--from-file", str(tmp_path))


def test_a_name_and_a_future_expiry_are_needed(token_file):
    path = token_file(tokens.generate(tokens.WORKER_TOKEN_PREFIX)[0])

    with pytest.raises(CommandError, match="A worker token needs a name"):
        run("--name", "  ", "--from-file", path)
    with pytest.raises(CommandError, match="--expires-days must be 1 or more"):
        run("--name", "helm", "--from-file", path, "--expires-days", "0")
    assert not WorkerToken.objects.exists()


def test_a_token_whose_prefix_another_token_has_is_refused(token_file):
    stored, raw = tokens.create(WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="admin-made")
    impostor = raw[: tokens.PREFIX_LENGTH] + ("A" if raw[-1] != "A" else "B") * 43
    impostor = impostor[: len(raw)]

    with pytest.raises(CommandError) as raised:
        run("--name", "helm", "--from-file", token_file(impostor))

    assert str(raised.value) == (
        f"Another worker token already uses the prefix {stored.prefix}...; this one cannot be "
        "registered. Put a newly generated token in the file."
    )
    assert WorkerToken.objects.get().token_hash == tokens.hash_token(raw)


def test_a_revoked_token_is_not_registered_again(token_file):
    stored, raw = tokens.create(WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="helm")
    revoked_at = timezone.now().replace(year=2026, month=10, day=1, hour=9, minute=30)
    WorkerToken.objects.filter(pk=stored.pk).update(revoked_at=revoked_at)

    with pytest.raises(CommandError) as raised:
        run("--name", "helm", "--from-file", token_file(raw))

    assert str(raised.value) == (
        f"Worker token {stored.prefix}... (helm) was revoked at 2026-10-01 09:30 UTC and cannot "
        "be registered again. Put a newly generated token in the file."
    )
    assert WorkerToken.objects.get().revoked_at == revoked_at


def test_an_expired_token_is_not_registered_again(token_file):
    expired_at = timezone.now() - timedelta(days=1)
    stored, raw = tokens.create(
        WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="helm", expires_at=expired_at
    )

    with pytest.raises(CommandError) as raised:
        run("--name", "helm", "--from-file", token_file(raw))

    assert str(raised.value) == (
        f"Worker token {stored.prefix}... (helm) expired at {expired_at:%Y-%m-%d %H:%M} UTC. "
        "Put a newly generated token in the file."
    )
