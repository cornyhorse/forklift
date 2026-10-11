"""Create a worker token and print it (once) on standard output, or register one from a file.

``--from-file`` registers a token made elsewhere (for example by the Helm chart, which keeps it
in a Kubernetes Secret) instead of generating one: it must have the generated tokens' format,
is stored the same way (prefix and SHA-256 hash), and registering it again does nothing.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from forklift_web.core.models import WorkerToken
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, tokens, workers

# What tokens.generate(WORKER_TOKEN_PREFIX) makes: the prefix and secrets.token_urlsafe(32).
TOKEN_FORMAT = re.compile(r"^" + re.escape(tokens.WORKER_TOKEN_PREFIX) + r"[A-Za-z0-9_-]{43}$")
MAX_FILE_BYTES = 4096


class Command(BaseCommand):
    help = (
        "Create a worker token for /internal/v1 and print it on standard output (the only time "
        "it is shown); give it to the workers as FORKLIFT_WORKER_TOKEN. With --from-file, "
        "register the token in that file instead (idempotent; nothing secret is printed)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--name", required=True, help="Names the token, e.g. the worker pool")
        parser.add_argument(
            "--expires-days", type=int, default=None, help="Days until it expires (default: never)"
        )
        parser.add_argument(
            "--from-file",
            type=Path,
            default=None,
            help="Register the token held in this file (fkw_ followed by 43 URL-safe "
            "characters, as this command prints them) instead of generating one; a token "
            "that is already registered is left as it is",
        )

    def handle(self, *args, name, expires_days, from_file, **options):
        expires_at = None
        if expires_days is not None:
            expires_at = timezone.now() + timedelta(days=expires_days)
        if from_file is not None:
            self._register(from_file, name=name, expires_at=expires_at)
            return
        token, raw = workers.create_worker_token(
            Actor.for_system("create_worker_token"), name=name, expires_at=expires_at
        )
        self.stderr.write(f"Created worker token {token.prefix}... ({name}); it is shown once:")
        self.stdout.write(raw)

    def _register(self, path: Path, *, name: str, expires_at) -> None:
        actor = Actor.for_system("create_worker_token")
        check(actor, Action.WORKER_MANAGE)
        if not name.strip():
            raise CommandError("A worker token needs a name (for example the worker pool's).")
        if expires_at is not None and expires_at <= timezone.now():
            raise CommandError("--expires-days must be 1 or more.")
        raw = read_token(path)
        prefix = raw[: tokens.PREFIX_LENGTH]
        existing = WorkerToken.objects.filter(prefix=prefix).first()
        if existing is not None:
            self.stdout.write(already_registered(existing, raw))
            return
        token = WorkerToken.objects.create(
            prefix=prefix, token_hash=tokens.hash_token(raw), name=name, expires_at=expires_at
        )
        audit.record(
            actor, "worker_token.create", token, {"expires_at": expires_at, "from_file": True}
        )
        self.stdout.write(f"Registered worker token {prefix}... ({name}) from {path}.")


def read_token(path: Path) -> str:
    """The worker token in ``path`` (surrounding white space removed); the messages never
    include the file's contents."""
    try:
        with open(path, "rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
    except OSError as error:
        raise CommandError(
            f"Cannot read the worker token file {path} ({type(error).__name__})."
        ) from None
    try:
        raw = data.decode("ascii").strip()
    except UnicodeDecodeError:
        raw = ""
    if len(data) > MAX_FILE_BYTES or not TOKEN_FORMAT.match(raw):
        raise CommandError(
            f"The file {path} does not hold a worker token: expected "
            f"{tokens.WORKER_TOKEN_PREFIX} followed by 43 URL-safe characters (A-Z, a-z, 0-9, "
            "'-' and '_'), the format create_worker_token prints, alone in the file."
        )
    return raw


def already_registered(existing: WorkerToken, raw: str) -> str:
    """The message for a token whose prefix is taken; a CommandError unless it is this very
    token and still usable."""
    if existing.token_hash != tokens.hash_token(raw):
        raise CommandError(
            f"Another worker token already uses the prefix {existing.prefix}...; this one "
            "cannot be registered. Put a newly generated token in the file."
        )
    if existing.revoked_at is not None:
        raise CommandError(
            f"Worker token {existing.prefix}... ({existing.name}) was revoked at "
            f"{existing.revoked_at:%Y-%m-%d %H:%M} UTC and cannot be registered again. Put a "
            "newly generated token in the file."
        )
    if not existing.is_usable():
        raise CommandError(
            f"Worker token {existing.prefix}... ({existing.name}) expired at "
            f"{existing.expires_at:%Y-%m-%d %H:%M} UTC. Put a newly generated token in the file."
        )
    return (
        f"Worker token {existing.prefix}... is already registered ({existing.name}); "
        "nothing to do."
    )
