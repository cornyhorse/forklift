"""Re-encrypt every connection secret with the first key of FORKLIFT_SECRETS_KEYS."""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from forklift_web import secret_backend
from forklift_web.core.models import Connection


class Command(BaseCommand):
    help = (
        "Re-encrypt connection secrets with the current (first) key. To rotate: put a new key "
        "first in FORKLIFT_SECRETS_KEYS, keep the old one after it, run this, then remove the "
        "old key."
    )

    def handle(self, *args, **options):
        backend = secret_backend.backend()
        rotated = 0
        with transaction.atomic():
            for connection in Connection.objects.select_for_update().exclude(secret_ciphertext=""):
                try:
                    connection.secret_ciphertext = backend.rotate(connection.secret_ciphertext)
                except secret_backend.SecretError as error:
                    raise CommandError(f"Connection {connection.name!r}: {error}") from None
                connection.save(update_fields=["secret_ciphertext"])
                rotated += 1
        self.stdout.write(f"Re-encrypted the secrets of {rotated} connections.")
