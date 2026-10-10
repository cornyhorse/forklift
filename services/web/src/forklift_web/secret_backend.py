"""The secret backend: encrypts connection secrets at rest (design section 5.6).

The ``env`` backend encrypts a connection's secrets (a small JSON object such as
``{"password": ...}``) with Fernet keys from ``FORKLIFT_SECRETS_KEYS``: the first key encrypts,
every key decrypts, so keys rotate without downtime (``forklift-web rotate_secrets``). Secrets
are decrypted only to sign URLs or to hand one SQL job its connection string, and never logged.
Kubernetes Secrets and Vault can implement the same interface later.
"""

from __future__ import annotations

import json
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


class SecretError(Exception):
    """A secret cannot be decrypted (wrong or rotated-away key, damaged ciphertext)."""


class EnvSecretBackend:
    name = "env"

    def __init__(self, keys: list):
        if not keys:
            raise ImproperlyConfigured(
                "FORKLIFT_SECRETS_KEYS must name at least one Fernet key (comma-separated) to "
                "encrypt connection secrets; generate one with "
                '`python -c "from cryptography.fernet import Fernet; '
                'print(Fernet.generate_key().decode())"`.'
            )
        try:
            self._fernet = MultiFernet([Fernet(key) for key in keys])
        except ValueError:
            raise ImproperlyConfigured(
                "FORKLIFT_SECRETS_KEYS contains a value that is not a Fernet key (32 url-safe "
                "base64-encoded bytes)."
            ) from None

    def encrypt(self, secrets: dict) -> str:
        if not secrets:
            return ""
        return self._fernet.encrypt(json.dumps(secrets, sort_keys=True).encode()).decode()

    def decrypt(self, ciphertext: str) -> dict:
        if not ciphertext:
            return {}
        try:
            return json.loads(self._fernet.decrypt(ciphertext.encode()))
        except InvalidToken:
            raise SecretError(
                "A connection secret could not be decrypted with any key in "
                "FORKLIFT_SECRETS_KEYS (was a key removed before rotate_secrets ran?)."
            ) from None

    def rotate(self, ciphertext: str) -> str:
        """``ciphertext`` re-encrypted with the current (first) key."""
        if not ciphertext:
            return ""
        try:
            return self._fernet.rotate(ciphertext.encode()).decode()
        except InvalidToken:
            raise SecretError(
                "A connection secret could not be decrypted with any key in "
                "FORKLIFT_SECRETS_KEYS, so it cannot be rotated."
            ) from None


@lru_cache(maxsize=1)
def backend() -> EnvSecretBackend:
    if settings.FORKLIFT_SECRET_BACKEND != "env":
        raise ImproperlyConfigured(
            f"FORKLIFT_SECRET_BACKEND={settings.FORKLIFT_SECRET_BACKEND!r} is not supported; "
            "the only backend so far is 'env'."
        )
    return EnvSecretBackend(settings.FORKLIFT_SECRETS_KEYS)
