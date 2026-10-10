"""Bearer tokens: generation, hashing and lookup (API tokens and worker tokens alike).

A token is ``<kind prefix><43 random url-safe characters>``: ``fkl_`` for API tokens, ``fkw_``
for worker tokens, so that one can never be mistaken for the other and secret scanners can
recognise both. The first 12 characters are stored as ``prefix`` to find the token; the rest is
only ever stored as a SHA-256 hash.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

API_TOKEN_PREFIX = "fkl_"
WORKER_TOKEN_PREFIX = "fkw_"
PREFIX_LENGTH = 12


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def generate(kind_prefix: str) -> tuple:
    """A new token and its (prefix, hash)."""
    token = kind_prefix + secrets.token_urlsafe(32)
    return token, token[:PREFIX_LENGTH], hash_token(token)


def find(queryset, token: str, kind_prefix: str):
    """The token in ``queryset`` matching ``token``, or None (wrong kind, unknown, or hash
    mismatch)."""
    if not token.startswith(kind_prefix) or len(token) <= PREFIX_LENGTH:
        return None
    candidate = queryset.filter(prefix=token[:PREFIX_LENGTH]).first()
    if candidate is None or not hmac.compare_digest(candidate.token_hash, hash_token(token)):
        return None
    return candidate


def create(model, kind_prefix: str, **fields):
    """Create a token row; returns (row, token). Retries the (unlikely) prefix collision."""
    for _ in range(5):
        token, prefix, digest = generate(kind_prefix)
        if not model.objects.filter(prefix=prefix).exists():
            return model.objects.create(prefix=prefix, token_hash=digest, **fields), token
    raise RuntimeError("Could not generate a token with an unused prefix after 5 attempts.")
