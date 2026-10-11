"""Checking an ID token (OpenID Connect Core 1.0, §3.1.3.7) with PyJWT.

A token is accepted only when every check passes:

- **signature**: the algorithm in its header is one both FORKLIFT_OIDC_ALGORITHMS and the
  provider allow (asymmetric only: ``none`` and HMAC never are, so a token "signed" with the
  provider's public key as an HMAC secret is refused before any key is looked at); the key is
  the provider's key with the header's ``kid`` (or its only key, when the header names none),
  it must suit that algorithm (key type, curve, an ``alg`` of its own, RSA keys of at least
  2048 bits), and the signature must verify with it. A ``kid`` the cached key set lacks makes
  the provider's key set be fetched again (at most once a minute, forklift_web.oidc.provider);
- **claims**: ``iss`` equals the configured issuer, ``aud`` contains the client id, and with
  several audiences ``azp`` is required (``azp``, when present, must be the client id); ``exp``,
  ``nbf`` and ``iat`` are checked with :data:`LEEWAY_SECONDS` of clock skew; ``sub`` is a
  non-empty string; ``nonce`` equals the one this sign-in sent (compared in constant time).

:class:`InvalidIdToken` says which check failed, never what the token held.
"""

from __future__ import annotations

import hmac
from typing import Optional

import jwt

from forklift_web.oidc.provider import Provider

LEEWAY_SECONDS = 60
MAX_SUBJECT_LENGTH = 255


class InvalidIdToken(Exception):
    """An ID token that failed a check: ``problem`` completes "The ID token ..." for people
    and the log, ``reason`` is a code for the audit log."""

    def __init__(self, problem: str, reason: str):
        super().__init__(problem)
        self.problem = problem
        self.reason = reason


def _find(keys: list, kid: Optional[str]) -> tuple:
    """(the signing key with ``kid``, or without a kid the only signing key there is, or None;
    whether there was no kid and several keys to choose from)."""
    signing = [key for key in keys if key.get("use", "sig") == "sig"]
    if kid is not None:
        return next((key for key in signing if key.get("kid") == kid), None), False
    return (signing[0] if len(signing) == 1 else None), len(signing) > 1


def _key(provider: Provider, kid: Optional[str], alg: str) -> jwt.PyJWK:
    keys, fresh = provider.keys()
    found, several = _find(keys, kid)
    if found is None and not fresh:
        found, several = _find(provider.keys(refresh=True)[0], kid)
    if found is None:
        if several:
            raise InvalidIdToken(
                "names no key (kid) while the provider publishes several, so it cannot be "
                "checked",
                "unknown_key",
            )
        raise InvalidIdToken(
            f"is signed with a key the provider does not publish (kid {kid!r})", "unknown_key"
        )
    if found.get("alg") not in (None, alg):
        raise InvalidIdToken(
            f"is signed with {alg}, but the provider's key {kid!r} is for {found['alg']}",
            "algorithm_not_allowed",
        )
    try:
        return jwt.PyJWK(found, algorithm=alg)
    except jwt.PyJWTError:
        raise InvalidIdToken(
            f"is signed with {alg}, which the provider's key {kid!r} cannot be used with",
            "unusable_key",
        ) from None


# PyJWT's errors, most specific first, and what each says about the token.
_ERRORS = (
    (jwt.InvalidSignatureError, "has a signature that does not verify", "bad_signature"),
    (jwt.ExpiredSignatureError, "has expired", "expired"),
    (jwt.ImmatureSignatureError, "is not valid yet (nbf or iat lies ahead)", "not_yet_valid"),
    (jwt.InvalidIssuerError, "was issued by another issuer (iss)", "wrong_issuer"),
    (jwt.InvalidAudienceError, "was issued for another client (aud)", "wrong_audience"),
    (jwt.InvalidKeyError, "is signed with a key that is too weak", "unusable_key"),
)


def _decode(token: str, key: jwt.PyJWK, alg: str, provider: Provider) -> dict:
    config = provider.config
    try:
        return jwt.decode(
            token,
            key=key,
            algorithms=[alg],
            audience=config.client_id,
            issuer=config.issuer,
            leeway=LEEWAY_SECONDS,
            options={
                "require": ["iss", "sub", "aud", "exp", "iat"],
                "enforce_minimum_key_length": True,
            },
        )
    except jwt.MissingRequiredClaimError as error:
        raise InvalidIdToken(f"has no {error.claim} claim", "missing_claim") from None
    except jwt.PyJWTError as error:
        for kind, problem, reason in _ERRORS:
            if isinstance(error, kind):
                raise InvalidIdToken(problem, reason) from None
        raise InvalidIdToken(f"is malformed ({error})", "malformed") from None


def validate(provider: Provider, token: str, *, nonce: str) -> dict:
    """The claims of ``token`` if it passes every check, else InvalidIdToken."""
    try:
        header = jwt.get_unverified_header(token)  # also: a kid, if any, is a string
    except jwt.PyJWTError:
        raise InvalidIdToken("is not a signed JSON Web Token", "malformed") from None
    alg = header.get("alg")
    allowed = provider.metadata().algorithms
    if alg not in allowed:
        raise InvalidIdToken(
            f"is signed with {alg!r}, which is not allowed (allowed: {', '.join(allowed)})",
            "algorithm_not_allowed",
        )
    claims = _decode(token, _key(provider, header.get("kid"), alg), alg, provider)
    client_id = provider.config.client_id
    audiences = claims["aud"] if isinstance(claims["aud"], list) else [claims["aud"]]
    if len(audiences) > 1 and "azp" not in claims:
        raise InvalidIdToken(
            "names several audiences but no authorized party (azp)", "wrong_audience"
        )
    if "azp" in claims and claims["azp"] != client_id:
        raise InvalidIdToken("was issued to another client (azp)", "wrong_audience")
    subject = claims["sub"]
    if not isinstance(subject, str) or not 0 < len(subject) <= MAX_SUBJECT_LENGTH:
        raise InvalidIdToken(
            f"has a subject (sub) that is not a string of 1 to {MAX_SUBJECT_LENGTH} characters",
            "malformed",
        )
    sent = claims.get("nonce")
    if not isinstance(sent, str) or not hmac.compare_digest(sent.encode(), nonce.encode()):
        raise InvalidIdToken(
            "does not carry this sign-in's nonce (it was replayed, or meant for another "
            "sign-in)",
            "nonce_mismatch",
        )
    return claims
