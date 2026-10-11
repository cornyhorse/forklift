"""ID tokens signed by the fake provider's real RSA and EC keys: what passes, and every check
that refuses a token (signature, algorithm, keys and their rotation, issuer, audience,
authorized party, times, subject and nonce)."""

from __future__ import annotations

import json
import time

import jwt
import pytest
from jwt.warnings import InsecureKeyLengthWarning
from oidc_support import (  # noqa: F401
    CLIENT_ID,
    REMOVE,
    Grant,
    b64url,
    hand_signed,
    idp,
    public_pem,
    rsa_key,
)

from forklift_web.oidc.id_token import LEEWAY_SECONDS, InvalidIdToken, validate
from forklift_web.oidc.provider import Provider

NONCE = "the-nonce-of-this-sign-in"


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def provider(idp, clock):  # noqa: F811
    return Provider(idp.config(), clock=clock)


def token(idp, **claims) -> str:  # noqa: F811
    """An ID token for alice from the provider, with ``claims`` changed (REMOVE drops one)."""
    person = idp.people.get("alice") or idp.person("alice")
    idp.token_claims = claims
    return idp.id_token(Grant(CLIENT_ID, "https://f/cb", "", NONCE, "openid", person))


def serve_keys(idp, keys: list) -> None:  # noqa: F811
    idp.raw["/certs"] = (
        200,
        {"Content-Type": "application/json"},
        json.dumps({"keys": keys}).encode(),
    )


def refused(provider, raw: str, reason: str, match=None) -> InvalidIdToken:
    with pytest.raises(InvalidIdToken, match=match) as error:
        validate(provider, raw, nonce=NONCE)
    assert error.value.reason == reason
    return error.value


def test_a_good_token(idp, provider):  # noqa: F811
    claims = validate(provider, token(idp), nonce=NONCE)
    assert (claims["iss"], claims["sub"], claims["aud"]) == (idp.issuer, "alice", CLIENT_ID)
    assert claims["preferred_username"] == "user-alice"


def test_an_ec_signed_token(idp, provider):  # noqa: F811
    idp.signing_kid = "ec-1"
    assert validate(provider, token(idp), nonce=NONCE)["sub"] == "alice"


def test_a_pss_signed_token(idp, provider):  # noqa: F811
    idp.keys["ps-1"] = (rsa_key(), "PS256")
    idp.signing_kid, idp.published = "ps-1", ["rsa-1", "ps-1"]
    assert validate(provider, token(idp), nonce=NONCE)["sub"] == "alice"


# --------------------------------------------------------------------------- signatures


def test_a_signature_by_another_key(idp, provider):  # noqa: F811
    good = token(idp)
    forged = jwt.encode(jwt.decode(good, options={"verify_signature": False}), rsa_key(), "RS256")
    header, payload, _ = good.split(".")
    other = forged.split(".")[2]
    refused(provider, f"{header}.{payload}.{other}", "bad_signature", "does not verify")


def test_a_changed_claim_breaks_the_signature(idp, provider):  # noqa: F811
    good = token(idp)
    header, _, signature = good.split(".")
    _, payload, _ = token(idp, sub="mallory").split(".")
    refused(provider, f"{header}.{payload}.{signature}", "bad_signature")


def test_alg_none_is_refused(idp, provider):  # noqa: F811
    idp.forge = lambda claims, header: hand_signed(header, claims, alg="none")
    refused(provider, token(idp), "algorithm_not_allowed", "signed with 'none'")


def test_hmac_with_the_providers_public_key_is_refused(idp, provider):  # noqa: F811
    public = public_pem(idp.keys["rsa-1"][0])
    idp.forge = lambda claims, header: hand_signed(header, claims, secret=public, alg="HS256")
    error = refused(provider, token(idp), "algorithm_not_allowed", "signed with 'HS256'")
    assert "allowed: RS256, PS256, ES256" in error.problem


def test_an_algorithm_the_provider_does_not_use(idp, provider):  # noqa: F811
    # ES384 is in FORKLIFT_OIDC_ALGORITHMS but not in the provider's discovery document
    header = b64url(json.dumps({"alg": "ES384", "kid": "ec-1"}).encode())
    refused(provider, f"{header}.e30.c2ln", "algorithm_not_allowed", "signed with 'ES384'")


def test_a_key_published_for_another_algorithm(idp, provider):  # noqa: F811
    # The token says PS256, the key with its kid says RS256: never verified with PS256.
    idp.token_header = {"alg": "PS256"}
    refused(
        provider, token(idp), "algorithm_not_allowed", "the provider's key 'rsa-1' is for RS256"
    )


def test_a_key_of_the_wrong_type(idp, provider):  # noqa: F811
    keys = idp.jwks()["keys"]
    for key in keys:
        del key["alg"]
    serve_keys(idp, keys)
    idp.signing_kid = "rsa-1"
    idp.token_header = {"kid": "ec-1"}  # an RS256 token naming the EC key
    refused(provider, token(idp), "unusable_key", "which the provider's key 'ec-1' cannot be used")


def test_weak_rsa_keys_are_refused(idp, provider):  # noqa: F811
    idp.keys["rsa-1"] = (rsa_key(1024), "RS256")
    with pytest.warns(InsecureKeyLengthWarning):  # PyJWT signs with it, warning
        weak = token(idp)
    refused(provider, weak, "unusable_key", "too weak")


def test_malformed_tokens(idp, provider):  # noqa: F811
    refused(provider, "not a token", "malformed", "is not a signed JSON Web Token")
    header = b64url(json.dumps({"alg": "RS256", "kid": 5}).encode())
    refused(provider, f"{header}.e30.c2ln", "malformed", "is not a signed JSON Web Token")


# --------------------------------------------------------------------------- keys


def test_an_unknown_key_fetches_the_key_set_once_more(idp, provider, clock):  # noqa: F811
    validate(provider, token(idp), nonce=NONCE)  # the key set is cached now
    assert len(idp.seen("/certs")) == 1
    idp.token_header = {"kid": "nobody-knows"}
    refused(provider, token(idp), "unknown_key", "a key the provider does not publish")
    assert len(idp.seen("/certs")) == 1  # fetched less than a minute ago: not again
    clock.now += 61
    refused(provider, token(idp), "unknown_key")
    assert len(idp.seen("/certs")) == 2  # fetched again, once
    refused(provider, token(idp), "unknown_key")
    assert len(idp.seen("/certs")) == 2


def test_a_key_rotation_is_picked_up(idp, provider, clock):  # noqa: F811
    validate(provider, token(idp), nonce=NONCE)
    clock.now += 600
    new = idp.rotate()
    claims = validate(provider, token(idp), nonce=NONCE)
    assert claims["sub"] == "alice" and len(idp.seen("/certs")) == 2
    assert jwt.get_unverified_header(token(idp))["kid"] == new
    # the old key is gone from the key set: its tokens no longer verify
    idp.signing_kid = "rsa-1"
    clock.now += 61
    refused(provider, token(idp), "unknown_key")


def test_tokens_without_a_kid(idp, provider):  # noqa: F811
    idp.token_header = {"kid": REMOVE}
    refused(provider, token(idp), "unknown_key", "names no key .* publishes several")
    idp.published = ["rsa-1"]
    provider.clock.now += 61
    assert validate(provider, token(idp), nonce=NONCE)["sub"] == "alice"
    idp.published = []
    provider.clock.now += 3600
    refused(provider, token(idp), "unknown_key", "kid None")


def test_encryption_keys_are_not_signing_keys(idp, provider):  # noqa: F811
    keys = idp.jwks()["keys"]
    for key in keys:
        key["use"] = "enc"
    serve_keys(idp, keys)
    refused(provider, token(idp), "unknown_key")


# --------------------------------------------------------------------------- claims


def test_the_issuer_must_match(idp, provider):  # noqa: F811
    refused(provider, token(idp, iss=idp.issuer + "/"), "wrong_issuer", "another issuer")
    refused(provider, token(idp, iss=REMOVE), "missing_claim", "has no iss claim")


def test_the_audience_must_be_this_client(idp, provider):  # noqa: F811
    refused(
        provider, token(idp, aud="another-client"), "wrong_audience", r"another client \(aud\)"
    )
    refused(provider, token(idp, aud=["another-client"]), "wrong_audience")


def test_several_audiences_need_the_authorized_party(idp, provider):  # noqa: F811
    both = [CLIENT_ID, "another-client"]
    refused(provider, token(idp, aud=both), "wrong_audience", r"no authorized party \(azp\)")
    refused(provider, token(idp, aud=both, azp="another-client"), "wrong_audience", r"\(azp\)")
    assert validate(provider, token(idp, aud=both, azp=CLIENT_ID), nonce=NONCE)
    refused(provider, token(idp, azp="another-client"), "wrong_audience")


def test_times_with_a_minute_of_leeway(idp, provider):  # noqa: F811
    now = int(time.time())
    refused(provider, token(idp, exp=now - LEEWAY_SECONDS - 30), "expired", "has expired")
    assert validate(provider, token(idp, exp=now - LEEWAY_SECONDS + 30), nonce=NONCE)
    refused(provider, token(idp, nbf=now + 600), "not_yet_valid", "not valid yet")
    refused(provider, token(idp, iat=now + 600), "not_yet_valid")
    assert validate(provider, token(idp, nbf=now + 30, iat=now + 30), nonce=NONCE)
    refused(provider, token(idp, exp=REMOVE), "missing_claim", "has no exp claim")
    refused(provider, token(idp, iat=REMOVE), "missing_claim", "has no iat claim")
    refused(provider, token(idp, exp="soon"), "malformed", "is malformed")


def test_the_subject(idp, provider):  # noqa: F811
    refused(provider, token(idp, sub=REMOVE), "missing_claim", "has no sub claim")
    refused(provider, token(idp, sub=""), "malformed", "subject")
    refused(provider, token(idp, sub="x" * 256), "malformed", "1 to 255 characters")
    refused(provider, token(idp, sub=42), "malformed")


def test_the_nonce_must_be_this_sign_ins(idp, provider):  # noqa: F811
    refused(provider, token(idp, nonce="another"), "nonce_mismatch", "replayed")
    refused(provider, token(idp, nonce=REMOVE), "nonce_mismatch")
    refused(provider, token(idp, nonce=5), "nonce_mismatch")
