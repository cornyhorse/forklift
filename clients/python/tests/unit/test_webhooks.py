"""verify_webhook_signature, checked against signatures made the way the gateway makes them
(services/web: ``hmac.new(secret.encode(), f"{timestamp}.".encode() + body, sha256)``)."""

from __future__ import annotations

import hashlib
import hmac
import time

import pytest

from forklift_client import verify_webhook_signature
from forklift_client.webhooks import SIGNATURE_HEADER, TOLERANCE_SECONDS

SECRET = "fkwh_3q2-secret-of-the-webhook"
BODY = b'{"id":"5b0f8d1e","event":"job.failed","job":{"status":"failed"}}'
NOW = 1_760_100_000


def sign(body: bytes, secret: str = SECRET, timestamp: int = NOW) -> str:
    """The gateway's Forklift-Signature header (services/web/.../services/webhooks.py)."""
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"t={timestamp},v1={digest.hexdigest()}"


def test_the_documented_defaults():
    assert TOLERANCE_SECONDS == 300 and SIGNATURE_HEADER == "Forklift-Signature"


def test_a_genuine_delivery():
    assert verify_webhook_signature(BODY, sign(BODY), SECRET, now=NOW)
    assert verify_webhook_signature(bytearray(BODY), sign(BODY), SECRET, now=NOW + 300)
    assert verify_webhook_signature(memoryview(BODY), sign(BODY), SECRET, now=NOW - 300)


def test_the_current_time_is_the_default():
    now = int(time.time())
    assert verify_webhook_signature(BODY, sign(BODY, timestamp=now), SECRET)
    assert not verify_webhook_signature(BODY, sign(BODY, timestamp=now - 3600), SECRET)


@pytest.mark.parametrize(
    "body, header, secret",
    [
        (BODY + b" ", sign(BODY), SECRET),  # the body was changed
        (BODY, sign(BODY), "fkwh_another-secret"),
        (BODY, sign(BODY, timestamp=NOW + 1).replace(f"t={NOW + 1}", f"t={NOW}"), SECRET),
        (BODY, sign(BODY).upper().replace("T=", "t=").replace("V1=", "v1="), SECRET),
    ],
)
def test_forgeries(body, header, secret):
    assert not verify_webhook_signature(body, header, secret, now=NOW)


def test_old_and_future_timestamps():
    assert not verify_webhook_signature(BODY, sign(BODY), SECRET, now=NOW + 301)
    assert not verify_webhook_signature(BODY, sign(BODY), SECRET, now=NOW - 301)
    assert verify_webhook_signature(BODY, sign(BODY), SECRET, now=NOW + 3000, tolerance=3600)
    assert not verify_webhook_signature(BODY, sign(BODY), SECRET, now=NOW + 1, tolerance=0)


@pytest.mark.parametrize(
    "header",
    [None, "", "garbage", f"t={NOW}", "v1=abc", "t=soon,v1=abc", f"t={NOW},v1=é", "t,v1"],
)
def test_headers_that_are_not_signatures(header):
    assert not verify_webhook_signature(BODY, header, SECRET, now=NOW)


def test_while_the_secret_is_rotated_either_one_is_accepted():
    new = "fkwh_the-new-secret"
    assert verify_webhook_signature(BODY, sign(BODY, new), [SECRET, new], now=NOW)
    assert verify_webhook_signature(BODY, sign(BODY), (SECRET, new), now=NOW)
    assert not verify_webhook_signature(BODY, sign(BODY, "fkwh_x"), [SECRET, new], now=NOW)


def test_misuse_is_an_error_not_a_false():
    with pytest.raises(TypeError, match="raw request body"):
        verify_webhook_signature(BODY.decode(), sign(BODY), SECRET, now=NOW)  # type: ignore
    with pytest.raises(ValueError, match="tolerance"):
        verify_webhook_signature(BODY, sign(BODY), SECRET, tolerance=-1)
    for secret in ("", [], ["fkwh_a", ""], [None]):
        with pytest.raises(ValueError, match="signing secret"):
            verify_webhook_signature(BODY, sign(BODY), secret, now=NOW)  # type: ignore
