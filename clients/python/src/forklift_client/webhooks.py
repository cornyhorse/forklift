"""Checking that a webhook delivery came from the gateway (docs/platform/webhooks.md).

``Forklift-Signature`` is ``t=<unix seconds>,v1=<signature>``: the lower-case hex HMAC-SHA256,
keyed with the webhook's signing secret (its UTF-8 bytes, ``fkwh_`` prefix included), of the
timestamp, a ``.`` and the raw request body. A receiver checks it against the raw body before
parsing it, with a constant-time comparison, and refuses timestamps more than five minutes away
from its own clock, so a recorded delivery cannot be replayed later.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional, Sequence, Union

TOLERANCE_SECONDS = 300
SIGNATURE_HEADER = "Forklift-Signature"


def verify_webhook_signature(
    body: bytes,
    header: Optional[str],
    secret: Union[str, Sequence[str]],
    *,
    tolerance: float = TOLERANCE_SECONDS,
    now: Optional[float] = None,
) -> bool:
    """Whether a delivery is genuine.

    ``body`` is the raw request body (bytes, unparsed), ``header`` the ``Forklift-Signature``
    header, ``secret`` the webhook's signing secret, or several (while a rotation is under way:
    the receiver accepts the old and the new one). ``tolerance`` is the most seconds the
    timestamp may be away from ``now`` (default: the current time).
    """
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise TypeError("body must be the raw request body as bytes, before any parsing")
    if tolerance < 0:
        raise ValueError("tolerance must not be negative")
    secrets = [secret] if isinstance(secret, str) else list(secret)
    if not secrets or not all(isinstance(item, str) and item for item in secrets):
        raise ValueError("secret must be the webhook's signing secret (fkwh_...)")
    try:
        fields = dict(part.split("=", 1) for part in (header or "").split(","))
        timestamp = int(fields["t"])
        signature = fields["v1"]
    except (KeyError, ValueError):
        return False
    if abs((time.time() if now is None else now) - timestamp) > tolerance:
        return False
    message = str(timestamp).encode() + b"." + bytes(body)
    given = signature.encode("utf-8", "replace")  # bytes: compare_digest refuses non-ASCII str
    valid = False
    for item in secrets:  # every secret is compared, so the time taken does not tell which
        expected = hmac.new(item.encode(), message, hashlib.sha256).hexdigest().encode()
        valid |= hmac.compare_digest(expected, given)
    return valid
