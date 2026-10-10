"""Request plumbing: which surface (public or internal) a request is on, and its request id.

``SurfaceMiddleware`` gives requests that arrived on the internal port the internal URL
configuration; forklift_web.wsgi marks them in the WSGI environ (a key no HTTP header can set),
so the public port never routes /internal/v1 and the internal port routes nothing else.
"""

from __future__ import annotations

import re
import uuid
from typing import Optional

from django.conf import settings

SURFACE_KEY = "forklift.surface"
PUBLIC = "public"
INTERNAL = "internal"

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class SurfaceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.META.get(SURFACE_KEY) == INTERNAL:
            request.urlconf = settings.FORKLIFT_INTERNAL_URLCONF
        return self.get_response(request)


def client_ip(request) -> Optional[str]:
    """The client's address: the connection's, or the one ``FORKLIFT_TRUSTED_PROXIES`` reverse
    proxies put into X-Forwarded-For (counted from the right, so a client cannot forge it)."""
    proxies = settings.FORKLIFT_TRUSTED_PROXIES
    if proxies:
        forwarded = [
            part.strip()
            for part in request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")
            if part.strip()
        ]
        if len(forwarded) >= proxies:
            return forwarded[-proxies]
    return request.META.get("REMOTE_ADDR") or None


class RequestIdMiddleware:
    """Gives every request an id (the caller's X-Request-ID when well-formed) and the client
    address, for logs and the audit log, and echoes the id in the response."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        incoming = request.headers.get("X-Request-ID", "")
        request.request_id = incoming if _REQUEST_ID.match(incoming) else uuid.uuid4().hex
        request.client_ip = client_ip(request)
        response = self.get_response(request)
        response["X-Request-ID"] = request.request_id
        return response
