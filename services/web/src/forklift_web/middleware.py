"""Request plumbing: which surface (public or internal) a request is on, and its request id.

``SurfaceMiddleware`` gives requests that arrived on the internal port the internal URL
configuration; forklift_web.wsgi marks them in the WSGI environ (a key no HTTP header can set),
so the public port never routes /internal/v1 and the internal port routes nothing else.
"""

from __future__ import annotations

import ipaddress
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


def parse_address(value: str):
    """The IP address in ``value``, without the port or brackets some proxies write into
    X-Forwarded-For ("192.0.2.1:5555", "[2001:db8::1]:443"); None if it holds none."""
    host = value
    if host.startswith("["):
        host = host[1:].partition("]")[0]
    elif host.count(":") == 1:  # an IPv6 address has at least two
        host = host.partition(":")[0]
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def client_ip(request) -> Optional[str]:
    """The client's address: the connection's, or the one ``FORKLIFT_TRUSTED_PROXIES`` reverse
    proxies put into X-Forwarded-For (counted from the right, so a client cannot forge it),
    without a port; an entry that is no address is passed on as it is."""
    proxies = settings.FORKLIFT_TRUSTED_PROXIES
    if proxies:
        forwarded = [
            part.strip()
            for part in request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")
            if part.strip()
        ]
        if len(forwarded) >= proxies:
            entry = forwarded[-proxies]
            address = parse_address(entry)
            return entry if address is None else str(address)
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
