"""Authentication of /api/v1: an API token (``Authorization: Bearer fkl_...``) or a session.

A request with an Authorization header is authenticated by the token alone (a bad token is a
401 even when a session cookie is also present). Session requests that change something need
the CSRF token (``X-CSRFToken`` header), as in every Django form.
"""

from __future__ import annotations

from typing import Optional

from django.conf import settings
from ninja.errors import HttpError
from ninja.security import HttpBearer
from ninja.security.base import AuthBase
from ninja.utils import check_csrf

from forklift_web.policy import Actor
from forklift_web.services import accounts


def _context(request) -> dict:
    return {
        "request_id": getattr(request, "request_id", ""),
        "ip": getattr(request, "client_ip", None),
    }


def actor_for_request(request) -> Actor:
    """The actor of a session request (anonymous when nobody is signed in): what HTML views
    pass to the service layer."""
    user = request.user
    if user.is_authenticated:
        return Actor.for_user(user, **_context(request))
    return Actor.anonymous(**_context(request))


class ApiTokenAuth(HttpBearer):
    def authenticate(self, request, token: str) -> Optional[Actor]:
        return accounts.authenticate_api_token(token, **_context(request))


class SessionCookieAuth(AuthBase):
    """The Django session, with CSRF protection for unsafe methods."""

    openapi_type = "apiKey"
    openapi_in = "cookie"
    openapi_name = settings.SESSION_COOKIE_NAME

    def __call__(self, request) -> Optional[Actor]:
        if "Authorization" in request.headers or not request.user.is_authenticated:
            return None
        if check_csrf(request) is not None:
            raise HttpError(
                403,
                "CSRF check failed: send the value of the csrftoken cookie in an X-CSRFToken "
                "header (or use an API token).",
            )
        return actor_for_request(request)
