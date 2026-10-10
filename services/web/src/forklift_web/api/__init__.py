"""The public API, /api/v1 (design section 5.4), built with Django Ninja.

Every operation authenticates (API token or session), then calls the service layer, which
applies the permission policy; service errors become ``{"detail", "code"}`` responses with
their status. The OpenAPI document is served at /api/v1/openapi.json and checked in as
contracts/openapi.json (``forklift-web export_openapi``).
"""

from __future__ import annotations

from django.conf import settings
from ninja import NinjaAPI
from ninja.errors import AuthenticationError

from forklift_web import __version__
from forklift_web.api.auth import ApiTokenAuth, SessionCookieAuth
from forklift_web.api.routes_admin import router as admin_router
from forklift_web.api.routes_catalog import connections_router, datasets_router, schemas_router
from forklift_web.api.routes_jobs import artifacts_router, jobs_router, uploads_router
from forklift_web.api.routes_me import router as me_router
from forklift_web.errors import ServiceError

api = NinjaAPI(
    title="Forklift API",
    version="1",
    description=(
        "The public API of the forklift gateway (forklift-web "
        f"{__version__}). Authenticate with an API token (Authorization: Bearer fkl_...) "
        "or a session; scopes of a token only narrow its owner's role."
    ),
    urls_namespace="api-v1",
    auth=[ApiTokenAuth(), SessionCookieAuth()],
    docs_url="/docs" if settings.FORKLIFT_API_DOCS else None,
)

api.add_router("", me_router)
api.add_router("/uploads", uploads_router)
api.add_router("/schemas", schemas_router)
api.add_router("/datasets", datasets_router)
api.add_router("/jobs", jobs_router)
api.add_router("/artifacts", artifacts_router)
api.add_router("/connections", connections_router)
api.add_router("/admin", admin_router)


@api.exception_handler(ServiceError)
def _service_error(request, error: ServiceError):
    return api.create_response(
        request, {"detail": error.message, "code": error.code}, status=error.status
    )


@api.exception_handler(AuthenticationError)
def _not_authenticated(request, error: AuthenticationError):
    return api.create_response(
        request,
        {
            "detail": "Sign in, or send an API token (Authorization: Bearer fkl_...); unknown, "
            "revoked and expired tokens and worker tokens are refused.",
            "code": "not_authenticated",
        },
        status=401,
    )
