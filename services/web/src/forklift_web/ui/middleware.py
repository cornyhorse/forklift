"""Static files (public port only) and the Content-Security-Policy of the HTML pages."""

from __future__ import annotations

from urllib.parse import urlsplit

from django.conf import settings
from whitenoise.middleware import WhiteNoiseMiddleware

from forklift_web.middleware import INTERNAL, SURFACE_KEY

# The browser reaches AWS S3 under regional host names when no public endpoint is configured.
AWS_S3_ORIGINS = "https://*.amazonaws.com"


class StaticFilesMiddleware(WhiteNoiseMiddleware):
    """WhiteNoise serving STATIC_URL, on the public port only: the internal port answers
    /internal/v1 and health checks and nothing else."""

    def __call__(self, request):
        if request.META.get(SURFACE_KEY) == INTERNAL:
            return self.get_response(request)
        return super().__call__(request)


def store_origin(endpoint_url) -> str:
    """The origin browsers upload to and fetch artifacts from (scheme://host[:port])."""
    if not endpoint_url:
        return AWS_S3_ORIGINS
    parts = urlsplit(endpoint_url)
    return f"{parts.scheme}://{parts.netloc}"


def content_security_policy() -> str:
    """Scripts and styles only from the gateway's static files (no inline code); connections
    to the gateway and to the store's public endpoint (uploads, previews, reports)."""
    store = store_origin(settings.FORKLIFT_STORE["public_endpoint_url"])
    return "; ".join(
        [
            "default-src 'self'",
            "script-src 'self'",
            "style-src 'self'",
            "img-src 'self' data:",
            f"connect-src 'self' {store}",
            "form-action 'self'",
            "frame-ancestors 'none'",
            "base-uri 'none'",
            "object-src 'none'",
        ]
    )


class ContentSecurityPolicyMiddleware:
    """Adds the policy to HTML responses outside /api/ (the interactive API documentation
    loads its viewer from a CDN and keeps Django Ninja's own headers)."""

    def __init__(self, get_response):
        self.get_response = get_response
        self.policy = content_security_policy()

    def __call__(self, request):
        response = self.get_response(request)
        page = response.get("Content-Type", "").startswith("text/html")
        if page and not request.path.startswith("/api/"):
            response.setdefault("Content-Security-Policy", self.policy)
        return response
