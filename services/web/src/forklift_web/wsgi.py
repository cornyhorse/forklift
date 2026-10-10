"""WSGI entry points.

``application`` serves both ports from one process (gunicorn with two ``--bind`` addresses, as
the Docker image runs it): a request on ``FORKLIFT_INTERNAL_PORT`` sees only the internal URL
configuration (/internal/v1), every other request only the public one. The decision uses the
local port of the connection (``gunicorn.socket``), never a header, so a client cannot choose
its surface. ``public_application`` and ``internal_application`` serve one surface each, for
servers that run them on separate listeners.
"""

from __future__ import annotations

import os

from django.conf import settings
from django.core.wsgi import get_wsgi_application

from forklift_web.middleware import INTERNAL, PUBLIC, SURFACE_KEY

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forklift_web.settings")

_django = get_wsgi_application()


def surface_of(environ) -> str:
    """INTERNAL for a connection accepted on the internal port, else PUBLIC."""
    sock = environ.get("gunicorn.socket")
    if sock is not None:
        local = sock.getsockname()
        if isinstance(local, tuple) and local[1] == settings.FORKLIFT_INTERNAL_PORT:
            return INTERNAL
    return PUBLIC


def application(environ, start_response):
    environ[SURFACE_KEY] = surface_of(environ)
    return _django(environ, start_response)


def public_application(environ, start_response):
    environ[SURFACE_KEY] = PUBLIC
    return _django(environ, start_response)


def internal_application(environ, start_response):
    environ[SURFACE_KEY] = INTERNAL
    return _django(environ, start_response)
