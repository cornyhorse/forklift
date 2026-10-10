"""Health checks. The HTML user interface is forklift_web.ui; sign-in and sign-out are
Django's views with the templates in ui/templates/registration."""

from __future__ import annotations

from django.db import connection
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from forklift_web import __version__


@require_GET
def healthz(request):
    """Liveness: the process answers (no database access)."""
    return JsonResponse({"status": "ok", "version": __version__})


@require_GET
def readyz(request):
    """Readiness: the database answers."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
    except Exception as error:  # any database failure means "not ready"
        return JsonResponse(
            {"status": "unavailable", "reason": f"database: {type(error).__name__}"}, status=503
        )
    return JsonResponse({"status": "ok", "version": __version__})
