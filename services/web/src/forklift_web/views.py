"""Health checks, sign-in pages and the placeholder home page.

The HTML user interface (built on the service layer) replaces ``home``; sign-in and sign-out
are Django's views with the templates in core/templates/registration.
"""

from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
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


@login_required
@require_GET
def home(request):
    return render(request, "forklift_web/home.html", {"version": __version__})
