"""What every UI view shares: the signed-in actor, service errors as pages, HTMX, pagination."""

from __future__ import annotations

import uuid
from functools import wraps
from typing import Optional
from urllib.parse import urlsplit

from django.contrib.auth.views import redirect_to_login
from django.core.paginator import Paginator
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.utils.http import url_has_allowed_host_and_scheme

from forklift_web.api.auth import actor_for_request
from forklift_web.errors import ServiceError

PER_PAGE = 50
# Errors a form can show next to its fields; the others are pages of their own.
PAGE_ERRORS = {401, 403, 404}
TITLES = {
    400: "That did not work",
    403: "Not allowed",
    404: "Not found",
    409: "Conflict",
    410: "Gone",
    502: "The object store did not answer",
}


def is_htmx(request) -> bool:
    return request.headers.get("HX-Request") == "true"


def error_response(request, error: ServiceError):
    """``error`` as a page (or, for HTMX requests, a fragment) with its status."""
    template = "ui/_error.html" if is_htmx(request) else "ui/error.html"
    context = {"error": error, "title": TITLES.get(error.status, "That did not work")}
    return render(request, template, context, status=error.status)


def _sign_in(request):
    if is_htmx(request):
        # HTMX follows HX-Redirect with a full navigation, back to the page the user was on.
        current = urlsplit(request.headers.get("HX-Current-URL", ""))
        target = current.path or request.path
        response = HttpResponse(status=401)
        response["HX-Redirect"] = redirect_to_login(target)["Location"]
        return response
    return redirect_to_login(request.get_full_path())


def page(view):
    """A UI view for signed-in users: called as ``view(request, actor, ...)``; service errors
    that escape it become error pages (403 for refusals, 404 for missing objects, ...)."""

    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return _sign_in(request)
        try:
            return view(request, actor_for_request(request), *args, **kwargs)
        except ServiceError as error:
            return error_response(request, error)

    return wrapper


def refuse_on_page(error: ServiceError) -> None:
    """Re-raise errors that are pages of their own (the form cannot help with those)."""
    if error.status in PAGE_ERRORS:
        raise error


def form_failed(form, error: ServiceError) -> int:
    """Show a recoverable service error on ``form``; returns the status to answer with."""
    refuse_on_page(error)
    form.add_error(None, error.message)
    return error.status


def paginate(request, items, per_page: int = PER_PAGE):
    return Paginator(items, per_page).get_page(request.GET.get("page"))


def new_key() -> str:
    """An idempotency key for a form that starts a job, so a double submit starts one job."""
    return uuid.uuid4().hex


def safe_next(request, default: str) -> str:
    """The ``next`` field of a form when it is a URL of this site, else ``default``."""
    target: Optional[str] = request.POST.get("next") or request.GET.get("next")
    if target and url_has_allowed_host_and_scheme(
        target, allowed_hosts={request.get_host()}, require_https=request.is_secure()
    ):
        return target
    return default


def back(request, default: str):
    return redirect(safe_next(request, default))


def token_created(request, token, raw: str, *, back: str, what: str = "API token"):
    """The page that shows a new token's value: the only time anyone sees it."""
    response = render(
        request,
        "ui/account/token_created.html",
        {"token": token, "raw": raw, "back": back, "what": what},
    )
    response["Cache-Control"] = "no-store"
    return response
