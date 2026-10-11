"""Single sign-on pages (forklift_web.services.oidc) and signing out.

- ``GET /accounts/oidc/login/?next=...`` starts a sign-in and sends the browser to the
  provider. It is a link, not a form: the Content-Security-Policy's ``form-action 'self'``
  would stop a form whose answer redirects to another site;
- ``GET /accounts/oidc/callback/`` is where the provider sends the browser back (the redirect
  URI to register at the provider);
- signing out (``POST /accounts/logout/``) ends the session and, with
  FORKLIFT_OIDC_PROVIDER_LOGOUT, continues to the provider's end-session endpoint from a page of
  its own (a redirect would be stopped by ``form-action`` as well).

Without single sign-on configured the first two answer 404.
"""

from __future__ import annotations

import logging

from django.conf import settings
from django.contrib.auth import login
from django.contrib.auth import views as auth_views
from django.http import Http404
from django.shortcuts import redirect, render, resolve_url
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET

from forklift_web.oidc.provider import ProviderError
from forklift_web.services import oidc
from forklift_web.ui.views.account import sign_in_page
from forklift_web.ui.views.base import safe_next

logger = logging.getLogger(__name__)

BACKEND = "django.contrib.auth.backends.ModelBackend"


def _configured():
    if settings.FORKLIFT_OIDC is None:
        raise Http404("Single sign-on is not configured on this installation.")


@never_cache
@require_GET
def sign_in(request):
    """Start a sign-in at the provider (or go on to ``next`` when already signed in)."""
    _configured()
    next_url = safe_next(request, resolve_url(settings.LOGIN_REDIRECT_URL))
    if request.user.is_authenticated:
        return redirect(next_url)
    try:
        target = oidc.begin(request, next_url)
    except ProviderError as failure:
        logger.warning("Single sign-on could not start", extra={"detail": failure.detail})
        return sign_in_page(request, error=failure.message, status=502)
    return redirect(target)


@never_cache
@require_GET
def callback(request):
    """Where the provider sends the browser back: sign in, or show why not."""
    _configured()
    try:
        done = oidc.complete(request)
    except oidc.SignInRefused as refused:
        response = sign_in_page(request, error=refused.message, status=refused.status)
    else:
        request.forklift_sign_in = done.audit_details  # for the user.login audit entry
        login(request, done.user, backend=BACKEND)
        request.session.set_expiry(None)  # a sign-in's lifetime (FORKLIFT_SESSION_SECONDS)
        request.session.pop(oidc.FLOWS_KEY, None)
        request.session[oidc.SIGNED_IN_KEY] = done.session
        response = redirect(done.next_url)
    # This URL carries the code and the state: no Referer with it, from this page's links and
    # styles either
    response["Referrer-Policy"] = "no-referrer"
    return response


class SignOutView(auth_views.LogoutView):
    """Django's sign-out (POST), then, with FORKLIFT_OIDC_PROVIDER_LOGOUT, on to the provider's
    end-session endpoint for a session that signed in there."""

    def post(self, request, *args, **kwargs):
        target = oidc.provider_logout_url(request, request.session.get(oidc.SIGNED_IN_KEY))
        response = super().post(request, *args, **kwargs)
        if target is None:
            return response
        context = {"target": target, "provider_name": settings.FORKLIFT_OIDC.provider_name}
        return render(request, "registration/signed_out.html", context)
