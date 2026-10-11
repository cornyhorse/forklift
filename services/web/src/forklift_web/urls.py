"""URLs of the public port: the UI, sign-in, /api/v1 and health checks (never /internal/v1).

The HTML UI mounts its own URL configuration at "" (``forklift_web.ui.urls``, namespace
``ui``); sign-in and sign-out are ``forklift-login`` and ``forklift-logout``, single sign-on
``forklift-oidc-login`` and ``forklift-oidc-callback`` (the redirect URI at the provider).
"""

from django.urls import include, path

from forklift_web import views
from forklift_web.api import api
from forklift_web.ui.views import oidc
from forklift_web.ui.views.account import SignInView

urlpatterns = [
    path("healthz", views.healthz, name="forklift-healthz"),
    path("readyz", views.readyz, name="forklift-readyz"),
    path("accounts/login/", SignInView.as_view(), name="forklift-login"),
    path("accounts/logout/", oidc.SignOutView.as_view(), name="forklift-logout"),
    path("accounts/oidc/login/", oidc.sign_in, name="forklift-oidc-login"),
    path("accounts/oidc/callback/", oidc.callback, name="forklift-oidc-callback"),
    path("api/v1/", api.urls),
    path("", include("forklift_web.ui.urls")),
]
