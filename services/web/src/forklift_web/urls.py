"""URLs of the public port: the UI, sign-in, /api/v1 and health checks (never /internal/v1).

The HTML UI mounts its own URL configuration at "" (``forklift_web.ui.urls``, namespace
``ui``) once it exists; the names below (``forklift-login``, ``forklift-logout``,
``forklift-home``) are what it links to.
"""

from django.contrib.auth import views as auth_views
from django.urls import path

from forklift_web import views
from forklift_web.api import api

urlpatterns = [
    path("healthz", views.healthz, name="forklift-healthz"),
    path("readyz", views.readyz, name="forklift-readyz"),
    path(
        "accounts/login/",
        auth_views.LoginView.as_view(redirect_authenticated_user=True),
        name="forklift-login",
    ),
    path("accounts/logout/", auth_views.LogoutView.as_view(), name="forklift-logout"),
    path("api/v1/", api.urls),
    path("", views.home, name="forklift-home"),
]
