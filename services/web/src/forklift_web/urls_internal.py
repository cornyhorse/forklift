"""URLs of the internal port: /internal/v1 for workers, and health checks. Nothing else."""

from django.urls import path

from forklift_web import views
from forklift_web.internal import internal_api

urlpatterns = [
    path("healthz", views.healthz, name="forklift-internal-healthz"),
    path("readyz", views.readyz, name="forklift-internal-readyz"),
    path("internal/v1/", internal_api.urls),
]
