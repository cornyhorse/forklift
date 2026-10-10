"""The template context every page gets: what the signed-in user may do (for navigation and
controls), the version and whether the API documentation is served."""

from __future__ import annotations

from django.conf import settings
from django.utils.functional import SimpleLazyObject

from forklift_web import __version__
from forklift_web.api.auth import actor_for_request
from forklift_web.policy import Action, allowed

# Flag name -> the action it shows controls for. Hiding a control is a convenience: the
# service layer checks every call again.
FLAGS = {
    "upload": Action.UPLOAD_CREATE,
    "uploads": Action.UPLOAD_VIEW,
    "schemas": Action.SCHEMA_VIEW,
    "schema_edit": Action.SCHEMA_EDIT,
    "validate": Action.SCHEMA_VALIDATE,
    "datasets": Action.DATASET_VIEW,
    "dataset_edit": Action.DATASET_EDIT,
    "schedules": Action.SCHEDULE_VIEW,
    "run": Action.JOB_RUN,
    "jobs": Action.JOB_VIEW,
    "tokens": Action.TOKEN_VIEW,
    "webhooks": Action.WEBHOOK_VIEW,
    "admin": Action.WORKER_VIEW,
    "admin_users": Action.USER_VIEW,
    "admin_tokens": Action.ANY_TOKEN_VIEW,
    "admin_workers": Action.WORKER_VIEW,
    "admin_connections": Action.CONNECTION_MANAGE,
    "admin_retention": Action.RETENTION_VIEW,
    "admin_audit": Action.AUDIT_VIEW,
    "admin_settings": Action.SETTINGS_VIEW,
    "admin_webhooks": Action.ANY_WEBHOOK_VIEW,
}


# URL name prefix -> (section of the main navigation, section of the admin navigation)
SECTIONS = (
    ("admin-user", ("admin", "users")),
    ("admin-token", ("admin", "tokens")),
    ("admin-worker", ("admin", "workers")),
    ("admin-connection", ("admin", "connections")),
    ("admin-retention", ("admin", "retention")),
    ("admin-audit", ("admin", "audit")),
    ("admin-setting", ("admin", "settings")),
    ("admin-jobs", ("admin", "jobs")),
    ("admin-webhook", ("admin", "webhooks")),
    ("admin", ("admin", "overview")),
    ("schema", ("schemas", "")),
    ("version", ("schemas", "")),
    ("job-validation", ("schemas", "")),
    ("dataset", ("datasets", "")),
    ("schedule", ("schedules", "")),
    ("uploads", ("uploads", "")),
    ("upload-", ("uploads", "")),
    ("upload", ("upload", "")),
    ("job", ("jobs", "")),
    ("artifact", ("jobs", "")),
    ("token", ("account", "")),
    ("webhook", ("webhooks", "")),
    ("password", ("account", "")),
)


def permissions(actor) -> dict:
    return {name: allowed(actor, action) for name, action in FLAGS.items()}


def sections(url_name: str) -> dict:
    """Which navigation entries mark the current page (aria-current)."""
    for prefix, (main, admin) in SECTIONS:
        if url_name.startswith(prefix):
            return {"main": main, "admin": admin}
    return {"main": url_name, "admin": ""}


def ui(request) -> dict:
    url_name = getattr(getattr(request, "resolver_match", None), "url_name", None)
    return {
        "can": SimpleLazyObject(lambda: permissions(actor_for_request(request))),
        "nav": sections(url_name or ""),
        "version": __version__,
        "api_docs": settings.FORKLIFT_API_DOCS,
    }
