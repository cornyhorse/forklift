"""URLs of the HTML user interface (namespace ``ui``), mounted at the root of the public port.

Sign-in and sign-out are ``forklift-login`` / ``forklift-logout`` in ``forklift_web.urls``.
"""

from django.urls import path

from forklift_web.ui.views import account, admin, catalog, schedules, webhooks, work

app_name = "ui"

urlpatterns = [
    path("", account.home, name="home"),
    # The signed-in user's own account
    path("account/password/", account.PasswordChangeView.as_view(), name="password"),
    path("account/tokens/", account.tokens, name="tokens"),
    path("account/tokens/<uuid:token_id>/revoke/", account.revoke_token, name="token-revoke"),
    path("account/webhooks/", webhooks.webhook_list, name="webhooks"),
    path("account/webhooks/<uuid:webhook_id>/", webhooks.webhook_detail, name="webhook"),
    path(
        "account/webhooks/<uuid:webhook_id>/rotate/",
        webhooks.webhook_rotate,
        name="webhook-rotate",
    ),
    path("account/webhooks/<uuid:webhook_id>/test/", webhooks.webhook_test, name="webhook-test"),
    path(
        "account/webhooks/<uuid:webhook_id>/deliveries/",
        webhooks.webhook_deliveries,
        name="webhook-deliveries",
    ),
    path(
        "account/webhooks/<uuid:webhook_id>/delete/",
        webhooks.webhook_delete,
        name="webhook-delete",
    ),
    path(
        "account/webhooks/<uuid:webhook_id>/deliveries/<uuid:delivery_id>/redeliver/",
        webhooks.webhook_redeliver,
        name="webhook-redeliver",
    ),
    # Schemas
    path("schemas/", catalog.schema_list, name="schemas"),
    path("schemas/new/", catalog.schema_new, name="schema-new"),
    path("schemas/validate/", catalog.schema_validate, name="schema-validate"),
    path("schemas/check/", catalog.schema_check, name="schema-check"),
    path("schemas/generate/", catalog.schema_generate, name="schema-generate"),
    path("schemas/<uuid:schema_id>/", catalog.schema_detail, name="schema"),
    path("schemas/<uuid:schema_id>/edit/", catalog.schema_edit, name="schema-edit"),
    path("schemas/<uuid:schema_id>/diff/", catalog.schema_diff, name="schema-diff"),
    path("schemas/<uuid:schema_id>/versions/new/", catalog.version_new, name="version-new"),
    path(
        "schemas/<uuid:schema_id>/versions/<int:number>/",
        catalog.schema_version,
        name="schema-version",
    ),
    # Datasets
    path("datasets/", catalog.dataset_list, name="datasets"),
    path("datasets/new/", catalog.dataset_new, name="dataset-new"),
    path("datasets/<uuid:dataset_id>/", catalog.dataset_detail, name="dataset"),
    path("datasets/<uuid:dataset_id>/edit/", catalog.dataset_edit, name="dataset-edit"),
    path("datasets/<uuid:dataset_id>/delete/", catalog.dataset_delete, name="dataset-delete"),
    path("datasets/<uuid:dataset_id>/run/", catalog.dataset_run, name="dataset-run"),
    # Schedules
    path("schedules/", schedules.schedule_list, name="schedules"),
    path("schedules/preview/", schedules.schedule_preview, name="schedule-preview"),
    path("datasets/<uuid:dataset_id>/schedules/new/", schedules.schedule_new, name="schedule-new"),
    path("schedules/<uuid:schedule_id>/edit/", schedules.schedule_edit, name="schedule-edit"),
    path(
        "schedules/<uuid:schedule_id>/enable/", schedules.schedule_enable, name="schedule-enable"
    ),
    path(
        "schedules/<uuid:schedule_id>/delete/", schedules.schedule_delete, name="schedule-delete"
    ),
    # Uploads, jobs and downloads
    path("upload/", work.upload_new, name="upload"),
    path("uploads/", work.upload_list, name="uploads"),
    path("uploads/<uuid:upload_id>/", work.upload_detail, name="upload-detail"),
    path("uploads/<uuid:upload_id>/run/", work.upload_run, name="upload-run"),
    path("uploads/<uuid:upload_id>/delete/", work.upload_delete, name="upload-delete"),
    path("jobs/", work.job_list, name="jobs"),
    path("jobs/<uuid:job_id>/", work.job_detail, name="job"),
    path("jobs/<uuid:job_id>/live/", work.job_live, name="job-live"),
    path("jobs/<uuid:job_id>/validation/", catalog.validation, name="job-validation"),
    path("jobs/<uuid:job_id>/generation/", catalog.generation, name="job-generation"),
    path("jobs/<uuid:job_id>/cancel/", work.job_cancel, name="job-cancel"),
    path(
        "artifacts/<uuid:artifact_id>/download/", work.artifact_download, name="artifact-download"
    ),
    # Administration
    path("admin/", admin.overview, name="admin"),
    path("admin/store/", admin.store_status, name="admin-store"),
    path("admin/users/", admin.users, name="admin-users"),
    path("admin/users/new/", admin.user_new, name="admin-user-new"),
    path("admin/users/<int:user_id>/", admin.user_detail, name="admin-user"),
    path("admin/users/<int:user_id>/password/", admin.user_password, name="admin-user-password"),
    path("admin/users/<int:user_id>/unlock/", admin.user_unlock, name="admin-user-unlock"),
    path("admin/tokens/", admin.tokens, name="admin-tokens"),
    path("admin/tokens/<uuid:token_id>/revoke/", admin.token_revoke, name="admin-token-revoke"),
    path("admin/workers/", admin.workers_page, name="admin-workers"),
    path(
        "admin/workers/tokens/<uuid:token_id>/revoke/",
        admin.worker_token_revoke,
        name="admin-worker-token-revoke",
    ),
    path("admin/connections/", admin.connections_page, name="admin-connections"),
    path("admin/connections/new/<str:kind>/", admin.connection_new, name="admin-connection-new"),
    path(
        "admin/connections/<uuid:connection_id>/",
        admin.connection_detail,
        name="admin-connection",
    ),
    path(
        "admin/connections/<uuid:connection_id>/test/",
        admin.connection_test,
        name="admin-connection-test",
    ),
    path(
        "admin/connections/<uuid:connection_id>/delete/",
        admin.connection_delete,
        name="admin-connection-delete",
    ),
    path("admin/retention/", admin.retention_page, name="admin-retention"),
    path("admin/retention/policy/", admin.retention_save, name="admin-retention-save"),
    path("admin/retention/policy/delete/", admin.retention_delete, name="admin-retention-delete"),
    path("admin/retention/sweep/", admin.retention_sweep, name="admin-retention-sweep"),
    path("admin/audit/", admin.audit_page, name="admin-audit"),
    path("admin/audit/export.csv", admin.audit_export, name="admin-audit-export"),
    path("admin/settings/", admin.settings_page, name="admin-settings"),
    path("admin/settings/<str:key>/", admin.setting_save, name="admin-setting"),
    path("admin/settings/<str:key>/reset/", admin.setting_reset, name="admin-setting-reset"),
    path("admin/jobs/", admin.all_jobs, name="admin-jobs"),
    path("admin/webhooks/", webhooks.admin_webhooks, name="admin-webhooks"),
    path(
        "admin/webhooks/<uuid:webhook_id>/disable/",
        webhooks.admin_webhook_disable,
        name="admin-webhook-disable",
    ),
]
