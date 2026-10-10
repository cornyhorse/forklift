"""The admin screens: overview, users, tokens, workers, connections, retention, audit log,
installation settings and all jobs."""

from __future__ import annotations

import csv
import io
from datetime import timedelta

import pytest
from django.contrib.auth import authenticate
from django.urls import reverse
from django.utils import timezone
from test_ui_matrix import retention_data
from ui_support import start, worker_principal
from world import PASSWORD, World, make_job, make_user

from forklift_web import secret_backend, storage
from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.models import (
    ApiToken,
    AuditLog,
    Connection,
    Dataset,
    InstallationSetting,
    RetentionPolicy,
    User,
    Worker,
    WorkerToken,
)
from forklift_web.errors import StoreUnavailable
from forklift_web.policy import Actor
from forklift_web.services import installation
from forklift_web.ui.forms import SettingForm

pytestmark = pytest.mark.django_db

HTMX = {"HTTP_HX_REQUEST": "true"}
NEW_PASSWORD = "a much better passphrase 7"


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def admin(client, world):
    client.force_login(world.admin)
    return client


# --------------------------------------------------------------------------- overview


def test_the_overview(world, admin):
    Worker.objects.create(
        worker_id="old-worker", lanes=["sql"], last_seen_at=timezone.now() - timedelta(hours=2)
    )
    running = start(make_job(world.operator, world.upload), worker_principal())
    failed = make_job(world.operator, world.upload, status=JobStatus.FAILED)
    page = admin.get(reverse("ui:admin"))
    context = page.context
    assert [w.worker_id for w in context["online"]] == ["worker-1"]
    assert [w.worker_id for w in context["offline"]] == ["old-worker"]
    lanes = {row["lane"]: row["count"] for row in context["lanes"]}
    assert lanes == {"interactive": 0, "batch": 1, "sql": 0}
    assert [job.pk for job in context["running"]] == [running.pk]
    assert failed.pk in [job.pk for job in context["failed"]] and context["failed_today"] == 1
    assert len(context["warnings"]) == 3
    content = page.content.decode()
    assert "Sensitive uploads have no expiry" in content and "1 more not seen recently" in content
    assert f'hx-get="{reverse("ui:admin-store")}"' in content


def test_the_overview_without_workers(world, admin):
    Worker.objects.all().delete()
    assert b"No worker is online" in admin.get(reverse("ui:admin")).content


def test_the_store_check(world, admin, monkeypatch):
    fragment = admin.get(reverse("ui:admin-store"), **HTMX)
    assert b"<html" not in fragment.content and b"is reachable" in fragment.content
    page = admin.get(reverse("ui:admin-store"))
    assert b"<html" in page.content and b"badge ok" in page.content

    def refuse(self):
        raise StoreUnavailable("The object store refused or failed HEAD of bucket x (403).")

    monkeypatch.setattr(storage.Bucket, "check_access", refuse)
    failed = admin.get(reverse("ui:admin-store"), **HTMX)
    assert b"unreachable" in failed.content and b"(403)" in failed.content


# --------------------------------------------------------------------------- users


def test_finding_users(world, admin):
    User.objects.filter(pk=world.viewer.pk).update(email="vic@example.org", is_active=False)
    by_email = admin.get(reverse("ui:admin-users"), {"q": "vic@"})
    assert [u.pk for u in by_email.context["page"]] == [world.viewer.pk]
    authors = admin.get(reverse("ui:admin-users"), {"role": "author"})
    assert [u.pk for u in authors.context["page"]] == [world.author.pk]
    inactive = admin.get(reverse("ui:admin-users"), {"state": "inactive"}, **HTMX)
    assert [u.pk for u in inactive.context["page"]] == [world.viewer.pk]
    assert b"<html" not in inactive.content and b'id="user-table"' in inactive.content
    active = admin.get(reverse("ui:admin-users"), {"state": "active", "role": "root"})
    assert world.viewer.pk not in [u.pk for u in active.context["page"]]
    assert b"No users match." in admin.get(reverse("ui:admin-users"), {"q": "nobody"}).content


@pytest.mark.parametrize(
    "fields,status,message",
    [
        ({"password1": NEW_PASSWORD, "password2": "other"}, 400, "not the same"),
        ({"password1": "123", "password2": "123"}, 400, "The password is not accepted"),
        (
            {"is_service_account": "on", "password1": NEW_PASSWORD, "password2": NEW_PASSWORD},
            400,
            "Service accounts sign in with API tokens",
        ),
    ],
)
def test_creating_a_user_that_cannot_be_created(world, admin, fields, status, message):
    response = admin.post(
        reverse("ui:admin-user-new"), {"username": "casey", "role": "viewer", **fields}
    )
    assert response.status_code == status and message in response.content.decode()
    assert not User.objects.filter(username="casey").exists()


def test_creating_users(world, admin):
    created = admin.post(
        reverse("ui:admin-user-new"),
        {
            "username": "casey",
            "role": "author",
            "email": "casey@example.org",
            "can_view_raw_rows": "on",
            "password1": NEW_PASSWORD,
            "password2": NEW_PASSWORD,
        },
        follow=True,
    )
    casey = User.objects.get(username="casey")
    assert created.redirect_chain[-1][0] == reverse("ui:admin-user", kwargs={"user_id": casey.pk})
    assert (casey.role, casey.can_view_raw_rows) == ("author", True)
    assert authenticate(username="casey", password=NEW_PASSWORD) == casey
    assert AuditLog.objects.filter(action="user.create", object_id=str(casey.pk)).exists()
    duplicate = admin.post(reverse("ui:admin-user-new"), {"username": "casey", "role": "viewer"})
    assert duplicate.status_code == 409
    admin.post(
        reverse("ui:admin-user-new"),
        {"username": "robot", "role": "operator", "is_service_account": "on"},
    )
    robot = User.objects.get(username="robot")
    assert robot.is_service_account and not robot.has_usable_password()
    page = admin.get(reverse("ui:admin-user", kwargs={"user_id": robot.pk})).content.decode()
    assert "service account" in page and "Set a password" not in page


def test_editing_a_user_and_the_last_admin(world, admin):
    url = reverse("ui:admin-user", kwargs={"user_id": world.viewer.pk})
    saved = admin.post(url, {"role": "operator", "is_active": "on", "first_name": "Vic"})
    assert saved.status_code == 302
    world.viewer.refresh_from_db()
    assert (world.viewer.role, world.viewer.first_name) == ("operator", "Vic")
    entry = AuditLog.objects.get(action="user.update")
    assert entry.details["changed"]["role"] == {"from": "viewer", "to": "operator"}

    own = reverse("ui:admin-user", kwargs={"user_id": world.admin.pk})
    page = admin.get(own)
    assert page.context["last_admin"] and b"the only active admin" in page.content
    demoted = admin.post(own, {"role": "viewer", "is_active": "on"})
    assert demoted.status_code == 409 and b"is the last active admin" in demoted.content
    deactivated = admin.post(own, {"role": "admin"})  # the checkbox left empty
    assert deactivated.status_code == 409
    world.admin.refresh_from_db()
    assert world.admin.role == "admin" and world.admin.is_active

    make_user(Role.ADMIN)
    assert not admin.get(own).context["last_admin"]
    assert admin.post(own, {"role": "author", "is_active": "on"}).status_code == 302
    world.admin.refresh_from_db()
    assert world.admin.role == "author"
    assert admin.get(own).status_code == 403  # no longer an admin


def test_an_invalid_edit_is_shown_on_the_form(world, admin):
    url = reverse("ui:admin-user", kwargs={"user_id": world.viewer.pk})
    response = admin.post(url, {"role": "root", "email": "not an email"})
    assert response.status_code == 400 and response.context["edit_form"].errors


def test_setting_a_password(world, admin):
    url = reverse("ui:admin-user-password", kwargs={"user_id": world.viewer.pk})
    mismatch = admin.post(url, {"password1": NEW_PASSWORD, "password2": "x"})
    assert mismatch.status_code == 400 and b"not the same" in mismatch.content
    weak = admin.post(url, {"password1": "password", "password2": "password"})
    assert weak.status_code == 400 and b"too common" in weak.content
    done = admin.post(url, {"password1": NEW_PASSWORD, "password2": NEW_PASSWORD}, follow=True)
    assert b"was set." in done.content
    assert authenticate(username=world.viewer.username, password=NEW_PASSWORD)
    robot = make_user(Role.VIEWER)
    User.objects.filter(pk=robot.pk).update(is_service_account=True)
    refused = admin.post(
        reverse("ui:admin-user-password", kwargs={"user_id": robot.pk}),
        {"password1": NEW_PASSWORD, "password2": NEW_PASSWORD},
    )
    assert refused.status_code == 400 and b"is a service account" in refused.content


# --------------------------------------------------------------------------- API tokens


def test_everyones_tokens(world, admin, api_token):
    api_token(world.viewer, ["jobs:read"], expires_at=timezone.now() - timedelta(days=1))
    page = admin.get(reverse("ui:admin-tokens"))
    states = {token.pk: token.state for token in page.context["rows"]}
    assert set(states.values()) == {"active", "expired"}
    only = admin.get(reverse("ui:admin-tokens"), {"owner": str(world.viewer.pk)})
    assert {token.owner_id for token in only.context["rows"]} == {world.viewer.pk}
    assert only.context["form"].initial["owner"] == str(world.viewer.pk)
    assert b"show all" in only.content


def test_a_token_form_without_scopes(world, admin):
    response = admin.post(
        reverse("ui:admin-tokens"), {"owner": str(world.viewer.pk), "name": "svc"}
    )
    assert response.status_code == 400 and response.context["form"].errors["scopes"]


def test_creating_and_revoking_someones_token(world, admin):
    beyond = admin.post(
        reverse("ui:admin-tokens"),
        {"owner": str(world.viewer.pk), "name": "svc", "scopes": ["jobs:run"], "expires_in": "7"},
    )
    assert beyond.status_code == 400 and b"go beyond the viewer role" in beyond.content
    created = admin.post(
        reverse("ui:admin-tokens"),
        {"owner": str(world.viewer.pk), "name": "svc", "scopes": ["jobs:read"], "expires_in": "7"},
    )
    assert created.status_code == 200
    token = ApiToken.objects.get(name="svc")
    assert token.owner == world.viewer and created.context["raw"] in created.content.decode()
    back_to = reverse("ui:admin-tokens") + f"?owner={world.viewer.pk}"
    revoked = admin.post(
        reverse("ui:admin-token-revoke", kwargs={"token_id": token.pk}), {"next": back_to}
    )
    assert revoked["Location"] == back_to
    page = admin.get(back_to).content.decode()
    assert "was revoked" in page and "badge revoked" in page
    again = admin.post(reverse("ui:admin-token-revoke", kwargs={"token_id": token.pk}))
    assert again.status_code == 409


# --------------------------------------------------------------------------- workers


def test_workers_and_their_jobs(world, admin):
    principal = worker_principal()
    job = start(make_job(world.operator, world.upload), principal)
    job.lease_worker = world.worker
    job.save()
    Worker.objects.create(
        worker_id="old", lanes=["sql"], last_seen_at=timezone.now() - timedelta(days=1)
    )
    page = admin.get(reverse("ui:admin-workers"))
    workers = {w.worker_id: w for w in page.context["workers"]}
    assert workers["worker-1"].online and not workers["old"].online
    assert workers["worker-1"].current_jobs == [job]
    assert str(job.pk)[:8] in page.content.decode()


def test_worker_tokens(world, admin):
    blank = admin.post(reverse("ui:admin-workers"), {"name": "", "expires_in": ""})
    assert blank.status_code == 400
    spaces = admin.post(reverse("ui:admin-workers"), {"name": " ", "expires_in": ""})
    assert spaces.status_code == 400
    created = admin.post(reverse("ui:admin-workers"), {"name": "sql pool", "expires_in": "90"})
    assert created.status_code == 200 and b"Your new worker token" in created.content
    token = WorkerToken.objects.get(name="sql pool")
    assert created.context["raw"].startswith("fkw_") and token.expires_at is not None
    url = reverse("ui:admin-worker-token-revoke", kwargs={"token_id": token.pk})
    assert b"was revoked" in admin.post(url, follow=True).content
    assert admin.post(url).status_code == 409


# --------------------------------------------------------------------------- connections


S3_SECRETS = {"secret_access_key_id": "AKIA-UI-TEST", "secret_secret_access_key": "s3-ui-secret"}


def s3_fields(world, **changes):
    return {
        "name": "landing",
        "description": "",
        "bucket": storage.store().bucket,
        "endpoint_url": world.connection.config["endpoint_url"],
        "prefix": "",
        "region": "",
        "addressing_style": "path",
        "allowed_roles": ["author", "admin"],
        **changes,
    }


def test_creating_an_s3_connection(world, admin):
    missing = admin.post(
        reverse("ui:admin-connection-new", kwargs={"kind": "s3"}), s3_fields(world)
    )
    assert missing.status_code == 400  # both secrets are required for a new bucket
    created = admin.post(
        reverse("ui:admin-connection-new", kwargs={"kind": "s3"}),
        {**s3_fields(world), **S3_SECRETS},
        follow=True,
    )
    connection = Connection.objects.get(name="landing")
    assert b"was created." in created.content
    assert connection.config["region"] == "us-east-1"
    assert connection.secret_fields == ["access_key_id", "secret_access_key"]
    assert secret_backend.backend().decrypt(connection.secret_ciphertext) == {
        "access_key_id": "AKIA-UI-TEST",
        "secret_access_key": "s3-ui-secret",
    }
    bad = admin.post(
        reverse("ui:admin-connection-new", kwargs={"kind": "s3"}),
        {**s3_fields(world, name="bad", bucket="No_Such"), **S3_SECRETS},
    )
    assert bad.status_code == 400 and b"is not a valid bucket name" in bad.content


def test_creating_a_sql_connection(world, admin):
    admin.post(
        reverse("ui:admin-connection-new", kwargs={"kind": "sql"}),
        {
            "name": "warehouse",
            "dialect": "sqlserver",
            "host": "db.example.org",
            "port": "",
            "database": "dw",
            "username": "loader",
            "driver": "",
            "options": '{"Encrypt": "yes"}',
            "secret_password": "sql-ui-secret",
            "allowed_roles": ["admin"],
        },
    )
    connection = Connection.objects.get(name="warehouse")
    assert connection.config["port"] == 1433
    assert connection.config["driver"] == "ODBC Driver 18 for SQL Server"
    assert connection.config["options"] == {"Encrypt": "yes"}
    assert connection.allowed_roles == ["admin"]


def test_an_unknown_connection_kind(admin):
    response = admin.get(reverse("ui:admin-connection-new", kwargs={"kind": "ftp"}))
    assert response.status_code == 404 and b"no connection kind" in response.content


def test_secrets_are_kept_replaced_and_removed(world, admin):
    url = reverse("ui:admin-connection", kwargs={"connection_id": world.connection.pk})
    page = admin.get(url)
    assert "remove_session_token" not in page.context["form"].fields  # not set yet
    assert b"Set. Type a new value to replace it" in page.content
    fields = s3_fields(world, name=world.connection.name)
    admin.post(url, {**fields, "secret_session_token": "session-ui-secret"})
    world.connection.refresh_from_db()
    secrets = secret_backend.backend().decrypt(world.connection.secret_ciphertext)
    assert secrets["session_token"] == "session-ui-secret"
    assert secrets["secret_access_key"]  # kept

    form = admin.get(url).context["form"]
    assert "remove_session_token" in form.fields  # optional and set: it can be removed
    assert "remove_secret_access_key" not in form.fields  # required ones are only replaced
    admin.post(url, {**fields, "remove_session_token": "on", "secret_access_key_id": "NEW-ID"})
    world.connection.refresh_from_db()
    secrets = secret_backend.backend().decrypt(world.connection.secret_ciphertext)
    assert "session_token" not in secrets and secrets["access_key_id"] == "NEW-ID"
    entry = AuditLog.objects.filter(action="connection.update").latest("id")
    assert entry.details["secrets_changed"] == ["access_key_id", "session_token"]


def test_editing_a_connection_that_cannot_be_saved(world, admin):
    url = reverse("ui:admin-connection", kwargs={"connection_id": world.connection.pk})
    clash = admin.post(url, s3_fields(world, name=world.spare_connection.name))
    assert clash.status_code == 409 and b"already exists" in clash.content
    invalid = admin.post(url, s3_fields(world, name=""))
    assert invalid.status_code == 400


def test_testing_a_connection(world, admin, tmp_path):
    url = reverse("ui:admin-connection-test", kwargs={"connection_id": world.connection.pk})
    ok = admin.post(url, **HTMX)
    assert b"<html" not in ok.content and b"badge ok" in ok.content
    page = admin.post(url, follow=True)
    assert b"message success" in page.content and b"is reachable" in page.content

    folder = Connection.objects.create(
        name="exports", kind="localfs", config={"root_path": "/nonexistent/forklift-ui"}
    )
    url = reverse("ui:admin-connection-test", kwargs={"connection_id": folder.pk})
    unknown = admin.post(url, **HTMX)
    assert (
        b"badge unknown" in unknown.content and b"is not visible to the gateway" in unknown.content
    )
    assert b"message info" in admin.post(url, follow=True).content

    closed = Connection.objects.create(
        name="nowhere",
        kind="sql",
        config={
            "dialect": "postgresql",
            "host": "127.0.0.1",
            "port": 1,
            "database": "d",
            "username": "u",
            "driver": "PostgreSQL Unicode",
            "options": {},
        },
        secret_ciphertext=secret_backend.backend().encrypt({"password": "p"}),
        secret_fields=["password"],
    )
    url = reverse("ui:admin-connection-test", kwargs={"connection_id": closed.pk})
    assert b"badge error" in admin.post(url, **HTMX).content
    assert b"message error" in admin.post(url, follow=True).content
    listed = admin.get(reverse("ui:admin-connections")).content.decode()
    assert "badge unknown" in listed and "badge error" in listed and "badge ok" in listed


def test_deleting_a_connection(world, admin):
    Dataset.objects.filter(pk=world.dataset.pk).update(source_connection=world.connection)
    url = reverse("ui:admin-connection-delete", kwargs={"connection_id": world.connection.pk})
    kept = admin.post(url, follow=True)
    assert b"is used by the datasets" in kept.content
    assert Connection.objects.filter(pk=world.connection.pk).exists()
    url = reverse(
        "ui:admin-connection-delete", kwargs={"connection_id": world.spare_connection.pk}
    )
    assert b"was deleted." in admin.post(url, follow=True).content


# --------------------------------------------------------------------------- retention


def test_the_retention_page(world, admin):
    RetentionPolicy.objects.filter(scope="installation").update(days={"previews": 7, "data": None})
    page = admin.get(reverse("ui:admin-retention"))
    levels = {level["title"]: level for level in page.context["levels"]}
    assert levels["The installation"]["cells"][1:4] == ["until deleted", "", "7 days"]
    assert levels["Public data"]["cells"][1] == "30 days"
    assert levels[f"Dataset {world.spare_dataset.name}"]["cells"][1] == "1 day"
    assert levels["Internal data"]["policy"] is None
    choices = [d.pk for d in page.context["new_dataset"]["datasets"]]
    assert world.dataset.pk in choices and world.spare_dataset.pk not in choices
    assert b"Sensitive uploads have no expiry" in page.content


def test_saving_policies(world, admin):
    data = retention_data(
        "classification-sensitive", "classification", "sensitive", uploads=7, data=30, bad_rows=30
    )
    data["classification-sensitive-job_records_mode"] = "forever"
    saved = admin.post(reverse("ui:admin-retention-save"), data, follow=True)
    assert b"The retention policy was saved." in saved.content
    policy = RetentionPolicy.objects.get(scope="classification", classification="sensitive")
    assert policy.days == {"uploads": 7, "data": 30, "bad_rows": 30, "job_records": None}
    assert saved.context["warnings"] == []
    assert b"Sensitive data expires at every level." in saved.content

    added = retention_data("dataset-new", "dataset", str(world.dataset.pk), data=3)
    admin.post(reverse("ui:admin-retention-save"), added)
    assert RetentionPolicy.objects.get(dataset=world.dataset).days == {"data": 3}
    page = admin.get(reverse("ui:admin-retention"))
    assert b"Every dataset has a policy of its own" in page.content


def test_a_policy_that_cannot_be_saved(world, admin):
    data = retention_data("installation", "installation")
    data["installation-uploads_mode"] = "days"  # without a number of days
    missing = admin.post(reverse("ui:admin-retention-save"), data)
    assert (
        missing.status_code == 400
        and b"Give the number of days to keep Uploads." in missing.content
    )
    bound = {level["prefix"]: level["form"] for level in missing.context["levels"]}
    assert bound["installation"].is_bound and bound["installation"].errors
    unknown = retention_data("classification-x", "classification", "secret", data=1)
    refused = admin.post(reverse("ui:admin-retention-save"), unknown)
    assert refused.status_code == 400 and b"Unknown classification" in refused.content


def test_removing_a_policy_and_sweeping(world, admin):
    url = reverse("ui:admin-retention-delete")
    removed = admin.post(url, {"scope": "classification", "target": "public"}, follow=True)
    assert b"its level inherits again" in removed.content
    assert not RetentionPolicy.objects.filter(scope="classification").exists()
    missing = admin.post(url, {"scope": "classification", "target": "public"})
    assert missing.status_code == 404
    dataset = admin.post(url, {"scope": "dataset", "target": str(world.spare_dataset.pk)})
    assert dataset.status_code == 302

    dry = admin.post(reverse("ui:admin-retention-sweep"), {"dry_run": "1"})
    assert dry.status_code == 200 and dry.context["report"].dry_run
    assert b"What a sweep would delete now" in dry.content
    real = admin.post(reverse("ui:admin-retention-sweep"), {"dry_run": "0"})
    assert not real.context["report"].dry_run and b"The sweep deleted" in real.content


# --------------------------------------------------------------------------- audit log


def test_filtering_the_audit_log(world, admin):
    actor = Actor.for_user(world.admin)
    installation.update(actor, {"lease_seconds": 30})
    page = admin.get(reverse("ui:admin-audit"))
    assert "settings.update" in dict(page.context["form"].fields["action"].choices)
    by_action = admin.get(reverse("ui:admin-audit"), {"action": "settings.update"})
    assert [e.action for e in by_action.context["page"]] == ["settings.update"]
    by_actor = admin.get(reverse("ui:admin-audit"), {"actor": world.admin.username}, **HTMX)
    assert by_actor.context["page"].paginator.count >= 1
    assert b"<html" not in by_actor.content and b'id="audit-table"' in by_actor.content
    nobody = admin.get(reverse("ui:admin-audit"), {"actor": "nobody-at-all"})
    assert nobody.context["page"].paginator.count == 0
    later = (timezone.now() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    future = admin.get(reverse("ui:admin-audit"), {"since": later})
    assert future.context["page"].paginator.count == 0
    window = admin.get(
        reverse("ui:admin-audit"), {"until": later, "object_type": "", "object_id": ""}
    )
    assert window.context["page"].paginator.count >= 1
    invalid = admin.get(reverse("ui:admin-audit"), {"since": "yesterday"})
    assert invalid.context["problem"].startswith("The filters are not valid: since:")
    assert invalid.context["page"].paginator.count == 0


def test_exporting_the_audit_log_as_csv(world, admin):
    User.objects.create(username="=HYPERLINK(1)", role="viewer")
    actor = Actor.for_user(world.admin)
    from forklift_web.services import accounts

    accounts.update_user(actor, User.objects.get(username="=HYPERLINK(1)").pk, role="operator")
    response = admin.get(reverse("ui:admin-audit-export"), {"action": "user.update"})
    assert response["Content-Type"].startswith("text/csv")
    assert response["Content-Disposition"].startswith('attachment; filename="forklift-audit-')
    rows = list(csv.reader(io.StringIO(b"".join(response.streaming_content).decode())))
    assert rows[0][:5] == ["id", "created_at", "actor", "token_prefix", "action"]
    assert len(rows) == 2 and rows[1][4] == "user.update"
    assert rows[1][7] == "'=HYPERLINK(1)"  # a spreadsheet would run it as a formula otherwise
    assert '"role"' in rows[1][8]
    invalid = admin.get(reverse("ui:admin-audit-export"), {"until": "soon"})
    assert invalid.status_code == 400 and b"The filters are not valid" in invalid.content


# --------------------------------------------------------------------------- settings


def test_the_settings_page(world, admin):
    page = admin.get(reverse("ui:admin-settings"))
    shapes = {row["key"]: row["form"].shape for row in page.context["rows"]}
    assert shapes["lease_seconds"] == "integer"
    assert shapes["token_max_days"] == "optional_integer"
    assert shapes["default_classification"] == "choice"
    assert shapes["lane_limits"] == "table"
    assert b"(2.0 GiB)" in page.content  # stage_max_bytes, in readable units


def setting(client, key, **fields):
    data = {f"{key}-{name}": value for name, value in fields.items()}
    return client.post(reverse("ui:admin-setting", kwargs={"key": key}), data)


def test_changing_and_resetting_settings(world, admin):
    saved = setting(admin, "lease_seconds", value="30")
    assert saved["Location"].endswith("#setting-lease_seconds")
    assert installation.get("lease_seconds") == 30
    assert AuditLog.objects.filter(action="settings.update").exists()
    too_small = setting(admin, "lease_seconds", value="5")
    assert too_small.status_code == 400 and b"must be at least 10" in too_small.content
    rows = {row["key"]: row["form"] for row in too_small.context["rows"]}
    assert rows["lease_seconds"].is_bound and not rows["max_attempts"].is_bound
    assert setting(admin, "lease_seconds", value="soon").status_code == 400

    setting(admin, "default_classification", value="sensitive")
    assert installation.get("default_classification") == "sensitive"
    setting(admin, "token_max_days", value="30")
    assert installation.get("token_max_days") == 30
    setting(admin, "token_max_days", value="")
    assert installation.get("token_max_days") is None

    setting(admin, "lane_limits", **{"interactive__max_rows": "1000", "batch__max_seconds": ""})
    limits = installation.get("lane_limits")
    assert limits["interactive"]["max_rows"] == 1000 and limits["batch"]["max_seconds"] is None
    assert b"Reset to the default" in admin.get(reverse("ui:admin-settings")).content
    reset = admin.post(reverse("ui:admin-setting-reset", kwargs={"key": "lane_limits"}))
    assert reset["Location"].endswith("#setting-lane_limits")
    assert not InstallationSetting.objects.filter(key="lane_limits").exists()


def test_unknown_settings(admin):
    assert setting(admin, "colour", value="1").status_code == 404
    reset = admin.post(reverse("ui:admin-setting-reset", kwargs={"key": "colour"}))
    assert reset.status_code == 404 and b"no installation setting" in reset.content


def test_settings_of_other_shapes_are_edited_as_json():
    form = SettingForm(
        {"x-value": '["a", "b"]'},
        key="x",
        setting={"value": ["a"], "default": [], "description": "A list."},
        prefix="x",
    )
    assert form.shape == "json" and form.is_valid() and form.setting_value() == ["a", "b"]
    broken = SettingForm(
        {"x-value": "[1"},
        key="x",
        setting={"value": [], "default": [], "description": ""},
        prefix="x",
    )
    assert not broken.is_valid() and "not valid JSON" in str(broken.errors)


# --------------------------------------------------------------------------- all jobs


def test_all_jobs(world, admin):
    sql_job = make_job(world.author, world.upload, lane="sql")
    by_lane = admin.get(reverse("ui:admin-jobs"), {"lane": "sql"})
    assert [job.pk for job in by_lane.context["rows"]] == [sql_job.pk]
    by_requester = admin.get(reverse("ui:admin-jobs"), {"requester": world.author.username})
    assert [job.pk for job in by_requester.context["rows"]] == [sql_job.pk]
    assert all(job.can_cancel for job in by_requester.context["rows"])  # admins cancel any job
    fragment = admin.get(reverse("ui:admin-jobs"), {"status": "queued"}, **HTMX)
    assert b"<html" not in fragment.content and b'<th scope="col">Lane</th>' in fragment.content
    full = admin.get(reverse("ui:admin-jobs")).content.decode()
    assert "All jobs" in full and "worker-1" not in full
    assert admin.get(reverse("ui:admin-jobs"), {"status": "lost"}).status_code == 200


def test_admins_sign_in_with_their_password(world, client):
    assert client.login(username=world.admin.username, password=PASSWORD)
