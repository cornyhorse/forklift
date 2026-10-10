"""Security properties of the HTML UI: CSRF on every form, secrets never rendered, the
Content-Security-Policy, static files only on the public port, and HTMX requests of signed-out
users."""

from __future__ import annotations

import pytest
from django.test import Client
from django.urls import reverse
from test_ui_matrix import ROWS, call
from ui_support import PREVIEW, Page, as_json, finish
from world import World, make_job

from forklift_web import secret_backend
from forklift_web.core.choices import JobKind
from forklift_web.core.models import ApiToken, Connection, User
from forklift_web.middleware import INTERNAL, SURFACE_KEY
from forklift_web.policy import Actor
from forklift_web.services import accounts, workers
from forklift_web.ui.middleware import AWS_S3_ORIGINS, store_origin

pytestmark = pytest.mark.django_db

POST_ROWS = [row for row in ROWS if row.method == "POST"]
SECRETS = {
    "access_key_id": "AKIA-NEVER-SHOWN-1",
    "secret_access_key": "never-shown-secret-key-2",
    "session_token": "never-shown-session-3",
}
SQL_PASSWORD = "never-shown-sql-password-4"


def success_caller(row) -> str:
    """A role for which ``row`` succeeds (the CSRF check must refuse it anyway)."""
    statuses = row.statuses()
    return next(caller for caller in ("admin", "operator") if statuses[caller] in {200, 302})


@pytest.mark.parametrize("row", POST_ROWS, ids=[row.name for row in POST_ROWS])
def test_every_form_action_needs_the_csrf_token(row):
    world = World.build()
    user = getattr(world, success_caller(row))
    client = Client(enforce_csrf_checks=True)
    client.force_login(user)
    response = call(row, world, client, user)
    assert response.status_code == 403, response.content[:300]
    assert b"CSRF" in response.content


def test_signing_in_and_out_need_the_csrf_token(make_user):
    user = make_user("viewer")
    client = Client(enforce_csrf_checks=True)
    login = client.post(reverse("forklift-login"), {"username": user.username, "password": "x"})
    assert login.status_code == 403
    client.force_login(user)
    assert client.post(reverse("forklift-logout")).status_code == 403
    assert client.get("/").status_code == 200  # still signed in


def pages(world: World) -> list:
    """Every page an admin can open, including the forms of each connection kind."""
    urls = [
        row.url(world) for row in ROWS if row.method == "GET" and row.name != "artifact-download"
    ]
    for connection in Connection.objects.all():
        urls.append(reverse("ui:admin-connection", kwargs={"connection_id": connection.pk}))
    for kind in ("s3", "sql", "localfs"):
        urls.append(reverse("ui:admin-connection-new", kwargs={"kind": kind}))
    urls.append(reverse("ui:admin-audit") + "?action=connection.create")
    return urls


def text(response) -> str:
    if response.streaming:
        return b"".join(response.streaming_content).decode()
    return response.content.decode()


def build_world() -> World:
    world = World.build()
    world.extra["validation"] = make_job(
        world.operator, world.upload, kind=JobKind.VALIDATE_SCHEMA
    )
    finish(
        make_job(world.operator, world.upload, kind=JobKind.PREVIEW),
        {"preview.json": ("preview", as_json(PREVIEW))},
    )
    return world


def test_every_form_on_every_page_carries_the_csrf_token():
    world = build_world()
    client = Client()
    client.force_login(world.admin)
    checked = 0
    for url in pages(world):
        response = client.get(url)
        assert response.status_code == 200, url
        for form in Page(text(response)).forms:
            if form["method"] == "post":
                assert "csrfmiddlewaretoken" in form["fields"], (url, form)
                checked += 1
    assert checked > 40


def test_secrets_are_never_rendered(api_token):
    world = build_world()
    admin = Actor.for_user(world.admin)
    from forklift_web.services import connections

    s3 = connections.create_connection(
        admin,
        name="secret-bucket",
        kind="s3",
        config={"bucket": "some-bucket", "endpoint_url": "http://127.0.0.1:19000"},
        secrets=SECRETS,
    )
    sql = connections.create_connection(
        admin,
        name="secret-db",
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": SQL_PASSWORD},
    )
    _, raw_api = accounts.create_token(admin, name="kept", scopes=["jobs:read"])
    _, raw_worker = workers.create_worker_token(admin, name="kept")
    password_hash = User.objects.get(pk=world.admin.pk).password
    forbidden = [*SECRETS.values(), SQL_PASSWORD, raw_api, raw_worker, password_hash]
    forbidden += [token.token_hash for token in ApiToken.objects.all()]
    forbidden.append(secret_backend.backend().encrypt({"x": "y"})[:20])  # no ciphertexts either
    forbidden.append(Connection.objects.get(pk=s3.pk).secret_ciphertext)

    client = Client()
    client.force_login(world.admin)
    for url in pages(world):
        content = text(client.get(url))
        for value in forbidden:
            assert value not in content, (url, value[:6])

    # A form that is shown again after an error never repeats what was typed into a secret
    edit = reverse("ui:admin-connection", kwargs={"connection_id": sql.pk})
    again = client.post(
        edit, {"name": "", "dialect": "postgresql", "secret_password": "typed-secret-5"}
    )
    assert again.status_code == 400 and b"typed-secret-5" not in again.content
    new = client.post(
        reverse("ui:admin-connection-new", kwargs={"kind": "s3"}),
        {
            "name": "x",
            "bucket": "Bad_Bucket",
            "secret_access_key_id": "typed-secret-6",
            "secret_secret_access_key": "typed-secret-7",
            "addressing_style": "path",
        },
    )
    assert new.status_code == 400
    assert b"typed-secret-6" not in new.content and b"typed-secret-7" not in new.content
    user = client.post(
        reverse("ui:admin-user-new"),
        {"username": "x", "role": "viewer", "password1": "typed-secret-8", "password2": "other"},
    )
    assert user.status_code == 400 and b"typed-secret-8" not in user.content


def test_html_pages_carry_a_strict_content_security_policy(client, settings, make_user):
    page = client.get(reverse("forklift-login"))
    assert b"csrfmiddlewaretoken" in page.content
    policy = page["Content-Security-Policy"]
    assert "script-src 'self'" in policy and "style-src 'self'" in policy
    assert "frame-ancestors 'none'" in policy and "unsafe" not in policy
    assert (
        f"connect-src 'self' {store_origin(settings.FORKLIFT_STORE['public_endpoint_url'])}"
        in policy
    )
    assert page["X-Frame-Options"] == "DENY"
    client.force_login(make_user("viewer"))
    assert "Content-Security-Policy" in client.get("/")
    assert "Content-Security-Policy" not in client.get("/api/v1/me")  # JSON
    assert "Content-Security-Policy" not in client.get("/api/v1/docs")  # its viewer uses a CDN


def test_the_store_origin_for_the_policy():
    assert store_origin("https://s3.example.org:9000/") == "https://s3.example.org:9000"
    assert store_origin(None) == AWS_S3_ORIGINS


def test_pages_use_no_inline_scripts_or_styles(make_user):
    world = build_world()
    client = Client()
    client.force_login(world.admin)
    for url in pages(world):
        content = text(client.get(url))
        assert "<script>" not in content and " style=" not in content, url
        assert " onclick=" not in content and "hx-on" not in content, url


def test_static_files_are_served_on_the_public_port_only():
    public = Client().get("/static/ui/forklift.css")
    assert public.status_code == 200
    assert b"prefers-color-scheme: dark" in b"".join(public.streaming_content)
    htmx = Client().get("/static/ui/vendor/htmx-2.0.11.min.js")
    assert htmx.status_code == 200
    internal = Client(**{SURFACE_KEY: INTERNAL}).get("/static/ui/forklift.css")
    assert internal.status_code == 404


def test_htmx_requests_of_signed_out_users_go_to_the_sign_in_page(client):
    job_live = "/jobs/00000000-0000-0000-0000-000000000000/live/"
    response = client.get(
        job_live, HTTP_HX_REQUEST="true", HTTP_HX_CURRENT_URL="http://testserver/jobs/?mine=on"
    )
    assert response.status_code == 401
    assert response["HX-Redirect"] == reverse("forklift-login") + "?next=/jobs/"
    without = client.get(job_live, HTTP_HX_REQUEST="true")
    assert without["HX-Redirect"] == reverse("forklift-login") + "?next=" + job_live


def test_htmx_errors_are_fragments(client, make_user):
    client.force_login(make_user("viewer"))
    missing = client.get(
        "/jobs/00000000-0000-0000-0000-000000000000/live/", HTTP_HX_REQUEST="true"
    )
    assert missing.status_code == 404
    content = missing.content.decode()
    assert "<html" not in content and 'data-error-code="not_found"' in content
    page = client.get("/jobs/00000000-0000-0000-0000-000000000000/")
    assert page.status_code == 404 and b"<h1>Not found</h1>" in page.content


def test_job_pages_are_not_cached(client, make_user):
    world = World.build()
    client.force_login(world.operator)
    response = client.get(reverse("ui:job", kwargs={"job_id": world.queued_job.pk}))
    assert "no-store" in response["Cache-Control"]
