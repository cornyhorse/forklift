"""The UI in a real browser (Chromium, through Playwright) against pytest-django's live server
and the test store (RustFS): sign in, upload a CSV through the real presigned PUT (and a large
file through a multipart upload), see the job queued, follow it while a fake worker finishes
it, read the preview the browser fetches from the store, and the admin flows.

The store's CORS rule allows the live server's origin, as a deployment's must allow the UI's
(see the bucket CORS fixture). Jobs do not run without a worker: tests finish them through the
queue service (tests/ui_support.py).
"""

from __future__ import annotations

import io
import re

import pytest
from django.core.management import call_command
from django.urls import reverse
from playwright.sync_api import expect, sync_playwright
from ui_support import PREVIEW, as_json, finish, start, worker_principal
from webhook_support import Receiver
from world import PASSWORD, make_schema, make_user

from forklift_web.core.choices import JobKind, JobStatus, Role, UploadStatus
from forklift_web.core.models import AuditLog, Connection, Job, Upload, User, Webhook
from forklift_web.policy import Actor
from forklift_web.services import installation, webhooks

pytestmark = [pytest.mark.browser, pytest.mark.django_db(transaction=True)]

CSV = "id,name\n1,alice\n2,bob\n"
MIB = 1024 * 1024


@pytest.fixture(scope="module")
def browser():
    # Playwright's synchronous API runs an event loop in this thread; the ORM calls the tests
    # make between browser steps are ordinary synchronous calls.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("DJANGO_ALLOW_ASYNC_UNSAFE", "true")
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(args=["--no-proxy-server"])
            yield browser
            browser.close()


@pytest.fixture
def bucket_cors(live_server, test_bucket, s3):
    """The CORS rule a deployment needs on its bucket for the UI's origin."""
    call_command("configure_cors", "--origin", live_server.url, stdout=io.StringIO())
    yield
    s3.delete_bucket_cors(Bucket=test_bucket)


@pytest.fixture
def page(browser, live_server, bucket_cors):
    context = browser.new_context(base_url=live_server.url)
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "console",
        lambda message: errors.append(message.text) if message.type == "error" else None,
    )
    page.errors = errors
    yield page
    context.close()


def sign_in(page, user) -> None:
    page.goto(reverse("forklift-login"))
    page.get_by_label("Username").fill(user.username)
    page.get_by_label("Password").fill(PASSWORD)
    page.get_by_role("button", name="Sign in").click()
    expect(page.get_by_role("heading", level=1)).to_contain_text("Hello")


def upload(page, path, *, classification: str = "Internal") -> Upload:
    page.goto(reverse("ui:upload"))
    page.get_by_label("File").set_input_files(str(path))
    page.get_by_label("Classification").select_option(label=classification)
    page.get_by_role("button", name="Upload", exact=True).click()
    page.wait_for_url(re.compile(r"/uploads/[0-9a-f-]{36}/$"), timeout=30_000)
    upload_id = page.url.rstrip("/").rsplit("/", 1)[-1]
    return Upload.objects.get(pk=upload_id)


def test_sign_in_upload_a_csv_and_run_it(page, tmp_path):
    operator = make_user(Role.OPERATOR)
    version = make_schema(make_user(Role.AUTHOR), name="people")
    sign_in(page, operator)
    path = tmp_path / "people.csv"
    path.write_text(CSV)

    uploaded = upload(page, path)
    assert (uploaded.status, uploaded.size, uploaded.uploaded_by) == (
        UploadStatus.COMPLETE,
        len(CSV),
        operator,
    )
    expect(page.get_by_role("heading", level=1)).to_have_text("people.csv")

    page.get_by_label("Run it with a schema version").check()
    page.get_by_label("Schema version (to run or validate)").select_option(label="people v1")
    page.get_by_role("button", name="Start").click()
    page.wait_for_url(re.compile(r"/jobs/[0-9a-f-]{36}/$"))
    job = Job.objects.get(pk=page.url.rstrip("/").rsplit("/", 1)[-1])
    assert (job.kind, job.status, job.upload_id, job.schema_version_id) == (
        JobKind.RUN,
        JobStatus.QUEUED,
        uploaded.pk,
        version.pk,
    )
    expect(page.locator("#job-live .badge").first).to_have_text("Queued")

    # A worker picks it up: the page follows by itself (HTMX polling).
    principal = worker_principal()
    start(job, principal, progress={"rows_read": 1, "rows_rejected": 0, "bytes_read": 12})
    expect(page.locator("#job-live .badge").first).to_have_text("Running", timeout=10_000)
    expect(page.locator("#job-live")).to_contain_text("rows read")
    finish(
        job,
        {"data.parquet": ("data", b"PAR1-data"), "bad_rows.parquet": ("bad_rows", b"PAR1-bad")},
        principal=principal,
    )
    expect(page.locator("#job-live .badge").first).to_have_text("Succeeded", timeout=10_000)
    expect(page.get_by_role("link", name="Download data.parquet")).to_be_visible()
    assert page.locator("#job-live[hx-trigger]").count() == 0  # polling stopped
    assert page.errors == []


def test_a_large_file_goes_up_in_parts(page, tmp_path):
    installation.update(
        Actor.for_system("tests"),
        {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB},
    )
    operator = make_user(Role.OPERATOR)
    sign_in(page, operator)
    path = tmp_path / "big.csv"
    row = "1,a fairly long value to make the file larger than a few parts\n"
    path.write_text("id,text\n" + row * (11 * MIB // len(row) + 1))

    uploaded = upload(page, path)
    assert uploaded.is_multipart and uploaded.status == UploadStatus.COMPLETE
    assert uploaded.size == path.stat().st_size
    assert page.errors == []


def test_a_preview_is_fetched_by_the_browser_and_shown_as_text(page, tmp_path):
    operator = make_user(Role.OPERATOR)
    sign_in(page, operator)
    path = tmp_path / "people.csv"
    path.write_text(CSV)
    uploaded = upload(page, path)
    page.get_by_label("Preview its first rows").check()
    page.get_by_role("button", name="Start").click()
    page.wait_for_url(re.compile(r"/jobs/[0-9a-f-]{36}/$"))
    job = Job.objects.get(pk=page.url.rstrip("/").rsplit("/", 1)[-1])
    assert (job.kind, job.upload_id) == (JobKind.PREVIEW, uploaded.pk)

    hostile = dict(PREVIEW, rows=[["1", "<img src=x onerror=alert(1)>"], ["2", None]])
    finish(job, {"preview.json": ("preview", as_json(hostile))})
    table = page.locator("table.preview")
    expect(table).to_be_visible(timeout=10_000)
    expect(table.locator("thead th")).to_have_text(["#", "id", "name"])
    expect(table.locator("tbody tr").first).to_contain_text("<img src=x onerror=alert(1)>")
    assert table.locator("img").count() == 0  # cells are text, never HTML
    expect(table.locator("td.null")).to_have_text("null")
    assert AuditLog.objects.filter(action="artifact.download", actor=operator).exists()
    assert page.errors == []


def test_a_generated_schema_is_shown_with_its_columns_in_order(page, tmp_path):
    operator = make_user(Role.OPERATOR)
    sign_in(page, operator)
    path = tmp_path / "years.csv"
    path.write_text("2024,id\n1,2\n")
    upload(page, path)
    page.get_by_label("Generate a schema from it").check()
    page.get_by_role("button", name="Start").click()
    page.wait_for_url(re.compile(r"/jobs/[0-9a-f-]{36}/$"))
    job = Job.objects.get(pk=page.url.rstrip("/").rsplit("/", 1)[-1])
    assert job.kind == JobKind.GENERATE_SCHEMA

    # The file names "id" before "2024"; JSON.parse would put the integer-like "2024" first
    text = (
        '{\n  "properties": {\n    "id": {"type": "integer"},\n'
        '    "2024": {"type": "integer"}\n  }\n}'
    )
    finish(job, {"schema.json": ("schema", text.encode())})
    shown = page.locator("[data-kind='schema'] [data-viewer-body] pre")
    expect(shown).to_be_visible(timeout=10_000)
    assert shown.text_content() == text  # exactly the file, in its order
    assert page.errors == []


def test_an_admin_creates_a_user_and_a_connection(page):
    admin = make_user(Role.ADMIN)
    sign_in(page, admin)
    page.get_by_role("navigation", name="Main").get_by_role("link", name="Admin").click()
    expect(page.get_by_role("heading", level=1)).to_have_text("Administration")
    expect(page.locator("#store-heading + div")).to_contain_text("reachable")  # loaded by HTMX

    page.get_by_role("navigation", name="Administration").get_by_role("link", name="Users").click()
    page.get_by_role("link", name="New user").click()
    page.get_by_label("Username").fill("casey")
    page.get_by_label("Role").select_option("operator")
    page.get_by_label("Password", exact=True).fill("a much better passphrase 7")
    page.get_by_label("Password again").fill("a much better passphrase 7")
    page.get_by_role("button", name="Create the user").click()
    expect(page.get_by_role("status")).to_contain_text("The user 'casey' was created.")
    assert User.objects.get(username="casey").role == Role.OPERATOR

    page.get_by_role("navigation", name="Administration").get_by_role(
        "link", name="Connections"
    ).click()
    page.get_by_role("link", name="New SQL database").click()
    page.get_by_label("Name").fill("warehouse")
    page.get_by_label("Dialect").select_option("postgresql")
    page.get_by_label("Host").fill("127.0.0.1")
    page.get_by_label("Port").fill("15432")
    page.get_by_label("Database").fill("forklift_test")
    page.get_by_label("Login").fill("loader")
    page.get_by_label("Password").fill("browser-secret-value-1")
    page.get_by_role("button", name="Create the connection").click()
    expect(page.get_by_role("heading", level=1)).to_have_text("warehouse")
    connection = Connection.objects.get(name="warehouse")
    assert connection.secret_fields == ["password"]
    assert "browser-secret-value-1" not in page.content()

    page.get_by_role("button", name="Test the connection").click()
    expect(page.locator("#test-result")).to_contain_text("15432")  # swapped in by HTMX
    assert page.errors == []


def test_the_user_search_filters_as_you_type(page):
    admin = make_user(Role.ADMIN)
    make_user(Role.VIEWER, name="findme-viewer")
    make_user(Role.VIEWER, name="someone-else")
    sign_in(page, admin)
    page.goto(reverse("ui:admin-users"))
    page.get_by_label("Name or email contains").fill("findme")
    expect(page.locator("#user-table tbody tr")).to_have_count(1)
    expect(page.locator("#user-table")).to_contain_text("findme-viewer")
    assert "q=findme" in page.url  # the filter is in the address, so it can be shared
    assert page.errors == []


def test_the_sign_in_page_says_when_to_try_again(page):
    installation.update(Actor.for_system("test"), {"sign_in_max_failures": 2})
    viewer = make_user(Role.VIEWER)
    page.goto(reverse("forklift-login"))
    alert = page.get_by_role("alert")
    for password, message in (
        ("not the password", "did not match"),
        ("not it either", "Too many failed sign-in attempts. Try again in 15 minutes, after"),
        (PASSWORD, "Too many failed sign-in attempts"),  # refused, not checked, while locked
    ):
        page.get_by_label("Username").fill(viewer.username)
        page.get_by_label("Password").fill(password)
        page.get_by_role("button", name="Sign in").click()
        expect(alert).to_contain_text(message)
    expect(page.get_by_role("heading", level=1)).to_have_text("Sign in to Forklift")
    # Chromium reports each 429 page it loads; nothing else went wrong.
    status = "Failed to load resource: the server responded with a status of 429"
    assert [error.startswith(status) for error in page.errors] == [True, True]


def test_keyboard_only_use(page):
    viewer = make_user(Role.VIEWER)
    page.goto(reverse("forklift-login"))
    expect(page.get_by_label("Username")).to_be_focused()  # autofocus
    page.keyboard.type(viewer.username)
    page.keyboard.press("Tab")
    expect(page.get_by_label("Password")).to_be_focused()
    page.keyboard.type(PASSWORD)
    page.keyboard.press("Enter")
    expect(page.get_by_role("heading", level=1)).to_contain_text("Hello")
    page.keyboard.press("Tab")
    skip = page.get_by_role("link", name="Skip to the content")
    expect(skip).to_be_focused()
    page.keyboard.press("Enter")
    expect(page.locator("main")).to_be_focused()
    nav = page.get_by_role("navigation", name="Main")
    expect(nav.get_by_role("link", name="Home")).to_have_attribute("aria-current", "page")
    expect(nav.get_by_role("link", name="Upload")).to_have_count(0)  # not for viewers
    expect(nav.get_by_role("link", name="Admin")).to_have_count(0)
    assert page.errors == []


def test_a_webhook_is_created_shown_once_and_tested(page, settings):
    receiver = Receiver(204)
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["127.0.0.1"]
    try:
        sign_in(page, make_user(Role.VIEWER))
        page.get_by_role("link", name="Webhooks").click()
        expect(page.get_by_role("heading", level=1)).to_have_text("Your webhooks")
        page.get_by_label("Name").fill("pipeline")
        page.get_by_label("URL").fill(f"http://127.0.0.1:{receiver.port}/forklift")
        page.get_by_role("group", name="Events").get_by_label("Job succeeded").uncheck()
        page.get_by_role("button", name="Create the webhook").click()
        expect(page.get_by_role("heading", level=1)).to_have_text("Your new webhook")
        secret = page.locator("#secret-value").inner_text()
        assert secret.startswith("fkwh_")
        expect(page.get_by_role("button", name="Copy the secret")).to_be_visible()
        page.get_by_role("link", name="Done").click()
        expect(page.get_by_role("heading", level=1)).to_contain_text("pipeline")
        assert secret not in page.content()
        page.get_by_role("button", name="Send a test event").click()
        expect(page.locator(".messages")).to_contain_text("The test event is queued")
        log = page.get_by_role("region", name="Deliveries")
        expect(log).to_contain_text("Pending")
        assert receiver.requests == []  # nothing is sent while the gateway answers a request
        assert webhooks.deliver_due()["delivered"] == 1  # the dispatcher's next pass
        # The log polls while the test waits, then announces how it went and stops
        expect(page.locator("#webhook-announce")).to_have_text(
            "The test event was delivered: the receiver answered 204.", timeout=10_000
        )
        expect(log).to_contain_text("Delivered")
        expect(page.locator("#webhook-log")).not_to_have_attribute("hx-trigger", "every 2s")
    finally:
        receiver.stop()
    assert Webhook.objects.get().events == ["job.cancelled", "job.failed"]
    assert receiver.requests[0].headers["forklift-event"] == "webhook.test"
    assert page.errors == []
