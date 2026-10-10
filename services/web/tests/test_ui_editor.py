"""The schema editor in a real browser (Chromium, through Playwright): CodeMirror under the real
Content-Security-Policy, diagnostics from the browser and from the gateway, completion from the
schema standards, the Columns view, the starting points, keyboard use, and the page without
JavaScript.

The fixtures (a browser, a page that collects console errors, the bucket's CORS rule) are the
UI browser tests' own (tests/test_ui_browser.py); a CSP violation is a console error.
"""

from __future__ import annotations

import json
import re

import pytest
import test_ui_browser
from django.urls import reverse
from playwright.sync_api import expect
from test_ui_browser import sign_in
from ui_support import REPORT, as_json, finish
from world import make_upload, make_user

from forklift_web.core.choices import JobKind, Role
from forklift_web.core.models import Job, Schema, SchemaVersion

pytestmark = [pytest.mark.browser, pytest.mark.django_db(transaction=True)]
# The UI browser tests' fixtures, used here by name
browser = test_ui_browser.browser
bucket_cors = test_ui_browser.bucket_cors
page = test_ui_browser.page

EDITOR = "[data-schema-editor] .cm-content"
STATUS = "#id_document-status"
# Key order and number text matter: "2024" must stay second (JSON.parse would put it first)
# and 10.0 must stay 10.0; "x-note" and "x-csv" are keys the Columns view does not show.
ORDERS = """{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/orders.json",
  "title": "Orders",
  "type": "object",
  "properties": {
    "order_id": {
      "type": "integer",
      "x-note": "kept as it is"
    },
    "2024": {
      "type": "number",
      "minimum": 10.0
    },
    "customer": {
      "type": "string"
    }
  },
  "required": [
    "order_id"
  ],
  "x-csv": {
    "delimiter": ";",
    "header": {
      "mode": "present"
    }
  }
}"""


def orders_schema(author) -> Schema:
    schema = Schema.objects.create(name="orders", created_by=author)
    SchemaVersion.objects.create(
        schema=schema, number=1, document=json.loads(ORDERS), sha256="0" * 64, author=author
    )
    return schema


def open_editor(page, url: str) -> None:
    page.goto(url)
    expect(page.locator(EDITOR)).to_be_visible()


def set_text(page, text: str) -> None:
    """Replace the editor's text, as pasting would."""
    page.locator(EDITOR).click()
    page.keyboard.press("Control+a")
    page.keyboard.insert_text(text)


def editor_text(page) -> str:
    # The text area is the form field: it has the editor's text at every moment.
    return page.locator("#id_document").input_value()


def accept(page) -> None:
    """Pick the selected completion with Enter. CodeMirror ignores Enter for 75 ms after the
    list opens, so that typing fast does not pick an option by accident."""
    page.wait_for_timeout(100)
    page.keyboard.press("Enter")


def columns_view(page):
    page.get_by_role("button", name="Columns", exact=True).click()
    return page.locator("[data-columns-view]")


def test_the_editor_works_under_the_content_security_policy(page):
    sign_in(page, make_user(Role.AUTHOR))
    response = page.goto(reverse("ui:schema-new"))
    policy = response.headers["content-security-policy"]
    assert "style-src 'self';" in policy and "script-src 'self';" in policy
    assert "unsafe-inline" not in policy and "nonce" not in policy
    editor = page.locator("[data-schema-editor] .cm-editor")
    expect(editor).to_be_visible()
    # CodeMirror's own styles apply (they are constructed style sheets in the shadow root)
    assert editor.evaluate("node => getComputedStyle(node).display") == "flex"
    assert page.locator("#id_document").is_hidden()
    expect(page.locator(STATUS)).to_have_text("Empty: type or paste a JSON object.")
    set_text(page, '{"type":"object","properties":{"id":{"type":"integer"}}}')
    expect(page.locator(STATUS)).to_contain_text("Valid JSON: 1 column in “properties”.")
    page.get_by_role("button", name="Format the JSON").click()
    assert editor_text(page).startswith('{\n  "type": "object",\n  "properties": {\n    "id"')
    page.get_by_label("Name").fill("people")
    page.get_by_role("button", name="Create the schema").click()
    expect(page.get_by_role("heading", level=1)).to_have_text("people")
    assert SchemaVersion.objects.get(schema__name="people").document["properties"] == {
        "id": {"type": "integer"}
    }
    assert page.errors == []  # a refused style or script is a console error


def test_syntax_errors_show_at_once_and_the_gateways_problems_in_their_place(page):
    sign_in(page, make_user(Role.AUTHOR))
    open_editor(page, reverse("ui:schema-new"))
    status = page.locator(STATUS)
    set_text(page, '{"type": "object", "properties": {"id": }}')
    expect(status).to_have_text(
        "Not valid JSON yet (line 1, column 41): expected a value (a string, number, object, "
        "array, true, false or null)."
    )
    expect(page.locator(".cm-lintRange-error")).to_have_text("}")

    document = json.loads(ORDERS)
    document["required"] = ["order_id", "custmer"]
    del document["$id"]
    set_text(page, json.dumps(document, indent=2))
    expect(status).to_have_text(
        "Valid JSON: 3 columns in “properties”. 2 problems to look at, listed below."
    )
    problems = page.locator("[data-problems] li")
    expect(problems).to_have_text(
        [
            re.compile(r"^line 1, column 1: The engine needs an \"\$id\" under https://github"),
            "line 20, column 5: required names 'custmer', which is not a column of this schema.",
        ]
    )
    expect(page.locator(".cm-lintRange-warning").last).to_have_text('"custmer"')
    problems.last.get_by_role("button").click()
    expect(page.locator("[data-schema-editor] .cm-editor")).to_have_class(re.compile("cm-focused"))
    expect(page.locator(".cm-activeLine")).to_have_text('    "custmer"')
    assert page.errors == []


def test_completion_offers_what_the_standards_use_where_it_fits(page):
    sign_in(page, make_user(Role.AUTHOR))
    open_editor(page, reverse("ui:schema-new"))
    completions = page.locator(".cm-tooltip-autocomplete")

    set_text(page, '{\n  "properties": {\n    "id": {\n      \n    }\n  }\n}')
    page.keyboard.press("Control+Home")
    for _ in range(3):
        page.keyboard.press("ArrowDown")
    page.keyboard.press("End")
    page.keyboard.type('"ty')
    expect(completions).to_contain_text("type")
    accept(page)
    page.keyboard.type('"int')
    expect(completions.locator("li").first).to_contain_text("integer")
    accept(page)
    assert (
        editor_text(page)
        == '{\n  "properties": {\n    "id": {\n      "type": "integer"\n    }\n  }\n}'
    )

    set_text(page, '{\n  "x-pri')
    page.keyboard.press("Control+Space")
    expect(completions).to_contain_text("x-primaryKey")
    expect(page.locator(".cm-completionInfo")).to_contain_text("Primary key configuration")

    # column names where a column belongs, once the document has columns
    set_text(page, '{"properties": {"id": {"type": "integer"}, "name": {}}, "required": []}')
    expect(page.locator(STATUS)).to_contain_text("Valid JSON: 2 columns")
    page.keyboard.press("Control+End")
    page.keyboard.press("ArrowLeft")
    page.keyboard.press("ArrowLeft")
    page.keyboard.press("Control+Space")
    expect(completions.locator("li")).to_have_text([re.compile("^id"), re.compile("^name")])
    page.keyboard.press("Escape")  # closes the list; the editor keeps the focus
    expect(completions).to_have_count(0)
    expect(page.locator("[data-schema-editor] .cm-editor")).to_have_class(re.compile("cm-focused"))
    assert page.errors == []


def test_the_columns_view_edits_the_json_and_keeps_what_it_does_not_show(page):
    author = make_user(Role.AUTHOR)
    schema = orders_schema(author)
    sign_in(page, author)
    open_editor(page, reverse("ui:version-new", kwargs={"schema_id": schema.pk}))
    assert editor_text(page) == ORDERS
    view = columns_view(page)
    expect(view.locator("legend")).to_have_text(
        ["Column 1: order_id", "Column 2: 2024", "Column 3: customer"]
    )
    customer = view.locator('[data-column="2"]')
    customer.get_by_label("Format", exact=True).select_option("email")
    customer.get_by_label("Nullable: values may be empty").check()
    customer.get_by_label("Required: the file must have it").check()
    view.locator('[data-column="1"]').get_by_text("Rules").click()
    view.locator('[data-column="1"]').get_by_label("Largest value").fill("99.50")
    view.locator('[data-column="1"]').get_by_label("Largest value").press("Tab")

    expected = json.loads(ORDERS)
    expected["properties"]["customer"] = {"type": ["string", "null"], "format": "email"}
    expected["properties"]["2024"]["maximum"] = 99.5
    expected["required"].append("customer")
    text = editor_text(page)
    assert json.loads(text) == expected
    assert list(json.loads(text)["properties"]) == ["order_id", "2024", "customer"]
    assert '"minimum": 10.0' in text and '"maximum": 99.50' in text  # numbers as written
    assert '"x-note": "kept as it is"' in text and '"delimiter": ";"' in text

    # and the other way round: the JSON view's edits show in the form
    page.get_by_role("button", name="JSON", exact=True).click()
    set_text(page, text.replace('"customer"', '"buyer"'))
    view = columns_view(page)
    expect(view.locator("legend").last).to_have_text("Column 3: buyer")
    expect(view.locator('[data-column="2"]').get_by_label("Format", exact=True)).to_have_value(
        "email"
    )
    assert page.errors == []


def test_columns_are_reordered_added_and_removed_with_the_keyboard(page):
    author = make_user(Role.AUTHOR)
    schema = orders_schema(author)
    sign_in(page, author)
    open_editor(page, reverse("ui:version-new", kwargs={"schema_id": schema.pk}))
    view = columns_view(page)

    page.get_by_role("button", name="Move up 2024").focus()
    page.keyboard.press("Enter")
    assert list(json.loads(editor_text(page))["properties"]) == ["2024", "order_id", "customer"]
    expect(page.get_by_role("button", name="Move down 2024")).to_be_focused()  # up is disabled
    expect(view.locator("[aria-live]")).to_have_text("Moved 2024 up: it is column 1 of 3 now.")
    page.keyboard.press("Enter")
    assert list(json.loads(editor_text(page))["properties"]) == ["order_id", "2024", "customer"]

    page.get_by_role("button", name="Remove order_id").focus()
    page.keyboard.press("Enter")
    document = json.loads(editor_text(page))
    assert list(document["properties"]) == ["2024", "customer"]
    assert document["required"] == []  # it named the removed column
    expect(view.get_by_label("Name").first).to_be_focused()

    page.get_by_role("button", name="Add a column").focus()
    page.keyboard.press("Enter")
    name = view.locator('[data-column="2"]').get_by_label("Name")
    expect(name).to_be_focused()
    page.keyboard.type("email")
    page.keyboard.press("Tab")
    properties = json.loads(editor_text(page))["properties"]
    assert list(properties) == ["2024", "customer", "email"]
    assert properties["email"] == {"type": "string"}
    assert page.errors == []


def test_saving_creates_a_version_identical_to_the_json_shown(page):
    author = make_user(Role.AUTHOR)
    schema = orders_schema(author)
    sign_in(page, author)
    open_editor(page, reverse("ui:version-new", kwargs={"schema_id": schema.pk}))
    view = columns_view(page)
    view.locator('[data-column="0"]').get_by_label("Primary key: unique and never empty").check()
    view.locator('[data-column="2"]').get_by_label(
        "Unique: no two rows have the same value"
    ).check()
    shown = editor_text(page)
    page.get_by_label("What changed in this version").fill("keys")
    page.get_by_role("button", name="Save version 2").click()
    expect(page.get_by_role("heading", level=1)).to_contain_text("version 2")
    saved = SchemaVersion.objects.get(schema=schema, number=2).document
    assert json.dumps(saved, indent=2, ensure_ascii=False) == shown
    assert saved["x-primaryKey"] == {"columns": ["order_id"]}
    assert saved["x-uniqueConstraints"] == [{"name": "customer_unique", "columns": ["customer"]}]
    assert page.errors == []


def test_a_generated_schema_is_a_starting_point(page):
    author = make_user(Role.AUTHOR)
    own = make_upload(author)
    sign_in(page, author)
    open_editor(page, reverse("ui:schema-new"))
    set_text(page, '{"type": "object"}')
    page.get_by_label("Generate from").select_option(str(own.pk))
    page.get_by_role("button", name="Generate a schema").click()
    expect(page.locator("#generation")).to_contain_text("Generating", timeout=10_000)
    job = Job.objects.get(kind=JobKind.GENERATE_SCHEMA, upload=own)
    generated = {"type": "object", "properties": {"id": {"type": "integer"}, "2024": {}}}
    finish(job, {"schema.json": ("schema", as_json(generated))})
    page.get_by_role("button", name="Use it in the editor").click(timeout=10_000)
    expect(page.locator("#generation")).to_contain_text("The draft is now the generated schema")
    assert editor_text(page) == json.dumps(generated, indent=2)  # "id" stays first
    page.locator(EDITOR).click()
    page.keyboard.press("Control+z")
    assert editor_text(page) == '{"type": "object"}'
    # the link on the job's page ("Edit and save it as a new schema") starts from it too
    page.goto(reverse("ui:schema-new") + f"?from_artifact={job.artifacts.get().pk}")
    expect(page.locator("#id_document")).to_have_value(json.dumps(generated, indent=2))
    assert page.errors == []


def test_ctrl_enter_checks_the_draft_against_a_file(page):
    author = make_user(Role.AUTHOR)
    own = make_upload(author)
    sign_in(page, author)
    open_editor(page, reverse("ui:schema-new") + f"?upload={own.pk}")
    set_text(
        page,
        '{"type": "object", "properties": {"email": {"type": "string"}}, "required": ["email"]}',
    )
    page.keyboard.press("Control+Enter")
    validation = page.locator("#validation")
    expect(validation).to_contain_text("Checking", timeout=10_000)
    job = Job.objects.get(kind=JobKind.VALIDATE_SCHEMA, upload=own)
    assert job.spec["schema"]["required"] == ["email"]  # the draft as typed, not saved
    finish(
        job,
        {"report.json": ("report", as_json(REPORT))},
        status="failed",
        error={"code": "COLUMN_MISSING", "message": "No column 'email'.", "retryable": False},
    )
    expect(validation).to_contain_text("The draft does not fit", timeout=10_000)
    expect(validation).to_contain_text("Schema columns missing from the input")
    assert Schema.objects.count() == 0
    assert page.errors == []


def test_the_editor_is_no_keyboard_trap(page):
    sign_in(page, make_user(Role.AUTHOR))
    open_editor(page, reverse("ui:schema-new"))
    page.get_by_label("Description").focus()
    page.keyboard.press("Tab")
    expect(page.get_by_role("button", name="JSON", exact=True)).to_be_focused()
    page.keyboard.press("Tab")
    page.keyboard.press("Tab")
    expect(page.locator("[data-schema-editor] .cm-editor")).to_have_class(re.compile("cm-focused"))
    page.keyboard.type('{"a": 1}')
    expect(page.locator(STATUS)).to_contain_text("problems to look at")
    page.keyboard.press("Tab")  # leaves the editor, typing no tab: the problems come next
    expect(page.locator("[data-problems] button").first).to_be_focused()
    assert editor_text(page) == '{"a": 1}'
    page.keyboard.press("Shift+Tab")
    editor = page.locator("[data-schema-editor] .cm-editor")
    expect(editor).to_have_class(re.compile("cm-focused"))
    page.keyboard.press("F8")  # the next problem, in a tooltip
    expect(page.locator(".cm-tooltip-lint")).to_be_visible()
    # CodeMirror's list of problems, and back (KeyM: as a keyboard sends M with Shift)
    page.keyboard.press("Control+Shift+KeyM")
    expect(page.locator(".cm-panel-lint li")).to_have_count(5)
    page.keyboard.press("Escape")
    expect(page.locator(".cm-panel-lint")).to_have_count(0)
    expect(editor).to_have_class(re.compile("cm-focused"))
    page.get_by_role("button", name="Format the JSON").focus()
    page.get_by_text("Schema document (JSON)").click()  # the label focuses the editor
    expect(editor).to_have_class(re.compile("cm-focused"))
    assert page.errors == []


def test_reduced_motion_and_narrow_screens(browser, live_server):
    context = browser.new_context(
        base_url=live_server.url, reduced_motion="reduce", viewport={"width": 360, "height": 740}
    )
    page = context.new_page()
    sign_in(page, make_user(Role.AUTHOR))
    open_editor(page, reverse("ui:schema-new"))
    page.locator(EDITOR).click()
    layer = page.locator("[data-schema-editor] .cm-cursorLayer")
    assert layer.evaluate("node => node.style.animationDuration") == "0ms"  # no blinking
    set_text(page, ORDERS)
    columns_view(page)
    width = page.evaluate("document.documentElement.scrollWidth")
    assert width <= 360, f"the page scrolls sideways ({width}px)"
    context.close()


def test_the_page_works_without_javascript(browser, live_server):
    context = browser.new_context(base_url=live_server.url, java_script_enabled=False)
    page = context.new_page()
    author = make_user(Role.AUTHOR)
    sign_in(page, author)
    page.goto(reverse("ui:schema-new"))
    expect(page.locator(".cm-editor")).to_have_count(0)
    expect(page.get_by_role("button", name="Columns")).to_be_hidden()
    page.get_by_label("Name").fill("people")
    page.get_by_label("Schema document (JSON)").fill('{"type": "object", "properties": {}}')
    page.get_by_role("button", name="Create the schema").click()
    expect(page.get_by_role("heading", level=1)).to_have_text("people")
    assert SchemaVersion.objects.get(schema__name="people").document == {
        "type": "object",
        "properties": {},
    }
    context.close()
