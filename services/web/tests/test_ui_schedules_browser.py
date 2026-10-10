"""The schedule form in a real browser: the live preview as you type, and keyboard-only use."""

from __future__ import annotations

import pytest
from django.urls import reverse
from playwright.sync_api import expect
from test_ui_browser import browser, bucket_cors, page, sign_in  # noqa: F401  (the fixtures)
from world import make_schema, make_user, s3_connection

from forklift_web.core.choices import Role
from forklift_web.core.models import Dataset, Schedule

pytestmark = [pytest.mark.browser, pytest.mark.django_db(transaction=True)]


def test_the_schedule_form_previews_runs_as_you_type(page):  # noqa: F811
    author = make_user(Role.AUTHOR)
    dataset = Dataset.objects.create(
        name="nightly people",
        schema_version=make_schema(author, name="people"),
        source_connection=s3_connection(),
        source_path="exports/people.csv",
    )
    sign_in(page, author)
    page.goto(reverse("ui:dataset", kwargs={"dataset_id": dataset.pk}))
    preview = page.locator("#schedule-preview")
    expect(preview).to_contain_text("Type a cron expression")

    page.get_by_label("When (cron expression)").focus()
    page.keyboard.type("0 9 * * mon")
    expect(preview).to_contain_text("The next runs of 0 9 * * mon in UTC:")
    expect(preview.locator("li")).to_have_count(5)
    expect(preview.locator("li").first).to_contain_text("Mon ")
    page.keyboard.press("Tab")
    zone = page.get_by_label("Time zone")
    expect(zone).to_be_focused()
    page.keyboard.press("Control+A")
    page.keyboard.type("America/New_York")
    page.keyboard.press("Tab")  # the time zone is checked once it is complete (on change)
    expect(preview).to_contain_text("in America/New_York:")
    expect(preview.locator("li").first).to_contain_text("09:00 E")

    page.get_by_label("When (cron expression)").fill("0 0 31 2 *")
    expect(preview).to_contain_text("never runs: no date matches it")
    expect(preview.locator("li")).to_have_count(0)

    page.get_by_label("When (cron expression)").fill("30 6 * * *")
    expect(preview).to_contain_text("The next runs of 30 6 * * *")
    page.get_by_role("button", name="Add the schedule").focus()
    page.keyboard.press("Enter")
    expect(page.locator(".messages")).to_contain_text("now runs on the schedule 30 6 * * *")
    table = page.get_by_role("region", name="Schedules")
    expect(table).to_contain_text("(America/New_York)")
    schedule = Schedule.objects.get(dataset=dataset)
    assert (schedule.cron, schedule.timezone, schedule.enabled) == (
        "30 6 * * *",
        "America/New_York",
        True,
    )

    page.get_by_role("button", name="Disable the schedule 30 6 * * *").focus()
    page.keyboard.press("Enter")
    expect(page.locator(".messages")).to_contain_text("was disabled")
    expect(page.get_by_role("button", name="Enable the schedule 30 6 * * *")).to_be_visible()
    schedule.refresh_from_db()
    assert not schedule.enabled
    assert page.errors == []


def test_a_mistake_is_announced_next_to_its_field(page):  # noqa: F811
    author = make_user(Role.AUTHOR)
    dataset = Dataset.objects.create(
        name="hourly people",
        schema_version=make_schema(author, name="people"),
        source_connection=s3_connection(),
        source_path="exports/people.csv",
    )
    sign_in(page, author)
    page.goto(reverse("ui:schedule-new", kwargs={"dataset_id": dataset.pk}))
    page.get_by_label("When (cron expression)").fill("61 * * * *")
    page.get_by_role("button", name="Add the schedule").click()
    cron = page.get_by_label("When (cron expression)")
    expect(cron).to_have_attribute("aria-invalid", "true")
    expect(cron).to_have_accessible_description(
        "Five fields, minute hour day-of-month month day-of-week, or a macro such as @daily. "
        "30 2 * * * runs at 02:30 every day, 0 6 * * mon-fri at 06:00 on weekdays. "
        "minute field '61': must be 0-59."
    )
    expect(page.locator("#schedule-preview")).to_contain_text("must be 0-59")  # shown again
    # Chromium reports the 400 page it loaded; nothing else went wrong.
    assert [
        e.startswith("Failed to load resource: the server responded with a status of 400")
        for e in page.errors
    ] == [True]
