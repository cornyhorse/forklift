"""Schedules in the UI: the dataset page's section, adding one with its live preview, changing,
enabling, disabling and deleting, the page of every schedule and scheduled jobs on the job
pages."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from django.urls import reverse
from ui_support import Page
from world import World, make_schedule

from forklift_web.core.choices import JobStatus
from forklift_web.core.models import Dataset, Job, Schedule
from forklift_web.services import schedules

pytestmark = pytest.mark.django_db

HTMX = {"HTTP_HX_REQUEST": "true"}
UTC = timezone.utc


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def author(client, world):
    client.force_login(world.author)
    return client


def dataset_page(client, dataset_id):
    return client.get(reverse("ui:dataset", kwargs={"dataset_id": dataset_id}))


def new_url(dataset_id) -> str:
    return reverse("ui:schedule-new", kwargs={"dataset_id": dataset_id})


def ran(world) -> Schedule:
    """A schedule whose 09:00 run the dispatcher queued."""
    schedule = make_schedule(
        world.author,
        world.version,
        world.connection,
        cron="0 * * * *",
        next_run_at=datetime(2026, 10, 10, 9, tzinfo=UTC),
    )
    schedules.fire_due(datetime(2026, 10, 10, 9, 0, 5, tzinfo=UTC))
    schedule.refresh_from_db()
    return schedule


def test_adding_a_schedule_on_the_dataset_page(world, author):
    dataset = world.schedule().dataset
    Schedule.objects.all().delete()
    page = dataset_page(author, dataset.pk)
    html = page.content.decode()
    assert 'id="schedules"' in html and "Add a schedule" in html
    form = next(f for f in Page(html).forms if f["action"] == new_url(dataset.pk))
    assert {"schedule-cron", "schedule-timezone", "schedule-enabled"} <= set(form["fields"])
    assert 'list="time-zones"' in html and '<option value="Europe/Berlin">' in html
    assert 'hx-get="/schedules/preview/"' in html and 'aria-live="polite"' in html
    added = author.post(
        new_url(dataset.pk),
        {
            "schedule-cron": "30 2 * * *",
            "schedule-timezone": "Europe/Berlin",
            "schedule-enabled": "on",
        },
    )
    assert added.status_code == 302
    assert added["Location"] == reverse("ui:dataset", kwargs={"dataset_id": dataset.pk}) + (
        "#schedules"
    )
    schedule = Schedule.objects.get()
    assert (schedule.cron, schedule.timezone, schedule.enabled) == (
        "30 2 * * *",
        "Europe/Berlin",
        True,
    )
    shown = dataset_page(author, dataset.pk).content.decode()
    assert "now runs on the schedule 30 2 * * *" in shown
    assert "<code>30 2 * * *</code>" in shown and "(Europe/Berlin)" in shown
    assert "02:30 CE" in shown  # the next run, in the schedule's time zone
    assert "not yet" in shown


def test_mistakes_are_shown_next_to_their_field(world, author):
    dataset = world.schedule().dataset
    bad_cron = author.post(
        new_url(dataset.pk), {"schedule-cron": "61 * * * *", "schedule-timezone": "UTC"}
    )
    assert bad_cron.status_code == 400
    html = bad_cron.content.decode()
    assert 'aria-invalid="true"' in html
    assert 'id="id_schedule-cron_error"' in html and "minute field &#x27;61&#x27;" in html
    assert 'hx-trigger="load, input changed' in html  # the preview shows the typed value again
    bad_zone = author.post(
        new_url(dataset.pk), {"schedule-cron": "@daily", "schedule-timezone": "Mars/Base"}
    )
    assert bad_zone.status_code == 400
    assert 'id="id_schedule-timezone_error"' in bad_zone.content.decode()
    missing = author.post(new_url(dataset.pk), {"schedule-timezone": "UTC"})
    assert missing.status_code == 400 and b"This field is required" in missing.content
    assert Schedule.objects.count() == 1  # only the world's own


def test_datasets_that_read_uploads_explain_why_they_cannot_be_scheduled(world, author):
    html = dataset_page(author, world.dataset.pk).content.decode()
    assert "reads uploaded files, so it cannot run on a schedule" in html
    assert new_url(world.dataset.pk) not in html
    page = author.get(new_url(world.dataset.pk))
    assert page.status_code == 200 and b"cannot run on a schedule" in page.content
    refused = author.post(
        new_url(world.dataset.pk), {"schedule-cron": "@daily", "schedule-timezone": "UTC"}
    )
    assert refused.status_code == 400
    assert b"reads uploaded files" in refused.content  # the service's message


def test_the_schedule_page_of_a_dataset(world, author):
    dataset = world.schedule().dataset
    page = author.get(new_url(dataset.pk))
    assert page.status_code == 200
    html = page.content.decode()
    assert f"Schedule {dataset.name}" in html and 'aria-labelledby="schedule-heading"' in html
    assert 'hx-trigger="input changed' in html  # nothing typed yet: no preview on load


def test_the_live_preview(world, author):
    url = reverse("ui:schedule-preview")
    shown = author.get(
        url, {"schedule-cron": "0 9 * * mon", "schedule-timezone": "America/New_York"}, **HTMX
    )
    html = shown.content.decode()
    assert shown.status_code == 200
    assert "The next runs of <code>0 9 * * mon</code> in America/New_York:" in html
    assert html.count("<li><time") == 5 and html.count("Mon ") == 5 and "09:00 E" in html
    invalid = author.get(url, {"schedule-cron": "0 0 31 2 *"}, **HTMX).content.decode()
    assert "never runs" in invalid and "<li>" not in invalid
    zone = author.get(url, {"schedule-cron": "@daily", "schedule-timezone": "Nowhere"}, **HTMX)
    assert b"Unknown time zone" in zone.content
    empty = author.get(url, **HTMX)
    assert b"Type a cron expression" in empty.content
    utc = author.get(url, {"schedule-cron": "@hourly", "schedule-timezone": " "}).content
    assert b"in UTC:" in utc


def test_enabling_disabling_and_deleting(world, author):
    schedule = world.schedule()
    back = reverse("ui:dataset", kwargs={"dataset_id": schedule.dataset_id}) + "#schedules"
    enable_url = reverse("ui:schedule-enable", kwargs={"schedule_id": schedule.pk})
    off = author.post(enable_url, {"enabled": "false", "next": back})
    assert (off.status_code, off["Location"]) == (302, back)
    schedule.refresh_from_db()
    assert not schedule.enabled and schedule.next_run_at is None
    html = dataset_page(author, schedule.dataset_id).content.decode()
    assert "was disabled" in html and "Disabled</span>" in html and ">Enable<" in html
    on = author.post(enable_url, {"enabled": "true"})
    assert on["Location"] == back  # without a next field: the dataset page
    schedule.refresh_from_db()
    assert schedule.enabled and schedule.next_run_at is not None
    # A dataset that changed into one that reads uploads cannot be enabled: a message says why
    Dataset.objects.filter(pk=schedule.dataset_id).update(source_connection=None, source_path="")
    author.post(enable_url, {"enabled": "false"})
    refused = author.post(enable_url, {"enabled": "true"}, follow=True)
    assert b"reads uploaded files, so it cannot run on a schedule" in refused.content
    deleted = author.post(
        reverse("ui:schedule-delete", kwargs={"schedule_id": schedule.pk}), follow=True
    )
    assert b"was deleted" in deleted.content and not Schedule.objects.exists()


def edit_url(schedule_id) -> str:
    return reverse("ui:schedule-edit", kwargs={"schedule_id": schedule_id})


def test_changing_a_schedule(world, author):
    schedule = world.schedule()
    html = dataset_page(author, schedule.dataset_id).content.decode()
    assert f'href="{edit_url(schedule.pk)}"' in html and ">Change<" in html
    page = author.get(edit_url(schedule.pk))
    assert page.status_code == 200
    html = page.content.decode()
    assert f"Change a schedule of {schedule.dataset.name}" in html and "counts from now" in html
    form = next(f for f in Page(html).forms if f["action"] == edit_url(schedule.pk))
    assert {"schedule-cron", "schedule-timezone", "schedule-enabled"} <= set(form["fields"])
    # filled in with the schedule's values
    assert 'value="0 2 * * *"' in html and 'name="schedule-timezone" value="UTC"' in html
    assert 'hx-trigger="load, input changed' in html  # its next runs show at once
    assert "Save the schedule" in html
    changed = author.post(
        edit_url(schedule.pk),
        {"schedule-cron": "15 6 * * mon-fri", "schedule-timezone": "Europe/Berlin"},
    )
    assert changed.status_code == 302
    assert changed["Location"] == reverse(
        "ui:dataset", kwargs={"dataset_id": schedule.dataset_id}
    ) + ("#schedules")
    schedule.refresh_from_db()
    assert (schedule.cron, schedule.timezone, schedule.enabled) == (
        "15 6 * * mon-fri",
        "Europe/Berlin",
        False,  # the box was not ticked
    )
    assert schedule.next_run_at is None
    shown = dataset_page(author, schedule.dataset_id).content.decode()
    assert "is now 15 6 * * mon-fri (Europe/Berlin), disabled." in shown
    enabled = author.post(
        edit_url(schedule.pk),
        {"schedule-cron": "@daily", "schedule-timezone": "UTC", "schedule-enabled": "on"},
        follow=True,
    )
    assert b"is now @daily (UTC)." in enabled.content
    schedule.refresh_from_db()
    assert schedule.enabled and schedule.next_run_at is not None


def test_changing_a_schedule_shows_mistakes_next_to_their_field(world, author):
    schedule = world.schedule()
    bad = author.post(
        edit_url(schedule.pk), {"schedule-cron": "0 0 31 2 *", "schedule-timezone": "UTC"}
    )
    assert bad.status_code == 400
    html = bad.content.decode()
    assert 'id="id_schedule-cron_error"' in html and "Save the schedule" in html
    schedule.refresh_from_db()
    assert schedule.cron == "0 2 * * *"  # unchanged


def test_changes_need_the_author_role(world, client):
    schedule = world.schedule()
    client.force_login(world.operator)
    html = dataset_page(client, schedule.dataset_id).content.decode()
    assert "<code>0 2 * * *</code>" in html  # seen
    assert "Add a schedule" not in html and ">Disable<" not in html and "cannot run" not in html
    refused = client.post(
        reverse("ui:schedule-enable", kwargs={"schedule_id": schedule.pk}), {"enabled": "false"}
    )
    assert refused.status_code == 403 and b"datasets:write" in refused.content
    assert client.get(edit_url(schedule.pk)).status_code == 403
    missing = client.post(
        reverse(
            "ui:schedule-delete", kwargs={"schedule_id": "00000000-0000-0000-0000-000000000000"}
        )
    )
    assert missing.status_code == 404


def test_the_page_of_every_schedule(world, author):
    queued = ran(world)
    failed = make_schedule(
        world.author,
        world.version,
        world.connection,
        cron="*/5 * * * *",
        next_run_at=datetime(2026, 10, 10, 9, tzinfo=UTC),
    )
    Dataset.objects.filter(pk=failed.dataset_id).update(source_connection=None, source_path="")
    schedules.fire_due(datetime(2026, 10, 10, 9, 0, 5, tzinfo=UTC))
    paused = make_schedule(world.author, world.version, world.connection, enabled=False)
    page = author.get(reverse("ui:schedules"))
    html = page.content.decode()
    assert page.status_code == 200 and 'aria-current="page">Schedules' in html
    assert queued.dataset.name in html and paused.dataset.name in html
    assert reverse("ui:job", kwargs={"job_id": queued.last_job_id}) in html
    assert '<span class="badge queued">Queued a run</span>' in html
    assert '<span class="badge failed">Could not queue a run</span>' in html
    assert "Could not queue the run due at 2026-10-10 09:00 (UTC)" in html
    only_paused = author.get(reverse("ui:schedules"), {"enabled": "false"})
    assert [s.pk for s in only_paused.context["rows"]] == [paused.pk]
    assert len(author.get(reverse("ui:schedules"), {"enabled": "true"}).context["rows"]) == 2
    assert len(author.get(reverse("ui:schedules"), {"enabled": "maybe"}).context["rows"]) == 3


def test_viewers_see_schedules_without_controls(world, client):
    world.schedule()
    client.force_login(world.viewer)
    html = client.get(reverse("ui:schedules")).content.decode()
    assert "<code>0 2 * * *</code>" in html and ">Delete<" not in html
    assert '<a href="/schedules/"' in client.get(reverse("ui:home")).content.decode()


def test_job_pages_say_a_schedule_started_the_job(world, author):
    schedule = ran(world)
    job = schedule.last_job
    detail = author.get(reverse("ui:job", kwargs={"job_id": job.pk})).content.decode()
    assert "Started by</dt>" in detail and "<code>0 * * * *</code> (UTC)" in detail
    assert "for the run due at 2026-10-10 09:00 UTC" in detail
    assert "Requested by" not in detail
    listed = author.get(reverse("ui:jobs")).content.decode()
    assert "<td>a schedule</td>" in listed
    Job.objects.filter(pk=job.pk).update(status=JobStatus.SUCCEEDED)
    schedule.delete()
    gone = author.get(reverse("ui:job", kwargs={"job_id": job.pk})).content.decode()
    assert "a schedule that was deleted since" in gone
    other = author.get(reverse("ui:job", kwargs={"job_id": world.queued_job.pk})).content
    assert b"Requested by" in other
