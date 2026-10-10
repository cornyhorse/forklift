"""Schedules: the service and its permissions, the dispatcher's rules (fire_due) and the API."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

import pytest
from django.db import IntegrityError, connection, transaction
from world import World, api_token, make_schedule

from forklift_web.core.choices import JobStatus, ScheduleOutcome
from forklift_web.core.models import AuditLog, Dataset, Job, Schedule
from forklift_web.errors import Conflict, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Actor
from forklift_web.services import datasets, installation, jobs, schedules

pytestmark = pytest.mark.django_db

UTC = timezone.utc


@pytest.fixture
def world():
    return World.build()


def actor(user, token=None) -> Actor:
    return Actor.for_user(user, token=token)


def source_dataset(world, name: str = "people") -> Dataset:
    return Dataset.objects.create(
        name=name,
        schema_version=world.version,
        source_connection=world.connection,
        source_path="exports/people.csv",
        created_by=world.author,
    )


def due(world, cron: str = "0 * * * *", *, at: datetime) -> Schedule:
    """A schedule of a new dataset whose next run is ``at``."""
    return make_schedule(world.author, world.version, world.connection, cron=cron, next_run_at=at)


def fire(now: datetime) -> dict:
    counts = schedules.fire_due(now)
    return {outcome: count for outcome, count in counts.items() if count}


# --------------------------------------------------------------------------- the service


def test_create_list_update_and_delete_are_audited(world):
    author = actor(world.author)
    dataset = source_dataset(world)
    before = datetime.now(UTC)
    schedule = schedules.create_schedule(
        author, dataset.pk, cron=" 30  2 * * * ", timezone="Europe/Berlin"
    )
    assert (schedule.cron, schedule.timezone, schedule.enabled) == (
        "30 2 * * *",
        "Europe/Berlin",
        True,
    )
    assert schedule.next_run_at > before and schedule.created_by == world.author
    assert schedule.last_outcome == "" and schedule.last_job is None
    assert schedule.next_run_at.astimezone(schedules.crontab.zone("Europe/Berlin")).hour == 2
    created = AuditLog.objects.get(action="schedule.create")
    assert created.object_id == str(schedule.pk)
    assert created.details == {
        "dataset_id": str(dataset.pk),
        "cron": "30 2 * * *",
        "timezone": "Europe/Berlin",
        "enabled": True,
    }

    paused = schedules.create_schedule(author, dataset.pk, cron="@hourly", enabled=False)
    assert paused.next_run_at is None and paused.timezone == "UTC"
    other = schedules.create_schedule(author, source_dataset(world, "other").pk, cron="@daily")
    listed = schedules.list_schedules(author)
    assert [s.pk for s in listed][-1] == paused.pk  # disabled last
    assert {s.pk for s in schedules.list_schedules(author, dataset_id=dataset.pk)} == {
        schedule.pk,
        paused.pk,
    }
    assert [s.pk for s in schedules.list_schedules(author, enabled=False)] == [paused.pk]
    assert {s.pk for s in schedules.list_schedules(author, enabled=True)} == {
        schedule.pk,
        other.pk,
    }

    changed = schedules.update_schedule(author, schedule.pk, cron="0 6 * * mon-fri")
    assert changed.cron == "0 6 * * mon-fri" and changed.next_run_at != schedule.next_run_at
    assert changed.next_run_at.astimezone(schedules.crontab.zone("Europe/Berlin")).hour == 6
    update = AuditLog.objects.get(action="schedule.update")
    assert update.details == {
        "changed": ["cron"],
        "cron": "0 6 * * mon-fri",
        "timezone": "Europe/Berlin",
        "enabled": True,
    }
    disabled = schedules.update_schedule(author, schedule.pk, enabled=False)
    assert disabled.next_run_at is None
    enabled = schedules.update_schedule(author, schedule.pk, enabled=True, timezone="UTC")
    assert enabled.next_run_at.hour == 6 and enabled.next_run_at > before
    schedules.update_schedule(author, schedule.pk, cron="0  6 * * mon-fri")  # the same
    assert AuditLog.objects.filter(action="schedule.update").count() == 3

    schedules.delete_schedule(author, schedule.pk)
    assert not Schedule.objects.filter(pk=schedule.pk).exists()
    deleted = AuditLog.objects.get(action="schedule.delete")
    assert deleted.details["dataset_id"] == str(dataset.pk)
    assert deleted.details["cron"] == "0 6 * * mon-fri"


def test_datasets_that_read_uploads_cannot_be_scheduled(world):
    author = actor(world.author)
    with pytest.raises(InvalidRequest, match="reads uploaded files, so it cannot run") as caught:
        schedules.create_schedule(author, world.dataset.pk, cron="@daily")
    assert caught.value.code == "dataset_reads_uploads"
    schedule = schedules.create_schedule(author, source_dataset(world).pk, cron="@daily")
    # The dataset changes into one that reads uploads: the schedule can be disabled, not enabled
    Dataset.objects.filter(pk=schedule.dataset_id).update(source_connection=None, source_path="")
    with pytest.raises(InvalidRequest, match="reads uploaded files"):
        schedules.update_schedule(author, schedule.pk, cron="@hourly")
    schedules.update_schedule(author, schedule.pk, enabled=False)
    with pytest.raises(InvalidRequest, match="reads uploaded files"):
        schedules.update_schedule(author, schedule.pk, enabled=True)


@pytest.mark.parametrize(
    "cron,zone,code,message",
    [
        ("61 * * * *", "UTC", "invalid_cron", "minute field '61': must be 0-59."),
        ("0 0 31 2 *", "UTC", "invalid_cron", "never runs"),
        ("* " * 100 + "*", "UTC", "invalid_cron", "at most 200 characters"),
        ("@daily", "Mars/Base", "invalid_time_zone", "Unknown time zone 'Mars/Base'"),
    ],
)
def test_expressions_and_time_zones_are_checked(world, cron, zone, code, message):
    author, dataset = actor(world.author), source_dataset(world)
    with pytest.raises(InvalidRequest, match=message) as caught:
        schedules.create_schedule(author, dataset.pk, cron=cron, timezone=zone)
    assert caught.value.code == code
    schedule = schedules.create_schedule(author, dataset.pk, cron="@daily")
    with pytest.raises(InvalidRequest, match=message):
        schedules.update_schedule(author, schedule.pk, cron=cron, timezone=zone)
    with pytest.raises(InvalidRequest, match=message):
        schedules.preview(author, cron=cron, timezone=zone)


def test_unknown_fields_and_missing_objects(world):
    author = actor(world.author)
    schedule = schedules.create_schedule(author, source_dataset(world).pk, cron="@daily")
    with pytest.raises(InvalidRequest, match="cannot be changed: dataset_id, next_run_at"):
        schedules.update_schedule(author, schedule.pk, dataset_id=None, next_run_at=None)
    missing = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(NotFound, match=f"no schedule with id {missing}"):
        schedules.get_schedule(author, missing)
    with pytest.raises(NotFound, match=f"no schedule with id {missing}"):
        schedules.update_schedule(author, missing, enabled=False)
    with pytest.raises(NotFound, match=f"no schedule with id {missing}"):
        schedules.delete_schedule(author, missing)
    with pytest.raises(NotFound, match=f"no dataset with id {missing}"):
        schedules.create_schedule(author, missing, cron="@daily")


def test_viewing_needs_datasets_read_and_changing_datasets_write(world):
    schedule = make_schedule(world.author, world.version, world.connection)
    for user in (world.viewer, world.operator):
        who = actor(user)
        assert list(schedules.list_schedules(who)) == [schedule]
        assert schedules.get_schedule(who, schedule.pk) == schedule
        assert len(schedules.preview(who, cron="@daily")[1]) == 5
        for change in (
            lambda: schedules.create_schedule(who, schedule.dataset_id, cron="@daily"),
            lambda: schedules.update_schedule(who, schedule.pk, enabled=False),
            lambda: schedules.delete_schedule(who, schedule.pk),
        ):
            with pytest.raises(PermissionDenied, match="needs the datasets:write scope") as e:
                change()
            assert e.value.code == "role_insufficient"
    with pytest.raises(Exception, match="Sign in"):
        schedules.list_schedules(Actor.anonymous())


def test_a_token_that_cannot_run_jobs_cannot_start_a_schedule(world):
    token, _ = api_token(world.author, ["datasets:read", "datasets:write"])
    who = actor(world.author, token)
    dataset = source_dataset(world)
    with pytest.raises(PermissionDenied, match="needs the jobs:run scope") as caught:
        schedules.create_schedule(who, dataset.pk, cron="@daily")
    assert caught.value.code == "scope_missing"
    paused = schedules.create_schedule(who, dataset.pk, cron="@daily", enabled=False)
    with pytest.raises(PermissionDenied, match="jobs:run"):
        schedules.update_schedule(who, paused.pk, enabled=True)
    running = schedules.create_schedule(actor(world.author), dataset.pk, cron="@hourly")
    schedules.update_schedule(who, running.pk, enabled=False)  # stopping one needs no jobs:run
    schedules.delete_schedule(who, paused.pk)


def test_preview_and_upcoming(world):
    expression, runs = schedules.preview(
        actor(world.viewer), cron="0  9 * * *", timezone="America/New_York"
    )
    assert expression == "0 9 * * *" and len(runs) == 5
    assert all(later - earlier == timedelta(days=1) for earlier, later in zip(runs, runs[1:]))
    assert {run.astimezone(schedules.crontab.zone("America/New_York")).hour for run in runs} == {9}
    schedule = make_schedule(
        world.author,
        world.version,
        world.connection,
        cron="*/10 * * * *",
        next_run_at=datetime(2026, 10, 10, 9, 0, tzinfo=UTC),
    )
    assert [run.minute for run in schedules.upcoming(schedule)] == [0, 10, 20, 30, 40]
    schedule.next_run_at = None
    assert schedules.upcoming(schedule) == []


def test_deleting_a_dataset_removes_schedules_that_never_ran(world):
    author = actor(world.author)
    schedule = schedules.create_schedule(author, source_dataset(world).pk, cron="@daily")
    datasets.delete_dataset(author, schedule.dataset_id)
    assert not Schedule.objects.exists()
    ran = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    with pytest.raises(Conflict, match="has jobs"):
        datasets.delete_dataset(author, ran.dataset_id)


def test_jobs_outlive_their_schedule_and_retention_outlives_last_job(world):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    job = Job.objects.get(schedule=schedule)
    schedules.delete_schedule(actor(world.author), schedule.pk)
    job.refresh_from_db()
    assert job.schedule is None and job.scheduled_for == _at(9)
    other = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    other.refresh_from_db()
    other.last_job.delete()  # as retention deletes job records
    other.refresh_from_db()
    assert other.last_job is None and other.last_outcome == "queued"


# --------------------------------------------------------------------------- the dispatcher


def _at(hour: int, minute: int = 0, second: int = 0, day: int = 10) -> datetime:
    return datetime(2026, 10, day, hour, minute, second, tzinfo=UTC)


def test_a_due_slot_queues_one_run_started_by_the_scheduler(world):
    schedule = due(world, at=_at(9))
    assert fire(_at(9, 0, 7)) == {"queued": 1}
    schedule.refresh_from_db()
    job = schedule.last_job
    assert job.requested_by is None and job.requested_with_token is None
    assert job.schedule == schedule and job.scheduled_for == _at(9)
    assert job.dataset_id == schedule.dataset_id and job.kind == "run"
    assert job.status == JobStatus.QUEUED
    assert job.idempotency_key == f"schedule:{schedule.pk}:2026-10-10T09:00:00+00:00"
    assert job.spec["input"]["location"]["type"] == "gateway:object"
    assert (schedule.last_run_at, schedule.last_outcome, schedule.last_message) == (
        _at(9),
        ScheduleOutcome.QUEUED,
        "",
    )
    assert schedule.next_run_at == _at(10)
    assert fire(_at(9, 0, 17)) == {}  # nothing is due until 10:00
    assert Job.objects.filter(schedule=schedule).count() == 1


def test_disabled_and_future_schedules_wait(world):
    due(world, at=_at(10))
    paused = due(world, at=_at(9))
    Schedule.objects.filter(pk=paused.pk).update(enabled=False)
    assert fire(_at(9, 30)) == {}
    assert not Job.objects.filter(schedule__isnull=False).exists()


def test_a_slot_is_skipped_while_the_previous_run_is_queued_or_running(world):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    first = Job.objects.get(schedule=schedule)
    assert fire(_at(10, 0, 5)) == {"skipped_overlap": 1}
    schedule.refresh_from_db()
    assert schedule.last_outcome == ScheduleOutcome.SKIPPED_OVERLAP
    assert schedule.last_message == (
        f"Skipped the run due at 2026-10-10 10:00 (UTC): job {first.pk} of an earlier run is "
        "still queued."
    )
    assert schedule.last_job == first and schedule.last_run_at == _at(10)
    assert schedule.next_run_at == _at(11)
    Job.objects.filter(pk=first.pk).update(status=JobStatus.RUNNING)
    assert fire(_at(11, 0, 5)) == {"skipped_overlap": 1}
    schedule.refresh_from_db()
    assert schedule.last_message.endswith("is still running.")
    Job.objects.filter(pk=first.pk).update(status=JobStatus.SUCCEEDED)
    assert fire(_at(12, 0, 5)) == {"queued": 1}
    assert Job.objects.filter(schedule=schedule).count() == 2


def test_after_downtime_only_the_latest_missed_slot_runs(world):
    schedule = due(world, at=_at(9))
    # Down from before 09:00 until 11:40: 09:00, 10:00 and 11:00 passed; 11:00 is recent enough
    assert fire(_at(11, 40)) == {"queued": 1}
    schedule.refresh_from_db()
    job = Job.objects.get(schedule=schedule)
    assert job.scheduled_for == _at(11) and schedule.last_run_at == _at(11)
    assert schedule.last_message == (
        "Caught up the run due at 2026-10-10 11:00 (UTC) late; the runs due from 2026-10-10 "
        "09:00 (UTC) until then were missed."
    )
    assert schedule.next_run_at == _at(12)


def test_of_several_missed_slots_within_the_window_the_latest_runs(world):
    schedule = due(world, "*/15 * * * *", at=_at(9))
    assert fire(_at(9, 50)) == {"queued": 1}
    schedule.refresh_from_db()
    assert schedule.last_job.scheduled_for == _at(9, 45)
    assert "the runs due from 2026-10-10 09:00 (UTC) until then were missed" in (
        schedule.last_message
    )
    assert schedule.next_run_at == _at(10)


def test_a_single_late_slot_is_caught_up(world):
    schedule = due(world, "0 9 * * *", at=_at(9))
    assert fire(_at(9, 50)) == {"queued": 1}
    schedule.refresh_from_db()
    assert schedule.last_message == "Caught up the run due at 2026-10-10 09:00 (UTC) late."
    assert schedule.next_run_at == _at(9, day=11)


def test_a_slot_older_than_the_catch_up_window_is_missed(world):
    schedule = due(world, "0 2 * * *", at=_at(2))
    assert fire(_at(5)) == {"missed": 1}
    schedule.refresh_from_db()
    assert not Job.objects.filter(schedule=schedule).exists()
    assert schedule.last_outcome == ScheduleOutcome.MISSED and schedule.last_run_at == _at(2)
    assert schedule.last_message == (
        "Missed the run due at 2026-10-10 02:00 (UTC) and any after it: the dispatcher was "
        "not running, and a late run may start at most 3600 seconds after its time (the "
        "installation setting schedule_catch_up_seconds)."
    )
    assert schedule.next_run_at == _at(2, day=11)


def test_the_catch_up_window_is_an_installation_setting(world, admin_actor):
    installation.update(admin_actor, {"schedule_catch_up_seconds": 6 * 3600})
    due(world, "0 2 * * *", at=_at(2))
    assert fire(_at(5)) == {"queued": 1}
    installation.update(admin_actor, {"schedule_catch_up_seconds": 0})
    late, on_time = due(world, at=_at(9)), due(world, "1 * * * *", at=_at(9, 1))
    # With catching up off, a slot still runs when the dispatcher finds it within a minute
    assert fire(_at(9, 1, 50)) == {"missed": 1, "queued": 1}
    late.refresh_from_db()
    on_time.refresh_from_db()
    assert "at most 60 seconds after its time" in late.last_message
    assert (on_time.last_outcome, on_time.last_message) == (ScheduleOutcome.QUEUED, "")
    with pytest.raises(InvalidRequest, match="at least 0 and at most 604800"):
        installation.update(admin_actor, {"schedule_catch_up_seconds": 8 * 24 * 3600})


def test_a_run_that_cannot_be_queued_is_recorded_and_the_schedule_stays(world):
    schedule = due(world, at=_at(9))
    Dataset.objects.filter(pk=schedule.dataset_id).update(source_connection=None, source_path="")
    assert fire(_at(9, 0, 5)) == {"failed_to_enqueue": 1}
    schedule.refresh_from_db()
    assert schedule.enabled and schedule.last_outcome == ScheduleOutcome.FAILED_TO_ENQUEUE
    assert schedule.last_message.startswith(
        "Could not queue the run due at 2026-10-10 09:00 (UTC): Dataset "
    )
    assert "reads uploads: give the upload_id" in schedule.last_message
    assert schedule.next_run_at == _at(10) and schedule.last_job is None
    assert fire(_at(9, 0, 15)) == {}  # it does not try again before its next slot
    assert fire(_at(10, 0, 5)) == {"failed_to_enqueue": 1}


def test_times_in_messages_are_the_schedules_wall_clock(world):
    schedule = due(world, "30 2 * * *", at=datetime(2026, 3, 29, 1, 0, tzinfo=UTC))
    Schedule.objects.filter(pk=schedule.pk).update(timezone="Europe/Berlin")
    Dataset.objects.filter(pk=schedule.dataset_id).update(source_connection=None, source_path="")
    fire(datetime(2026, 3, 29, 1, 0, 5, tzinfo=UTC))
    schedule.refresh_from_db()
    # 02:30 did not exist that night: the run was due at the end of the gap, 03:00 CEST
    assert "the run due at 2026-03-29 03:00 (Europe/Berlin)" in schedule.last_message
    assert schedule.next_run_at == datetime(2026, 3, 30, 0, 30, tzinfo=UTC)


def test_an_unexpected_error_in_one_schedule_does_not_stop_the_others(world, monkeypatch, caplog):
    broken, fine = due(world, at=_at(8)), due(world, at=_at(9))
    create_job = jobs.create_job

    def failing(actor, request, **kwargs):
        if request.dataset_id == broken.dataset_id:
            raise RuntimeError("a bug")
        return create_job(actor, request, **kwargs)

    monkeypatch.setattr(jobs, "create_job", failing)
    with caplog.at_level(logging.ERROR, logger="forklift_web.services.schedules"):
        assert fire(_at(9, 0, 5)) == {"queued": 1, "errors": 1}
    record = next(r for r in caplog.records if r.message == "Schedule could not be handled")
    assert record.schedule_id == str(broken.pk) and record.exc_info
    broken.refresh_from_db()
    assert broken.next_run_at == _at(8) and broken.last_outcome == ""  # still due, unchanged
    assert Job.objects.filter(schedule=fine).count() == 1
    monkeypatch.setattr(jobs, "create_job", create_job)
    assert fire(_at(9, 0, 15)) == {"queued": 1}  # the next pass tries it again


def test_a_slot_whose_job_exists_replays_it(world):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    job = Job.objects.get(schedule=schedule)
    Job.objects.filter(pk=job.pk).update(status=JobStatus.SUCCEEDED)
    Schedule.objects.filter(pk=schedule.pk).update(next_run_at=_at(9))  # rewound by hand
    assert fire(_at(9, 0, 10)) == {"queued": 1}
    assert list(Job.objects.filter(schedule=schedule)) == [job]


def test_the_database_refuses_a_second_job_for_a_slot(world):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    job = Job.objects.get(schedule=schedule)
    copy = Job(
        kind=job.kind,
        lane=job.lane,
        classification=job.classification,
        spec=job.spec,
        dataset=job.dataset,
        schedule=schedule,
        idempotency_key=job.idempotency_key,
    )
    with pytest.raises(IntegrityError, match="job_schedule_slot"), transaction.atomic():
        copy.save()


def test_scheduled_runs_are_cancelled_by_those_who_manage_schedules(world):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    job = Job.objects.get(schedule=schedule)
    with pytest.raises(PermissionDenied, match="is a scheduled run; only those who manage"):
        jobs.cancel_job(actor(world.operator), job.pk)
    assert jobs.cancel_job(actor(world.author), job.pk).status == JobStatus.CANCELLED
    with pytest.raises(PermissionDenied, match="requested by another user"):
        jobs.cancel_job(actor(world.author), world.queued_job.pk)
    # An admin may, even with a token that has jobs:run but not datasets:write
    token, _ = api_token(world.admin, ["jobs:run"])
    second = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    queued = Job.objects.get(schedule=second)
    assert jobs.cancel_job(actor(world.admin, token), queued.pk).status == JobStatus.CANCELLED


# --------------------------------------------------------------------------- two dispatchers


def _in_thread(target, results: list, *args):
    def run():
        try:
            results.append(target(*args))
        except Exception as error:  # surfaced by the test's assertions
            results.append(error)
        finally:
            connection.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread


@pytest.mark.django_db(transaction=True)
def test_a_schedule_another_dispatcher_holds_is_skipped(monkeypatch):
    world = World.build()
    schedule = due(world, at=_at(9))
    holding, release = threading.Event(), threading.Event()
    fire_one = schedules._fire

    def slow(schedule, now, window):
        holding.set()
        assert release.wait(10)
        return fire_one(schedule, now, window)

    monkeypatch.setattr(schedules, "_fire", slow)
    first: list = []
    thread = _in_thread(schedules.fire_due, first, _at(9, 0, 5))
    assert holding.wait(10)
    monkeypatch.setattr(schedules, "_fire", fire_one)
    # The first dispatcher holds the row: the second skips it instead of waiting
    assert fire(_at(9, 0, 5)) == {}
    release.set()
    thread.join(10)
    assert first[0]["queued"] == 1
    assert fire(_at(9, 0, 6)) == {}  # moved on: nothing left for anyone
    assert Job.objects.filter(schedule=schedule).count() == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_dispatchers_enqueue_each_slot_once():
    world = World.build()
    many = [due(world, at=_at(9)) for _ in range(12)]
    start = threading.Barrier(4)
    results: list = []

    def dispatch():
        start.wait(10)
        return schedules.fire_due(_at(9, 0, 5))

    threads = [_in_thread(dispatch, results) for _ in range(4)]
    for thread in threads:
        thread.join(30)
    assert all(isinstance(result, dict) for result in results), results
    assert sum(result["queued"] for result in results) == len(many)
    assert Job.objects.filter(schedule__isnull=False).count() == len(many)
    for schedule in many:
        schedule.refresh_from_db()
        assert schedule.next_run_at == _at(10) and schedule.last_job.scheduled_for == _at(9)


# --------------------------------------------------------------------------- the API


def test_the_api(world, as_user):
    author, viewer = as_user(world.author), as_user(world.viewer)
    dataset = source_dataset(world)
    created = author.post(
        f"/api/v1/datasets/{dataset.pk}/schedules",
        {"cron": "0 2 * * *", "timezone": "Europe/Berlin"},
    )
    assert created.status_code == 201, created.content
    body = created.json()
    assert body["dataset_id"] == str(dataset.pk) and body["dataset_name"] == "people"
    assert (body["cron"], body["timezone"], body["enabled"]) == (
        "0 2 * * *",
        "Europe/Berlin",
        True,
    )
    assert len(body["next_runs"]) == 5 and body["next_runs"][0] == body["next_run_at"]
    assert (body["last_outcome"], body["last_job_id"], body["last_run_at"]) == (None, None, None)
    assert body["created_by_id"] == world.author.pk
    schedule_id = body["id"]

    listed = viewer.get(f"/api/v1/datasets/{dataset.pk}/schedules").json()
    assert [item["id"] for item in listed["items"]] == [schedule_id]
    assert viewer.get("/api/v1/schedules?enabled=false").json()["items"] == []
    assert viewer.get(f"/api/v1/schedules?dataset_id={dataset.pk}").json()["count"] == 1
    assert viewer.get(f"/api/v1/schedules/{schedule_id}").json()["cron"] == "0 2 * * *"

    patched = author.patch(f"/api/v1/schedules/{schedule_id}", {"enabled": False}).json()
    assert patched["enabled"] is False and patched["next_run_at"] is None
    assert patched["next_runs"] == []
    refused = author.patch(f"/api/v1/schedules/{schedule_id}", {"timezone": "Nowhere"})
    assert refused.status_code == 400 and refused.json()["code"] == "invalid_time_zone"

    preview = viewer.post(
        "/api/v1/schedules/preview", {"cron": "@hourly", "timezone": "Asia/Kolkata"}
    )
    assert preview.status_code == 200
    assert preview.json()["cron"] == "@hourly" and len(preview.json()["next_runs"]) == 5
    invalid = viewer.post("/api/v1/schedules/preview", {"cron": "0 0 31 2 *"})
    assert invalid.status_code == 400
    assert invalid.json() == {
        "detail": "The cron expression '0 0 31 2 *' never runs: no date matches it.",
        "code": "invalid_cron",
    }
    uploads_only = author.post(
        f"/api/v1/datasets/{world.dataset.pk}/schedules", {"cron": "@daily"}
    )
    assert uploads_only.status_code == 400
    assert uploads_only.json()["code"] == "dataset_reads_uploads"

    assert author.delete(f"/api/v1/schedules/{schedule_id}").status_code == 204
    assert viewer.get(f"/api/v1/schedules/{schedule_id}").status_code == 404


def test_the_api_shows_what_a_schedule_did_and_which_jobs_it_started(world, as_user):
    schedule = due(world, at=_at(9))
    fire(_at(9, 0, 5))
    job = Job.objects.get(schedule=schedule)
    caller = as_user(world.viewer)
    body = caller.get(f"/api/v1/schedules/{schedule.pk}").json()
    assert (body["last_outcome"], body["last_job_id"], body["last_message"]) == (
        "queued",
        str(job.pk),
        "",
    )
    assert body["last_run_at"].startswith("2026-10-10T09:00:00")
    detail = caller.get(f"/api/v1/jobs/{job.pk}").json()
    assert detail["requested_by_id"] is None and detail["schedule_id"] == str(schedule.pk)
    assert detail["scheduled_for"].startswith("2026-10-10T09:00:00")
    other = caller.get(f"/api/v1/jobs/{world.queued_job.pk}").json()
    assert (other["schedule_id"], other["scheduled_for"]) == (None, None)
