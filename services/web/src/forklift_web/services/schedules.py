"""Schedules: datasets that run on a timer, and the dispatcher pass that enqueues their runs.

A schedule is a cron expression read in an IANA time zone (:mod:`forklift_web.cron`). Only
datasets that read from a source connection (s3 or sql) can be scheduled: one that reads
uploads needs a new file for every run.

:func:`fire_due` is one pass of the dispatcher (``forklift-web dispatch``). It claims each due
schedule with ``SELECT ... FOR UPDATE SKIP LOCKED`` (so several dispatchers can run), enqueues
the dataset's run as the system with the idempotency key ``schedule:<id>:<slot>`` (one slot is
one job), records what happened and moves ``next_run_at`` to the first slot after now:

- a slot found less than ``ON_TIME_SECONDS`` after its time runs;
- after the dispatcher was not running, only the most recent missed slot can run, and only if
  it is at most ``schedule_catch_up_seconds`` (an installation setting) old; otherwise the
  outcome is ``missed``;
- while a job the schedule started is still queued or running, the slot is skipped
  (``skipped_overlap``);
- when the run cannot be enqueued (the dataset no longer reads from a connection, a limit
  refuses it, ...), the outcome is ``failed_to_enqueue`` with the reason, and the schedule stays.

Scheduled jobs have no requester (``requested_by`` is empty: the system asked); their
``schedule`` and ``scheduled_for`` (the slot) say where they came from.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from django.db import transaction
from django.db.models import F
from django.utils.timezone import now as utc_now

from forklift_web import cron as crontab
from forklift_web.core.choices import JobKind, JobStatus, ScheduleOutcome
from forklift_web.core.models import Job, Schedule
from forklift_web.errors import InvalidRequest, NotFound, ServiceError
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, datasets, installation, jobs

logger = logging.getLogger(__name__)

EDITABLE = ("cron", "timezone", "enabled")
ON_TIME_SECONDS = 60  # a dispatcher pass every few seconds finds a slot well within this
UPCOMING = 5
MAX_CRON_LENGTH = 200  # Schedule.cron
SCHEDULER = Actor.for_system("scheduler")


def _checked(expression: str, time_zone: str, now: datetime) -> str:
    if len(expression) > MAX_CRON_LENGTH:
        raise InvalidRequest(
            f"A cron expression is at most {MAX_CRON_LENGTH} characters.", code="invalid_cron"
        )
    try:
        return crontab.validate(expression, time_zone, now=now)
    except crontab.CronError as error:
        code = "invalid_cron" if error.field == "cron" else "invalid_time_zone"
        raise InvalidRequest(error.message, code=code) from None


def _schedulable(dataset) -> None:
    if dataset.source_connection_id is None:
        raise InvalidRequest(
            f"Dataset {dataset.name!r} reads uploaded files, so it cannot run on a schedule: "
            "each run needs a new file. Schedule a dataset that reads from an s3 or sql "
            "connection.",
            code="dataset_reads_uploads",
        )


def list_schedules(actor: Actor, *, dataset_id=None, enabled: Optional[bool] = None):
    """Schedules, the next to run first (disabled ones last)."""
    check(actor, Action.SCHEDULE_VIEW)
    found = Schedule.objects.select_related("dataset", "last_job").order_by(
        F("next_run_at").asc(nulls_last=True), "created_at"
    )
    if dataset_id is not None:
        found = found.filter(dataset_id=dataset_id)
    if enabled is not None:
        found = found.filter(enabled=enabled)
    return found


def get_schedule(actor: Actor, schedule_id) -> Schedule:
    schedule = list_schedules(actor).filter(pk=schedule_id).first()
    if schedule is None:
        raise NotFound(f"There is no schedule with id {schedule_id}.")
    return schedule


def upcoming(schedule: Schedule, count: int = UPCOMING) -> list:
    """The schedule's next ``count`` runs (none while it is disabled)."""
    if schedule.next_run_at is None:
        return []
    later = crontab.next_runs(schedule.cron, schedule.timezone, schedule.next_run_at, count - 1)
    return [schedule.next_run_at, *later]


def preview(actor: Actor, *, cron: str, timezone: str = "UTC", count: int = UPCOMING) -> tuple:
    """(the expression as stored, its next ``count`` runs) for a draft schedule."""
    check(actor, Action.SCHEDULE_VIEW)
    now = utc_now()
    expression = _checked(cron, timezone, now)
    return expression, crontab.next_runs(expression, timezone, now, count)


def create_schedule(
    actor: Actor, dataset_id, *, cron: str, timezone: str = "UTC", enabled: bool = True
) -> Schedule:
    check(actor, Action.SCHEDULE_EDIT)
    if enabled:
        check(actor, Action.DATASET_RUN)  # a schedule runs the dataset: it needs what a run does
    dataset = datasets.get_dataset(actor, dataset_id)
    _schedulable(dataset)
    now = utc_now()
    expression = _checked(cron, timezone, now)
    with transaction.atomic():
        schedule = Schedule.objects.create(
            dataset=dataset,
            cron=expression,
            timezone=timezone,
            enabled=enabled,
            next_run_at=crontab.next_after(expression, timezone, now) if enabled else None,
            created_by=actor.user,
            created_at=now,
            updated_at=now,
        )
        audit.record(
            actor,
            "schedule.create",
            schedule,
            {
                "dataset_id": str(dataset.pk),
                "cron": expression,
                "timezone": timezone,
                "enabled": enabled,
            },
        )
    return get_schedule(actor, schedule.pk)


def update_schedule(actor: Actor, schedule_id, **changes) -> Schedule:
    """Change any of ``EDITABLE``. A changed expression or time zone, and enabling, start
    counting from now: slots that passed while the schedule was disabled do not run."""
    check(actor, Action.SCHEDULE_EDIT)
    unknown = sorted(set(changes) - set(EDITABLE))
    if unknown:
        raise InvalidRequest(
            f"These schedule fields cannot be changed: {', '.join(unknown)} (changeable: "
            f"{', '.join(EDITABLE)})."
        )
    with transaction.atomic():
        # Locked, so that a dispatcher pass does not move it on with the old expression.
        schedule = Schedule.objects.select_for_update().filter(pk=schedule_id).first()
        if schedule is None:
            raise NotFound(f"There is no schedule with id {schedule_id}.")
        values = {field: getattr(schedule, field) for field in EDITABLE}
        values.update(changes)
        if values["enabled"]:
            check(actor, Action.DATASET_RUN)
            _schedulable(schedule.dataset)
        now = utc_now()
        values["cron"] = _checked(values["cron"], values["timezone"], now)
        changed = sorted(field for field in EDITABLE if getattr(schedule, field) != values[field])
        if changed:
            for field in changed:
                setattr(schedule, field, values[field])
            schedule.next_run_at = (
                crontab.next_after(schedule.cron, schedule.timezone, now)
                if schedule.enabled
                else None
            )
            schedule.updated_at = now
            schedule.save()
            audit.record(
                actor, "schedule.update", schedule, {"changed": changed, **_values(schedule)}
            )
    return get_schedule(actor, schedule.pk)


def _values(schedule: Schedule) -> dict:
    return {"cron": schedule.cron, "timezone": schedule.timezone, "enabled": schedule.enabled}


def delete_schedule(actor: Actor, schedule_id) -> None:
    """Delete a schedule; the jobs it started keep their ``scheduled_for``."""
    check(actor, Action.SCHEDULE_EDIT)
    schedule = get_schedule(actor, schedule_id)
    with transaction.atomic():
        audit.record(
            actor,
            "schedule.delete",
            schedule,
            {"dataset_id": str(schedule.dataset_id), **_values(schedule)},
        )
        schedule.delete()


# --------------------------------------------------------------------------- the dispatcher


def _when(schedule: Schedule, instant: datetime) -> str:
    local = instant.astimezone(crontab.zone(schedule.timezone))
    return f"{local:%Y-%m-%d %H:%M} ({schedule.timezone})"


def _latest_slot(schedule: Schedule, now: datetime, window: timedelta) -> Optional[datetime]:
    """The most recent slot due by ``now`` that is at most ``window`` old, if there is one."""
    start = max(schedule.next_run_at, now - window)
    slot = crontab.next_after(schedule.cron, schedule.timezone, start - timedelta(microseconds=1))
    if slot > now:
        return None
    while (later := crontab.next_after(schedule.cron, schedule.timezone, slot)) <= now:
        slot = later
    return slot


def _enqueue(schedule: Schedule, slot: datetime, now: datetime) -> tuple:
    """Enqueue the run for ``slot``; returns (outcome, message, job)."""
    first = schedule.next_run_at
    running = (
        Job.objects.filter(schedule=schedule, status__in=[JobStatus.QUEUED, JobStatus.RUNNING])
        .order_by("created_at")
        .first()
    )
    if running is not None:
        return (
            ScheduleOutcome.SKIPPED_OVERLAP,
            f"Skipped the run due at {_when(schedule, slot)}: job {running.pk} of an earlier "
            f"run is still {running.status}.",
            None,
        )
    try:
        job, _ = jobs.create_job(
            SCHEDULER,
            jobs.JobRequest(kind=JobKind.RUN, dataset_id=schedule.dataset_id),
            idempotency_key=f"schedule:{schedule.pk}:{slot.isoformat()}",
            schedule=schedule,
            scheduled_for=slot,
        )
    except ServiceError as error:
        return (
            ScheduleOutcome.FAILED_TO_ENQUEUE,
            f"Could not queue the run due at {_when(schedule, slot)}: {error.message}",
            None,
        )
    message = ""
    if slot != first or now - slot > timedelta(seconds=ON_TIME_SECONDS):
        message = f"Caught up the run due at {_when(schedule, slot)} late"
        message += (
            f"; the runs due from {_when(schedule, first)} until then were missed."
            if slot != first
            else "."
        )
    return ScheduleOutcome.QUEUED, message, job


def _fire(schedule: Schedule, now: datetime, window: timedelta) -> str:
    slot = _latest_slot(schedule, now, window)
    if slot is None:
        outcome, job, slot = ScheduleOutcome.MISSED, None, schedule.next_run_at
        message = (
            f"Missed the run due at {_when(schedule, slot)} and any after it: the dispatcher "
            f"was not running, and a late run may start at most {int(window.total_seconds())} "
            "seconds after its time (the installation setting schedule_catch_up_seconds)."
        )
    else:
        outcome, message, job = _enqueue(schedule, slot, now)
    schedule.last_run_at = slot
    schedule.last_outcome = outcome
    schedule.last_message = message
    schedule.last_job = job or schedule.last_job
    schedule.next_run_at = crontab.next_after(schedule.cron, schedule.timezone, now)
    schedule.save(
        update_fields=["last_run_at", "last_outcome", "last_message", "last_job", "next_run_at"]
    )
    logger.info(
        "Schedule slot handled",
        extra={
            "schedule_id": str(schedule.pk),
            "outcome": outcome,
            "job_id": str(job.pk) if job else None,
        },
    )
    return outcome


def _claim(now: datetime, skip: list) -> Optional[Schedule]:
    return (
        Schedule.objects.select_for_update(skip_locked=True)
        .filter(enabled=True, next_run_at__lte=now)
        .exclude(pk__in=skip)
        .order_by("next_run_at")
        .first()
    )


def fire_due(now: Optional[datetime] = None) -> dict:
    """One dispatcher pass: handle every enabled schedule due by ``now``, each in a transaction
    of its own; returns how many ended in each outcome, plus ``errors``."""
    now = now or utc_now()
    catch_up = installation.get("schedule_catch_up_seconds")
    window = timedelta(seconds=max(ON_TIME_SECONDS, catch_up))
    counts = {**dict.fromkeys(ScheduleOutcome.values, 0), "errors": 0}
    failed: list = []
    while True:
        with transaction.atomic():
            schedule = _claim(now, failed)
            if schedule is None:
                return counts
            try:
                with transaction.atomic():
                    outcome = _fire(schedule, now, window)
            except Exception:
                # One broken schedule must not hold up the others: log it, leave it due (the
                # next pass tries again) and go on.
                logger.exception(
                    "Schedule could not be handled", extra={"schedule_id": str(schedule.pk)}
                )
                failed.append(schedule.pk)
                counts["errors"] += 1
            else:
                counts[outcome] += 1
