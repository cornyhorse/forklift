"""Schedules: a dataset's recurring runs (adding, changing, enabling, deleting), the page of
every schedule and the live preview."""

from __future__ import annotations

from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.cron import time_zones
from forklift_web.errors import ServiceError
from forklift_web.policy import Action, allowed, check
from forklift_web.services import datasets, schedules
from forklift_web.ui.forms import ScheduleFilterForm, ScheduleForm
from forklift_web.ui.views.base import back, page, paginate, refuse_on_page

PREFIX = "schedule"
# Service error code -> the form field it is about
FIELDS = {"invalid_cron": "cron", "invalid_time_zone": "timezone"}
OUTCOME_BADGES = {
    "queued": "queued",
    "skipped_overlap": "cancelled",
    "missed": "expired",
    "failed_to_enqueue": "failed",
}


def _rows(found) -> list:
    rows = list(found)
    for schedule in rows:
        schedule.badge = OUTCOME_BADGES.get(schedule.last_outcome, "")
    return rows


def _dataset_url(dataset_id) -> str:
    return reverse("ui:dataset", kwargs={"dataset_id": dataset_id}) + "#schedules"


def _form_context(dataset, form) -> dict:
    return {
        "schedule_form": form if dataset.source_connection_id is not None else None,
        "time_zones": sorted(time_zones()),
    }


def dataset_section(actor, dataset) -> dict:
    """What the dataset page shows about the dataset's schedules."""
    can_edit = allowed(actor, Action.SCHEDULE_EDIT)
    form = ScheduleForm(prefix=PREFIX) if can_edit else None
    return {
        "schedules": _rows(schedules.list_schedules(actor, dataset_id=dataset.pk)),
        "can_edit_schedules": can_edit,
        **(_form_context(dataset, form) if can_edit else {}),
    }


@require_GET
@page
def schedule_list(request, actor):
    form = ScheduleFilterForm(request.GET or None)
    choice = form.cleaned_data["enabled"] if form.is_valid() else ""
    found = schedules.list_schedules(actor, enabled={"true": True, "false": False}.get(choice))
    current = paginate(request, found)
    return render(
        request,
        "ui/schedules/list.html",
        {
            "form": form,
            "page": current,
            "rows": _rows(current),
            "can_edit_schedules": allowed(actor, Action.SCHEDULE_EDIT),
        },
    )


@require_GET
@page
def schedule_preview(request, actor):
    """The next runs of the expression being typed (a fragment the form's HTMX swaps in)."""
    expression = request.GET.get(f"{PREFIX}-cron", "").strip()
    time_zone = request.GET.get(f"{PREFIX}-timezone", "").strip() or "UTC"
    context = {"time_zone": time_zone, "runs": [], "error": "", "expression": expression}
    if expression:
        try:
            context["expression"], context["runs"] = schedules.preview(
                actor, cron=expression, timezone=time_zone
            )
        except ServiceError as error:
            refuse_on_page(error)
            context["error"] = error.message
    return render(request, "ui/schedules/_preview.html", context)


def _form_page(request, dataset, form, status=200, editing=None):
    return render(
        request,
        "ui/schedules/new.html",
        {"dataset": dataset, "editing": editing, **_form_context(dataset, form)},
        status=status,
    )


def _submit(request, dataset, form, save, done, editing=None):
    """Save a valid ``form`` with ``save`` (a service call) and go to the dataset page; a
    refusal is shown next to the field it is about."""
    status = 400
    if form.is_valid():
        try:
            schedule = save(form.cleaned_data)
        except ServiceError as error:
            refuse_on_page(error)
            form.add_error(FIELDS.get(error.code), error.message)
            status = error.status
        else:
            messages.success(request, done(schedule))
            return redirect(_dataset_url(dataset.pk))
    return _form_page(request, dataset, form, status, editing)


@never_cache
@page
def schedule_new(request, actor, dataset_id):
    check(actor, Action.SCHEDULE_EDIT)
    dataset = datasets.get_dataset(actor, dataset_id)
    if request.method != "POST":
        return _form_page(request, dataset, ScheduleForm(prefix=PREFIX))
    return _submit(
        request,
        dataset,
        ScheduleForm(request.POST, prefix=PREFIX),
        lambda values: schedules.create_schedule(actor, dataset.pk, **values),
        lambda schedule: f"Dataset {dataset.name!r} now runs on the schedule {schedule.cron}.",
    )


@never_cache
@page
def schedule_edit(request, actor, schedule_id):
    """Change a schedule's expression, time zone or state; a change counts from now."""
    check(actor, Action.SCHEDULE_EDIT)
    schedule = schedules.get_schedule(actor, schedule_id)
    dataset = schedule.dataset
    if request.method != "POST":
        initial = {field: getattr(schedule, field) for field in schedules.EDITABLE}
        form = ScheduleForm(prefix=PREFIX, initial=initial)
        return _form_page(request, dataset, form, editing=schedule)
    return _submit(
        request,
        dataset,
        ScheduleForm(request.POST, prefix=PREFIX),
        lambda values: schedules.update_schedule(actor, schedule.pk, **values),
        lambda saved: f"The schedule of {dataset.name!r} is now {saved.cron} ({saved.timezone})"
        + ("." if saved.enabled else ", disabled."),
        editing=schedule,
    )


def _change(request, actor, schedule_id, change, done: str):
    """Apply ``change`` (a service call) and go back; refusals the person can act on become a
    message on the page they came from."""
    schedule = schedules.get_schedule(actor, schedule_id)
    try:
        change(schedule)
    except ServiceError as error:
        refuse_on_page(error)
        messages.error(request, error.message)
    else:
        messages.success(
            request, f"The schedule {schedule.cron} of {schedule.dataset.name!r} was {done}."
        )
    return back(request, _dataset_url(schedule.dataset_id))


@require_POST
@page
def schedule_enable(request, actor, schedule_id):
    enabled = request.POST.get("enabled") == "true"
    return _change(
        request,
        actor,
        schedule_id,
        lambda schedule: schedules.update_schedule(actor, schedule.pk, enabled=enabled),
        "enabled" if enabled else "disabled",
    )


@require_POST
@page
def schedule_delete(request, actor, schedule_id):
    return _change(
        request,
        actor,
        schedule_id,
        lambda schedule: schedules.delete_schedule(actor, schedule.pk),
        "deleted",
    )
