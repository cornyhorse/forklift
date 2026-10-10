"""/api/v1/schedules and /api/v1/datasets/{id}/schedules."""

import uuid
from typing import Optional

from ninja import Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import (
    ScheduleIn,
    ScheduleOut,
    SchedulePatch,
    SchedulePreviewIn,
    SchedulePreviewOut,
)
from forklift_web.services import schedules

schedules_router = Router(tags=["schedules"])
dataset_schedules_router = Router(tags=["schedules"])


@dataset_schedules_router.get(
    "/{dataset_id}/schedules",
    response=responses({200: list[ScheduleOut]}),
    summary="A dataset's schedules",
)
@paginate
def list_dataset_schedules(request, dataset_id: uuid.UUID):
    return schedules.list_schedules(request.auth, dataset_id=dataset_id)


@dataset_schedules_router.post(
    "/{dataset_id}/schedules",
    response=responses({201: ScheduleOut}),
    summary="Schedule a dataset that reads from a connection (cron expression and time zone)",
)
def create_schedule(request, dataset_id: uuid.UUID, payload: ScheduleIn):
    return Status(201, schedules.create_schedule(request.auth, dataset_id, **payload.model_dump()))


@schedules_router.get(
    "",
    response=responses({200: list[ScheduleOut]}),
    summary="Every schedule, the next to run first",
)
@paginate
def list_schedules(
    request, enabled: Optional[bool] = None, dataset_id: Optional[uuid.UUID] = None
):
    return schedules.list_schedules(request.auth, enabled=enabled, dataset_id=dataset_id)


@schedules_router.post(
    "/preview",
    response=responses({200: SchedulePreviewOut}),
    summary="The next runs of a cron expression in a time zone (nothing is saved)",
)
def preview_schedule(request, payload: SchedulePreviewIn):
    expression, runs = schedules.preview(
        request.auth, cron=payload.cron, timezone=payload.timezone
    )
    return {"cron": expression, "timezone": payload.timezone, "next_runs": runs}


@schedules_router.get(
    "/{schedule_id}", response=responses({200: ScheduleOut}), summary="A schedule"
)
def get_schedule(request, schedule_id: uuid.UUID):
    return schedules.get_schedule(request.auth, schedule_id)


@schedules_router.patch(
    "/{schedule_id}",
    response=responses({200: ScheduleOut}),
    summary="Change, enable or disable a schedule",
)
def update_schedule(request, schedule_id: uuid.UUID, payload: SchedulePatch):
    return schedules.update_schedule(
        request.auth, schedule_id, **payload.model_dump(exclude_unset=True)
    )


@schedules_router.delete(
    "/{schedule_id}",
    response=responses({204: None}),
    summary="Delete a schedule (the jobs it started stay)",
)
def delete_schedule(request, schedule_id: uuid.UUID):
    schedules.delete_schedule(request.auth, schedule_id)
    return Status(204, None)
