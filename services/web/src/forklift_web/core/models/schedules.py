"""Schedules: datasets that run on a timer."""

from __future__ import annotations

import uuid

from django.db import models
from django.db.models import Q
from django.utils.timezone import now  # the module's name is taken by a field

from forklift_web.core.choices import ScheduleOutcome
from forklift_web.core.models.accounts import User
from forklift_web.core.models.catalog import Dataset
from forklift_web.core.models.jobs import Job


class Schedule(models.Model):
    """A dataset's recurring run: a cron expression read in an IANA time zone.

    ``next_run_at`` is the instant of the next slot (null while the schedule is disabled); the
    dispatcher (``forklift-web dispatch``) enqueues a run once it is due and moves it on. The
    ``last_*`` fields say what the dispatcher did with the latest slot it handled: the slot's
    time, the outcome, why (``last_message``) and the job it queued. A schedule belongs to its
    dataset and goes with it; a dataset that has run cannot be deleted anyway (its jobs protect
    it), so only schedules that never queued anything can go that way.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="schedules")
    cron = models.CharField(max_length=200)
    timezone = models.CharField(max_length=64, default="UTC")
    enabled = models.BooleanField(default=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_outcome = models.CharField(
        max_length=32, choices=ScheduleOutcome.choices, blank=True, default=""
    )
    last_message = models.TextField(blank=True, default="")
    last_job = models.ForeignKey(
        Job, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_at = models.DateTimeField(default=now)
    updated_at = models.DateTimeField(default=now)

    class Meta:
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["next_run_at"], condition=Q(enabled=True), name="schedule_due")
        ]

    def __str__(self) -> str:
        return f"{self.cron} ({self.timezone}) for {self.dataset.name}"
