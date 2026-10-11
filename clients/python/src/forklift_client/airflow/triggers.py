"""The trigger that waits for a forklift job to end, in Airflow's triggerer.

It polls ``GET /api/v1/jobs/{id}`` every ``poll_interval`` seconds (in a thread, so the
triggerer's event loop never blocks) until the job is ``succeeded``, ``failed`` or
``cancelled`` (``publishing`` is not the end), and fires one event:

    {"status": "succeeded" | "failed" | "cancelled" | "timeout" | "error",
     "job_id": "...", "message": "..."}

Connection errors and transient answers (429, 502-504) are logged and polled through; other
errors (401, 403, 404) end it with ``status: "error"``. ``end_time`` (Unix seconds) bounds the
wait; it is absolute, so a triggerer that restarts does not start the wait over.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from forklift_client import TERMINAL_STATUSES, ApiError, Conflict, ForkliftError, TransportError
from forklift_client.airflow.hooks import ForkliftHook


def is_transient(error: ForkliftError) -> bool:
    """Worth polling through: no answer, or a 429, 502, 503 or 504."""
    if isinstance(error, TransportError):
        return error.retryable
    return isinstance(error, ApiError) and error.transient


class ForkliftJobTrigger(BaseTrigger):
    """Wait for a forklift job to end.

    :param job_id: the job
    :param forklift_conn_id: the ``forklift`` connection
    :param poll_interval: seconds between polls
    :param end_time: Unix time after which the trigger gives up (``status: "timeout"``)
    :param cancel_on_kill: cancel the job when a user kills the deferred task
    """

    def __init__(
        self,
        job_id: str,
        forklift_conn_id: str = ForkliftHook.default_conn_name,
        poll_interval: float = 10.0,
        end_time: Optional[float] = None,
        cancel_on_kill: bool = False,
    ):
        super().__init__()
        self.job_id = job_id
        self.forklift_conn_id = forklift_conn_id
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.cancel_on_kill = cancel_on_kill

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "job_id": self.job_id,
                "forklift_conn_id": self.forklift_conn_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
                "cancel_on_kill": self.cancel_on_kill,
            },
        )

    def _event(self, status: str, message: str) -> TriggerEvent:
        return TriggerEvent({"status": status, "job_id": self.job_id, "message": message})

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            client = await ForkliftHook(self.forklift_conn_id).aget_conn()
        except Exception as error:  # the connection is missing or unusable
            yield self._event("error", f"Cannot use connection {self.forklift_conn_id!r}: {error}")
            return
        while True:
            try:
                job = await asyncio.to_thread(client.get_job, self.job_id)
            except ForkliftError as error:
                if not is_transient(error):
                    yield self._event("error", f"Waiting for job {self.job_id} failed: {error}")
                    return
                self.log.warning("Waiting for job %s: %s; polling on", self.job_id, error)
            else:
                if job.status in TERMINAL_STATUSES:
                    error = f": {job.error.code}: {job.error.message}" if job.error else ""
                    yield self._event(job.status, f"Job {job.id} {job.status}{error}")
                    return
                self.log.info("Job %s is %s", job.id, job.status)
            pause = self.poll_interval
            if self.end_time is not None:
                left = self.end_time - time.time()
                if left <= 0:
                    yield self._event(
                        "timeout", f"Job {self.job_id} had not ended when the wait timed out"
                    )
                    return
                pause = min(pause, left)
            await asyncio.sleep(pause)

    async def on_kill(self) -> None:
        """Cancel the job when a user kills the deferred task (``cancel_on_kill``)."""
        if not self.cancel_on_kill:
            return
        client = await ForkliftHook(self.forklift_conn_id).aget_conn()
        try:
            await asyncio.to_thread(client.cancel_job, self.job_id)
        except Conflict as error:  # it has ended, or is publishing: nothing to stop
            self.log.info("Job %s was not cancelled: %s", self.job_id, error)
        else:
            self.log.info("Cancelled job %s", self.job_id)
