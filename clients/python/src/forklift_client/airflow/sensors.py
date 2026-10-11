"""The Airflow sensor that waits for a forklift job to end.

``ForkliftJobSensor`` pokes ``GET /api/v1/jobs/{id}`` (``mode="poke"`` or ``"reschedule"``) or,
with ``deferrable=True``, waits in the triggerer
(:class:`~forklift_client.airflow.triggers.ForkliftJobTrigger`). It succeeds with the job (as
JSON) as its XCom once the job has succeeded (``publishing`` is not the end), and fails without
retries when the job failed or was cancelled (with ``soft_fail``, it is skipped instead): the
job will not change by asking again. ``timeout`` bounds the wait, as for every sensor.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Sequence

from airflow.sdk import BaseSensorOperator, PokeReturnValue, conf
from airflow.sdk.exceptions import AirflowException, AirflowFailException, AirflowSensorTimeout

from forklift_client import TERMINAL_STATUSES, Job
from forklift_client.airflow.hooks import ForkliftHook
from forklift_client.airflow.triggers import ForkliftJobTrigger


def _outcome(job: Job) -> Dict[str, Any]:
    if job.status == "succeeded":
        return job.to_dict()
    error = f": {job.error.code}: {job.error.message}" if job.error else ""
    raise AirflowFailException(f"Forklift job {job.id} ({job.kind}) {job.status}{error}")


class ForkliftJobSensor(BaseSensorOperator):
    """Wait for a forklift job to end; succeed if it succeeded.

    :param job_id: the job (templated: ``{{ ti.xcom_pull(task_ids="load", key="job_id") }}``)
    :param deferrable: wait in the triggerer (default: ``[operators] default_deferrable``)
    :param forklift_conn_id: the ``forklift`` connection
    """

    template_fields: Sequence[str] = ("job_id",)
    ui_color = "#e7f0e3"

    def __init__(
        self,
        *,
        job_id: str,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        forklift_conn_id: str = ForkliftHook.default_conn_name,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.deferrable = deferrable
        self.forklift_conn_id = forklift_conn_id

    def poke(self, context: Any) -> PokeReturnValue:
        job = ForkliftHook(self.forklift_conn_id).get_conn().get_job(self.job_id)
        if job.status in TERMINAL_STATUSES:
            return PokeReturnValue(is_done=True, xcom_value=_outcome(job))
        self.log.info("Forklift job %s is %s", job.id, job.status)
        return PokeReturnValue(is_done=False)

    def execute(self, context: Any) -> Any:
        if not self.deferrable:
            return super().execute(context)
        # The trigger polls at once: a job that has ended resumes the task right away, and
        # resume_execution applies soft_fail to whatever execute_complete raises.
        self.defer(
            trigger=ForkliftJobTrigger(
                job_id=self.job_id,
                forklift_conn_id=self.forklift_conn_id,
                poll_interval=self.poke_interval,
                end_time=time.time() + self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: Dict[str, Any]) -> Dict[str, Any]:
        """Resumed by the trigger's event."""
        if event["status"] == "timeout":
            raise AirflowSensorTimeout(event["message"])
        if event["status"] not in TERMINAL_STATUSES:
            raise AirflowException(event["message"])
        return _outcome(ForkliftHook(self.forklift_conn_id).get_conn().get_job(event["job_id"]))
