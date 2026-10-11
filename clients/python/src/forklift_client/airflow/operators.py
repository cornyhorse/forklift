"""The Airflow operator that runs a forklift job and waits for its result.

``ForkliftJobOperator`` submits a job (``POST /api/v1/jobs``: a dataset run, or any job with the
fields of ``JobIn``), pushes its id as the XCom ``job_id``, and by default waits for it to end:
in the worker (polling), or with ``deferrable=True`` in Airflow's triggerer
(:class:`~forklift_client.airflow.triggers.ForkliftJobTrigger`), which frees the worker slot.
The task succeeds with the job (``GET /api/v1/jobs/{id}`` as JSON) as its return value, and fails
when the job failed or was cancelled, with the job's error code and message.

Retries re-attach. The job is submitted with an ``Idempotency-Key`` derived from the DAG, the
task, the DAG run, the map index and the first try of the task instance's current tries (not
from the try number), so a retry of the task, after a worker crash or a lost connection, gets
the job its earlier try submitted instead of starting a second one: a job that is still queued,
running or publishing is waited for again, one that succeeded is returned. Clearing the task
starts a new series of tries, and so a new job. A job that failed or was cancelled is not
re-attached to: the retry submits the job again, under the same key with ``:1``, ``:2`` ...
appended, so that a retry of the retry finds it again.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, Optional, Sequence

from airflow.sdk import BaseOperator, conf
from airflow.sdk.exceptions import AirflowException, AirflowFailException

from forklift_client import TERMINAL_STATUSES, Conflict, Job, JobTimeout
from forklift_client.airflow.hooks import ForkliftHook
from forklift_client.airflow.triggers import ForkliftJobTrigger

MAX_RESUBMISSIONS = 100  # keys tried after the first, for jobs that failed or were cancelled


def default_idempotency_key(context: Any, retries: int) -> str:
    """``airflow:`` and the sha256 of the DAG id, task id, run id, map index and first try."""
    ti = context["ti"]
    first_try = max(1, ti.max_tries - retries + 1)  # the try a clear (or the first run) began
    identity = [ti.dag_id, ti.task_id, ti.run_id, ti.map_index, first_try]
    return "airflow:" + hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def job_outcome(job: Job) -> Dict[str, Any]:
    """The job as the task's return value; raises for a job that did not succeed (failed: the
    task may be retried; cancelled: it is not, someone stopped the job on purpose)."""
    if job.status == "succeeded":
        return job.to_dict()
    error = f": {job.error.code}: {job.error.message}" if job.error else ""
    message = f"Forklift job {job.id} ({job.kind}) {job.status}{error}"
    if job.status == "cancelled":
        raise AirflowFailException(message)
    raise AirflowException(message)


class ForkliftJobOperator(BaseOperator):
    """Run a forklift job and wait for it.

    The job's fields are those of ``POST /api/v1/jobs`` (docs/platform/client.md): a dataset run
    is ``dataset_id`` (and ``upload_id`` for datasets that read uploads); other jobs name an
    ``upload_id`` with a ``format`` and a ``schema_version_id`` or an inline ``schema``.

    :param kind: ``run`` (default), ``preview``, ``validate_schema`` or ``generate_schema``
    :param idempotency_key: the job's key (templated); default: derived from the task instance
        so that retries re-attach (see the module's documentation)
    :param wait_for_completion: wait for the job to end (default), or return it once queued
    :param deferrable: wait in the triggerer instead of the worker (default: the
        ``[operators] default_deferrable`` setting)
    :param poll_interval: seconds between polls of the job
    :param wait_timeout: seconds to wait at most (None: no limit); the job is not cancelled when
        the wait times out
    :param cancel_on_kill: cancel the job when the task is killed (marked failed, cleared ...)
    :param forklift_conn_id: the ``forklift`` connection
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "upload_id",
        "format",
        "input_options",
        "schema_version_id",
        "schema",
        "compression",
        "options",
        "limits",
        "classification",
        "idempotency_key",
    )
    template_fields_renderers = {
        "input_options": "json",
        "schema": "json",
        "options": "json",
        "limits": "json",
    }
    ui_color = "#e7f0e3"

    def __init__(
        self,
        *,
        kind: str = "run",
        dataset_id: Optional[str] = None,
        upload_id: Optional[str] = None,
        format: Optional[str] = None,
        input_options: Optional[Dict[str, Any]] = None,
        schema_version_id: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        compression: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        limits: Optional[Dict[str, Any]] = None,
        classification: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 10.0,
        wait_timeout: Optional[float] = None,
        cancel_on_kill: bool = True,
        forklift_conn_id: str = ForkliftHook.default_conn_name,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if poll_interval <= 0:
            raise ValueError("poll_interval must be a positive number of seconds")
        self.kind = kind
        self.dataset_id = dataset_id
        self.upload_id = upload_id
        self.format = format
        self.input_options = input_options
        self.schema_version_id = schema_version_id
        self.schema = schema
        self.compression = compression
        self.options = options
        self.limits = limits
        self.classification = classification
        self.idempotency_key = idempotency_key
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.wait_timeout = wait_timeout
        self.cancel_on_kill = cancel_on_kill
        self.forklift_conn_id = forklift_conn_id
        self.job_id: Optional[str] = None

    @property
    def hook(self) -> ForkliftHook:
        return ForkliftHook(self.forklift_conn_id)

    def _submit(self, client, context) -> Job:
        base = self.idempotency_key or default_idempotency_key(context, self.retries or 0)
        fields = {
            "dataset_id": self.dataset_id,
            "upload_id": self.upload_id,
            "format": self.format,
            "input_options": self.input_options,
            "schema_version_id": self.schema_version_id,
            "schema": self.schema,
            "compression": self.compression,
            "options": self.options,
            "limits": self.limits,
            "classification": self.classification,
        }
        for generation in range(MAX_RESUBMISSIONS + 1):
            key = base if generation == 0 else f"{base}:{generation}"
            try:
                job = client.submit_job(self.kind, idempotency_key=key, **fields)
            except Conflict as error:
                if error.code != "idempotency_key_reused":
                    raise
                raise AirflowFailException(
                    f"The job's Idempotency-Key {key} was used for a different job: the task's "
                    "job changed since an earlier try submitted it. Clear the task to submit the "
                    "changed job, or give it an idempotency_key of its own."
                ) from None
            if job.status not in ("failed", "cancelled"):
                if job.status != "queued":
                    self.log.info(
                        "Re-attached to job %s (%s), submitted earlier", job.id, job.status
                    )
                return job
            self.log.info(
                "Job %s, submitted earlier with this key, %s; submitting the job again",
                job.id,
                job.status,
            )
        raise AirflowException(
            f"The job was submitted {MAX_RESUBMISSIONS + 1} times for this task and failed each "
            "time; clear the task to start over"
        )

    def execute(self, context: Any) -> Dict[str, Any]:
        client = self.hook.get_conn()
        job = self._submit(client, context)
        self.job_id = job.id
        context["ti"].xcom_push(key="job_id", value=job.id)
        self.log.info("Forklift job %s (%s) is %s", job.id, job.kind, job.status)
        if not self.wait_for_completion:
            return job.to_dict()
        if job.status in TERMINAL_STATUSES:
            return job_outcome(job)
        if self.deferrable:
            end_time = None if self.wait_timeout is None else time.time() + self.wait_timeout
            self.defer(
                trigger=ForkliftJobTrigger(
                    job_id=job.id,
                    forklift_conn_id=self.forklift_conn_id,
                    poll_interval=self.poll_interval,
                    end_time=end_time,
                    cancel_on_kill=self.cancel_on_kill,
                ),
                method_name="execute_complete",
            )
        last = [job.status]

        def report(current: Job) -> None:
            if current.status != last[0]:
                self.log.info("Forklift job %s is %s", current.id, current.status)
                last[0] = current.status

        try:
            job = client.wait_for_job(
                job,
                timeout=self.wait_timeout,
                poll_interval=self.poll_interval,
                max_poll_interval=self.poll_interval,
                on_poll=report,
            )
        except JobTimeout as error:
            raise AirflowException(
                f"Forklift job {job.id} is still {error.job.status} after {self.wait_timeout:g} "
                "seconds (it was not cancelled)"
            ) from None
        return job_outcome(job)

    def execute_complete(self, context: Any, event: Dict[str, Any]) -> Dict[str, Any]:
        """Resumed by the trigger's event: the job as it ended, or why the wait did."""
        self.job_id = event["job_id"]
        if event["status"] not in TERMINAL_STATUSES:
            raise AirflowException(event["message"])
        return job_outcome(self.hook.get_conn().get_job(event["job_id"]))

    def on_kill(self) -> None:
        if not (self.cancel_on_kill and self.job_id):
            return
        try:
            self.hook.get_conn().cancel_job(self.job_id)
        except Conflict as error:  # it has ended, or is publishing: nothing to stop
            self.log.info("Job %s was not cancelled: %s", self.job_id, error)
        else:
            self.log.info("Cancelled forklift job %s", self.job_id)
