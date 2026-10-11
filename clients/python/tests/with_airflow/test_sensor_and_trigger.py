"""ForkliftJobSensor run by ``dag.test()`` (poke and deferrable), and the trigger itself."""

from __future__ import annotations

import asyncio
import time

import pytest
from dag_helpers import states, xcom

from forklift_client import ApiError, ServerError, TransportError
from forklift_client.airflow.triggers import ForkliftJobTrigger, is_transient


def sensor(job_id: str, **options: str) -> str:
    extra = "".join(f", {name}={value}" for name, value in options.items())
    return f'ForkliftJobSensor(task_id="wait", job_id={job_id!r}, poke_interval=0.01{extra})'


class TestSensor:
    def test_poking_until_the_job_succeeded(self, fake, run_dag):
        job = fake.add_job("queued")
        fake.script(job["id"], "running", "publishing", "succeeded")
        dagrun = run_dag(sensor(job["id"]))
        assert states(dagrun) == {"wait": "success"}
        assert xcom(dagrun, "wait")["status"] == "succeeded"
        assert len(fake.sent("GET /jobs/{id}")) == 3

    def test_a_failed_job_fails_the_sensor_without_retries(self, fake, run_dag):
        job = fake.add_job("failed")
        dagrun = run_dag(sensor(job["id"], retries="2"))
        assert states(dagrun) == {"wait": "failed"}
        assert len(fake.sent("GET /jobs/{id}")) == 1

    def test_soft_fail_skips(self, fake, run_dag):
        job = fake.add_job("cancelled")
        dagrun = run_dag(sensor(job["id"], soft_fail="True"))
        assert states(dagrun) == {"wait": "skipped"}

    def test_the_job_id_from_the_operators_xcom(self, fake, run_dag):
        fake.new_job_script = ["running", "succeeded"]
        dagrun = run_dag(
            'submit = ForkliftJobOperator(task_id="submit", wait_for_completion=False)\n'
            'wait = ForkliftJobSensor(task_id="wait", poke_interval=0.01, deferrable=True,\n'
            "    job_id=\"{{ ti.xcom_pull(task_ids='submit', key='job_id') }}\")\n"
            "submit >> wait\n"
        )
        assert states(dagrun) == {"submit": "success", "wait": "success"}
        (job,) = fake.jobs.values()
        assert xcom(dagrun, "wait")["id"] == job["id"]

    def test_deferred(self, fake, run_dag):
        job = fake.add_job("running")
        fake.script(job["id"], "running", "succeeded")
        dagrun = run_dag(sensor(job["id"], deferrable="True"))
        assert states(dagrun) == {"wait": "success"}

    def test_deferred_and_failed(self, fake, run_dag):
        job = fake.add_job("failed")
        dagrun = run_dag(sensor(job["id"], deferrable="True"))
        assert states(dagrun) == {"wait": "failed"}

    def test_deferred_and_timed_out(self, fake, run_dag, capfd):
        job = fake.add_job("running")
        dagrun = run_dag(sensor(job["id"], deferrable="True", timeout="0.05"))
        assert states(dagrun) == {"wait": "failed"}
        assert "AirflowSensorTimeout" in capfd.readouterr().out

    def test_deferred_and_the_job_does_not_exist(self, fake, run_dag, capfd):
        dagrun = run_dag(sensor("6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1", deferrable="True"))
        assert states(dagrun) == {"wait": "failed"}
        assert "There is no job with id" in capfd.readouterr().out


def collect(trigger):
    async def run():
        return [event.payload async for event in trigger.run()]

    return asyncio.run(run())


class TestTrigger:
    def test_serialize_round_trips(self):
        trigger = ForkliftJobTrigger(
            "j1", "conn", poll_interval=3, end_time=12.5, cancel_on_kill=True
        )
        path, kwargs = trigger.serialize()
        assert path == "forklift_client.airflow.triggers.ForkliftJobTrigger"
        assert ForkliftJobTrigger(**kwargs).serialize() == (path, kwargs)

    def test_one_event_when_the_job_ends(self, fake, connection):
        job = fake.add_job("running")
        fake.script(job["id"], "publishing", {"status": "cancelled", "error": None})
        events = collect(ForkliftJobTrigger(job["id"], poll_interval=0.01))
        assert events == [
            {"status": "cancelled", "job_id": job["id"], "message": f"Job {job['id']} cancelled"}
        ]

    def test_a_connection_that_does_not_exist(self):
        (event,) = collect(ForkliftJobTrigger("j", "forklift_missing"))
        assert event["status"] == "error"
        assert "Cannot use connection 'forklift_missing'" in event["message"]

    def test_an_error_that_is_not_transient_ends_it(self, fake, connection):
        (event,) = collect(ForkliftJobTrigger("6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1"))
        assert event["status"] == "error" and "HTTP 404 (not_found)" in event["message"]

    def test_a_deadline_in_the_past(self, fake, connection):
        job = fake.add_job("running")
        (event,) = collect(ForkliftJobTrigger(job["id"], end_time=time.time() - 1))
        assert event["status"] == "timeout"

    def test_on_kill_cancels_when_asked_to(self, fake, connection):
        job = fake.add_job("running")
        asyncio.run(ForkliftJobTrigger(job["id"]).on_kill())
        assert fake.sent("POST /jobs/{id}/cancel") == []
        asyncio.run(ForkliftJobTrigger(job["id"], cancel_on_kill=True).on_kill())
        assert job["cancel_requested"]
        done = fake.add_job("succeeded")
        asyncio.run(ForkliftJobTrigger(done["id"], cancel_on_kill=True).on_kill())
        assert len(fake.sent("POST /jobs/{id}/cancel")) == 2

    @pytest.mark.parametrize(
        "error, transient",
        [
            (TransportError("GET", "h", "reset"), True),
            (TransportError("GET", "h", "TLS", retryable=False), False),
            (ServerError("GET", "/x", 503), True),
            (ApiError("GET", "/x", 404), False),
        ],
    )
    def test_is_transient(self, error, transient):
        assert is_transient(error) is transient
