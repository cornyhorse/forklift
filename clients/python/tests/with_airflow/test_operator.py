"""ForkliftJobOperator run by ``dag.test()``: waiting in the worker and deferred to the
triggerer, failures, timeouts, and retries that re-attach to the job an earlier try submitted."""

from __future__ import annotations

import json

import pytest
from airflow.sdk.exceptions import AirflowException, AirflowFailException
from dag_helpers import states, tries, xcom

from forklift_client import Job
from forklift_client.airflow import operators
from forklift_client.airflow.operators import (
    ForkliftJobOperator,
    default_idempotency_key,
    job_outcome,
)

SUCCESS = ["running", "publishing", "succeeded"]


def keys(fake):
    return [entry["headers"]["idempotency-key"] for entry in fake.sent("POST /jobs")]


class TestWaitingInTheWorker:
    def test_a_dataset_run_that_succeeds(self, fake, run_dag):
        dataset = fake.add_dataset()
        fake.new_job_script = list(SUCCESS)

        dataset_id = dataset["id"]
        dagrun = run_dag(
            f'ForkliftJobOperator(task_id="load", dataset_id={dataset_id!r}, poll_interval=0.01)'
        )

        assert states(dagrun) == {"load": "success"}
        (job,) = fake.jobs.values()
        assert job["dataset_id"] == dataset["id"] and job["status"] == "succeeded"
        result = xcom(dagrun, "load")
        assert result["id"] == job["id"] and result["status"] == "succeeded"
        assert Job.from_dict(result).result == {"counts": job["result"]["counts"]}
        assert xcom(dagrun, "load", key="job_id") == job["id"]
        (key,) = keys(fake)
        assert key.startswith("airflow:") and len(key) == 72

    def test_a_job_spec_with_templates(self, fake, run_dag):
        fake.new_job_script = ["succeeded"]
        upload = "6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1"
        dagrun = run_dag(f"""
ForkliftJobOperator(
    task_id="clean",
    upload_id={upload!r},
    format="csv",
    schema={{"properties": {{"id": {{"type": "integer"}}}}}},
    input_options={{"delimiter": ";"}},
    options={{"run": "{{{{ run_id }}}}"}},
    limits={{"max_rows": 100}},
    classification="sensitive",
    compression="zstd",
    idempotency_key="clean-{{{{ dag_run.dag_id }}}}",
    poll_interval=0.01,
)
""")
        assert states(dagrun) == {"clean": "success"}
        (sent,) = fake.sent("POST /jobs")
        body = json.loads(sent["body"])
        assert body["upload_id"] == upload and body["format"] == "csv"
        assert body["options"] == {"run": dagrun.run_id}
        assert body["limits"] == {"max_rows": 100} and body["classification"] == "sensitive"
        assert sent["headers"]["idempotency-key"] == f"clean-{dagrun.dag_id}"

    def test_a_job_that_fails_fails_the_task(self, fake, run_dag, capfd):
        fake.new_job_script = ["running", "failed"]
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", dataset_id=None, poll_interval=0.01)'
        )
        assert states(dagrun) == {"load": "failed"}
        assert (
            "failed: BAD_ROWS_THRESHOLD_EXCEEDED: 1 of 3 rows rejected" in capfd.readouterr().out
        )

    def test_a_cancelled_job_is_not_retried(self, fake, run_dag):
        fake.new_job_script = [{"status": "cancelled"}]
        dagrun = run_dag('ForkliftJobOperator(task_id="load", retries=2, poll_interval=0.01)')
        assert states(dagrun) == {"load": "failed"} and tries(dagrun) == {"load": 1}
        assert len(fake.jobs) == 1

    def test_without_waiting(self, fake, run_dag):
        dagrun = run_dag('ForkliftJobOperator(task_id="submit", wait_for_completion=False)')
        assert states(dagrun) == {"submit": "success"}
        assert xcom(dagrun, "submit")["status"] == "queued"
        assert fake.sent("GET /jobs/{id}") == []

    def test_a_wait_that_times_out(self, fake, run_dag, capfd):
        fake.next_job_status = "running"
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", poll_interval=0.01, wait_timeout=0.05)'
        )
        assert states(dagrun) == {"load": "failed"}
        out = capfd.readouterr().out
        assert "is still running after 0.05 seconds (it was not cancelled)" in out
        (job,) = fake.jobs.values()
        assert not job["cancel_requested"]


class TestRetriesReattach:
    def test_a_retry_waits_for_the_job_the_first_try_submitted(self, fake, run_dag):
        fake.new_job_script = ["running", "running", "succeeded"]
        # The first try loses its gateway while it waits (a 401 ends a wait at once), the
        # retry submits again with the same key and gets the same job.
        fake.fail("GET /jobs/{id}", status=401, body={"detail": "Token expired.", "code": "x"})

        dagrun = run_dag('ForkliftJobOperator(task_id="load", retries=1, poll_interval=0.01)')

        assert states(dagrun) == {"load": "success"} and tries(dagrun) == {"load": 2}
        assert len(fake.jobs) == 1, "the retry did not start a second job"
        first, second = keys(fake)
        assert first == second
        assert [r["route"] for r in fake.requests].count("POST /jobs") == 2

    def test_a_retry_after_a_failed_job_submits_it_again_under_the_next_key(self, fake, run_dag):
        fake.new_job_script = ["failed"]
        original = fake._submit

        def second_succeeds(request, key):
            status, job = original(request, key)
            fake.new_job_script = ["succeeded"]
            return status, job

        fake._submit = second_succeeds
        dagrun = run_dag('ForkliftJobOperator(task_id="load", retries=1, poll_interval=0.01)')

        assert states(dagrun) == {"load": "success"} and tries(dagrun) == {"load": 2}
        assert len(fake.jobs) == 2
        first, again, fresh = keys(fake)
        assert again == first and fresh == f"{first}:1"

    def test_a_job_that_already_succeeded_is_returned(self, fake, run_dag, capfd):
        fake.next_job_status = "succeeded"
        dagrun = run_dag('ForkliftJobOperator(task_id="load", poll_interval=0.01)')
        assert states(dagrun) == {"load": "success"}
        assert "Re-attached to job" in capfd.readouterr().out

    def test_a_changed_job_under_the_same_key_fails_without_retries(self, fake, run_dag):
        fake.keys["fixed-key"] = ("another request", fake.add_job()["id"])
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", idempotency_key="fixed-key", retries=2)'
        )
        assert states(dagrun) == {"load": "failed"} and tries(dagrun) == {"load": 1}

    def test_other_conflicts_are_raised(self, fake, run_dag):
        fake.fail("POST /jobs", status=409, body={"detail": "Busy.", "code": "conflict"})
        dagrun = run_dag('ForkliftJobOperator(task_id="load")')
        assert states(dagrun) == {"load": "failed"}

    def test_resubmissions_are_bounded(self, fake, run_dag, monkeypatch, capfd):
        monkeypatch.setattr(operators, "MAX_RESUBMISSIONS", 1)
        fake.next_job_status = "failed"
        dagrun = run_dag('ForkliftJobOperator(task_id="load")')
        assert states(dagrun) == {"load": "failed"}
        assert "was submitted 2 times for this task and failed each time" in capfd.readouterr().out


class TestDeferred:
    def test_a_deferred_wait(self, fake, run_dag):
        fake.new_job_script = list(SUCCESS)
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", deferrable=True, poll_interval=0.01)'
        )
        assert states(dagrun) == {"load": "success"}
        assert xcom(dagrun, "load")["status"] == "succeeded"

    def test_a_deferred_failure(self, fake, run_dag, capfd):
        fake.new_job_script = ["running", "failed"]
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", deferrable=True, poll_interval=0.01)'
        )
        assert states(dagrun) == {"load": "failed"}
        assert "BAD_ROWS_THRESHOLD_EXCEEDED" in capfd.readouterr().out

    def test_transient_errors_are_polled_through(self, fake, run_dag):
        fake.new_job_script = ["succeeded"]
        fake.fail("GET /jobs/{id}", status=503, times=6)  # more than the client retries
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", deferrable=True, poll_interval=0.01)'
        )
        assert states(dagrun) == {"load": "success"}

    def test_a_job_that_is_gone_ends_the_wait(self, fake, run_dag, capfd):
        fake.fail("GET /jobs/{id}", status=404, body={"detail": "No job.", "code": "not_found"})
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", deferrable=True, poll_interval=0.01)'
        )
        assert states(dagrun) == {"load": "failed"}
        assert "failed with HTTP 404 (not_found): No job." in capfd.readouterr().out

    def test_a_deferred_wait_that_times_out(self, fake, run_dag, capfd):
        fake.next_job_status = "running"
        dagrun = run_dag(
            'ForkliftJobOperator(task_id="load", deferrable=True, poll_interval=0.01, '
            "wait_timeout=0.05)"
        )
        assert states(dagrun) == {"load": "failed"}
        assert "had not ended when the wait timed out" in capfd.readouterr().out

    def test_a_job_that_ended_at_once_is_not_deferred(self, fake, run_dag):
        fake.next_job_status = "succeeded"
        dagrun = run_dag('ForkliftJobOperator(task_id="load", deferrable=True)')
        assert states(dagrun) == {"load": "success"}
        assert fake.sent("GET /jobs/{id}") == []


class Ti:
    dag_id, task_id, run_id, map_index = "d", "t", "manual__1", -1

    def __init__(self, max_tries):
        self.max_tries = max_tries


class TestUnits:
    def test_the_default_key_stays_across_retries_and_changes_when_cleared(self):
        first = default_idempotency_key({"ti": Ti(max_tries=2)}, retries=2)
        assert first.startswith("airflow:")
        # a retry: max_tries does not change; a clear: max_tries = try_number + retries
        assert default_idempotency_key({"ti": Ti(max_tries=2)}, retries=2) == first
        assert default_idempotency_key({"ti": Ti(max_tries=5)}, retries=2) != first
        assert default_idempotency_key(
            {"ti": Ti(max_tries=0)}, retries=3
        ) == default_idempotency_key(
            {"ti": Ti(max_tries=0)}, retries=0
        ), "never a first try below 1"

    def test_job_outcome(self):
        from fake_api import _job_doc

        assert job_outcome(Job.from_dict(_job_doc("succeeded")))["status"] == "succeeded"
        with pytest.raises(AirflowFailException, match=r"\(run\) cancelled: CANCELLED"):
            job_outcome(Job.from_dict(_job_doc("cancelled")))
        failed = _job_doc("failed")
        failed["error"] = None
        with pytest.raises(AirflowException, match=r"\(run\) failed$"):
            job_outcome(Job.from_dict(failed))

    def test_poll_interval_must_be_positive(self):
        with pytest.raises(ValueError, match="poll_interval"):
            ForkliftJobOperator(task_id="x", poll_interval=0)

    def test_on_kill_cancels_the_job(self, fake, connection):
        job = fake.add_job("running")
        operator = ForkliftJobOperator(task_id="x")
        operator.on_kill()  # nothing submitted yet: nothing to cancel
        operator.job_id = job["id"]
        operator.on_kill()
        assert job["cancel_requested"]
        finished = fake.add_job("succeeded")
        operator.job_id = finished["id"]
        operator.on_kill()  # a finished job: logged, not raised
        assert len(fake.sent("POST /jobs/{id}/cancel")) == 2
        keep = ForkliftJobOperator(task_id="y", cancel_on_kill=False)
        keep.job_id = job["id"]
        keep.on_kill()
        assert len(fake.sent("POST /jobs/{id}/cancel")) == 2
