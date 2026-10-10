"""End-to-end: a schedule of a running Compose stack (run these as test_stack.py says).

A dataset reads an object of an s3 connection to the stack's own store (an uploaded file, read
through the connection rather than as an upload). A schedule that runs every minute is due
within a minute; the dispatcher service queues its run and the worker runs it.
"""

from __future__ import annotations

import time
import uuid

from test_stack import (  # noqa: F401  (admin and settings are the session fixtures)
    JOB_TIMEOUT_SECONDS,
    PEOPLE,
    SCHEMA,
    admin,
    settings,
    upload,
)

# The dispatcher passes every 10 seconds; an every-minute schedule is due within 60.
DISPATCH_TIMEOUT_SECONDS = 120


def wait_for(client, path: str, ready, timeout: float):
    deadline = time.monotonic() + timeout
    while True:
        _, current = client.request("GET", path)
        if ready(current):
            return current
        assert time.monotonic() < deadline, f"still waiting: {current}"
        time.sleep(2)


def scheduled_dataset(admin, settings) -> str:  # noqa: F811
    """A dataset that reads people.csv through an s3 connection to the stack's bucket."""
    upload_id = upload(admin, "people.csv", PEOPLE)
    name = f"e2e-schedule-{uuid.uuid4().hex[:8]}"
    _, connection = admin.request(
        "POST",
        "/api/v1/connections",
        {
            "name": name,
            "kind": "s3",
            # The bucket of the stack as the gateway and the worker see it (the internal network)
            "config": {"bucket": "forklift", "endpoint_url": "http://rustfs:9000"},
            "secrets": {
                "access_key_id": settings["FORKLIFT_STORE_ACCESS_KEY"],
                "secret_access_key": settings["FORKLIFT_STORE_SECRET_KEY"],
            },
        },
    )
    _, schema = admin.request("POST", "/api/v1/schemas", {"name": name, "document": SCHEMA})
    _, versions = admin.request("GET", f"/api/v1/schemas/{schema['id']}/versions")
    _, dataset = admin.request(
        "POST",
        "/api/v1/datasets",
        {
            "name": name,
            "schema_version_id": versions["items"][0]["id"],
            "source_connection_id": connection["id"],
            "source_path": f"uploads/{upload_id}/people.csv",
        },
    )
    return dataset["id"]


def test_the_dispatcher_queues_a_due_schedule_for_the_worker(admin, settings):  # noqa: F811
    dataset_id = scheduled_dataset(admin, settings)
    _, schedule = admin.request(
        "POST", f"/api/v1/datasets/{dataset_id}/schedules", {"cron": "* * * * *"}
    )
    try:
        fired = wait_for(
            admin,
            f"/api/v1/schedules/{schedule['id']}",
            lambda current: current["last_job_id"] is not None,
            DISPATCH_TIMEOUT_SECONDS,
        )
        # Stop it before the next minute, so that one run is all this test starts
        admin.request("PATCH", f"/api/v1/schedules/{schedule['id']}", {"enabled": False})

        assert fired["last_outcome"] == "queued", fired["last_message"]
        assert fired["next_run_at"] > fired["last_run_at"]
        job = wait_for(
            admin,
            f"/api/v1/jobs/{fired['last_job_id']}",
            lambda current: current["status"] in ("succeeded", "failed", "cancelled"),
            JOB_TIMEOUT_SECONDS,
        )
        assert job["status"] == "succeeded", job["error"]
        assert job["result"]["counts"]["valid_rows"] == 2
        assert job["dataset_id"] == dataset_id and job["requested_by_id"] is None
        assert job["schedule_id"] == schedule["id"]
        assert job["scheduled_for"] == fired["last_run_at"]
        _, page = admin.request("GET", f"/api/v1/jobs?dataset_id={dataset_id}")
        assert [listed["id"] for listed in page["items"]] == [job["id"]]
    finally:
        admin.request("DELETE", f"/api/v1/schedules/{schedule['id']}")
