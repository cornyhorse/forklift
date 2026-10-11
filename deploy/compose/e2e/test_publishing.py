"""End-to-end: a run published to an s3 destination (run these as test_stack.py says).

A dataset reads an uploaded file through an s3 connection to the stack's own store and
publishes to that connection under a prefix. Its worker completes the run, which stays
``publishing`` until the publisher service has copied the outputs; only then is it succeeded.
"""

from __future__ import annotations

import uuid

from test_schedules import wait_for
from test_stack import (  # noqa: F401  (admin and settings are the session fixtures)
    JOB_TIMEOUT_SECONDS,
    PEOPLE,
    SCHEMA,
    admin,
    settings,
    store_config,
    upload,
)


def published_dataset(admin, settings) -> str:  # noqa: F811
    upload_id = upload(admin, "people.csv", PEOPLE)
    name = f"e2e-publish-{uuid.uuid4().hex[:8]}"
    _, connection = admin.request(
        "POST",
        "/api/v1/connections",
        {
            "name": name,
            "kind": "s3",
            "config": store_config(settings),
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
            "destination_connection_id": connection["id"],
            "destination_prefix": f"published/{name}",
        },
    )
    return dataset["id"]


def test_a_run_succeeds_once_the_publisher_has_published_it(admin, settings):  # noqa: F811
    dataset_id = published_dataset(admin, settings)
    _, created = admin.request("POST", f"/api/v1/datasets/{dataset_id}/run", {})

    job = wait_for(
        admin,
        f"/api/v1/jobs/{created['id']}",
        lambda current: current["status"] in ("succeeded", "failed", "cancelled"),
        JOB_TIMEOUT_SECONDS,
    )

    assert job["status"] == "succeeded", job["error"]
    assert job["publish_attempts"] == 1 and job["publish_error"] is None
    _, page = admin.request("GET", f"/api/v1/jobs/{job['id']}/events?limit=500")
    events = page["items"]
    states = [event["payload"]["status"] for event in events if event["type"] == "state"]
    assert states[-2:] == ["publishing", "succeeded"], states
    [published] = [event["payload"] for event in events if "published_to" in event["payload"]]
    assert published["files"] >= 2 and published["prefix"].endswith(job["id"])
