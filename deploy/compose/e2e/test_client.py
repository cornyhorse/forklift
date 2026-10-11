"""End-to-end: forklift-client (clients/python) against a running Compose stack.

The typed client is used the way a pipeline uses it: upload a CSV (one presigned PUT, and a
large one in parts), create a schema and a dataset that reads uploads, run it, wait, download
its outputs and read them, follow its events; submit a job twice with one Idempotency-Key;
cancel a queued job; and, with the webhook receiver of webhooks.yml, check a delivery's
signature with verify_webhook_signature. Nothing is mocked.

    pip install ./clients/python pytest pyarrow
    FORKLIFT_E2E_ENV_FILE=deploy/compose/.env FORKLIFT_E2E_RECEIVER_URL=http://localhost:18099 \\
      python -m pytest deploy/compose/e2e/test_client.py --no-cov

Skipped when forklift_client is not installed, and the webhook test without
FORKLIFT_E2E_RECEIVER_URL.
"""

from __future__ import annotations

import json
import os
import random
import time
import urllib.request
import uuid

import pyarrow.parquet as pq
import pytest
from test_stack import PEOPLE, SCHEMA, Client, _settings

forklift_client = pytest.importorskip("forklift_client")

from forklift_client import (  # noqa: E402
    Conflict,
    ForkliftClient,
    JobFailed,
    NotFound,
    Unauthorized,
    verify_webhook_signature,
)

JOB_TIMEOUT_SECONDS = 180
DELIVERY_TIMEOUT_SECONDS = 90  # the dispatcher passes every 10 seconds
MIB = 1024 * 1024


@pytest.fixture(scope="module")
def settings():
    return _settings()


@pytest.fixture(scope="module")
def raw(settings) -> Client:
    """test_stack's JSON client with an admin token, for what forklift-client does not do
    (admin settings, datasets, connections and webhooks are created through it)."""
    session = Client(settings["FORKLIFT_PUBLIC_URL"])
    session.sign_in(settings["FORKLIFT_ADMIN_USERNAME"], settings["FORKLIFT_ADMIN_PASSWORD"])
    _, me = session.request("GET", "/api/v1/me")
    _, created = session.request(
        "POST",
        "/api/v1/tokens",
        {"name": f"e2e-client-{uuid.uuid4().hex[:8]}", "scopes": me["role_scopes"]},
    )
    return Client(settings["FORKLIFT_PUBLIC_URL"], token=created["token"])


@pytest.fixture(scope="module")
def client(settings, raw) -> ForkliftClient:
    return ForkliftClient(settings["FORKLIFT_PUBLIC_URL"], raw.token, timeout=60)


@pytest.fixture(scope="module")
def people(client, tmp_path_factory):
    path = tmp_path_factory.mktemp("upload") / "people.csv"
    path.write_bytes(PEOPLE)
    return client.upload_file(path)


def wait(client: ForkliftClient, job, **options):
    return client.wait_for_job(job, timeout=JOB_TIMEOUT_SECONDS, poll_interval=0.5, **options)


class TestPipeline:
    def test_upload_a_dataset_run_and_its_downloads(self, client, raw, people, tmp_path):
        assert people.status == "complete" and not people.multipart
        assert people.size == len(PEOPLE) and people.content_type == "text/csv"
        schema = client.create_schema(f"people-{uuid.uuid4().hex[:8]}", SCHEMA, notes="e2e")
        version = client.get_schema_version(schema, 1)
        assert version.document == SCHEMA
        _, dataset = raw.request(
            "POST",
            "/api/v1/datasets",
            {"name": f"people-{uuid.uuid4().hex[:8]}", "schema_version_id": version.id},
        )

        job = client.run_dataset(dataset["id"], upload_id=people.id)
        done = wait(client, job, raise_on_failure=True)

        assert done.id == job.id and done.status == "succeeded"
        assert done.dataset_id == dataset["id"] and done.result["counts"]["valid_rows"] == 2
        found = {artifact.kind: artifact for artifact in client.list_artifacts(done)}
        data = client.download_artifact(found["data"], tmp_path)
        assert data.name == found["data"].name.split("/")[-1]
        assert pq.read_table(data).to_pylist() == [
            {"id": 1, "name": "Ana", "age": 34},
            {"id": 2, "name": "Bo", "age": 41},
        ]
        bad = client.download_artifact(found["bad_rows"].id, tmp_path / "bad.parquet")
        assert [row["id"] for row in pq.read_table(bad).to_pylist()] == ["3"]
        with client.open_artifact(found["data"]) as stream:
            assert stream.read() == data.read_bytes() and stream.verified
        statuses = [
            e.payload.get("status") for e in client.iter_job_events(done) if e.type == "state"
        ]
        assert statuses[0] == "queued" and statuses[-1] == "succeeded"
        assert client.get_dataset(dataset["id"]).schema_version_id == version.id
        assert dataset["id"] in {d.id for d in client.paginate(client.list_datasets, page_size=2)}

    def test_a_large_file_goes_up_in_parts(self, client, raw, tmp_path):
        rng = random.Random(5)
        rows = 300_000
        body = b"id,name\n" + b"".join(
            b"%d,%032x\n" % (i, rng.getrandbits(128)) for i in range(rows)
        )
        path = tmp_path / "wide.csv"
        path.write_bytes(body)
        raw.request(
            "PATCH",
            "/api/v1/admin/settings",
            {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB},
        )
        try:
            upload = client.upload_file(path)
        finally:
            raw.request(
                "PATCH",
                "/api/v1/admin/settings",
                {"multipart_threshold_bytes": None, "multipart_part_bytes": None},
            )
        assert upload.multipart and upload.size == len(body) > 2 * 5 * MIB

        job = client.submit_job(
            "run",
            upload_id=upload.id,
            format="csv",
            schema={"properties": {"id": {"type": "integer"}, "name": {"type": "string"}}},
        )
        done = wait(client, job, raise_on_failure=True)

        assert done.result["counts"]["valid_rows"] == rows, "every byte of every part arrived"

    def test_generate_and_validate_a_schema(self, client, people):
        generated = wait(
            client, client.submit_job("generate_schema", upload_id=people.id, format="csv")
        )
        assert generated.status == "succeeded", generated.error
        (artifact,) = [a for a in client.list_artifacts(generated) if a.kind == "schema"]
        with client.open_artifact(artifact) as stream:
            schema = json.loads(stream.read())
        assert set(schema["properties"]) == {"id", "name", "age"}

        checked = client.validate_schema(upload_id=people.id, schema=schema, format="csv")
        assert checked.kind == "validate_schema"
        assert wait(client, checked).status == "succeeded"

    def test_who_am_i_and_the_lists(self, client):
        me = client.me()
        assert me.user.role == "admin" and me.token is not None
        assert client.list_jobs(limit=1).count >= 1
        assert client.list_schedules().count >= 0
        assert client.list_webhooks().count >= 0


class TestIdempotency:
    def test_one_key_gives_one_job(self, client, people):
        key = f"e2e-{uuid.uuid4().hex}"
        first = client.submit_job(
            "preview", upload_id=people.id, format="csv", idempotency_key=key
        )
        again = client.submit_job(
            "preview", upload_id=people.id, format="csv", idempotency_key=key
        )
        assert again.id == first.id

        with pytest.raises(Conflict) as raised:
            client.submit_job(
                "generate_schema", upload_id=people.id, format="csv", idempotency_key=key
            )
        assert raised.value.code == "idempotency_key_reused" and raised.value.request_id
        wait(client, first)


class TestCancel:
    def test_a_queued_job_is_cancelled(self, client, raw, people):
        """A run that writes a database table goes to the sql lane, which no worker of the
        stack serves: it stays queued, so the cancellation is certain to find it there."""
        name = f"e2e-db-{uuid.uuid4().hex[:8]}"
        _, connection = raw.request(
            "POST",
            "/api/v1/connections",
            {
                "name": name,
                "kind": "sql",
                "config": {
                    "dialect": "postgresql",
                    "host": "db.invalid",
                    "database": "d",
                    "username": "u",
                },
                "secrets": {"password": "not-used"},
            },
        )
        assert client.get_connection(connection["id"]).secret_fields == ("password",)
        version = client.get_schema_version(client.create_schema(name, SCHEMA), 1)
        _, dataset = raw.request(
            "POST",
            "/api/v1/datasets",
            {
                "name": name,
                "schema_version_id": version.id,
                "destination_connection_id": connection["id"],
                "destination_options": {"table": "people_clean"},
            },
        )
        job = client.run_dataset(dataset["id"], upload_id=people.id)
        assert job.lane == "sql" and job.status == "queued"

        cancelled = client.cancel_job(job)

        assert cancelled.status == "cancelled" and cancelled.error.code == "CANCELLED"
        with pytest.raises(JobFailed) as raised:
            wait(client, job, raise_on_failure=True)
        assert raised.value.code == "CANCELLED"
        with pytest.raises(Conflict, match="already finished"):
            client.cancel_job(job)


class TestErrors:
    def test_a_missing_job(self, client):
        with pytest.raises(NotFound) as raised:
            client.get_job(str(uuid.uuid4()))
        assert raised.value.code == "not_found" and raised.value.request_id

    def test_a_wrong_token(self, settings):
        stranger = ForkliftClient(settings["FORKLIFT_PUBLIC_URL"], "fkl_not-a-real-token")
        with pytest.raises(Unauthorized) as raised:
            stranger.me()
        assert "fkl_not-a-real-token" not in str(raised.value)


@pytest.fixture(scope="module")
def receiver_url() -> str:
    url = os.environ.get("FORKLIFT_E2E_RECEIVER_URL")
    if not url:
        pytest.skip(
            "start the stack with deploy/compose/e2e/webhooks.yml and set "
            "FORKLIFT_E2E_RECEIVER_URL to the receiver's published address"
        )
    return url.rstrip("/")


class TestWebhookSignature:
    def test_a_delivery_verifies_with_the_webhooks_secret(self, client, raw, receiver_url):
        path = f"/client-{uuid.uuid4().hex}"
        _, webhook = raw.request(
            "POST",
            "/api/v1/webhooks",
            {
                "name": f"e2e-client-{uuid.uuid4().hex[:6]}",
                "url": f"http://webhook-receiver:8000{path}",
                "events": ["job.succeeded"],
            },
        )
        secret = webhook["secret"]
        assert client.get_webhook(webhook["id"]).secret_prefix == webhook["secret_prefix"]
        raw.request("POST", f"/api/v1/webhooks/{webhook['id']}/test", expect=(202,))

        deadline = time.monotonic() + DELIVERY_TIMEOUT_SECONDS
        while True:
            with urllib.request.urlopen(f"{receiver_url}/deliveries", timeout=10) as response:
                found = [item for item in json.loads(response.read()) if item["path"] == path]
            if found:
                break
            assert time.monotonic() < deadline, "the dispatcher did not send the test event"
            time.sleep(2)

        (delivery,) = found
        body = delivery["body"].encode("utf-8")
        header = delivery["headers"]["forklift-signature"]
        assert verify_webhook_signature(body, header, secret)
        assert verify_webhook_signature(body, header, ["fkwh_old-secret", secret])
        assert not verify_webhook_signature(body, header, "fkwh_" + "x" * 40)
        assert not verify_webhook_signature(body + b" ", header, secret)
        assert not verify_webhook_signature(body, header, secret, now=time.time() + 3600)
