"""ForkliftClient against the fake gateway: authentication, every resource group, errors,
pagination, idempotent job submission and waiting for jobs."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime

import pytest
from fake_api import TOKEN

import forklift_client
from forklift_client import (
    NO_RETRIES,
    BadRequest,
    Conflict,
    Forbidden,
    ForkliftClient,
    Gone,
    Job,
    JobFailed,
    JobSummary,
    JobTimeout,
    NotFound,
    Page,
    RetryPolicy,
    ServerError,
    TooManyRequests,
    TransportError,
    Unauthorized,
    UnexpectedResponse,
)
from forklift_client.errors import ApiError


def bodies(fake, route):
    return [json.loads(entry["body"]) if entry["body"] else None for entry in fake.sent(route)]


class TestConstruction:
    def test_the_base_url_may_end_in_the_api_prefix_or_a_slash(self, fake):
        for given in (fake.url, fake.url + "/", fake.url + "/api/v1", fake.url + "/api/v1/"):
            assert ForkliftClient(given, TOKEN).base_url == fake.url

    def test_a_base_url_under_a_path_keeps_it(self):
        client = ForkliftClient("https://example.org/forklift/api/v1", TOKEN)
        assert client.base_url == "https://example.org/forklift"

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://example.org",
            "example.org",
            "https://",
            "https://user:secret@example.org",
            "https://example.org/?a=1",
            "https://example.org/#x",
        ],
    )
    def test_base_urls_that_are_not_a_gateways_are_refused_without_echoing_them(self, url):
        with pytest.raises(ValueError, match="base_url must be") as raised:
            ForkliftClient(url, TOKEN)
        assert "secret" not in str(raised.value)

    @pytest.mark.parametrize("token", ["", "  ", "fkl_a b", "fkl_a\nb", "fkl_ü"])
    def test_bad_tokens_are_refused_without_echoing_them(self, token):
        with pytest.raises(ValueError, match="token") as raised:
            ForkliftClient("https://example.org", token)
        if token.strip():
            assert token.strip() not in str(raised.value)

    def test_a_token_read_from_a_file_may_end_in_a_newline(self, fake):
        client = ForkliftClient(fake.url, TOKEN + "\n")
        assert client.me().user.username == "ana"

    def test_timeout_must_be_positive(self):
        with pytest.raises(ValueError, match="timeout"):
            ForkliftClient("https://example.org", TOKEN, timeout=0)

    def test_repr_never_shows_the_token(self, client):
        assert TOKEN not in repr(client)
        assert repr(client) == f"ForkliftClient({client.base_url!r})"
        assert client.timeout == 10

    def test_from_env(self, fake, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_text("")
        client = ForkliftClient.from_env(
            {"FORKLIFT_URL": fake.url, "FORKLIFT_TOKEN": TOKEN}, timeout=3
        )
        assert client.me().user.username == "ana"
        assert client.timeout == 3
        with pytest.raises(ValueError, match="FORKLIFT_URL and FORKLIFT_TOKEN"):
            ForkliftClient.from_env({})
        with pytest.raises(ValueError, match="set FORKLIFT_TOKEN"):
            ForkliftClient.from_env({"FORKLIFT_URL": fake.url})
        with pytest.raises(ValueError, match="is not a readable PEM bundle of CA certificates"):
            ForkliftClient.from_env(
                {"FORKLIFT_URL": fake.url, "FORKLIFT_TOKEN": TOKEN, "FORKLIFT_CA_FILE": str(ca)}
            )
        with pytest.raises(ValueError, match="FileNotFoundError"):
            ForkliftClient(fake.url, TOKEN, ca_file=tmp_path / "missing.pem")

    def test_from_the_process_environment(self, fake, monkeypatch):
        monkeypatch.setenv("FORKLIFT_URL", fake.url)
        monkeypatch.setenv("FORKLIFT_TOKEN", TOKEN)
        monkeypatch.delenv("FORKLIFT_CA_FILE", raising=False)
        assert ForkliftClient.from_env().me().user.username == "ana"

    def test_requests_carry_the_token_a_request_id_and_the_user_agent(self, fake):
        client = ForkliftClient(fake.url, TOKEN, user_agent="my-pipeline/2")
        client.me()
        (sent,) = fake.sent("GET /me")
        assert sent["headers"]["authorization"] == f"Bearer {TOKEN}"
        assert len(sent["headers"]["x-request-id"]) == 32
        agent = sent["headers"]["user-agent"]
        assert agent.startswith(f"my-pipeline/2 forklift-client/{forklift_client.__version__} ")
        assert "Python/" in agent


class TestErrors:
    def test_a_wrong_token_is_unauthorized_and_the_message_never_has_it(self, fake):
        client = ForkliftClient(fake.url, "fkl_wrong-token-value")
        with pytest.raises(Unauthorized) as raised:
            client.me()
        error = raised.value
        assert error.status == 401 and error.code == "not_authenticated"
        assert error.method == "GET" and error.path == "/api/v1/me"
        assert error.request_id == fake.sent("GET /me")[0]["headers"]["x-request-id"]
        assert "fkl_wrong-token-value" not in str(error)
        assert str(error).startswith("GET /api/v1/me failed with HTTP 401 (not_authenticated): ")
        assert not error.transient

    @pytest.mark.parametrize(
        "status, kind",
        [
            (400, BadRequest),
            (403, Forbidden),
            (404, NotFound),
            (409, Conflict),
            (410, Gone),
            (418, ApiError),
            (500, ServerError),
        ],
    )
    def test_statuses_map_to_exceptions(self, fake, client, status, kind):
        fake.fail("GET /me", status=status, body={"detail": "Nope.", "code": "nope"})
        with pytest.raises(kind) as raised:
            client.me()
        assert type(raised.value) is kind
        assert (raised.value.status, raised.value.code, raised.value.detail) == (
            status,
            "nope",
            "Nope.",
        )

    def test_a_422_lists_the_fields_without_the_input(self, fake, client):
        detail = [
            {"type": "missing", "loc": ["body", "payload", "kind"], "msg": "Field required"},
            {"type": "x", "loc": [], "msg": "Bad", "input": "fkl_secret"},
            {"msg": "no location"},
            "not a dict",
        ]
        fake.fail("GET /me", status=422, body={"detail": detail})
        with pytest.raises(BadRequest) as raised:
            client.me()
        assert raised.value.detail == "body.payload.kind: Field required; Bad; no location"
        assert raised.value.code == ""
        assert "fkl_secret" not in str(raised.value)

    def test_error_bodies_that_are_not_the_apis(self, fake, client):
        fake.fail("GET /me", status=404, body=b"<html>Not Found</html>")
        with pytest.raises(NotFound, match="is base_url the gateway's public URL"):
            client.me()
        fake.fail("GET /me", status=400, body=["a", "list"])
        with pytest.raises(BadRequest) as raised:
            client.me()
        assert (raised.value.code, raised.value.detail) == ("", "")
        fake.fail("GET /me", status=400, body={"detail": "x", "code": 7})
        with pytest.raises(BadRequest) as raised:
            client.me()
        assert raised.value.code == ""

    def test_an_error_made_by_hand(self):
        error = ApiError("GET", "/api/v1/x", 400)
        assert str(error) == "GET /api/v1/x failed with HTTP 400"
        assert (error.code, error.detail, error.request_id, error.retry_after) == (
            "",
            "",
            "",
            None,
        )

    def test_a_request_id_comes_from_the_answer_when_it_has_one(self, fake, client):
        fake.fail("GET /me", status=403, headers={"X-Request-ID": "gateway-id-1"})
        with pytest.raises(Forbidden) as raised:
            client.me()
        assert raised.value.request_id == "gateway-id-1"
        assert "[request id gateway-id-1]" in str(raised.value)

    def test_a_success_that_is_not_json(self, fake, client):
        fake.fail("GET /me", status=200, body=b"<html>a login page</html>")
        with pytest.raises(UnexpectedResponse, match="not JSON .*application/json"):
            client.me()

    def test_documents_without_the_fields_the_client_needs(self, fake, client):
        fake.fail("GET /me", status=200, body={"user": {}})
        with pytest.raises(UnexpectedResponse, match=r"^Me.user has no 'id'; is the gateway"):
            client.me()
        fake.fail("GET /me", status=200, body=["not", "an", "object"])
        with pytest.raises(UnexpectedResponse, match="should answer an object"):
            client.me()

    def test_lists_and_pages_of_the_wrong_shape(self, fake, client):
        job = fake.add_job()
        fake.fail("GET /jobs/{id}/artifacts", status=200, body={"not": "a list"})
        with pytest.raises(UnexpectedResponse, match="should answer a list"):
            client.list_artifacts(job["id"])
        fake.fail("GET /jobs", status=200, body={"items": [], "count": "3"})
        with pytest.raises(UnexpectedResponse, match="should answer a page"):
            client.list_jobs()
        fake.fail("GET /jobs", status=200, body=[])
        with pytest.raises(UnexpectedResponse, match="should answer a page"):
            client.list_jobs()
        fake.fail("GET /jobs", status=200, body={"count": 1})
        with pytest.raises(UnexpectedResponse, match="should answer a page"):
            client.list_jobs()


class TestRetries:
    def test_gets_are_retried_after_5xx_and_connection_drops(self, fake, client):
        fake.fail("GET /me", status=502)
        fake.fail("GET /me", drop="before")
        assert client.me().user.username == "ana"
        ids = [entry["headers"]["x-request-id"] for entry in fake.sent("GET /me")]
        assert len(ids) == 3 and len(set(ids)) == 1, "one request id across the retries"

    def test_the_last_error_is_raised_when_the_attempts_run_out(self, fake, client):
        fake.fail("GET /me", status=503, times=3)
        with pytest.raises(ServerError) as raised:
            client.me()
        assert raised.value.transient
        assert len(fake.sent("GET /me")) == 3
        fake.fail("GET /me", drop="before", times=3)
        with pytest.raises(TransportError) as dropped:
            client.me()
        assert dropped.value.sent and dropped.value.retryable
        assert dropped.value.what == "GET /api/v1/me"

    def test_retry_after_is_waited(self, fake, client, waits):
        fake.fail("GET /me", status=429, headers={"Retry-After": "3"})
        fake.fail("GET /me", status=503, headers={"Retry-After": "2"})
        assert client.me()
        assert waits == [3.0, 2.0]

    def test_a_retry_after_longer_than_the_policy_allows_is_raised(self, fake, client, waits):
        fake.fail("GET /me", status=429, headers={"Retry-After": "600"})
        with pytest.raises(TooManyRequests) as raised:
            client.me()
        assert raised.value.retry_after == 600.0 and raised.value.transient
        assert waits == []

    def test_unsafe_posts_are_not_retried_after_a_drop_or_a_502(self, fake, client):
        fake.fail("POST /schemas", drop="before")
        with pytest.raises(TransportError):
            client.create_schema("people", {"properties": {}})
        fake.fail("POST /schemas", status=502)
        with pytest.raises(ServerError):
            client.create_schema("people", {"properties": {}})
        fake.fail("POST /schemas", status=503)  # no Retry-After: the gateway may have acted
        with pytest.raises(ServerError):
            client.create_schema("people", {"properties": {}})
        assert len(fake.sent("POST /schemas")) == 3

    def test_unsafe_posts_are_retried_on_429_and_503_with_retry_after(self, fake, client, waits):
        fake.fail("POST /schemas", status=429)
        fake.fail("POST /schemas", status=503, headers={"Retry-After": "0"})
        schema = client.create_schema("people", {"properties": {}})
        assert schema.name == "people"
        assert len(fake.sent("POST /schemas")) == 3
        assert len(waits) == 2 and waits[1] == 0.0

    def test_no_retries(self, fake):
        client = ForkliftClient(fake.url, TOKEN, retry=NO_RETRIES)
        fake.fail("GET /me", status=503)
        with pytest.raises(ServerError):
            client.me()
        assert len(fake.sent("GET /me")) == 1

    def test_retries_are_logged(self, fake, client, caplog):
        caplog.set_level(logging.INFO, logger="forklift_client")
        fake.fail("GET /me", status=504)
        fake.fail("GET /me", drop="before")
        client.me()
        messages = [record.getMessage() for record in caplog.records]
        assert any("GET /api/v1/me: HTTP 504; trying again" in m for m in messages)
        assert any("GET /api/v1/me: RemoteDisconnected" in m for m in messages)
        assert all(TOKEN not in m for m in messages)


class TestMe:
    def test_me(self, client):
        me = client.me()
        assert me.user.username == "ana" and me.user.role == "author"
        assert isinstance(me.user.date_joined, datetime) and me.user.date_joined.tzinfo
        assert me.scopes == ("jobs:read", "jobs:write")
        assert me.token is not None and me.token.name == "pipeline"
        assert me.to_dict()["user"]["username"] == "ana"


class TestSchemas:
    def test_create_list_get_and_versions(self, fake, client):
        created = client.create_schema(
            "people", {"properties": {"id": {"type": "integer"}}}, description="d", notes="n"
        )
        assert created.latest_version == 1 and created.description == "d"
        assert bodies(fake, "POST /schemas") == [
            {
                "name": "people",
                "document": {"properties": {"id": {"type": "integer"}}},
                "description": "d",
                "notes": "n",
            }
        ]
        assert client.get_schema(created.id) == created
        assert client.get_schema(uuid.UUID(created.id)).name == "people"
        assert client.get_schema(created).name == "people", "a resource stands for its id"
        version = client.create_schema_version(created, {"properties": {}}, notes="second")
        assert version.number == 2 and version.notes == "second"
        assert client.get_schema_version(created.id, 1).document == {
            "properties": {"id": {"type": "integer"}}
        }
        versions = client.list_schema_versions(created.id)
        assert [v.number for v in versions] == [2, 1] and versions.count == 2
        page = client.list_schemas(limit=1)
        assert isinstance(page, Page) and len(page) == 1 and page.count == 1

    def test_a_conflict(self, client):
        client.create_schema("people", {})
        with pytest.raises(Conflict, match="exists"):
            client.create_schema("people", {})

    def test_an_id_with_slashes_stays_one_path_segment(self, fake, client):
        with pytest.raises(NotFound):
            client.get_schema("a/../b")
        assert fake.sent("GET /schemas/a%2F..%2Fb")
        with pytest.raises(ValueError, match="empty"):
            client.get_schema("")

    def test_validate_schema(self, fake, client):
        job = client.validate_schema(
            upload_id="6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1",
            schema={"properties": {}},
            format="csv",
            input_options={"delimiter": ";"},
            wait_seconds=2.5,
        )
        assert job.kind == "validate_schema" and job.status == "succeeded"
        (body,) = bodies(fake, "POST /schemas/validate")
        assert body == {
            "upload_id": "6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1",
            "dataset_id": None,
            "schema": {"properties": {}},
            "schema_version_id": None,
            "format": "csv",
            "input_options": {"delimiter": ";"},
            "options": {},
            "wait_seconds": 2.5,
        }
        fake.validate_finished = False
        dataset = fake.add_dataset()
        assert client.validate_schema(dataset_id=dataset["id"]).status == "queued"


class TestCatalog:
    def test_connections(self, fake, client):
        lake = fake.add_connection("lake", "s3")
        fake.add_connection("db", "sql")
        assert [c.name for c in client.list_connections()] == ["lake", "db"]
        assert [c.name for c in client.list_connections(kind="sql")] == ["db"]
        found = client.get_connection(lake["id"])
        assert found.secret_fields == ("access_key_id", "secret_access_key")
        assert found.config == {"bucket": "lake"}

    def test_datasets_and_runs(self, fake, client):
        dataset = fake.add_dataset("people")
        assert [d.name for d in client.list_datasets()] == ["people"]
        found = client.get_dataset(dataset["id"])
        assert found.schema_version == 1
        job = client.run_dataset(found, upload_id="6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1")
        assert job.dataset_id == dataset["id"] and job.kind == "run"
        (sent,) = fake.sent("POST /datasets/{id}/run")
        assert len(sent["headers"]["idempotency-key"]) == 32
        assert json.loads(sent["body"]) == {"upload_id": "6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1"}
        again = client.run_dataset(dataset["id"], idempotency_key="nightly-2026-10-11")
        same = client.run_dataset(dataset["id"], idempotency_key="nightly-2026-10-11")
        assert again.id == same.id != job.id

    def test_schedules_and_webhooks(self, fake, client):
        schedule = fake.add_schedule(fake.add_dataset())
        assert client.get_schedule(schedule["id"]).timezone == "Europe/Berlin"
        page = client.list_schedules(enabled=True, dataset_id=schedule["dataset_id"])
        assert [s.id for s in page] == [schedule["id"]]
        assert "enabled=true" in fake.sent("GET /schedules")[0]["path"]
        hook = fake.add_webhook()
        assert [w.name for w in client.list_webhooks()] == ["alerts"]
        assert client.get_webhook(hook["id"]).secret_prefix == "fkwh_abc"


class TestPagination:
    def test_pages_and_paginate(self, fake, client):
        for _ in range(7):
            fake.add_job()
        first = client.list_jobs(limit=3)
        assert len(first) == 3 and first.count == 7 and first.has_more and first.next_offset == 3
        assert all(isinstance(job, JobSummary) for job in first)
        last = client.list_jobs(limit=3, offset=6)
        assert len(last) == 1 and not last.has_more
        everything = list(client.paginate(client.list_jobs, page_size=3))
        assert len(everything) == 7 and len({job.id for job in everything}) == 7
        offsets = [entry["path"] for entry in fake.sent("GET /jobs")][2:]
        assert [p.split("offset=")[1] for p in offsets] == ["0", "3", "6"]

    def test_an_empty_list(self, client):
        page = client.list_jobs()
        assert len(page) == 0 and not page.has_more
        assert list(client.paginate(client.list_jobs)) == []

    def test_filters(self, fake, client):
        dataset = fake.add_dataset()
        fake.add_job("failed", dataset_id=dataset["id"])
        fake.add_job("succeeded")
        failed = client.list_jobs(status="failed", kind="run", dataset_id=dataset["id"], mine=True)
        assert [job.status for job in failed] == ["failed"]
        assert "mine=true" in fake.sent("GET /jobs")[0]["path"]
        client.list_jobs()
        assert "mine" not in fake.sent("GET /jobs")[1]["path"]

    @pytest.mark.parametrize("limit, offset", [(0, 0), (501, 0), (10, -1)])
    def test_page_bounds_are_checked_before_asking(self, client, limit, offset):
        with pytest.raises(ValueError):
            client.list_jobs(limit=limit, offset=offset)
        with pytest.raises(ValueError):
            list(client.paginate(client.list_jobs, page_size=0))


class TestJobs:
    def test_submit_sends_jobin_with_an_idempotency_key(self, fake, client):
        job = client.submit_job(
            "run",
            upload_id="6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1",
            format="csv",
            schema={"properties": {}},
            input_options={"header_mode": "absent"},
            options={"bad_rows_threshold": 0.5},
            limits={"max_rows": 10, "max_seconds": 2.5},
            compression="zstd",
            classification="sensitive",
        )
        assert (
            isinstance(job, Job)
            and job.status == "queued"
            and job.spec
            == {
                "version": 1,
                "kind": "run",
            }
        )
        (sent,) = fake.sent("POST /jobs")
        key = sent["headers"]["idempotency-key"]
        assert len(key) == 32
        assert json.loads(sent["body"])["limits"] == {"max_rows": 10, "max_seconds": 2.5}

    def test_a_lost_answer_does_not_create_a_second_job(self, fake, client, caplog):
        caplog.set_level(logging.INFO, logger="forklift_client")
        fake.fail("POST /jobs", drop="after")  # the job is created, its answer is lost
        job = client.submit_job("generate_schema", upload_id=str(uuid.uuid4()), format="csv")
        assert len(fake.jobs) == 1 and job.id in fake.jobs
        keys = {entry["headers"]["idempotency-key"] for entry in fake.sent("POST /jobs")}
        assert len(fake.sent("POST /jobs")) == 2 and len(keys) == 1
        assert any("was used before" in record.getMessage() for record in caplog.records)

    def test_a_key_used_for_another_request_is_a_conflict(self, client):
        client.submit_job(
            "run", dataset_id=None, upload_id=str(uuid.uuid4()), idempotency_key="k1"
        )
        with pytest.raises(Conflict) as raised:
            client.submit_job("preview", upload_id=str(uuid.uuid4()), idempotency_key="k1")
        assert raised.value.code == "idempotency_key_reused"

    @pytest.mark.parametrize("key", ["x" * 256, "has space", "tab\tkey", "ü"])
    def test_keys_must_fit_a_header(self, client, key):
        with pytest.raises(ValueError, match="Idempotency-Key"):
            client.submit_job("run", idempotency_key=key)
        with pytest.raises(ValueError, match="Idempotency-Key"):
            client.run_dataset("d", idempotency_key=key)

    def test_ids_values_may_be_uuids_and_datetimes_are_sent_as_iso(self, fake, client):
        upload = uuid.UUID("6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1")
        stamp = datetime(2026, 10, 11, 12, 0)
        options = {"as_of": stamp, "ref": upload}
        client.submit_job("run", upload_id=upload, format="csv", options=options)
        body = bodies(fake, "POST /jobs")[0]
        assert body["upload_id"] == str(upload)
        assert body["options"] == {"as_of": "2026-10-11T12:00:00", "ref": str(upload)}
        with pytest.raises(TypeError, match="set values cannot be sent as JSON"):
            client.submit_job("run", options={"bad": {1, 2}})

    def test_get_list_events_and_cancel(self, fake, client):
        job = fake.add_job("running")
        assert client.get_job(job["id"]).status == "running"
        fake.add_job("queued")
        events = list(client.iter_job_events(job["id"]))
        assert [event.type for event in events] == ["state"]
        assert client.list_job_events(job["id"], after=events[-1].id).items == ()
        cancelled = client.cancel_job(job["id"])
        assert cancelled.cancel_requested and cancelled.status == "running"
        with pytest.raises(Conflict, match="already finished"):
            client.cancel_job(fake.add_job("succeeded")["id"])

    def test_events_are_read_with_the_after_cursor(self, fake, client):
        job = fake.add_job("running")
        for _ in range(4):
            fake._event(job, "progress", {"rows": 1})
        events = list(client.iter_job_events(job["id"], page_size=2))
        assert len(events) == 5 and [e.id for e in events] == sorted(e.id for e in events)
        paths = [entry["path"] for entry in fake.sent("GET /jobs/{id}/events")]
        assert "after" not in paths[0] and f"after={events[1].id}" in paths[1]
        assert f"after={events[3].id}" in paths[2]

    def test_a_queued_job_is_cancelled_at_once(self, fake, client):
        job = fake.add_job("queued")
        cancelled = client.cancel_job(job["id"])
        assert cancelled.status == "cancelled" and cancelled.error.code == "CANCELLED"


class TestWaitForJob:
    def test_publishing_is_not_the_end(self, fake, client, waits):
        job = fake.add_job("queued")
        fake.script(job["id"], "running", "publishing", "publishing", "succeeded")
        seen = []
        done = client.wait_for_job(
            job["id"],
            poll_interval=1,
            max_poll_interval=2,
            on_poll=lambda j: seen.append(j.status),
        )
        assert done.status == "succeeded" and done.result["counts"]["valid_rows"] == 2
        assert seen == ["running", "publishing", "publishing", "succeeded"]
        assert len(fake.sent("GET /jobs/{id}")) == 4
        assert waits == [1, 1.5, 2]

    def test_a_job_that_already_ended(self, fake, client, waits):
        job = Job.from_dict(fake.add_job("failed"))
        assert client.wait_for_job(job).status == "failed"
        assert waits == []

    def test_raise_on_failure(self, fake, client):
        job = fake.add_job("running")
        fake.script(job["id"], "failed")
        with pytest.raises(JobFailed) as raised:
            client.wait_for_job(job["id"], raise_on_failure=True, poll_interval=0.001)
        error = raised.value
        assert error.code == "BAD_ROWS_THRESHOLD_EXCEEDED" and error.job.status == "failed"
        assert str(error) == (
            f"Job {job['id']} (run) failed: BAD_ROWS_THRESHOLD_EXCEEDED: 1 of 3 rows rejected"
        )
        succeeded = fake.add_job("succeeded")
        assert client.wait_for_job(succeeded["id"], raise_on_failure=True).status == "succeeded"

    def test_a_cancelled_job_without_an_error(self, fake, client):
        job = fake.add_job("running")
        fake.script(job["id"], {"status": "cancelled", "error": None})
        with pytest.raises(JobFailed) as raised:
            client.wait_for_job(job["id"], raise_on_failure=True, poll_interval=0.001)
        assert str(raised.value) == f"Job {job['id']} (run) cancelled"
        assert raised.value.code == raised.value.message == ""

    def test_timeout(self, fake, client):
        job = fake.add_job("running")
        with pytest.raises(JobTimeout) as raised:
            client.wait_for_job(
                job["id"], timeout=0.05, poll_interval=0.01, max_poll_interval=0.02
            )
        assert isinstance(raised.value, TimeoutError)
        assert raised.value.job.status == "running" and raised.value.timeout == 0.05
        assert "still running after 0.05 seconds" in str(raised.value)
        with pytest.raises(JobTimeout):
            client.wait_for_job(job["id"], timeout=0)

    def test_transient_errors_are_waited_out(self, fake, client, caplog):
        job = fake.add_job("running")
        fake.script(job["id"], "succeeded")
        fake.fail("GET /jobs/{id}", status=503, times=3)  # beyond the client's own retries
        done = client.wait_for_job(job["id"], poll_interval=0.001)
        assert done.status == "succeeded"
        assert any("polling on" in record.getMessage() for record in caplog.records)

    def test_transient_errors_past_the_deadline_are_raised(self, fake, client):
        job = fake.add_job("running")
        fake.fail("GET /jobs/{id}", drop="before", times=50)
        client.retry = RetryPolicy(attempts=1)
        with pytest.raises(TransportError):
            client.wait_for_job(
                job["id"], timeout=0.05, poll_interval=0.01, max_poll_interval=0.01
            )

    def test_other_errors_end_the_wait(self, client):
        with pytest.raises(NotFound):
            client.wait_for_job(str(uuid.uuid4()))

    def test_no_timeout(self, fake, client, waits):
        job = fake.add_job("queued")
        fake.script(job["id"], "running", "succeeded")
        assert client.wait_for_job(job["id"], timeout=None).status == "succeeded"
        assert waits == [1.0]

    def test_bad_intervals(self, client):
        with pytest.raises(ValueError, match="poll_interval"):
            client.wait_for_job("j", poll_interval=0)
        with pytest.raises(ValueError, match="poll_interval"):
            client.wait_for_job("j", poll_interval=5, max_poll_interval=1)


class TestRefusedConnections:
    def test_a_port_nobody_listens_on(self):
        import socket

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        client = ForkliftClient(f"http://127.0.0.1:{port}", TOKEN, retry=NO_RETRIES)
        with pytest.raises(TransportError) as raised:
            client.create_schema("x", {})
        assert raised.value.sent is False and raised.value.retryable
        assert raised.value.host == f"127.0.0.1:{port}"
        assert "ConnectionRefusedError" in raised.value.reason

    def test_a_name_that_does_not_resolve(self):
        client = ForkliftClient("http://forklift.invalid", TOKEN, retry=NO_RETRIES)
        with pytest.raises(TransportError) as raised:
            client.me()
        assert raised.value.sent is False

    def test_a_timeout(self, fake):
        client = ForkliftClient(fake.url, TOKEN, timeout=0.2, retry=NO_RETRIES)
        fake.fail("GET /me", delay=1.0)
        with pytest.raises(TransportError) as raised:
            client.me()
        assert raised.value.reason == "timed out" and raised.value.sent
