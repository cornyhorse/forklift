"""``POST /internal/v1/jobs/{id}/input-url``: a fresh URL for a streamed input (ADR 0006).

A worker asks when the engine's URL is about to expire or the store refused it. The answer is
the input signed again, for as long as a URL signed at lease time, and only for inputs that can
be streamed; an input that is gone answers 410 (409 would tell the worker its lease is lost).
"""

from __future__ import annotations

import uuid
from urllib.parse import parse_qs, urlsplit

import pytest
from conftest import get_url, put_url
from world import World, make_job, make_upload, make_user, s3_connection

from forklift_web import storage
from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.models import Dataset, Job
from forklift_web.errors import Gone, InvalidRequest
from forklift_web.policy import Actor
from forklift_web.services import installation, jobs, queue, specs
from forklift_web.services.workers import WorkerPrincipal

pytestmark = pytest.mark.django_db


@pytest.fixture
def principal(worker_token):
    token, _ = worker_token
    return WorkerPrincipal(token=token)


LANES = {"lanes": ["batch"], "spec_versions": [1]}


def lease(principal):
    return queue.lease(principal, worker_id="w-1", **LANES)


def lifetime(url: str) -> int:
    return int(parse_qs(urlsplit(url).query)["X-Amz-Expires"][0])


def stored_job(data: bytes = b"a,b\n"):
    """A queued run of an upload that is in the store."""
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner, size=len(data))
    put_url(
        storage.store().presign_put(upload.key, expires=60, audience=storage.Audience.GATEWAY),
        data,
    )
    return make_job(owner, upload)


def test_a_fresh_url_names_the_same_object_for_as_long_as_the_leased_one(principal):
    job = stored_job()
    leased = lease(principal).spec["input"]["location"]

    fresh = queue.refresh_input(principal, job.pk, attempt=1)

    assert fresh == {**leased, "url": fresh["url"]}  # presigned_url, the same size and etag
    assert urlsplit(fresh["url"])[:3] == urlsplit(leased["url"])[:3]
    assert lifetime(fresh["url"]) == lifetime(leased["url"]) == 1500  # max_seconds + margin
    assert get_url(fresh["url"]) == b"a,b\n"


@pytest.mark.parametrize(
    "limits, seconds",
    [({}, 24 * 3600 + 900), ({"max_seconds": 30 * 24 * 3600}, storage.MAX_PRESIGN_SECONDS)],
)
def test_the_lifetime_is_the_jobs_limit_plus_the_margin_up_to_seven_days(
    principal, limits, seconds
):
    job = stored_job()
    job.spec["limits"] = limits
    job.save()
    leased = lease(principal).spec["input"]["location"]
    fresh = queue.refresh_input(principal, job.pk, attempt=1)
    assert lifetime(fresh["url"]) == lifetime(leased["url"]) == seconds


def test_the_margin_set_now_applies(principal, admin_actor):
    job = stored_job()
    lease(principal)
    installation.update(admin_actor, {"input_url_margin_seconds": 60})
    assert lifetime(queue.refresh_input(principal, job.pk, attempt=1)["url"]) == 660


def test_objects_in_an_s3_connection_are_signed_again(principal, s3):
    world = World.build()
    store = s3_connection(prefix="incoming")
    s3.put_object(Bucket=store.config["bucket"], Key="incoming/people.csv", Body=b"id\n1\n")
    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=store, source_path="people.csv"
    )
    job, _ = jobs.run_dataset(Actor.for_user(world.operator), world.dataset.pk)
    Job.objects.exclude(pk=job.pk).update(status=JobStatus.CANCELLED)  # the world's other jobs
    assert lease(principal).job == job

    fresh = queue.refresh_input(principal, job.pk, attempt=1)
    assert (fresh["type"], fresh["size"]) == ("presigned_url", 5) and fresh["etag"]
    assert get_url(fresh["url"]) == b"id\n1\n"


@pytest.mark.parametrize(
    "input_format, location",
    [
        ("sql", {"type": specs.SQL, "connection_id": str(uuid.uuid4())}),
        ("excel", None),  # an upload, but only CSV inputs are streamed
    ],
)
def test_only_streamed_inputs_have_a_url_to_refresh(principal, input_format, location):
    stored_job()
    job = lease(principal).job
    job.spec["input"]["format"] = input_format
    job.spec["input"]["location"] = location or job.spec["input"]["location"]
    job.save()

    with pytest.raises(InvalidRequest) as raised:
        queue.refresh_input(principal, job.pk, attempt=1)
    assert raised.value.code == "not_streamed"
    assert f"The input of job {job.id} is not streamed" in raised.value.message


def test_an_input_that_is_gone_answers_410(principal):
    stored_job()
    job = lease(principal).job
    job.upload.status = "deleted"
    job.upload.save()

    with pytest.raises(Gone) as raised:
        queue.refresh_input(principal, job.pk, attempt=1)
    assert raised.value.status == 410 and raised.value.code == "input_unreadable"
    assert "no longer available" in raised.value.message


def test_an_output_that_cannot_be_used_now_does_not_stand_in_the_way(principal):
    stored_job()
    job = lease(principal).job
    job.spec["output"]["location"] = {
        "type": specs.SQL_TABLE,
        "connection_id": str(uuid.uuid4()),  # deleted while the job runs
        "table": "t",
        "mode": "create",
    }
    job.save()
    assert queue.refresh_input(principal, job.pk, attempt=1)["type"] == "presigned_url"


@pytest.fixture
def worker(worker_token, internal):
    _, raw = worker_token
    return internal(raw)


def test_over_http(worker):
    job = stored_job()
    leased = worker.post("/internal/v1/leases", {"worker_id": "w", **LANES})
    assert leased.json()["job_id"] == str(job.id)
    path = f"/internal/v1/jobs/{job.id}/input-url"

    fresh = worker.post(path, {"attempt": 1})
    assert fresh.status_code == 200 and fresh.json()["location"]["type"] == "presigned_url"
    lost = worker.post(path, {"attempt": 2})
    assert lost.status_code == 409 and lost.json()["code"] == "lease_lost"
    job.upload.status = "deleted"
    job.upload.save()
    gone = worker.post(path, {"attempt": 1})
    assert gone.status_code == 410 and gone.json()["code"] == "input_unreadable"
    Job.objects.filter(pk=job.pk).update(
        spec={**job.spec, "input": {**job.spec["input"], "format": "excel"}}
    )
    staged = worker.post(path, {"attempt": 1})
    assert staged.status_code == 400 and staged.json()["code"] == "not_streamed"
