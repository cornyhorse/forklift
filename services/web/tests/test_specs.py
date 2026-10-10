"""Rendering stored specs for a worker: presigned inputs, connection strings, the contract."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest
from conftest import get_url, put_url
from django.conf import settings
from world import World, make_job, make_upload, make_user, s3_connection

from forklift_web import secret_backend, storage
from forklift_web.core.choices import Role
from forklift_web.core.models import Connection, Dataset
from forklift_web.policy import Actor
from forklift_web.services import connections, installation, jobs, specs
from forklift_web.services.jobs import JobRequest

pytestmark = pytest.mark.django_db


def render(job):
    return specs.render(job, installation.current())


def test_upload_inputs_become_presigned_urls_valid_for_the_job(admin_actor):
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner, size=4)
    put_url(
        storage.store().presign_put(upload.key, expires=60, audience=storage.Audience.GATEWAY),
        b"a,b\n",
    )
    job = make_job(owner, upload)
    spec = render(job)
    location = spec["input"]["location"]
    assert location["type"] == "presigned_url" and (location["size"], location["etag"]) == (
        4,
        "etag-1",
    )
    assert "X-Amz-Expires=1500" in location["url"]  # max_seconds 600 + the 900 s margin
    assert get_url(location["url"]) == b"a,b\n"
    assert spec["output"] == {
        "location": {"type": "file", "path": "out/"},
        "compression": "snappy",
    }
    job.spec["limits"] = {}
    assert "X-Amz-Expires=87300" in render(job)["input"]["location"]["url"]  # a day + margin
    upload.etag = ""
    upload.save()
    assert render(job)["input"]["location"]["etag"] is None


def test_input_urls_are_signed_for_the_worker_endpoint(settings):
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    settings.FORKLIFT_STORE = {
        **settings.FORKLIFT_STORE,
        "worker_endpoint_url": "http://rustfs:9000",
    }
    storage.reset_store()
    try:
        url = render(job)["input"]["location"]["url"]
        assert urlsplit(url).netloc == "rustfs:9000"
        assert storage.store().host(storage.Audience.WORKER) == "rustfs"
    finally:
        storage.reset_store()


def test_objects_in_s3_connections(s3):
    world = World.build()
    store = s3_connection(prefix="incoming")
    key = "incoming/people.csv"
    s3.put_object(Bucket=store.config["bucket"], Key=key, Body=b"id\n1\n")
    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=store, source_path="people.csv"
    )
    job, _ = jobs.run_dataset(Actor.for_user(world.operator), world.dataset.pk)
    location = render(job)["input"]["location"]
    assert location["size"] == 5 and location["etag"]
    assert get_url(location["url"]) == b"id\n1\n"
    job.spec["input"]["location"]["key"] = "incoming/missing.csv"
    with pytest.raises(specs.SpecUnavailable) as raised:
        render(job)
    assert raised.value.code == "INPUT_UNREADABLE" and "missing.csv" in raised.value.message
    Connection.objects.filter(pk=store.pk).update(secret_ciphertext="damaged")
    with pytest.raises(specs.SpecUnavailable, match="could not be decrypted"):
        render(job)
    Connection.objects.filter(pk=store.pk).update(
        config={**store.config, "endpoint_url": "http://127.0.0.1:9"},
        secret_ciphertext=store.secret_ciphertext,
    )
    with pytest.raises(specs.SpecUnavailable, match="refused or failed HEAD"):
        render(job)
    job.spec["input"]["location"]["connection_id"] = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(specs.SpecUnavailable, match="was deleted"):
        render(job)


def test_sql_inputs_and_table_outputs_get_their_connection_string(admin_actor):
    world = World.build()
    db = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "s3cret"},
    )
    Dataset.objects.filter(pk=world.dataset.pk).update(
        source_connection=db,
        input_format="sql",
        destination_connection=db,
        destination_options={
            "table": "t",
            "mode": "upsert",
            "key_columns": ["id"],
            "schema_name": "clean",
        },
    )
    job, _ = jobs.run_dataset(Actor.for_user(world.operator), world.dataset.pk)
    assert "s3cret" not in str(job.spec)  # never stored
    spec = render(job)
    string = connections.sql_connection_string(db)
    assert spec["input"]["location"] == {"type": "sql", "connection_string": string}
    assert spec["output"]["location"] == {
        "type": "sql_table",
        "connection_string": string,
        "table": "t",
        "mode": "upsert",
        "schema_name": "clean",
        "key_columns": ["id"],
    }
    assert spec["output"]["artifacts"] == {"type": "file", "path": "out/"}
    Connection.objects.filter(pk=db.pk).update(secret_ciphertext="damaged")
    with pytest.raises(specs.SpecUnavailable) as raised:
        render(job)
    assert raised.value.code == "INTERNAL"


def test_non_csv_inputs_are_checked_as_the_supervisor_will_stage_them(admin_actor):
    owner = make_user(Role.OPERATOR)
    upload = make_upload(owner, size=50)
    job, _ = jobs.create_job(
        Actor.for_user(owner),
        JobRequest(kind="generate_schema", upload_id=upload.pk, format="excel"),
    )
    spec = render(job)
    assert spec["input"]["location"]["type"] == "presigned_url"  # the supervisor stages it
    installation.update(admin_actor, {"stage_max_bytes": 10})
    with pytest.raises(specs.SpecUnavailable) as raised:
        render(job)
    assert (
        raised.value.code == "LIMIT_EXCEEDED" and "Only CSV inputs can be" in raised.value.message
    )


def test_specs_that_break_the_contract_are_refused():
    owner = make_user(Role.OPERATOR)
    job = make_job(owner, make_upload(owner))
    job.spec["options"] = {"no_such_option": True}
    with pytest.raises(specs.SpecUnavailable) as raised:
        render(job)
    assert raised.value.code == "SPEC_INVALID"
    assert "Additional properties are not allowed ('no_such_option'" in raised.value.message


def test_secret_errors_never_contain_the_ciphertext(admin_actor):
    db = connections.create_connection(
        admin_actor,
        name="db",
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "pw"},
    )
    Connection.objects.filter(pk=db.pk).update(secret_ciphertext="gAAAAAdamaged")
    with pytest.raises(secret_backend.SecretError) as raised:
        connections.secrets_of(Connection.objects.get(pk=db.pk))
    assert "gAAAAA" not in str(raised.value)
    assert settings.FORKLIFT_SECRET_BACKEND == "env"
