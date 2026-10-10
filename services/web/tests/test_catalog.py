"""The schema registry and datasets."""

from __future__ import annotations

import pytest
from world import SCHEMA_DOCUMENT, World, make_schema, make_user, s3_connection

from forklift_web.core.choices import JobStatus, Role
from forklift_web.core.models import AuditLog, ImmutableError, Job, SchemaVersion
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Actor
from forklift_web.services import connections, datasets, installation, schemas

pytestmark = pytest.mark.django_db


@pytest.fixture
def author():
    return Actor.for_user(make_user(Role.AUTHOR))


# --------------------------------------------------------------------------- schemas


def test_schema_versions_are_numbered_immutable_and_audited(author, as_user):
    schema = schemas.create_schema(author, name="people", document=SCHEMA_DOCUMENT, notes="v1")
    assert schema.latest_version == 1
    second = schemas.create_version(author, schema.pk, document={"type": "object"}, notes="v2")
    assert second.number == 2
    with pytest.raises(Conflict, match="identical to version 2"):
        schemas.create_version(author, schema.pk, document={"type": "object"})
    with pytest.raises(ImmutableError, match="immutable; create a new version"):
        second.notes = "edited"
        second.save()
    with pytest.raises(ImmutableError, match="cannot be deleted"):
        second.delete()
    assert [v.number for v in schemas.list_versions(author, schema.pk)] == [2, 1]
    assert schemas.get_version(author, schema.pk, 1).document == SCHEMA_DOCUMENT
    assert schemas.get_schema(author, schema.pk).latest_version == 2
    assert {e.action for e in AuditLog.objects.all()} == {"schema.create", "schema.version.create"}
    listed = as_user(author.user).get("/api/v1/schemas").json()["items"]
    assert [(s["name"], s["latest_version"]) for s in listed] == [("people", 2)]
    version = as_user(author.user).get(f"/api/v1/schemas/{schema.pk}/versions/2").json()
    assert version["number"] == 2 and version["author_id"] == author.user.pk


def test_schema_document_checks(author, admin_actor):
    with pytest.raises(InvalidRequest, match="must be a JSON object"):
        schemas.create_schema(author, name="x", document=[1, 2])
    with pytest.raises(InvalidRequest, match="needs a name"):
        schemas.create_schema(author, name=" ", document={})
    installation.update(admin_actor, {"schema_max_bytes": 1024})
    with pytest.raises(InvalidRequest, match="at most 1024 \\(installation setting"):
        schemas.create_schema(author, name="big", document={"description": "x" * 2000})
    schemas.create_schema(author, name="taken", document={})
    with pytest.raises(Conflict, match="'taken' already exists"):
        schemas.create_schema(author, name="taken", document={})


def test_rename_and_describe(author):
    schema = schemas.create_schema(author, name="a", document={})
    schemas.create_schema(author, name="b", document={})
    renamed = schemas.update_schema(author, schema.pk, name="c", description="people")
    assert (renamed.name, renamed.description) == ("c", "people")
    assert AuditLog.objects.get(action="schema.update").details["name"] == {"from": "a", "to": "c"}
    schemas.update_schema(author, schema.pk, name="c")  # unchanged: nothing recorded
    assert AuditLog.objects.filter(action="schema.update").count() == 1
    with pytest.raises(Conflict, match="'b' already exists"):
        schemas.update_schema(author, schema.pk, name="b")
    with pytest.raises(InvalidRequest, match="needs a name"):
        schemas.update_schema(author, schema.pk, name="")


def test_missing_schemas_and_versions(author):
    missing = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(NotFound):
        schemas.get_schema(author, missing)
    with pytest.raises(NotFound):
        schemas.create_version(author, missing, document={})
    schema = schemas.create_schema(author, name="a", document={})
    with pytest.raises(NotFound, match="has no version 7"):
        schemas.get_version(author, schema.pk, 7)
    with pytest.raises(InvalidRequest, match="no schema version"):
        schemas.get_version_by_id(author, missing)


# --------------------------------------------------------------------------- datasets


def test_dataset_reading_uploads(author, as_user):
    version = make_schema(author.user)
    dataset = datasets.create_dataset(
        author, name="people", schema_version_id=version.pk, classification="sensitive"
    )
    assert dataset.source_connection is None and dataset.classification == "sensitive"
    body = as_user(author.user).get(f"/api/v1/datasets/{dataset.pk}").json()
    assert (body["schema_id"], body["schema_version"]) == (str(version.schema_id), 1)
    updated = datasets.update_dataset(author, dataset.pk, description="Monthly", options={"a": 1})
    assert updated.description == "Monthly"
    assert AuditLog.objects.get(action="dataset.update").details == {
        "changed": ["description", "options"]
    }
    datasets.update_dataset(author, dataset.pk, description="Monthly")
    assert AuditLog.objects.filter(action="dataset.update").count() == 1


def test_dataset_with_s3_source_and_destination(author):
    version = make_schema(author.user)
    store = s3_connection(prefix="incoming")
    dataset = datasets.create_dataset(
        author,
        name="from store",
        schema_version_id=version.pk,
        source_connection_id=store.pk,
        source_path="2026/people.csv",
        destination_connection_id=store.pk,
        destination_prefix="clean/people",
    )
    assert dataset.source_path == "2026/people.csv"
    assert dataset.destination_prefix == "clean/people"


def sql_connection(admin_actor, name="db"):
    return connections.create_connection(
        admin_actor,
        name=name,
        kind="sql",
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "p"},
    )


def test_dataset_with_sql_source_and_table_destination(author, admin_actor):
    version = make_schema(author.user)
    db = sql_connection(admin_actor)
    dataset = datasets.create_dataset(
        author,
        name="tables",
        schema_version_id=version.pk,
        input_format="sql",
        source_connection_id=db.pk,
        destination_connection_id=db.pk,
        destination_options={"table": "people_clean", "mode": "upsert", "key_columns": ["id"]},
    )
    assert dataset.destination_options["mode"] == "upsert"
    defaulted = datasets.create_dataset(
        author,
        name="appends",
        schema_version_id=version.pk,
        destination_connection_id=db.pk,
        destination_options={"table": "t"},
    )
    assert defaulted.destination_options == {"table": "t", "mode": "append"}


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"name": " "}, "needs a name"),
        ({"classification": "secret"}, "Unknown classification"),
        ({"input_format": "parquet"}, "Unknown input format"),
        ({"input_format": "sql"}, "needs a sql source connection"),
        ({"source_path": "x.csv"}, "source_path needs a source connection"),
        ({"input_options": []}, "input_options must be an object"),
        ({"options": "x"}, "options must be an object"),
        ({"destination_prefix": "out"}, "need a destination connection"),
        ({"destination_options": {"table": "t"}}, "need a destination connection"),
        ({"destination_options": []}, "destination_options must be an object"),
        ({"source": "store", "source_path": ""}, "need source_path"),
        ({"source": "store", "source_path": "dir/"}, "need source_path"),
        ({"source": "store", "source_path": "../x"}, "relative key prefix"),
        (
            {"source": "store", "source_path": "x.csv", "input_format": "sql"},
            "needs a sql source connection",
        ),
        ({"source": "db"}, "input_format must be 'sql'"),
        ({"source": "db", "input_format": "sql", "source_path": "t"}, "a sql source reads"),
        (
            {"destination": "store", "destination_options": {"table": "t"}},
            "destination_options are for sql destinations",
        ),
        ({"destination": "store", "destination_prefix": "/abs"}, "relative key prefix"),
        ({"destination": "db", "destination_prefix": "p"}, "destination_prefix is for s3"),
        (
            {"destination": "db", "destination_options": {"table": "t", "x": 1}},
            "Unknown destination_options: x",
        ),
        ({"destination": "db", "destination_options": {}}, "needs destination_options.table"),
        (
            {"destination": "db", "destination_options": {"table": "t", "mode": "merge"}},
            "mode must be one of",
        ),
        (
            {"destination": "db", "destination_options": {"table": "t", "key_columns": "id"}},
            "key_columns must be a list",
        ),
        (
            {"destination": "db", "destination_options": {"table": "t", "key_columns": [""]}},
            "key_columns must be a list",
        ),
        (
            {"destination": "db", "destination_options": {"table": "t", "mode": "upsert"}},
            "upsert' needs destination_options.key_columns",
        ),
        ({"destination": "files"}, "cannot be a dataset destination"),
    ],
)
def test_dataset_validation(author, admin_actor, fields, message, tmp_path):
    version = make_schema(author.user)
    named = {
        "store": s3_connection(),
        "db": sql_connection(admin_actor),
        "files": connections.create_connection(
            admin_actor, name="files", kind="localfs", config={"root_path": str(tmp_path)}
        ),
    }
    fields = dict(fields)
    if "source" in fields:
        fields["source_connection_id"] = named[fields.pop("source")].pk
    if "destination" in fields:
        fields["destination_connection_id"] = named[fields.pop("destination")].pk
    fields.setdefault("name", "ds")
    with pytest.raises(InvalidRequest, match=message):
        datasets.create_dataset(author, schema_version_id=version.pk, **fields)


def test_dataset_connections_must_be_allowed_for_the_role(admin_actor):
    restricted = connections.create_connection(
        admin_actor,
        name="admins only",
        kind="sql",
        allowed_roles=["admin"],
        config={"dialect": "postgresql", "host": "db", "database": "d", "username": "u"},
        secrets={"password": "p"},
    )
    author = Actor.for_user(make_user(Role.AUTHOR))
    version = make_schema(author.user)
    from forklift_web.errors import PermissionDenied

    with pytest.raises(PermissionDenied, match="not available to the author role"):
        datasets.create_dataset(
            author,
            name="x",
            schema_version_id=version.pk,
            input_format="sql",
            source_connection_id=restricted.pk,
        )


def test_dataset_update_rules_and_deletion():
    world = World.build()
    author = Actor.for_user(world.author)
    with pytest.raises(InvalidRequest, match="cannot be changed: created_by"):
        datasets.update_dataset(author, world.dataset.pk, created_by=None)
    with pytest.raises(Conflict, match=f"'{world.spare_dataset.name}' already exists"):
        datasets.update_dataset(author, world.dataset.pk, name=world.spare_dataset.name)
    with pytest.raises(Conflict, match="has jobs"):
        datasets.delete_dataset(author, world.dataset.pk)
    assert Job.objects.filter(dataset=world.dataset, status=JobStatus.SUCCEEDED).exists()
    datasets.delete_dataset(author, world.spare_dataset.pk)
    with pytest.raises(NotFound):
        datasets.get_dataset(author, world.spare_dataset.pk)
    with pytest.raises(Conflict, match="already exists"):
        datasets.create_dataset(
            author, name=world.dataset.name, schema_version_id=world.version.pk
        )
    assert SchemaVersion.objects.filter(pk=world.version.pk).exists()
