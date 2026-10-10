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


REVIEWED = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://github.com/cornyhorse/forklift/schema-standards/people.json",
    "title": "People",
    "type": "object",
    "properties": {
        "id": {"type": "integer", "minimum": 1, "maximum": 9.5},
        "name": {"type": ["string", "null"], "minLength": 0, "maxLength": 9},
        "flag": {"type": "boolean", "minimum": "ignored: not a number column"},
        "either": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
    },
    "required": ["id"],
    "x-primaryKey": {"columns": ["id", "load_date"]},
    "x-uniqueConstraints": ["ignored: not an object", {"columns": ["name", "full_name"]}],
    "x-calculatedColumns": {
        "constants": [{"name": "load_date"}, "ignored"],
        "expressions": [{"name": "full_name"}, {"name": 5}],
        "calculated": "ignored: not a list",
    },
}


def reviewed(**changes) -> list:
    document = {**REVIEWED, **changes}
    return [
        (problem["path"], problem["message"], problem["blocking"])
        for problem in schemas.review_document({k: v for k, v in document.items() if v != ...})
    ]


def test_a_draft_saving_refuses_has_that_one_problem(admin_actor):
    assert schemas.review_document([1]) == [
        {"path": [], "message": "A schema document must be a JSON object.", "blocking": True}
    ]
    installation.update(admin_actor, {"schema_max_bytes": 1024})
    [(path, message, blocking)] = reviewed(description="x" * 2000)
    assert (path, blocking) == ([], True) and "accepts at most 1024" in message


def test_a_draft_the_engine_would_load_has_no_problems():
    assert reviewed() == []
    # Mapped names are output names, which the gateway does not work out: not checked
    assert reviewed(**{"x-primaryKey": {"columns": ["Name"]}, "x-columnMapping": {}}) == []
    assert reviewed(**{"x-primaryKey": ["not", "an", "object"], "x-uniqueConstraints": {}}) == []
    no_calculated = {"x-calculatedColumns": "?", "x-primaryKey": ..., "x-uniqueConstraints": ...}
    assert reviewed(**no_calculated) == []


def test_the_review_names_the_keys_the_engine_needs():
    missing = reviewed(
        **{"$schema": ..., "$id": ..., "title": ..., "type": ..., "properties": ...}
    )
    assert [path for path, _, _ in missing] == [[], [], [], [], []]
    assert [message for _, message, _ in missing] == [
        'The engine reads JSON Schema 2020-12: "$schema": '
        '"https://json-schema.org/draft/2020-12/schema".',
        'The engine needs an "$id" under https://github.com/cornyhorse/forklift/schema-standards/'
        ", such as https://github.com/cornyhorse/forklift/schema-standards/people.json.",
        'The engine needs a "title" for the schema.',
        'The engine needs "type": "object" at the top level.',
        'The engine needs "properties": an object with one entry per column.',
    ]
    wrong = reviewed(
        **{"$schema": "draft-07", "$id": 7, "title": "", "type": "array", "properties": []}
    )
    assert [path for path, _, blocking in wrong if not blocking] == [
        ["$schema"],
        ["$id"],
        ["title"],
        ["type"],
        ["properties"],
    ]
    assert reviewed(**{"$id": "https://example.org/people.json"})[0][0] == ["$id"]


NOT_A_TYPE = (
    " is not a type; use string, integer, number, boolean, array or object, or a list such as "
    '["string", "null"] for a nullable column.'
)


def test_the_review_checks_each_column_where_it_is():
    columns = {
        "text": "string",
        "untyped": {"description": "no type"},
        "typo": {"type": "strin"},
        "odd": {"type": [{"type": "string"}]},
        "nothing": {"type": ["null"]},
        "low": {"type": "integer", "minimum": "1", "maximum": 5},
        "upside": {"type": "number", "minimum": 5, "maximum": 1.5},
        "lengths": {"type": "string", "minLength": -1, "maxLength": 2.0},
        "short": {"type": "string", "minLength": 5, "maxLength": 2},
    }
    problems = reviewed(
        properties=columns, required=[], **{"x-primaryKey": ..., "x-uniqueConstraints": ...}
    )
    assert [(path, message) for path, message, _ in problems] == [
        (["properties", "text"], 'Column \'text\' must be an object, such as {"type": "string"}.'),
        (
            ["properties", "untyped"],
            'Column \'untyped\' has no type; give one, such as "type": "string".',
        ),
        (["properties", "typo", "type"], "Column 'typo': \"strin\"" + NOT_A_TYPE),
        (["properties", "odd", "type"], 'Column \'odd\': {"type": "string"}' + NOT_A_TYPE),
        (
            ["properties", "nothing", "type"],
            'Column \'nothing\' needs a type besides null, such as ["string", "null"].',
        ),
        (["properties", "low", "minimum"], "minimum must be a number."),
        (["properties", "upside", "minimum"], "minimum (5) is larger than maximum (1.5)."),
        (["properties", "lengths", "minLength"], "minLength must be a whole number, 0 or more."),
        (["properties", "lengths", "maxLength"], "maxLength must be a whole number, 0 or more."),
        (["properties", "short", "minLength"], "minLength (5) is larger than maxLength (2)."),
    ]
    assert {blocking for _, _, blocking in problems} == {False}  # saving still takes it


def test_the_review_checks_the_column_names_lists_refer_to():
    assert reviewed(required="id") == [
        (["required"], "required must be a list of column names.", False)
    ]
    assert reviewed(required=["id", 3, "nmae"]) == [
        (["required", 1], "required lists column names (strings).", False),
        (["required", 2], "required names 'nmae', which is not a column of this schema.", False),
    ]
    keys = reviewed(
        **{
            "x-primaryKey": {"columns": ["idd"]},
            "x-uniqueConstraints": [{"columns": "name"}, {"columns": ["name", "nme"]}],
        }
    )
    assert keys == [
        (
            ["x-primaryKey", "columns", 0],
            "x-primaryKey.columns names 'idd', which is not a column of this schema.",
            False,
        ),
        (
            ["x-uniqueConstraints", 0, "columns"],
            "x-uniqueConstraints[0].columns must be a list of column names.",
            False,
        ),
        (
            ["x-uniqueConstraints", 1, "columns", 1],
            "x-uniqueConstraints[1].columns names 'nme', which is not a column of this schema.",
            False,
        ),
    ]


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
