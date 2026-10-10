"""The schema registry: named schemas with immutable, numbered versions.

The gateway stores schema documents and checks only their shape and size: it never interprets
them (no expressions, no regular expressions). Checking a schema against data is a
``validate_schema`` job on a worker (:func:`forklift_web.services.jobs.validate_schema`).
:func:`review_document` lists, for the editor, what saving refuses and the shape mistakes the
engine refuses when it loads a schema, each at its place in the document.
"""

from __future__ import annotations

import hashlib
import json
from typing import Optional

from django.db import IntegrityError, transaction
from django.db.models import Max
from django.utils import timezone

from forklift_web.core.models import Schema, SchemaVersion
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, installation


def canonical_json(document) -> bytes:
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode()


def check_document(document) -> str:
    """Raise unless ``document`` can be stored as a schema; returns its SHA-256."""
    if not isinstance(document, dict):
        raise InvalidRequest("A schema document must be a JSON object.")
    encoded = canonical_json(document)
    limit = installation.get("schema_max_bytes")
    if len(encoded) > limit:
        raise InvalidRequest(
            f"The schema document is {len(encoded)} bytes as compact JSON; this installation "
            f"accepts at most {limit} (installation setting schema_max_bytes)."
        )
    return hashlib.sha256(encoded).hexdigest()


def list_schemas(actor: Actor):
    check(actor, Action.SCHEMA_VIEW)
    return Schema.objects.annotate(latest_version=Max("versions__number"))


def get_schema(actor: Actor, schema_id) -> Schema:
    check(actor, Action.SCHEMA_VIEW)
    schema = Schema.objects.annotate(latest_version=Max("versions__number")).filter(pk=schema_id)
    found = schema.first()
    if found is None:
        raise NotFound(f"There is no schema with id {schema_id}.")
    return found


def create_schema(
    actor: Actor, *, name: str, document: dict, description: str = "", notes: str = ""
) -> Schema:
    """A new schema whose version 1 is ``document``."""
    check(actor, Action.SCHEMA_EDIT)
    if not name.strip():
        raise InvalidRequest("A schema needs a name.")
    digest = check_document(document)
    try:
        with transaction.atomic():
            schema = Schema.objects.create(
                name=name, description=description, created_by=actor.user
            )
            version = SchemaVersion.objects.create(
                schema=schema,
                number=1,
                document=document,
                sha256=digest,
                author=actor.user,
                notes=notes,
            )
            audit.record(actor, "schema.create", schema, {"version": 1, "sha256": digest})
    except IntegrityError:
        raise Conflict(f"A schema named {name!r} already exists.") from None
    schema.latest_version = version.number
    return schema


def update_schema(
    actor: Actor, schema_id, *, name: Optional[str] = None, description: Optional[str] = None
) -> Schema:
    """Rename a schema or change its description (documents change through new versions)."""
    check(actor, Action.SCHEMA_EDIT)
    schema = get_schema(actor, schema_id)
    changed = {}
    if name is not None and name != schema.name:
        if not name.strip():
            raise InvalidRequest("A schema needs a name.")
        changed["name"] = {"from": schema.name, "to": name}
        schema.name = name
    if description is not None and description != schema.description:
        changed["description"] = True
        schema.description = description
    if changed:
        schema.updated_at = timezone.now()
        try:
            with transaction.atomic():
                schema.save(update_fields=["name", "description", "updated_at"])
        except IntegrityError:
            raise Conflict(f"A schema named {name!r} already exists.") from None
        audit.record(actor, "schema.update", schema, changed)
    return schema


def list_versions(actor: Actor, schema_id):
    schema = get_schema(actor, schema_id)
    return SchemaVersion.objects.filter(schema=schema).select_related("schema", "author")


def list_all_versions(actor: Actor):
    """Every version of every schema, by schema name and newest first (for pickers)."""
    check(actor, Action.SCHEMA_VIEW)
    return SchemaVersion.objects.select_related("schema").order_by("schema__name", "-number")


def get_version(actor: Actor, schema_id, number: int) -> SchemaVersion:
    check(actor, Action.SCHEMA_VIEW)
    version = (
        SchemaVersion.objects.select_related("schema", "author")
        .filter(schema_id=schema_id, number=number)
        .first()
    )
    if version is None:
        raise NotFound(f"Schema {schema_id} has no version {number}.")
    return version


def get_version_by_id(actor: Actor, version_id) -> SchemaVersion:
    check(actor, Action.SCHEMA_VIEW)
    version = SchemaVersion.objects.select_related("schema").filter(pk=version_id).first()
    if version is None:
        raise InvalidRequest(f"There is no schema version with id {version_id}.")
    return version


def create_version(actor: Actor, schema_id, *, document: dict, notes: str = "") -> SchemaVersion:
    """The next version of a schema. A document identical to the latest version is refused."""
    check(actor, Action.SCHEMA_EDIT)
    digest = check_document(document)
    with transaction.atomic():
        schema = Schema.objects.select_for_update().filter(pk=schema_id).first()
        if schema is None:
            raise NotFound(f"There is no schema with id {schema_id}.")
        latest = schema.versions.order_by("-number").first()
        if latest.sha256 == digest:
            raise Conflict(
                f"The document is identical to version {latest.number} of {schema.name!r}; "
                "nothing to save."
            )
        version = SchemaVersion.objects.create(
            schema=schema,
            number=latest.number + 1,
            document=document,
            sha256=digest,
            author=actor.user,
            notes=notes,
        )
        Schema.objects.filter(pk=schema.pk).update(updated_at=timezone.now())
        audit.record(
            actor, "schema.version.create", schema, {"version": version.number, "sha256": digest}
        )
    return version


# --------------------------------------------------------------------------- reviewing drafts

DIALECT = "https://json-schema.org/draft/2020-12/schema"
ID_PREFIX = "https://github.com/cornyhorse/forklift/schema-standards/"
JSON_TYPES = ("string", "integer", "number", "boolean", "array", "object", "null")
# Keys of x-calculatedColumns whose items add columns the other extensions may refer to
CALCULATED = ("constants", "expressions", "calculated")


def _problem(path: list, message: str, *, blocking: bool = False) -> dict:
    return {"path": path, "message": message, "blocking": blocking}


def review_document(document) -> list:
    """The problems of a draft schema document, each with its ``path`` (keys and list indexes
    from the root) and whether saving refuses it (``blocking``).

    A document saving refuses gets that one problem. Otherwise the problems are the mistakes in
    shape the engine refuses when it loads the schema (the JSON Schema keys it needs, column
    types, bounds, and ``required`` and key columns that name no column), so that they show
    while the schema is written rather than when a job fails. Like saving, this never runs a
    regular expression or an expression from the document."""
    try:
        check_document(document)
    except InvalidRequest as error:
        return [_problem([], error.message, blocking=True)]
    problems = _root_problems(document)
    columns = document.get("properties")
    if not isinstance(columns, dict):
        message = 'The engine needs "properties": an object with one entry per column.'
        return problems + [_problem(_place(document, "properties"), message)]
    for name, definition in columns.items():
        problems += _column_problems(name, definition)
    problems += _name_list(document.get("required"), ["required"], set(columns), "required")
    if "x-columnMapping" not in document:  # mapped names are output names: not checked here
        problems += _key_problems(document, set(columns) | _calculated_names(document))
    return problems


def _root_problems(document: dict) -> list:
    schema_id = document.get("$id")
    needs = {
        "$schema": (
            document.get("$schema") == DIALECT,
            f'The engine reads JSON Schema 2020-12: "$schema": "{DIALECT}".',
        ),
        "$id": (
            isinstance(schema_id, str) and schema_id.startswith(ID_PREFIX),
            f'The engine needs an "$id" under {ID_PREFIX}, such as {ID_PREFIX}people.json.',
        ),
        "title": (bool(document.get("title")), 'The engine needs a "title" for the schema.'),
        "type": (
            document.get("type") == "object",
            'The engine needs "type": "object" at the top level.',
        ),
    }
    return [_problem(_place(document, key), text) for key, (ok, text) in needs.items() if not ok]


def _place(document: dict, key: str) -> list:
    """The path of ``key``, or of the document when the key is missing."""
    return [key] if key in document else []


def _column_problems(name: str, definition) -> list:
    where = ["properties", name]
    if not isinstance(definition, dict):
        return [
            _problem(where, f'Column {name!r} must be an object, such as {{"type": "string"}}.')
        ]
    if "anyOf" in definition or "oneOf" in definition:
        return []  # a union: the engine checks its branches
    declared = definition.get("type")
    if declared is None:
        return [
            _problem(where, f'Column {name!r} has no type; give one, such as "type": "string".')
        ]
    types = declared if isinstance(declared, list) else [declared]
    wrong = [item for item in types if item not in JSON_TYPES]
    if wrong:
        message = (
            f"Column {name!r}: {json.dumps(wrong[0])} is not a type; use string, integer, number, "
            'boolean, array or object, or a list such as ["string", "null"] for a nullable column.'
        )
        return [_problem(where + ["type"], message)]
    if not set(types) - {"null"}:
        message = f'Column {name!r} needs a type besides null, such as ["string", "null"].'
        return [_problem(where + ["type"], message)]
    problems = []
    if {"integer", "number"} & set(types):
        problems += _bounds(where, definition, "minimum", "maximum", whole=False)
    if "string" in types:
        problems += _bounds(where, definition, "minLength", "maxLength", whole=True)
    return problems


def _bounds(where: list, definition: dict, low: str, high: str, *, whole: bool) -> list:
    problems, values = [], {}
    for key in (low, high):
        value = definition.get(key)
        if value is None:
            continue
        if (isinstance(value, int) and value >= 0) if whole else isinstance(value, (int, float)):
            values[key] = value
        else:
            kind = "a whole number, 0 or more" if whole else "a number"
            problems.append(_problem(where + [key], f"{key} must be {kind}."))
    if len(values) == 2 and values[low] > values[high]:
        message = f"{low} ({values[low]}) is larger than {high} ({values[high]})."
        problems.append(_problem(where + [low], message))
    return problems


def _name_list(value, where: list, known: set, label: str) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        return [_problem(where, f"{label} must be a list of column names.")]
    problems = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            problems.append(_problem(where + [index], f"{label} lists column names (strings)."))
        elif item not in known:
            message = f"{label} names {item!r}, which is not a column of this schema."
            problems.append(_problem(where + [index], message))
    return problems


def _calculated_names(document: dict) -> set:
    section = document.get("x-calculatedColumns")
    names = set()
    for key in CALCULATED if isinstance(section, dict) else ():
        items = section.get(key)
        for item in items if isinstance(items, list) else ():
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                names.add(item["name"])
    return names


def _key_problems(document: dict, known: set) -> list:
    problems = []
    primary = document.get("x-primaryKey")
    if isinstance(primary, dict):
        where = ["x-primaryKey", "columns"]
        problems += _name_list(primary.get("columns"), where, known, "x-primaryKey.columns")
    unique = document.get("x-uniqueConstraints")
    for index, constraint in enumerate(unique if isinstance(unique, list) else ()):
        if isinstance(constraint, dict):
            where = ["x-uniqueConstraints", index, "columns"]
            label = f"x-uniqueConstraints[{index}].columns"
            problems += _name_list(constraint.get("columns"), where, known, label)
    return problems
