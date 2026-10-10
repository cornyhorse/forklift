"""The schema registry: named schemas with immutable, numbered versions.

The gateway stores schema documents and checks only their shape and size: it never interprets
them (no expressions, no regular expressions). Checking a schema against data is a
``validate_schema`` job on a worker (:func:`forklift_web.services.jobs.validate_schema`).
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
