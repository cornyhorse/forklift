"""Datasets: a source, a schema version and a destination that people run and permission.

Sources: uploads (each run names one), an object in an ``s3`` connection, or a ``sql``
connection (tables from the schema's ``x-sql``). Destinations: none (the job's artifacts only),
an ``s3`` connection prefix (published manifest-last when a run succeeds) or a ``sql``
connection table. ``localfs`` connections cannot be dataset sources or destinations yet: job
contract v1 has no location for a directory mounted into the worker.
"""

from __future__ import annotations

from typing import Optional

from django.db import IntegrityError, transaction
from django.db.models import ProtectedError
from django.utils import timezone

from forklift_web.core.choices import Classification, ConnectionKind, InputFormat
from forklift_web.core.models import Dataset
from forklift_web.errors import Conflict, InvalidRequest, NotFound
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, connections, schemas

TABLE_MODES = ("create", "append", "replace", "upsert")
EDITABLE = (
    "name",
    "description",
    "classification",
    "input_format",
    "input_options",
    "source_connection_id",
    "source_path",
    "schema_version_id",
    "destination_connection_id",
    "destination_prefix",
    "destination_options",
    "compression",
    "options",
)


def _object(value, what: str) -> dict:
    if not isinstance(value, dict):
        raise InvalidRequest(f"{what} must be an object.")
    return value


def _source(actor: Actor, values: dict) -> None:
    fmt = values["input_format"]
    if fmt not in InputFormat.values:
        raise InvalidRequest(
            f"Unknown input format {fmt!r}; formats: {', '.join(InputFormat.values)}."
        )
    connection_id = values["source_connection_id"]
    if connection_id is None:
        if fmt == InputFormat.SQL:
            raise InvalidRequest("A sql dataset needs a sql source connection.")
        if values["source_path"]:
            raise InvalidRequest("source_path needs a source connection (an s3 connection).")
        return
    connection = connections.usable_connection(
        actor, connection_id, kinds={ConnectionKind.S3, ConnectionKind.SQL}, purpose="source"
    )
    if connection.kind == ConnectionKind.SQL:
        if fmt != InputFormat.SQL:
            raise InvalidRequest(
                f"The source connection {connection.name!r} is a database: input_format must "
                "be 'sql' (the tables come from the schema's x-sql)."
            )
        if values["source_path"]:
            raise InvalidRequest(
                "source_path is for objects in s3 connections; a sql source reads the tables "
                "named in the schema's x-sql."
            )
        return
    if fmt == InputFormat.SQL:
        raise InvalidRequest("input_format 'sql' needs a sql source connection.")
    path = values["source_path"]
    if not path or path.endswith("/"):
        raise InvalidRequest(
            f"Datasets reading from the s3 connection {connection.name!r} need source_path: "
            "the key of the object, relative to the connection's prefix."
        )
    connections.relative_prefix(path, "source_path")


def _destination(actor: Actor, values: dict) -> None:
    connection_id = values["destination_connection_id"]
    options = _object(values["destination_options"], "destination_options")
    if connection_id is None:
        if values["destination_prefix"] or options:
            raise InvalidRequest(
                "destination_prefix and destination_options need a destination connection."
            )
        return
    connection = connections.usable_connection(
        actor,
        connection_id,
        kinds={ConnectionKind.S3, ConnectionKind.SQL},
        purpose="destination",
    )
    if connection.kind == ConnectionKind.S3:
        if options:
            raise InvalidRequest("destination_options are for sql destinations.")
        connections.relative_prefix(values["destination_prefix"], "destination_prefix")
        return
    if values["destination_prefix"]:
        raise InvalidRequest("destination_prefix is for s3 destinations; sql ones name a table.")
    unknown = sorted(set(options) - {"table", "schema_name", "mode", "key_columns"})
    if unknown:
        raise InvalidRequest(
            f"Unknown destination_options: {', '.join(unknown)} (known: table, schema_name, "
            "mode, key_columns)."
        )
    if not isinstance(options.get("table"), str) or not options["table"]:
        raise InvalidRequest("A sql destination needs destination_options.table.")
    mode = options.setdefault("mode", "append")
    if mode not in TABLE_MODES:
        raise InvalidRequest(f"destination_options.mode must be one of: {', '.join(TABLE_MODES)}.")
    keys = options.get("key_columns")
    if keys is not None and (
        not isinstance(keys, list) or not all(isinstance(key, str) and key for key in keys)
    ):
        raise InvalidRequest("destination_options.key_columns must be a list of column names.")
    if mode == "upsert" and not keys:
        raise InvalidRequest("mode 'upsert' needs destination_options.key_columns.")


def _validate(actor: Actor, values: dict) -> dict:
    if not values["name"].strip():
        raise InvalidRequest("A dataset needs a name.")
    if values["classification"] not in Classification.values:
        raise InvalidRequest(
            f"Unknown classification {values['classification']!r}; classifications: "
            f"{', '.join(Classification.values)}."
        )
    _object(values["input_options"], "input_options")
    _object(values["options"], "options")
    schemas.get_version_by_id(actor, values["schema_version_id"])
    _source(actor, values)
    _destination(actor, values)
    return values


def list_datasets(actor: Actor):
    check(actor, Action.DATASET_VIEW)
    return Dataset.objects.select_related(
        "schema_version__schema", "source_connection", "destination_connection"
    )


def get_dataset(actor: Actor, dataset_id) -> Dataset:
    dataset = list_datasets(actor).filter(pk=dataset_id).first()
    if dataset is None:
        raise NotFound(f"There is no dataset with id {dataset_id}.")
    return dataset


def create_dataset(
    actor: Actor,
    *,
    name: str,
    schema_version_id,
    classification: str = Classification.INTERNAL,
    description: str = "",
    input_format: str = InputFormat.CSV,
    input_options: Optional[dict] = None,
    source_connection_id=None,
    source_path: str = "",
    destination_connection_id=None,
    destination_prefix: str = "",
    destination_options: Optional[dict] = None,
    compression: str = "snappy",
    options: Optional[dict] = None,
) -> Dataset:
    check(actor, Action.DATASET_EDIT)
    values = _validate(
        actor,
        {
            "name": name,
            "description": description,
            "classification": classification,
            "input_format": input_format,
            "input_options": input_options if input_options is not None else {},
            "source_connection_id": source_connection_id,
            "source_path": source_path,
            "schema_version_id": schema_version_id,
            "destination_connection_id": destination_connection_id,
            "destination_prefix": destination_prefix,
            "destination_options": destination_options if destination_options is not None else {},
            "compression": compression,
            "options": options if options is not None else {},
        },
    )
    try:
        with transaction.atomic():
            dataset = Dataset.objects.create(created_by=actor.user, **values)
            audit.record(
                actor,
                "dataset.create",
                dataset,
                {"classification": classification, "schema_version_id": schema_version_id},
            )
    except IntegrityError:
        raise Conflict(f"A dataset named {name!r} already exists.") from None
    return get_dataset(actor, dataset.pk)


def update_dataset(actor: Actor, dataset_id, **changes) -> Dataset:
    """Change any of ``EDITABLE``; the result is validated as a whole."""
    check(actor, Action.DATASET_EDIT)
    unknown = sorted(set(changes) - set(EDITABLE))
    if unknown:
        raise InvalidRequest(
            f"These dataset fields cannot be changed: {', '.join(unknown)} (changeable: "
            f"{', '.join(EDITABLE)})."
        )
    dataset = get_dataset(actor, dataset_id)
    values = {field: getattr(dataset, field) for field in EDITABLE}
    values.update(changes)
    _validate(actor, values)
    changed = sorted(field for field in EDITABLE if getattr(dataset, field) != values[field])
    if changed:
        for field in changed:
            setattr(dataset, field, values[field])
        dataset.updated_at = timezone.now()
        try:
            with transaction.atomic():
                dataset.save()
        except IntegrityError:
            raise Conflict(f"A dataset named {values['name']!r} already exists.") from None
        audit.record(actor, "dataset.update", dataset, {"changed": changed})
    return get_dataset(actor, dataset.pk)


def delete_dataset(actor: Actor, dataset_id) -> None:
    check(actor, Action.DATASET_EDIT)
    dataset = get_dataset(actor, dataset_id)
    try:
        with transaction.atomic():
            audit.record(actor, "dataset.delete", dataset)
            dataset.delete()
    except ProtectedError:
        raise Conflict(
            f"Dataset {dataset.name!r} has jobs; datasets with history cannot be deleted."
        ) from None
