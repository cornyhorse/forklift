"""/api/v1/schemas, /api/v1/datasets and /api/v1/connections."""

import uuid
from typing import Optional

from ninja import Header, Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import (
    ConnectionIn,
    ConnectionOut,
    ConnectionPatch,
    ConnectionTestOut,
    DatasetIn,
    DatasetOut,
    DatasetPatch,
    DatasetRunIn,
    JobDetailOut,
    SchemaIn,
    SchemaOut,
    SchemaPatch,
    SchemaVersionIn,
    SchemaVersionOut,
    ValidateIn,
)
from forklift_web.services import connections, datasets, jobs, schemas

schemas_router = Router(tags=["schemas"])
datasets_router = Router(tags=["datasets"])
connections_router = Router(tags=["connections"])


# --------------------------------------------------------------------------- schemas


@schemas_router.get("", response=responses({200: list[SchemaOut]}), summary="List schemas")
@paginate
def list_schemas(request):
    return schemas.list_schemas(request.auth)


@schemas_router.post(
    "", response=responses({201: SchemaOut}), summary="Create a schema (its version 1)"
)
def create_schema(request, payload: SchemaIn):
    return Status(
        201,
        schemas.create_schema(
            request.auth,
            name=payload.name,
            document=payload.document,
            description=payload.description,
            notes=payload.notes,
        ),
    )


@schemas_router.post(
    "/validate",
    response=responses({200: JobDetailOut, 202: JobDetailOut}),
    summary="Validate a schema against an input (a validate_schema job; 202 while it runs)",
)
def validate_schema(request, payload: ValidateIn):
    job, finished = jobs.validate_schema(
        request.auth,
        upload_id=payload.upload_id,
        dataset_id=payload.dataset_id,
        schema=payload.schema_,
        schema_version_id=payload.schema_version_id,
        format=payload.format,
        input_options=payload.input_options,
        options=payload.options,
        wait_seconds=payload.wait_seconds,
    )
    return Status(200 if finished else 202, job)


@schemas_router.get("/{schema_id}", response=responses({200: SchemaOut}), summary="A schema")
def get_schema(request, schema_id: uuid.UUID):
    return schemas.get_schema(request.auth, schema_id)


@schemas_router.patch(
    "/{schema_id}", response=responses({200: SchemaOut}), summary="Rename or describe a schema"
)
def update_schema(request, schema_id: uuid.UUID, payload: SchemaPatch):
    return schemas.update_schema(
        request.auth, schema_id, name=payload.name, description=payload.description
    )


@schemas_router.get(
    "/{schema_id}/versions",
    response=responses({200: list[SchemaVersionOut]}),
    summary="Versions of a schema, newest first",
)
@paginate
def list_versions(request, schema_id: uuid.UUID):
    return schemas.list_versions(request.auth, schema_id)


@schemas_router.post(
    "/{schema_id}/versions",
    response=responses({201: SchemaVersionOut}),
    summary="Save a new version (versions are immutable)",
)
def create_version(request, schema_id: uuid.UUID, payload: SchemaVersionIn):
    return Status(
        201,
        schemas.create_version(
            request.auth, schema_id, document=payload.document, notes=payload.notes
        ),
    )


@schemas_router.get(
    "/{schema_id}/versions/{number}",
    response=responses({200: SchemaVersionOut}),
    summary="One version of a schema",
)
def get_version(request, schema_id: uuid.UUID, number: int):
    return schemas.get_version(request.auth, schema_id, number)


# --------------------------------------------------------------------------- datasets


@datasets_router.get("", response=responses({200: list[DatasetOut]}), summary="List datasets")
@paginate
def list_datasets(request):
    return datasets.list_datasets(request.auth)


@datasets_router.post("", response=responses({201: DatasetOut}), summary="Create a dataset")
def create_dataset(request, payload: DatasetIn):
    return Status(201, datasets.create_dataset(request.auth, **payload.model_dump()))


@datasets_router.get("/{dataset_id}", response=responses({200: DatasetOut}), summary="A dataset")
def get_dataset(request, dataset_id: uuid.UUID):
    return datasets.get_dataset(request.auth, dataset_id)


@datasets_router.patch(
    "/{dataset_id}", response=responses({200: DatasetOut}), summary="Change a dataset"
)
def update_dataset(request, dataset_id: uuid.UUID, payload: DatasetPatch):
    return datasets.update_dataset(
        request.auth, dataset_id, **payload.model_dump(exclude_unset=True)
    )


@datasets_router.delete(
    "/{dataset_id}", response=responses({204: None}), summary="Delete a dataset without jobs"
)
def delete_dataset(request, dataset_id: uuid.UUID):
    datasets.delete_dataset(request.auth, dataset_id)
    return Status(204, None)


@datasets_router.post(
    "/{dataset_id}/run",
    response=responses({200: JobDetailOut, 201: JobDetailOut}),
    summary="Run a dataset (201; 200 when the Idempotency-Key was already used)",
)
def run_dataset(
    request,
    dataset_id: uuid.UUID,
    payload: Optional[DatasetRunIn] = None,
    idempotency_key: str = Header("", alias="Idempotency-Key"),
):
    job, created = jobs.run_dataset(
        request.auth,
        dataset_id,
        upload_id=payload.upload_id if payload is not None else None,
        idempotency_key=idempotency_key,
    )
    return Status(201 if created else 200, job)


# --------------------------------------------------------------------------- connections


@connections_router.get(
    "",
    response=responses({200: list[ConnectionOut]}),
    summary="Connections the caller may use (all of them for admins)",
)
@paginate
def list_connections(request, kind: Optional[str] = None):
    return connections.list_connections(request.auth, kind=kind)


@connections_router.post(
    "", response=responses({201: ConnectionOut}), summary="Create a connection (admins)"
)
def create_connection(request, payload: ConnectionIn):
    return Status(201, connections.create_connection(request.auth, **payload.model_dump()))


@connections_router.get(
    "/{connection_id}", response=responses({200: ConnectionOut}), summary="A connection"
)
def get_connection(request, connection_id: uuid.UUID):
    return connections.get_connection(request.auth, connection_id)


@connections_router.patch(
    "/{connection_id}",
    response=responses({200: ConnectionOut}),
    summary="Change a connection (admins); secrets are write-only",
)
def update_connection(request, connection_id: uuid.UUID, payload: ConnectionPatch):
    return connections.update_connection(
        request.auth, connection_id, **payload.model_dump(exclude_unset=True)
    )


@connections_router.delete(
    "/{connection_id}",
    response=responses({204: None}),
    summary="Delete a connection no dataset uses (admins)",
)
def delete_connection(request, connection_id: uuid.UUID):
    connections.delete_connection(request.auth, connection_id)
    return Status(204, None)


@connections_router.post(
    "/{connection_id}/test",
    response=responses({200: ConnectionTestOut}),
    summary="Test that a connection can be reached (admins)",
)
def check_connection(request, connection_id: uuid.UUID):
    return connections.check_connection(request.auth, connection_id)
