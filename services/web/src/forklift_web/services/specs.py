"""Job specs (job contract v1): stored with placeholders, filled in when a worker leases.

A job record stores its spec with *placeholder* locations, never secrets or URLs:

- ``{"type": "gateway:upload", "upload_id": ...}``: an upload in the installation's store;
- ``{"type": "gateway:object", "connection_id": ..., "key": ...}``: an object in an s3
  connection;
- ``{"type": "gateway:sql", "connection_id": ...}``: a sql connection (input);
- ``{"type": "gateway:sql_table", "connection_id": ..., "table": ..., ...}``: a table of a sql
  connection (output).

:func:`render` turns them into contract locations for the worker that leased the job: inputs in
a store become ``presigned_url`` locations (a GET valid for the job's ``max_seconds`` plus a
margin, signed for the worker's endpoint), sql connections become ``sql`` / ``sql_table``
locations with the decrypted connection string. The result is validated against
``contracts/jobspec.schema.json`` (inputs that are not CSV are validated as the supervisor will
pass them on: staged into a ``file`` location, since only CSV can be streamed).
"""

from __future__ import annotations

import copy
from typing import Optional

from forklift_web import contracts, secret_backend, storage
from forklift_web.core.choices import InputFormat, UploadStatus
from forklift_web.core.models import Connection, Job, Upload
from forklift_web.errors import StoreUnavailable
from forklift_web.services.connections import bucket_of, sql_connection_string

SPEC_VERSION = 1
SUPPORTED_SPEC_VERSIONS = (1,)
OUTPUT_DIRECTORY = {"type": "file", "path": "out/"}
STAGED_INPUT = {"type": "file", "path": "in/input"}
DAY = 24 * 3600

UPLOAD = "gateway:upload"
OBJECT = "gateway:object"
SQL = "gateway:sql"
SQL_TABLE = "gateway:sql_table"


class SpecUnavailable(Exception):
    """The spec cannot be rendered (its input is gone, a secret cannot be decrypted, ...); the
    job fails with ``code``."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def template(
    job_id: str,
    kind: str,
    *,
    input_format: str,
    input_location: dict,
    input_options: dict,
    schema: Optional[dict],
    output_location: Optional[dict],
    compression: str,
    options: dict,
    limits: dict,
) -> dict:
    """The spec a job record stores (placeholders for anything secret or short-lived)."""
    output: dict = {"location": output_location or OUTPUT_DIRECTORY, "compression": compression}
    if output["location"].get("type") == SQL_TABLE:
        output["artifacts"] = OUTPUT_DIRECTORY
    return {
        "spec_version": SPEC_VERSION,
        "job_id": job_id,
        "kind": kind,
        "input": {"format": input_format, "location": input_location, "options": input_options},
        "schema": schema,
        "output": output,
        "options": options,
        "limits": {name: value for name, value in limits.items() if value is not None},
    }


def input_url_seconds(spec: dict, settings: dict) -> int:
    """How long an input URL must stay valid: the job's max_seconds plus the margin."""
    seconds = spec.get("limits", {}).get("max_seconds") or DAY
    return int(min(seconds + settings["input_url_margin_seconds"], storage.MAX_PRESIGN_SECONDS))


def _connection(connection_id) -> Connection:
    connection = Connection.objects.filter(pk=connection_id).first()
    if connection is None:
        raise SpecUnavailable(
            "INPUT_UNREADABLE", f"The connection {connection_id} of this job was deleted."
        )
    return connection


def _connection_string(connection: Connection, *, dry_run: bool) -> str:
    if dry_run:
        return "Driver={dry-run}"
    try:
        return sql_connection_string(connection)
    except secret_backend.SecretError as error:
        raise SpecUnavailable("INTERNAL", str(error)) from None


def _input(location: dict, spec: dict, settings: dict, *, dry_run: bool) -> dict:
    kind = location["type"]
    if kind == UPLOAD:
        upload = Upload.objects.filter(pk=location["upload_id"]).first()
        if upload is None or upload.status != UploadStatus.COMPLETE:
            raise SpecUnavailable(
                "INPUT_UNREADABLE",
                f"The input upload {location['upload_id']} is no longer available (deleted or "
                "expired by retention).",
            )
        url = (
            "https://dry-run.invalid/"
            if dry_run
            else storage.store().presign_get(
                upload.key,
                expires=input_url_seconds(spec, settings),
                audience=storage.Audience.WORKER,
            )
        )
        return {
            "type": "presigned_url",
            "url": url,
            "size": upload.size,
            "etag": upload.etag or None,
        }
    if kind == OBJECT:
        if dry_run:
            return {
                "type": "presigned_url",
                "url": "https://dry-run.invalid/",
                "size": None,
                "etag": None,
            }
        connection = _connection(location["connection_id"])
        try:
            bucket = bucket_of(connection)
            info = bucket.head(location["key"])
        except (secret_backend.SecretError, StoreUnavailable) as error:
            raise SpecUnavailable("INPUT_UNREADABLE", str(error)) from None
        if info is None:
            raise SpecUnavailable(
                "INPUT_UNREADABLE",
                f"There is no object {location['key']!r} in the bucket of connection "
                f"{connection.name!r}.",
            )
        url = bucket.presign_get(
            location["key"],
            expires=input_url_seconds(spec, settings),
            audience=storage.Audience.WORKER,
        )
        return {"type": "presigned_url", "url": url, "size": info.size, "etag": info.etag or None}
    connection = _connection(location["connection_id"])
    return {"type": "sql", "connection_string": _connection_string(connection, dry_run=dry_run)}


def _output(location: dict, *, dry_run: bool) -> dict:
    if location.get("type") != SQL_TABLE:
        return location
    connection = _connection(location["connection_id"])
    rendered = {
        "type": "sql_table",
        "connection_string": _connection_string(connection, dry_run=dry_run),
        "table": location["table"],
        "mode": location["mode"],
    }
    for name in ("schema_name", "key_columns"):
        if location.get(name):
            rendered[name] = location[name]
    return rendered


def check_contract(spec: dict) -> None:
    """Validate ``spec`` against the contract as the engine will receive it."""
    checked = spec
    location = spec["input"]["location"]
    if location.get("type") == "presigned_url" and spec["input"]["format"] != InputFormat.CSV:
        checked = copy.deepcopy(spec)
        checked["input"]["location"] = STAGED_INPUT
    contracts.validate(contracts.JOBSPEC, checked)


def render(job: Job, settings: dict, *, dry_run: bool = False) -> dict:
    """The spec for a worker (or, with ``dry_run``, a stand-in with the same shape that holds
    no URL or secret, to validate a job before it is queued)."""
    spec = copy.deepcopy(job.spec)
    location = spec["input"]["location"]
    spec["input"]["location"] = _input(location, spec, settings, dry_run=dry_run)
    staged_only = spec["input"]["format"] not in {InputFormat.CSV, InputFormat.SQL}
    size = spec["input"]["location"].get("size")
    if staged_only and size is not None and size > settings["stage_max_bytes"]:
        raise SpecUnavailable(
            "LIMIT_EXCEEDED",
            f"The {spec['input']['format']} input is {size} bytes. Only CSV inputs can be "
            f"streamed; other formats are staged into the worker's scratch directory, which "
            f"takes inputs up to stage_max_bytes ({settings['stage_max_bytes']} bytes).",
        )
    spec["output"]["location"] = _output(spec["output"]["location"], dry_run=dry_run)
    try:
        check_contract(spec)
    except contracts.ContractViolation as error:
        raise SpecUnavailable("SPEC_INVALID", str(error)) from None
    return spec
