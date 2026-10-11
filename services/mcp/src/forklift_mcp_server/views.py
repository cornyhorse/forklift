"""Records of the API (jobs, artifacts, datasets, ...) as tool results show them: the fields an
agent needs, with free texts and nested values bounded."""

from __future__ import annotations

from typing import Any, Dict, Optional

from forklift_client import Artifact, Connection, Dataset, Job, Page, Schema, Upload

from .values import bounded, text


def iso(moment) -> Optional[str]:
    return moment.isoformat() if moment is not None else None


def page_view(page: Page) -> Dict[str, Any]:
    return {
        "count": page.count,
        "offset": page.offset,
        "limit": page.limit,
        "next_offset": page.next_offset if page.has_more else None,
    }


def connection_view(connection: Connection) -> Dict[str, Any]:
    return {
        "id": connection.id,
        "name": connection.name,
        "kind": connection.kind,
        "description": text(connection.description),
        "config": bounded(connection.config),
        "secret_fields": list(connection.secret_fields),
        "allowed_roles": list(connection.allowed_roles),
        "last_tested_at": iso(connection.last_tested_at),
        "last_test_ok": connection.last_test_ok,
        "last_test_message": text(connection.last_test_message),
    }


def dataset_view(dataset: Dataset) -> Dict[str, Any]:
    return {
        "id": dataset.id,
        "name": dataset.name,
        "description": text(dataset.description),
        "classification": dataset.classification,
        "input_format": dataset.input_format,
        "input_options": bounded(dataset.input_options),
        "source": (
            {"connection_id": dataset.source_connection_id, "path": dataset.source_path}
            if dataset.source_connection_id is not None
            else "uploads (run it with upload_id)"
        ),
        "schema_id": dataset.schema_id,
        "schema_version": dataset.schema_version,
        "destination": (
            {
                "connection_id": dataset.destination_connection_id,
                "prefix": dataset.destination_prefix,
            }
            if dataset.destination_connection_id is not None
            else None
        ),
        "compression": dataset.compression,
        "options": bounded(dataset.options),
        "updated_at": iso(dataset.updated_at),
    }


def schema_view(schema: Schema) -> Dict[str, Any]:
    return {
        "id": schema.id,
        "name": schema.name,
        "description": text(schema.description),
        "latest_version": schema.latest_version,
        "updated_at": iso(schema.updated_at),
    }


def upload_view(upload: Upload) -> Dict[str, Any]:
    return {
        "id": upload.id,
        "filename": upload.filename,
        "size": upload.size,
        "status": upload.status,
        "classification": upload.classification,
        "created_at": iso(upload.created_at),
    }


def job_view(job: Job) -> Dict[str, Any]:
    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "lane": job.lane,
        "classification": job.classification,
        "dataset_id": job.dataset_id,
        "upload_id": job.upload_id,
        "schema_version_id": job.schema_version_id,
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "cancel_requested": job.cancel_requested,
        "progress": bounded(job.progress),
        "publish_error": text(job.publish_error),
        "created_at": iso(job.created_at),
        "started_at": iso(job.started_at),
        "finished_at": iso(job.finished_at),
    }


def artifact_view(artifact: Artifact) -> Dict[str, Any]:
    return {
        "id": artifact.id,
        "kind": artifact.kind,
        "name": artifact.name,
        "attempt": artifact.attempt,
        "rows": artifact.rows,
        "bytes": artifact.bytes,
        "deleted": artifact.deleted_at is not None,
        "expires_at": iso(artifact.expires_at),
    }
