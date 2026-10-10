"""Helpers for the UI tests: a fake worker that finishes jobs through the queue service, and an
HTML parser that finds forms and links in rendered pages."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from html.parser import HTMLParser

from conftest import put_url
from django.utils import timezone
from world import job_result

from forklift_web.core.choices import JobStatus
from forklift_web.core.models import Job, WorkerToken
from forklift_web.services import queue, tokens
from forklift_web.services.workers import WorkerPrincipal

PREVIEW = {
    "columns": ["id", "name"],
    "rows": [["1", "alice"], ["2", None]],
    "row_count": 2,
    "truncated": False,
    "limit": None,
    "truncated_cells": 0,
}
REPORT = {
    "valid": False,
    "columns": ["id", "name"],
    "schema_columns": ["id", "email"],
    "required_columns": ["email"],
    "columns_not_in_schema": ["name"],
    "schema_columns_not_in_input": ["email"],
    "sample_rows": 2,
    "error": {"code": "COLUMN_MISSING", "message": "The input has no column 'email'."},
}
GENERATED = {"type": "object", "properties": {"id": {"type": "integer"}}}


def worker_principal() -> WorkerPrincipal:
    token, _ = tokens.create(WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="ui test workers")
    return WorkerPrincipal(token=token)


def start(job: Job, principal: WorkerPrincipal, *, progress: dict | None = None) -> Job:
    """Give ``job`` to the fake worker (as a lease would), optionally with progress."""
    Job.objects.filter(pk=job.pk).update(
        status=JobStatus.RUNNING,
        attempt=job.attempt + 1,
        lease_token=principal.token,
        lease_expires_at=timezone.now() + timedelta(minutes=5),
        started_at=timezone.now(),
    )
    job.refresh_from_db()
    if progress:
        queue.heartbeat(principal, job.pk, attempt=job.attempt, progress=progress)
        job.refresh_from_db()
    return job


def finish(
    job: Job,
    outputs: dict,
    *,
    principal: WorkerPrincipal | None = None,
    status: str = "succeeded",
    error: dict | None = None,
    warnings: list = (),
) -> Job:
    """Finish ``job`` the way a worker does: presigned PUTs of ``outputs`` ({name: (kind,
    bytes)}), then complete with a JobResult."""
    principal = principal or worker_principal()
    if job.status != JobStatus.RUNNING:
        job = start(job, principal)
    files = [{"name": name, "bytes": len(data)} for name, (_, data) in outputs.items()]
    reported = []
    if files:
        for entry in queue.presign_outputs(principal, job.pk, attempt=job.attempt, files=files):
            kind, data = outputs[entry["name"]]
            put_url(entry["url"], data)
            reported.append(
                {
                    "kind": kind,
                    "name": entry["name"],
                    "key": entry["key"],
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "rows": 2 if kind in {"data", "bad_rows", "preview"} else None,
                }
            )
    result = job_result(str(job.pk), reported, status=status, error=error)
    result["warnings"] = list(warnings)
    queue.complete(principal, job.pk, attempt=job.attempt, result=result, artifacts=reported)
    job.refresh_from_db()
    return job


def as_json(document) -> bytes:
    return json.dumps(document).encode()


class Page(HTMLParser):
    """The forms (method, action, field names) and links of an HTML page."""

    def __init__(self, html: str):
        super().__init__()
        self.forms: list = []
        self.links: list = []
        self._form = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "form":
            self._form = {
                "method": (attributes.get("method") or "get").lower(),
                "action": attributes.get("action") or "",
                "fields": [],
                "hx": {k: v for k, v in attributes.items() if k.startswith("hx-")},
            }
            self.forms.append(self._form)
        elif tag in {"input", "select", "textarea", "button"} and self._form is not None:
            if attributes.get("name"):
                self._form["fields"].append(attributes["name"])
        elif tag == "a" and attributes.get("href"):
            self.links.append(attributes["href"])

    def handle_endtag(self, tag):
        if tag == "form":
            self._form = None
