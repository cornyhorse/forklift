"""Uploads (the browser PUTs straight to the store), jobs with live status, and downloads."""

from __future__ import annotations

import uuid

from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.core.choices import (
    TERMINAL_STATUSES,
    ArtifactKind,
    Classification,
    JobKind,
    UploadStatus,
)
from forklift_web.errors import ServiceError
from forklift_web.policy import Action, allowed, check
from forklift_web.services import artifacts, datasets, installation, jobs, retention, schemas
from forklift_web.services import uploads as uploads_service
from forklift_web.ui.forms import JobFilterForm, UploadRunForm
from forklift_web.ui.views.base import back, form_failed, is_htmx, new_key, page, paginate

# Stands in for an upload id in URL templates the upload script fills in.
PLACEHOLDER = uuid.UUID(int=0)
VIEWERS = {ArtifactKind.PREVIEW, ArtifactKind.REPORT, ArtifactKind.SCHEMA}
COUNT_LABELS = {
    "total_rows": "rows read",
    "valid_rows": "valid rows",
    "invalid_rows": "rows rejected (bad_rows)",
    "truncated_rows": "rows with extra fields cut",
    "rows_written": "rows written to the table",
}


# --------------------------------------------------------------------------- uploads


@require_GET
@page
def upload_new(request, actor):
    check(actor, Action.UPLOAD_CREATE)
    settings = installation.current()
    return render(
        request,
        "ui/uploads/new.html",
        {
            "classifications": Classification.choices,
            "default_classification": settings["default_classification"],
            "upload_max_bytes": settings["upload_max_bytes"],
            "multipart_threshold_bytes": settings["multipart_threshold_bytes"],
            "placeholder": PLACEHOLDER,
        },
    )


@require_GET
@page
def upload_list(request, actor):
    found = uploads_service.list_uploads(actor)
    return render(request, "ui/uploads/list.html", {"page": paginate(request, found)})


def _upload_actions(actor, upload) -> list:
    actions = []
    if allowed(actor, Action.JOB_RUN):
        actions += ["run", "validate", "generate", "dataset"]
    if allowed(actor, Action.JOB_PREVIEW, upload.classification):
        actions.append("preview")
    return actions


def _run_form(actor, upload, data=None) -> UploadRunForm:
    return UploadRunForm(
        data,
        versions=schemas.list_all_versions(actor),
        datasets=datasets.list_datasets(actor).filter(source_connection__isnull=True),
        actions=_upload_actions(actor, upload),
        initial={"idempotency_key": new_key()},
    )


def _upload_page(request, actor, upload, form=None, status=200):
    if form is None and upload.status == UploadStatus.COMPLETE and _upload_actions(actor, upload):
        form = _run_form(actor, upload)
    return render(
        request,
        "ui/uploads/detail.html",
        {
            "upload": upload,
            "jobs": jobs.list_jobs(actor).filter(upload=upload).select_related("upload")[:20],
            "form": form,
            "can_change": allowed(actor, Action.UPLOAD_CHANGE, upload),
            "expires_at": retention.upload_expires_at(upload),
        },
        status=status,
    )


@never_cache
@require_GET
@page
def upload_detail(request, actor, upload_id):
    return _upload_page(request, actor, uploads_service.get_upload(actor, upload_id))


def _start(actor, upload, form: UploadRunForm):
    data = form.cleaned_data
    action, key = data["action"], data["idempotency_key"]
    if action == "dataset":
        return jobs.run_dataset(actor, data["dataset"], upload_id=upload.pk, idempotency_key=key)
    common = {"format": data["format"], "input_options": form.input_options()}
    version = data["schema_version"] or None
    if action == "validate":
        return jobs.validate_schema(
            actor, upload_id=upload.pk, schema_version_id=version, wait_seconds=0, **common
        )
    request = jobs.JobRequest(
        kind=form.kind,
        upload_id=upload.pk,
        schema_version_id=version if form.kind == JobKind.RUN else None,
        **common,
    )
    return jobs.create_job(actor, request, idempotency_key=key)


@require_POST
@page
def upload_run(request, actor, upload_id):
    check(actor, Action.UPLOAD_USE)
    upload = uploads_service.get_upload(actor, upload_id)
    form = _run_form(actor, upload, request.POST)
    status = 400
    if form.is_valid():
        try:
            job, _ = _start(actor, upload, form)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            return redirect("ui:job", job_id=job.pk)
    return _upload_page(request, actor, upload, form, status)


@require_POST
@page
def upload_delete(request, actor, upload_id):
    upload = uploads_service.delete_upload(actor, upload_id)
    messages.success(request, f"The file {upload.filename!r} was deleted from the store.")
    return redirect("ui:uploads")


# --------------------------------------------------------------------------- jobs


def job_rows(actor, found):
    """Jobs with what the signed-in user may do to each of them."""
    rows = list(found)
    for job in rows:
        job.can_cancel = job.status not in TERMINAL_STATUSES and allowed(
            actor, Action.JOB_CANCEL, job
        )
    return rows


@require_GET
@page
def job_list(request, actor):
    form = JobFilterForm(request.GET or None)
    filters = form.cleaned_data if form.is_valid() else {}
    found = jobs.list_jobs(
        actor,
        status=filters.get("status") or None,
        kind=filters.get("kind") or None,
        mine=filters.get("mine", False),
    ).select_related("upload")
    current = paginate(request, found)
    context = {"form": form, "page": current, "rows": job_rows(actor, current)}
    template = "ui/jobs/_table.html" if is_htmx(request) else "ui/jobs/list.html"
    return render(request, template, context)


def _artifact_rows(actor, job) -> list:
    policies = retention.Policies.load()
    rows = []
    for artifact in artifacts.list_artifacts(actor, job.pk):
        rows.append(
            {
                "artifact": artifact,
                "deleted": artifact.deleted_at is not None,
                "can_download": artifact.deleted_at is None
                and allowed(actor, Action.ARTIFACT_DOWNLOAD, artifact),
                "expires_at": retention.artifact_expires_at(artifact, policies),
            }
        )
    return rows


def live_context(actor, job) -> dict:
    """What the job page shows and the HTMX poll refreshes: status, progress, results."""
    rows = _artifact_rows(actor, job)
    viewers = [row for row in rows if row["artifact"].kind in VIEWERS]
    progress = job.progress or {}
    size = job.upload.size if job.upload_id else None
    read = progress.get("bytes_read")
    result = job.result or {}
    return {
        "job": job,
        "finished": job.status in TERMINAL_STATUSES,
        "result": result,
        "counts": [
            (COUNT_LABELS.get(name, name.replace("_", " ")), count)
            for name, count in result.get("counts", {}).items()
        ],
        "artifacts": rows,
        "viewers": viewers,
        "progress": progress,
        "input_size": size,
        "percent": min(100, round(100 * read / size)) if size and read is not None else None,
        "events": list(jobs.list_events(actor, job.pk).order_by("-id")[:15]),
        "can_cancel": job.status not in TERMINAL_STATUSES
        and allowed(actor, Action.JOB_CANCEL, job),
        "can_save_schema": allowed(actor, Action.SCHEMA_EDIT),
        "can_see_upload": job.upload_id is not None
        and allowed(actor, Action.UPLOAD_VIEW, job.upload),
    }


@never_cache
@require_GET
@page
def job_detail(request, actor, job_id):
    job = jobs.get_job(actor, job_id)
    return render(request, "ui/jobs/detail.html", live_context(actor, job))


@never_cache
@require_GET
@page
def job_live(request, actor, job_id):
    job = jobs.get_job(actor, job_id)
    return render(request, "ui/jobs/_live.html", live_context(actor, job))


@require_POST
@page
def job_cancel(request, actor, job_id):
    job = jobs.cancel_job(actor, job_id)
    if job.status == "cancelled":
        messages.success(request, f"The {job.get_kind_display().lower()} job was cancelled.")
    else:
        messages.success(request, "Cancellation was requested; the worker stops at its next step.")
    return back(request, reverse("ui:job", kwargs={"job_id": job.pk}))


@require_GET
@page
def artifact_download(request, actor, artifact_id):
    download = artifacts.download(actor, artifact_id)
    return redirect(download.url)
