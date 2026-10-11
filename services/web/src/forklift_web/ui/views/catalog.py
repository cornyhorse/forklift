"""Schemas (versions, diffs, the JSON editor and live validation) and datasets."""

from __future__ import annotations

from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.core.choices import (
    ArtifactKind,
    ConnectionKind,
    InputFormat,
    JobKind,
    UploadStatus,
)
from forklift_web.core.models import Dataset
from forklift_web.errors import InvalidRequest, NotFound, ServiceError
from forklift_web.policy import Action, allowed, check
from forklift_web.services import artifacts, connections, datasets, jobs, schemas, uploads
from forklift_web.ui import diff
from forklift_web.ui.forms import (
    DatasetForm,
    DatasetRunForm,
    DraftForm,
    GenerateForm,
    SchemaCreateForm,
    SchemaMetaForm,
    ValidateForm,
    VersionForm,
)
from forklift_web.ui.views.base import form_failed, is_htmx, new_key, page, paginate
from forklift_web.ui.views.schedules import dataset_section

DATASET_SOURCES = {ConnectionKind.S3, ConnectionKind.SQL}
CHOICES = 50  # uploads offered in pickers


# --------------------------------------------------------------------------- schemas


@require_GET
@page
def schema_list(request, actor):
    found = schemas.list_schemas(actor).order_by("name")
    search = request.GET.get("q", "").strip()
    if search:
        found = found.filter(name__icontains=search)
    return render(
        request, "ui/schemas/list.html", {"page": paginate(request, found), "search": search}
    )


def _schema_page(request, actor, schema, meta_form=None, status=200):
    versions = list(schemas.list_versions(actor, schema.pk))
    used_by = datasets.list_datasets(actor).filter(schema_version__schema=schema)
    return render(
        request,
        "ui/schemas/detail.html",
        {
            "schema": schema,
            "versions": versions,
            "latest": versions[0],
            "used_by": used_by,
            "meta_form": meta_form
            or SchemaMetaForm(initial={"name": schema.name, "description": schema.description}),
            "can_edit": allowed(actor, Action.SCHEMA_EDIT),
        },
        status=status,
    )


@require_GET
@page
def schema_detail(request, actor, schema_id):
    return _schema_page(request, actor, schemas.get_schema(actor, schema_id))


@require_POST
@page
def schema_edit(request, actor, schema_id):
    check(actor, Action.SCHEMA_EDIT)
    schema = schemas.get_schema(actor, schema_id)
    form = SchemaMetaForm(request.POST)
    status = 400
    if form.is_valid():
        try:
            schemas.update_schema(actor, schema.pk, **form.cleaned_data)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, "The schema was saved.")
            return redirect("ui:schema", schema_id=schema.pk)
    return _schema_page(request, actor, schema, form, status)


@require_GET
@page
def schema_version(request, actor, schema_id, number):
    version = schemas.get_version(actor, schema_id, number)
    return render(
        request,
        "ui/schemas/version.html",
        {
            "version": version,
            "schema": version.schema,
            "can_edit": allowed(actor, Action.SCHEMA_EDIT),
        },
    )


def _number(request, name: str):
    value = request.GET.get(name, "")
    if not value.isdigit():
        raise InvalidRequest(f"{name} must be a version number, such as 1.")
    return int(value)


@require_GET
@page
def schema_diff(request, actor, schema_id):
    schema = schemas.get_schema(actor, schema_id)
    newer = _number(request, "to") if "to" in request.GET else schema.latest_version
    older = _number(request, "from") if "from" in request.GET else max(newer - 1, 1)
    old = schemas.get_version(actor, schema.pk, older)
    new = schemas.get_version(actor, schema.pk, newer)
    return render(
        request,
        "ui/schemas/diff.html",
        {
            "schema": schema,
            "old": old,
            "new": new,
            "numbers": range(schema.latest_version, 0, -1),
            "lines": diff.line_diff(old.document, new.document),
            "summary": diff.summary(old.document, new.document),
        },
    )


def _validation_sources(actor):
    """Uploads and datasets a draft can be validated against (CSV inputs)."""
    if not allowed(actor, Action.SCHEMA_VALIDATE):
        return None
    mine = uploads.list_uploads(actor).filter(
        uploaded_by=actor.user, status=UploadStatus.COMPLETE
    )[:CHOICES]
    sources = datasets.list_datasets(actor).filter(
        source_connection__isnull=False, input_format=InputFormat.CSV
    )
    return list(mine), list(sources)


def _generated_schema(request, actor):
    """The schema artifact named by ?from_artifact= (the browser loads its contents)."""
    artifact_id = request.GET.get("from_artifact")
    if not artifact_id:
        return None
    artifact = artifacts.get_artifact(actor, artifact_id)
    if artifact.kind != "schema":
        raise InvalidRequest(f"Artifact {artifact.name!r} is a {artifact.kind}, not a schema.")
    return artifact


def _editor(request, actor, form, *, schema=None, base=None, status=200):
    sources = _validation_sources(actor)
    validate_form = generate_form = None
    if allowed(actor, Action.JOB_RUN):
        generate_form = GenerateForm(uploads=_own_uploads(actor))
    if sources is not None:
        selected = request.GET.get("upload")
        validate_form = ValidateForm(
            uploads=sources[0],
            datasets=sources[1],
            initial={"source": f"upload:{selected}"} if selected else None,
        )
    return render(
        request,
        "ui/schemas/editor.html",
        {
            "form": form,
            "schema": schema,
            "base": base,
            "validate_form": validate_form,
            "generate_form": generate_form,
            "generated": _generated_schema(request, actor),
        },
        status=status,
    )


@never_cache
@page
def schema_new(request, actor):
    check(actor, Action.SCHEMA_EDIT)
    if request.method != "POST":
        return _editor(request, actor, SchemaCreateForm())
    form = SchemaCreateForm(request.POST)
    status = 400
    if form.is_valid():
        try:
            schema = schemas.create_schema(actor, **form.cleaned_data)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"Schema {schema.name!r} was created (version 1).")
            return redirect("ui:schema", schema_id=schema.pk)
    return _editor(request, actor, form, status=status)


@never_cache
@page
def version_new(request, actor, schema_id):
    check(actor, Action.SCHEMA_EDIT)
    schema = schemas.get_schema(actor, schema_id)
    number = _number(request, "base") if "base" in request.GET else schema.latest_version
    base = schemas.get_version(actor, schema.pk, number)
    if request.method != "POST":
        initial = {} if request.GET.get("from_artifact") else {"document": base.document}
        return _editor(request, actor, VersionForm(initial=initial), schema=schema, base=base)
    form = VersionForm(request.POST)
    status = 400
    if form.is_valid():
        try:
            version = schemas.create_version(actor, schema.pk, **form.cleaned_data)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"Version {version.number} of {schema.name!r} was saved.")
            return redirect("ui:schema-version", schema_id=schema.pk, number=version.number)
    return _editor(request, actor, form, schema=schema, base=base, status=status)


@require_POST
@page
def schema_check(request, actor):
    """The problems of the editor's draft as JSON ({"problems": [...]}, see
    services.schemas.review_document), read as saving reads it."""
    check(actor, Action.SCHEMA_EDIT)
    form = DraftForm(request.POST)
    if form.is_valid():
        problems = schemas.review_document(form.cleaned_data["document"])
    else:
        message = " ".join(form.errors["document"])
        problems = [{"path": [], "message": message, "blocking": True}]
    return JsonResponse({"problems": problems})


def _invalid(form) -> InvalidRequest:
    return InvalidRequest(" ".join(m for errors in form.errors.values() for m in errors))


def _validation(request, job, status=200):
    return render(request, "ui/schemas/_validation.html", {"job": job}, status=status)


@require_POST
@page
def schema_validate(request, actor):
    """Enqueue a validate_schema job for the editor's draft; the fragment polls the job."""
    check(actor, Action.SCHEMA_VALIDATE)
    sources = _validation_sources(actor)
    form = ValidateForm(request.POST, uploads=sources[0], datasets=sources[1])
    if not form.is_valid():
        raise _invalid(form)
    job, _ = jobs.validate_schema(
        actor,
        schema=form.cleaned_data["document"],
        input_options=form.input_options(),
        wait_seconds=0,
        **form.target(),
    )
    if not is_htmx(request):
        return redirect("ui:job", job_id=job.pk)
    return _validation(request, job)


@require_GET
@page
def validation(request, actor, job_id):
    job = jobs.get_job(actor, job_id)
    if job.kind != "validate_schema":
        raise NotFound(f"Job {job.pk} is a {job.kind} job, not a validation.")
    return _validation(request, job)


def _generation(request, job):
    found = [a for a in job.artifacts.all() if a.kind == ArtifactKind.SCHEMA and not a.deleted_at]
    context = {"job": job, "artifact": found[0] if found else None}
    return render(request, "ui/schemas/_generation.html", context)


@require_POST
@page
def schema_generate(request, actor):
    """Enqueue a generate_schema job for one of the author's uploads (the editor's starting
    point); the fragment polls the job and offers its schema to the editor."""
    check(actor, Action.JOB_RUN)
    form = GenerateForm(request.POST, uploads=_own_uploads(actor))
    if not form.is_valid():
        raise _invalid(form)
    job, _ = jobs.create_job(
        actor,
        jobs.JobRequest(
            kind=JobKind.GENERATE_SCHEMA,
            upload_id=form.cleaned_data["upload"],
            format=form.cleaned_data["format"],
        ),
    )
    if not is_htmx(request):
        return redirect("ui:job", job_id=job.pk)
    return _generation(request, job)


@require_GET
@page
def generation(request, actor, job_id):
    job = jobs.get_job(actor, job_id)
    if job.kind != JobKind.GENERATE_SCHEMA:
        raise NotFound(f"Job {job.pk} is a {job.kind} job, not a schema generation.")
    return _generation(request, job)


# --------------------------------------------------------------------------- datasets


@require_GET
@page
def dataset_list(request, actor):
    found = datasets.list_datasets(actor)
    search = request.GET.get("q", "").strip()
    if search:
        found = found.filter(name__icontains=search)
    return render(
        request, "ui/datasets/list.html", {"page": paginate(request, found), "search": search}
    )


def _own_uploads(actor):
    return uploads.list_uploads(actor).filter(
        uploaded_by=actor.user, status=UploadStatus.COMPLETE
    )[:CHOICES]


def _dataset_page(request, actor, dataset: Dataset, run_form=None, status=200):
    can_run = allowed(actor, Action.DATASET_RUN)
    if can_run and run_form is None:
        run_form = DatasetRunForm(
            uploads=_own_uploads(actor) if dataset.source_connection_id is None else [],
            initial={"idempotency_key": new_key()},
        )
    return render(
        request,
        "ui/datasets/detail.html",
        {
            "dataset": dataset,
            "jobs": jobs.list_jobs(actor, dataset_id=dataset.pk)[:20],
            "run_form": run_form if can_run else None,
            "can_edit": allowed(actor, Action.DATASET_EDIT),
            **dataset_section(actor, dataset),
        },
        status=status,
    )


@never_cache
@require_GET
@page
def dataset_detail(request, actor, dataset_id):
    return _dataset_page(request, actor, datasets.get_dataset(actor, dataset_id))


def _dataset_form(actor, *args, **kwargs) -> DatasetForm:
    usable = [c for c in connections.list_connections(actor) if c.kind in DATASET_SOURCES]
    return DatasetForm(
        *args, versions=schemas.list_all_versions(actor), connections=usable, **kwargs
    )


def _dataset_form_page(request, form, dataset=None, status=200):
    return render(
        request, "ui/datasets/form.html", {"form": form, "dataset": dataset}, status=status
    )


@page
def dataset_new(request, actor):
    check(actor, Action.DATASET_EDIT)
    if request.method != "POST":
        initial = {"schema_version": request.GET.get("schema_version", "")}
        return _dataset_form_page(request, _dataset_form(actor, initial=initial))
    form = _dataset_form(actor, request.POST)
    status = 400
    if form.is_valid():
        try:
            dataset = datasets.create_dataset(actor, **form.service_values())
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"Dataset {dataset.name!r} was created.")
            return redirect("ui:dataset", dataset_id=dataset.pk)
    return _dataset_form_page(request, form, status=status)


@page
def dataset_edit(request, actor, dataset_id):
    check(actor, Action.DATASET_EDIT)
    dataset = datasets.get_dataset(actor, dataset_id)
    if request.method != "POST":
        form = _dataset_form(actor, initial=DatasetForm.initial_for(dataset))
        return _dataset_form_page(request, form, dataset)
    form = _dataset_form(actor, request.POST)
    status = 400
    if form.is_valid():
        try:
            datasets.update_dataset(actor, dataset.pk, **form.service_values())
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"Dataset {form.cleaned_data['name']!r} was saved.")
            return redirect("ui:dataset", dataset_id=dataset.pk)
    return _dataset_form_page(request, form, dataset, status)


@require_POST
@page
def dataset_delete(request, actor, dataset_id):
    check(actor, Action.DATASET_EDIT)
    dataset = datasets.get_dataset(actor, dataset_id)
    try:
        datasets.delete_dataset(actor, dataset.pk)
    except ServiceError as error:
        messages.error(request, error.message)
        return redirect("ui:dataset", dataset_id=dataset.pk)
    messages.success(request, f"Dataset {dataset.name!r} was deleted.")
    return redirect("ui:datasets")


@require_POST
@page
def dataset_run(request, actor, dataset_id):
    check(actor, Action.DATASET_RUN)
    dataset = datasets.get_dataset(actor, dataset_id)
    form = DatasetRunForm(
        request.POST,
        uploads=_own_uploads(actor) if dataset.source_connection_id is None else [],
    )
    status = 400
    if form.is_valid():
        try:
            job, _ = jobs.run_dataset(
                actor,
                dataset.pk,
                upload_id=form.cleaned_data["upload"] or None,
                idempotency_key=form.cleaned_data["idempotency_key"],
            )
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            return redirect("ui:job", job_id=job.pk)
    return _dataset_page(request, actor, dataset, form, status)
