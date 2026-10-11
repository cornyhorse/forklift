"""Schemas (versions, diffs, the editor and live validation) and datasets in the UI."""

from __future__ import annotations

import json

import pytest
from django.urls import reverse
from ui_support import GENERATED, REPORT, as_json, finish
from world import SCHEMA_DOCUMENT, World, make_job, make_schema, make_upload

from forklift_web.core.choices import JobKind, JobStatus
from forklift_web.core.models import Dataset, Job, Schema, SchemaVersion
from forklift_web.policy import Actor
from forklift_web.services import schemas

pytestmark = pytest.mark.django_db

HTMX = {"HTTP_HX_REQUEST": "true"}


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def author(client, world):
    client.force_login(world.author)
    return client


def schema_url(name, world, **kwargs):
    return reverse(f"ui:{name}", kwargs={"schema_id": world.version.schema_id, **kwargs})


def second_version(world, document=None):
    return schemas.create_version(
        Actor.for_user(world.author),
        world.version.schema_id,
        document=document
        or {
            "type": "object",
            "properties": {"id": {"type": "string"}, "email": {"type": "string"}},
            "required": ["email"],
            "x-primaryKey": ["id"],
        },
        notes="ids are text now",
    )


# --------------------------------------------------------------------------- schemas


def test_the_schema_list_searches_names(world, author):
    make_schema(world.author, name="invoices")
    page = author.get(reverse("ui:schemas"), {"q": "invoic"})
    names = [schema.name for schema in page.context["page"]]
    assert names == ["invoices"]
    assert b"No schemas match" in author.get(reverse("ui:schemas"), {"q": "zzz"}).content


@pytest.mark.parametrize(
    "document,message",
    [
        ('{"type": "object",', "This is not valid JSON: Expecting property name"),
        ("[1, 2]", "This must be a JSON object"),
        ("", "A schema needs a document: a JSON object."),
    ],
)
def test_a_new_schema_needs_a_json_object(author, document, message):
    response = author.post(reverse("ui:schema-new"), {"name": "people", "document": document})
    assert response.status_code == 400
    assert message in response.content.decode()
    assert not Schema.objects.filter(name="people").exists()


def test_creating_a_schema(world, author):
    document = json.dumps(SCHEMA_DOCUMENT)
    created = author.post(
        reverse("ui:schema-new"),
        {"name": "people", "description": "everyone", "document": document, "notes": "first"},
        follow=True,
    )
    schema = Schema.objects.get(name="people")
    assert created.redirect_chain[-1][0] == reverse("ui:schema", kwargs={"schema_id": schema.pk})
    assert b"was created (version 1)" in created.content
    assert schema.versions.get().notes == "first"
    duplicate = author.post(reverse("ui:schema-new"), {"name": "people", "document": document})
    assert duplicate.status_code == 409
    assert b"A schema named &#x27;people&#x27; already exists." in duplicate.content


def test_the_schema_page_shows_versions_and_where_it_is_used(world, author):
    second_version(world)
    page = author.get(schema_url("schema", world))
    content = page.content.decode()
    assert page.context["latest"].number == 2
    assert "ids are text now" in content and world.dataset.name in content
    assert "Compare versions" in content and "Edit (save as version 3)" in content


def test_renaming_a_schema(world, author):
    other = make_schema(world.author, name="taken")
    renamed = author.post(
        schema_url("schema-edit", world), {"name": "renamed", "description": "d"}, follow=True
    )
    assert b"The schema was saved." in renamed.content
    assert Schema.objects.get(pk=world.version.schema_id).name == "renamed"
    clash = author.post(schema_url("schema-edit", world), {"name": other.schema.name})
    assert clash.status_code == 409 and b"already exists" in clash.content
    empty = author.post(schema_url("schema-edit", world), {"name": ""})
    assert empty.status_code == 400


def test_a_version_page(world, author):
    version = second_version(world)
    page = author.get(schema_url("schema-version", world, number=2)).content.decode()
    assert version.sha256 in page and "Changes from v1" in page
    assert "Start a new version from this one" in page
    missing = author.get(schema_url("schema-version", world, number=9))
    assert missing.status_code == 404 and b"has no version 9" in missing.content


def test_comparing_versions(world, author):
    second_version(world)
    page = author.get(schema_url("schema-diff", world))
    assert (page.context["old"].number, page.context["new"].number) == (1, 2)
    summary = page.context["summary"]
    assert summary.added == ["email"] and summary.changed == ["id"] and summary.removed == []
    assert summary.now_required == ["email"] and summary.other_keys == ["x-primaryKey"]
    kinds = {line.kind for line in page.context["lines"]}
    assert {"added", "removed"} <= kinds
    content = page.content.decode()
    assert "Columns added" in content and "x-primaryKey" in content
    back = author.get(schema_url("schema-diff", world), {"from": "2", "to": "1"})
    assert back.context["summary"].removed == ["email"]
    assert back.context["summary"].no_longer_required == ["email"]
    same = author.get(schema_url("schema-diff", world), {"from": "1", "to": "1"})
    assert b"The two documents are the same." in same.content
    bad = author.get(schema_url("schema-diff", world), {"from": "one"})
    assert bad.status_code == 400 and b"from must be a version number" in bad.content


def test_a_new_version_starts_from_the_latest_or_a_chosen_one(world, author):
    second_version(world)
    latest = author.get(schema_url("version-new", world))
    assert json.loads(latest.context["form"]["document"].value())["required"] == ["email"]
    first = author.get(schema_url("version-new", world), {"base": "1"})
    assert json.loads(first.context["form"]["document"].value()) == SCHEMA_DOCUMENT
    assert b"Starting from version 1." in first.content
    assert author.get(schema_url("version-new", world), {"base": "x"}).status_code == 400
    assert author.get(schema_url("version-new", world), {"base": "7"}).status_code == 404


def test_saving_a_version(world, author):
    document = '{"type": "object", "properties": {}}'
    saved = author.post(schema_url("version-new", world), {"document": document, "notes": "empty"})
    assert saved["Location"] == schema_url("schema-version", world, number=2)
    assert SchemaVersion.objects.get(schema_id=world.version.schema_id, number=2).notes == "empty"
    same = author.post(schema_url("version-new", world), {"document": document})
    assert same.status_code == 409 and b"identical to version 2" in same.content
    invalid = author.post(schema_url("version-new", world), {"document": "{"})
    assert invalid.status_code == 400


def test_the_editor_offers_the_authors_files_and_csv_datasets(world, author):
    assert b"you have no files yet" in author.get(reverse("ui:schema-new")).content
    upload = make_upload(world.author)
    dataset = Dataset.objects.create(
        name="from-the-store",
        schema_version=world.version,
        source_connection=world.connection,
        source_path="people.csv",
    )
    page = author.get(reverse("ui:schema-new"), {"upload": str(upload.pk)})
    form = page.context["validate_form"]
    groups = dict(form.fields["source"].choices)
    assert [value for value, _ in groups["Your uploads"]] == [f"upload:{upload.pk}"]
    assert [value for value, _ in groups["Datasets"]] == [f"dataset:{dataset.pk}"]
    assert form.initial == {"source": f"upload:{upload.pk}"}
    assert b'id="validate-button"' in page.content


def test_the_editor_of_an_actor_that_may_not_run_jobs(world, rf, api_token):
    """Editing and checking are separate permissions (a token can carry schemas:write without
    jobs:run): such an actor gets the editor without the check panel."""
    from forklift_web.ui.forms import SchemaCreateForm
    from forklift_web.ui.views import catalog

    token, _ = api_token(world.author, ["schemas:read", "schemas:write"])
    actor = Actor.for_user(world.author, token=token)
    request = rf.get(reverse("ui:schema-new"))
    request.user = world.author
    response = catalog._editor(request, actor, SchemaCreateForm())
    assert b"Checking drafts runs a job, which your role does not allow." in response.content
    assert b"Generating a schema runs a job, which your role does not allow." in response.content


def test_the_editor_loads_a_generated_schema(world, author):
    job = make_job(world.operator, world.upload, kind=JobKind.GENERATE_SCHEMA)
    finish(job, {"schema.json": ("schema", as_json(GENERATED))})
    artifact = job.artifacts.get()
    page = author.get(reverse("ui:schema-new"), {"from_artifact": str(artifact.pk)})
    url = reverse("api-v1:download_artifact", kwargs={"artifact_id": artifact.pk})
    assert f'data-load-artifact="{url}"'.encode() in page.content
    assert b"Starting from the schema generated by" in page.content
    version = author.get(schema_url("version-new", world), {"from_artifact": str(artifact.pk)})
    assert version.context["form"]["document"].value() in (None, "")
    report_job = make_job(world.operator, world.upload, kind=JobKind.VALIDATE_SCHEMA)
    finish(report_job, {"report.json": ("report", as_json(REPORT))})
    wrong = author.get(
        reverse("ui:schema-new"), {"from_artifact": str(report_job.artifacts.get().pk)}
    )
    assert wrong.status_code == 400 and b"is a report, not a schema" in wrong.content


def test_checking_a_draft_with_htmx_polls_its_job(world, author):
    upload = make_upload(world.author)
    response = author.post(
        reverse("ui:schema-validate"),
        {
            "source": f"upload:{upload.pk}",
            "document": json.dumps(SCHEMA_DOCUMENT),
            "delimiter": ";",
        },
        **HTMX,
    )
    assert response.status_code == 200
    job = Job.objects.get(kind=JobKind.VALIDATE_SCHEMA, upload=upload)
    assert job.spec["schema"] == SCHEMA_DOCUMENT
    assert job.spec["input"]["options"] == {"delimiter": ";"}
    content = response.content.decode()
    assert "<html" not in content  # a fragment
    poll = reverse("ui:job-validation", kwargs={"job_id": job.pk})
    assert f'hx-get="{poll}"' in content and 'hx-trigger="every 1s"' in content

    finish(job, {"report.json": ("report", as_json(REPORT))})
    done = author.get(poll, **HTMX).content.decode()
    assert "hx-trigger" not in done and "The draft fits the file" in done
    report = job.artifacts.get()
    assert reverse("api-v1:download_artifact", kwargs={"artifact_id": report.pk}) in done
    assert "TYPE_MISMATCH:id" in done  # the findings in the sample


def test_a_draft_that_does_not_fit(world, author):
    job = make_job(world.operator, world.upload, kind=JobKind.VALIDATE_SCHEMA)
    finish(
        job,
        {},
        status="failed",
        error={"code": "COLUMN_MISSING", "message": "No column 'email'.", "retryable": False},
        warnings=["The header has a trailing delimiter"],
    )
    done = author.get(reverse("ui:job-validation", kwargs={"job_id": job.pk})).content.decode()
    assert "The draft does not fit" in done and "COLUMN_MISSING" in done
    assert "trailing delimiter" in done
    job.refresh_from_db()
    Job.objects.filter(pk=job.pk).update(status=JobStatus.CANCELLED)
    cancelled = author.get(reverse("ui:job-validation", kwargs={"job_id": job.pk}))
    assert b"the check was cancelled" in cancelled.content


def test_the_validation_fragment_waits_for_a_worker(world, author):
    job = make_job(world.operator, world.upload, kind=JobKind.VALIDATE_SCHEMA)
    queued = author.get(reverse("ui:job-validation", kwargs={"job_id": job.pk}))
    assert b"Waiting for an interactive worker." in queued.content
    Job.objects.filter(pk=job.pk).update(status=JobStatus.RUNNING)
    running = author.get(reverse("ui:job-validation", kwargs={"job_id": job.pk}))
    assert b"A worker is reading the file." in running.content
    other = author.get(reverse("ui:job-validation", kwargs={"job_id": world.queued_job.pk}))
    assert other.status_code == 404 and b"not a validation" in other.content


def test_checking_a_draft_against_a_dataset_and_without_htmx(world, author):
    dataset = Dataset.objects.create(
        name="from-the-store",
        schema_version=world.version,
        source_connection=world.connection,
        source_path="people.csv",
    )
    response = author.post(
        reverse("ui:schema-validate"),
        {"source": f"dataset:{dataset.pk}", "document": json.dumps(SCHEMA_DOCUMENT)},
    )
    job = Job.objects.get(kind=JobKind.VALIDATE_SCHEMA, dataset=dataset)
    assert response["Location"] == reverse("ui:job", kwargs={"job_id": job.pk})


def test_a_draft_that_is_not_json_is_refused_in_the_fragment(world, author):
    upload = make_upload(world.author)
    response = author.post(
        reverse("ui:schema-validate"), {"source": f"upload:{upload.pk}", "document": "{"}, **HTMX
    )
    assert response.status_code == 400
    content = response.content.decode()
    assert "<html" not in content and "The document is not valid JSON" in content
    stranger = author.post(
        reverse("ui:schema-validate"),
        {"source": f"upload:{world.upload.pk}", "document": "{}"},
        **HTMX,
    )
    assert stranger.status_code == 400 and b"Select a valid choice" in stranger.content


def test_the_editor_asks_the_gateway_about_a_draft(world, author, client):
    check = reverse("ui:schema-check")
    document = {"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["nme"]}
    answer = author.post(check, {"document": json.dumps(document)})
    assert answer["Content-Type"] == "application/json"
    problems = answer.json()["problems"]
    assert {"path": ["required", 0], "blocking": False} in [
        {"path": p["path"], "blocking": p["blocking"]} for p in problems
    ]
    assert problems == schemas.review_document(document)  # the service's review, as it is
    # what saving says about text that is not a JSON object, at the document
    for text, message in (
        ("{", "This is not valid JSON: Expecting property name enclosed in double quotes"),
        ("[]", "This must be a JSON object: {...}."),
        ("", "A schema needs a document: a JSON object."),
    ):
        [problem] = author.post(check, {"document": text}).json()["problems"]
        assert problem["path"] == [] and problem["blocking"] is True
        assert problem["message"].startswith(message)
    client.force_login(world.operator)  # editing schemas needs schemas:write (Authors)
    refused = client.post(check, {"document": "{}"})
    assert refused.status_code == 403 and b"Not allowed" in refused.content


def test_the_editor_offers_the_authors_files_to_generate_from(world, author):
    assert b"and you have none yet" in author.get(reverse("ui:schema-new")).content
    upload = make_upload(world.author)
    form = author.get(reverse("ui:schema-new")).context["generate_form"]
    assert [value for value, _ in form.fields["upload"].choices] == [str(upload.pk)]
    assert form["upload"].auto_id == "id_generate-upload"


def test_generating_a_schema_from_the_editor(world, author):
    upload = make_upload(world.author)
    fields = {"generate-upload": str(upload.pk), "generate-format": "csv"}
    response = author.post(reverse("ui:schema-generate"), fields, **HTMX)
    job = Job.objects.get(kind=JobKind.GENERATE_SCHEMA, upload=upload)
    assert (job.lane, job.spec["input"]["format"], job.spec["schema"]) == (
        "interactive",
        "csv",
        None,
    )
    content = response.content.decode()
    poll = reverse("ui:job-generation", kwargs={"job_id": job.pk})
    assert "<html" not in content and f'hx-get="{poll}"' in content
    assert "Waiting for an interactive worker." in content
    Job.objects.filter(pk=job.pk).update(status=JobStatus.RUNNING)
    assert b"A worker is reading the file." in author.get(poll, **HTMX).content

    finish(job, {"schema.json": ("schema", as_json(GENERATED))})
    done = author.get(poll, **HTMX).content.decode()
    artifact = job.artifacts.get()
    download = reverse("api-v1:download_artifact", kwargs={"artifact_id": artifact.pk})
    assert "hx-trigger" not in done and f'data-use-generated="{download}"' in done
    assert "A schema was generated from people.csv." in done

    artifact.deleted_at = artifact.created_at
    artifact.save(update_fields=["deleted_at"])
    assert b"no longer kept (retention)" in author.get(poll, **HTMX).content

    without_htmx = author.post(reverse("ui:schema-generate"), fields)
    newest = Job.objects.filter(kind=JobKind.GENERATE_SCHEMA).latest("created_at")
    assert without_htmx["Location"] == reverse("ui:job", kwargs={"job_id": newest.pk})


def test_a_generation_that_fails_or_is_not_one(world, author):
    stranger = author.post(
        reverse("ui:schema-generate"),
        {"generate-upload": str(world.upload.pk), "generate-format": "csv"},  # not the author's
        **HTMX,
    )
    assert stranger.status_code == 400 and b"Select a valid choice" in stranger.content
    job = make_job(world.operator, world.upload, kind=JobKind.GENERATE_SCHEMA)
    finish(
        job,
        {},
        status="failed",
        error={"code": "INPUT_UNREADABLE", "message": "Not a CSV file.", "retryable": False},
    )
    poll = reverse("ui:job-generation", kwargs={"job_id": job.pk})
    failed = author.get(poll).content.decode()
    assert "No schema was generated." in failed and "INPUT_UNREADABLE" in failed
    Job.objects.filter(pk=job.pk).update(status=JobStatus.CANCELLED)
    assert b"(the job was cancelled)" in author.get(poll).content
    other = author.get(reverse("ui:job-generation", kwargs={"job_id": world.queued_job.pk}))
    assert other.status_code == 404 and b"not a schema generation" in other.content


# --------------------------------------------------------------------------- datasets


def dataset_fields(world, **changes):
    fields = {
        "name": "monthly",
        "description": "",
        "classification": "internal",
        "schema_version": str(world.version.pk),
        "input_format": "csv",
        "input_options": "",
        "source_connection": "",
        "source_path": "",
        "destination_connection": "",
        "destination_prefix": "",
        "destination_table": "",
        "destination_schema_name": "",
        "destination_mode": "",
        "destination_key_columns": "",
        "compression": "snappy",
        "options": "",
    }
    return {**fields, **changes}


def test_the_dataset_list_searches_names(world, author):
    page = author.get(reverse("ui:datasets"), {"q": world.spare_dataset.name[:8]})
    assert [d.name for d in page.context["page"]] == [world.spare_dataset.name]
    assert b"No datasets match" in author.get(reverse("ui:datasets"), {"q": "zzz"}).content


def test_creating_a_dataset_that_reads_a_bucket(world, author):
    created = author.post(
        reverse("ui:dataset-new"),
        dataset_fields(
            world,
            source_connection=str(world.connection.pk),
            source_path="exports/people.csv",
            input_options='{"delimiter": ";"}',
            options='{"batch_size": 500}',
            destination_connection=str(world.spare_connection.pk),
            destination_prefix="clean",
        ),
        follow=True,
    )
    dataset = Dataset.objects.get(name="monthly")
    assert b"Dataset &#x27;monthly&#x27; was created." in created.content
    assert dataset.source_connection == world.connection
    assert (dataset.input_options, dataset.options) == ({"delimiter": ";"}, {"batch_size": 500})
    assert dataset.destination_prefix == "clean" and dataset.destination_options == {}
    content = created.content.decode()
    assert "exports/people.csv" in content and world.spare_connection.name in content


def test_the_service_layer_explains_a_wrong_dataset(world, author):
    no_path = author.post(
        reverse("ui:dataset-new"),
        dataset_fields(world, source_connection=str(world.connection.pk)),
    )
    assert no_path.status_code == 400 and b"need source_path" in no_path.content
    duplicate = author.post(
        reverse("ui:dataset-new"), dataset_fields(world, name=world.dataset.name)
    )
    assert duplicate.status_code == 409
    bad_json = author.post(reverse("ui:dataset-new"), dataset_fields(world, options="[1]"))
    assert bad_json.status_code == 400 and b"This must be a JSON object" in bad_json.content


def test_a_dataset_with_a_database_table_destination(world, author):
    from forklift_web import secret_backend

    sql = world.connection.__class__.objects.create(
        name="warehouse",
        kind="sql",
        config={
            "dialect": "postgresql",
            "host": "db",
            "port": 5432,
            "database": "dw",
            "username": "loader",
            "driver": "PostgreSQL Unicode",
            "options": {},
        },
        secret_ciphertext=secret_backend.backend().encrypt({"password": "pw"}),
        secret_fields=["password"],
    )
    author.post(
        reverse("ui:dataset-new"),
        dataset_fields(
            world,
            destination_connection=str(sql.pk),
            destination_table="people",
            destination_schema_name="staging",
            destination_mode="upsert",
            destination_key_columns="id, region ,",
        ),
    )
    dataset = Dataset.objects.get(name="monthly")
    assert dataset.destination_options == {
        "table": "people",
        "schema_name": "staging",
        "mode": "upsert",
        "key_columns": ["id", "region"],
    }
    edit = author.get(reverse("ui:dataset-edit", kwargs={"dataset_id": dataset.pk}))
    assert edit.context["form"].initial["destination_key_columns"] == "id, region"


def test_editing_a_dataset(world, author):
    url = reverse("ui:dataset-edit", kwargs={"dataset_id": world.dataset.pk})
    form = author.get(url).context["form"]
    assert form.initial["name"] == world.dataset.name and form.initial["source_connection"] == ""
    saved = author.post(
        url, dataset_fields(world, name=world.dataset.name, description="monthly"), follow=True
    )
    assert b"was saved." in saved.content
    world.dataset.refresh_from_db()
    assert world.dataset.description == "monthly"
    clash = author.post(url, dataset_fields(world, name=world.spare_dataset.name))
    assert clash.status_code == 409
    invalid = author.post(url, dataset_fields(world, name="", compression="lzma"))
    assert invalid.status_code == 400 and invalid.context["form"].errors.keys() == {
        "name",
        "compression",
    }


def test_deleting_a_dataset(world, author):
    kept = author.post(
        reverse("ui:dataset-delete", kwargs={"dataset_id": world.dataset.pk}), follow=True
    )
    assert b"has jobs; datasets with history cannot be deleted." in kept.content
    assert Dataset.objects.filter(pk=world.dataset.pk).exists()
    deleted = author.post(
        reverse("ui:dataset-delete", kwargs={"dataset_id": world.spare_dataset.pk}), follow=True
    )
    assert b"was deleted." in deleted.content
    assert not Dataset.objects.filter(pk=world.spare_dataset.pk).exists()


def test_running_a_dataset_that_reads_uploads(world, client):
    client.force_login(world.operator)
    url = reverse("ui:dataset", kwargs={"dataset_id": world.dataset.pk})
    page = client.get(url)
    form = page.context["run_form"]
    assert str(world.upload.pk) in dict(form.fields["upload"].choices)
    key = form.initial["idempotency_key"]
    run = reverse("ui:dataset-run", kwargs={"dataset_id": world.dataset.pk})
    first = client.post(run, {"upload": str(world.upload.pk), "idempotency_key": key})
    again = client.post(run, {"upload": str(world.upload.pk), "idempotency_key": key})
    assert first["Location"] == again["Location"]  # a double submit starts one job
    job = Job.objects.get(dataset=world.dataset, kind=JobKind.RUN, status=JobStatus.QUEUED)
    assert first["Location"] == reverse("ui:job", kwargs={"job_id": job.pk})
    missing = client.post(run, {"upload": "", "idempotency_key": "other"})
    assert missing.status_code == 400 and b"give the upload_id" in missing.content
    someone_elses = make_upload(world.author)
    refused = client.post(run, {"upload": str(someone_elses.pk), "idempotency_key": "x"})
    assert refused.status_code == 400 and refused.context["run_form"].errors["upload"]


def test_running_a_dataset_that_reads_a_bucket(world, author):
    dataset = Dataset.objects.create(
        name="from-the-store",
        schema_version=world.version,
        source_connection=world.connection,
        source_path="people.csv",
    )
    page = author.get(reverse("ui:dataset", kwargs={"dataset_id": dataset.pk}))
    assert page.context["run_form"].fields["upload"].choices == []
    assert b"Run from-the-store" in page.content
    response = author.post(
        reverse("ui:dataset-run", kwargs={"dataset_id": dataset.pk}), {"idempotency_key": "k"}
    )
    assert Job.objects.get(dataset=dataset).upload is None
    assert response.status_code == 302


def test_who_sees_the_run_and_edit_controls(world, client):
    url = reverse("ui:dataset", kwargs={"dataset_id": world.dataset.pk})
    client.force_login(world.viewer)
    viewer = client.get(url)
    assert viewer.context["run_form"] is None and not viewer.context["can_edit"]
    assert b"Run it" not in viewer.content and b"Delete the dataset" not in viewer.content
    client.force_login(world.author)
    author = client.get(url)
    assert b"you have none yet" in author.content  # the author has no uploads of their own
