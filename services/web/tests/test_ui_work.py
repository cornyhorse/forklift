"""Uploads, jobs (live status, results, cancelling) and downloads in the UI."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest
from django.conf import settings
from django.urls import reverse
from django.utils import timezone
from ui_support import GENERATED, PREVIEW, as_json, finish, start, worker_principal
from world import World, make_job, make_upload

from forklift_web.core.choices import (
    ArtifactKind,
    Classification,
    JobKind,
    JobStatus,
    Role,
    UploadStatus,
)
from forklift_web.core.models import Artifact, AuditLog, Job
from forklift_web.policy import Actor
from forklift_web.services import installation
from forklift_web.ui.views import work

pytestmark = pytest.mark.django_db

HTMX = {"HTTP_HX_REQUEST": "true"}


@pytest.fixture
def world():
    return World.build()


@pytest.fixture
def operator(client, world):
    client.force_login(world.operator)
    return client


def upload_url(name, upload):
    return reverse(f"ui:{name}", kwargs={"upload_id": upload.pk})


def job_url(name, job):
    return reverse(f"ui:{name}", kwargs={"job_id": job.pk})


# --------------------------------------------------------------------------- uploads


def test_the_upload_page_gives_the_script_the_api_urls(world, operator):
    installation.update(Actor.for_system("tests"), {"default_classification": "sensitive"})
    page = operator.get(reverse("ui:upload"))
    content = page.content.decode()
    zero = "00000000-0000-0000-0000-000000000000"
    assert 'data-create-url="/api/v1/uploads"' in content
    assert f'data-complete-url="/api/v1/uploads/{zero}/complete"' in content
    assert f'data-parts-url="/api/v1/uploads/{zero}/parts"' in content
    assert f'data-detail-url="/uploads/{zero}/"' in content
    assert '<option value="sensitive" selected>' in content
    assert "upload.js" in content


def test_the_file_list_shows_own_files_and_admins_see_all(world, client):
    mine = make_upload(world.author)
    client.force_login(world.author)
    assert [u.pk for u in client.get(reverse("ui:uploads")).context["page"]] == [mine.pk]
    client.force_login(world.admin)
    listed = {u.pk for u in client.get(reverse("ui:uploads")).context["page"]}
    assert {mine.pk, world.upload.pk, world.spare_upload.pk} <= listed
    assert b"admins see everyone" in client.get(reverse("ui:uploads")).content


def test_what_can_be_done_with_a_file_depends_on_its_classification(world, client, make_user):
    sensitive = make_upload(world.operator, classification=Classification.SENSITIVE)
    client.force_login(world.operator)
    form = client.get(upload_url("upload-detail", sensitive)).context["form"]
    assert [value for value, _ in form.fields["action"].choices] == [
        "run",
        "validate",
        "generate",
        "dataset",
    ]
    refused = client.post(upload_url("upload-run", sensitive), {"action": "preview"})
    assert refused.status_code == 400 and not Job.objects.filter(kind="preview").exists()
    trusted = make_user(Role.OPERATOR, raw_rows=True)
    own = make_upload(trusted, classification=Classification.SENSITIVE)
    client.force_login(trusted)
    form = client.get(upload_url("upload-detail", own)).context["form"]
    assert "preview" in dict(form.fields["action"].choices)


def test_an_actor_that_may_not_run_jobs_is_offered_nothing(world, api_token):
    token, _ = api_token(world.operator, ["uploads:read"])
    assert work._upload_actions(Actor.for_user(world.operator, token=token), world.upload) == []


def test_a_file_that_is_not_complete(world, operator):
    pending = make_upload(world.operator, status=UploadStatus.PENDING)
    page = operator.get(upload_url("upload-detail", pending))
    assert page.context["form"] is None and b"has not been completed" in page.content
    assert b"Delete the file" in page.content
    refused = operator.post(
        upload_url("upload-run", pending), {"action": "generate", "format": "csv"}
    )
    assert refused.status_code == 400
    assert b"only complete uploads can be job inputs" in refused.content
    gone = make_upload(world.operator, status=UploadStatus.DELETED)
    page = operator.get(upload_url("upload-detail", gone))
    assert b"no longer in the store" in page.content and b"Delete the file" not in page.content


def run(client, upload, **fields):
    data = {"format": "csv", "idempotency_key": "", **fields}
    return client.post(upload_url("upload-run", upload), data)


def latest_job() -> Job:
    return Job.objects.order_by("-created_at").first()


def test_running_a_file_with_a_schema_version_and_reading_options(world, operator):
    response = run(
        operator,
        world.upload,
        action="run",
        schema_version=str(world.version.pk),
        delimiter=";",
        encoding="latin-1",
        header_mode="present",
        more_options='{"excess_column_mode": "reject"}',
        idempotency_key="key-1",
    )
    job = latest_job()
    assert response["Location"] == job_url("job", job)
    assert (job.kind, job.schema_version, job.upload) == ("run", world.version, world.upload)
    assert job.spec["input"]["options"] == {
        "excess_column_mode": "reject",
        "delimiter": ";",
        "encoding": "latin-1",
        "header_mode": "present",
    }
    again = run(
        operator,
        world.upload,
        action="run",
        schema_version=str(world.version.pk),
        delimiter=";",
        encoding="latin-1",
        header_mode="present",
        more_options='{"excess_column_mode": "reject"}',
        idempotency_key="key-1",
    )
    assert again["Location"] == response["Location"]  # the same job


def test_running_needs_a_schema_version(world, operator):
    response = run(operator, world.upload, action="run")
    assert response.status_code == 400
    assert b"Choose the schema version to use." in response.content
    dataset = run(operator, world.upload, action="dataset")
    assert b"Choose the dataset to run." in dataset.content


def test_validating_generating_and_previewing_a_file(world, operator):
    run(operator, world.upload, action="validate", schema_version=str(world.version.pk))
    validation = latest_job()
    assert (validation.kind, validation.schema_version) == ("validate_schema", world.version)
    run(operator, world.upload, action="generate", format="excel", sheet="2")
    generated = latest_job()
    assert generated.kind == "generate_schema" and generated.spec["schema"] is None
    assert generated.spec["input"] == {
        "format": "excel",
        "location": {"type": "gateway:upload", "upload_id": str(world.upload.pk)},
        "options": {"sheet": 2},
    }
    run(operator, world.upload, action="preview", sheet="People", format="excel")
    preview = latest_job()
    assert preview.kind == "preview" and preview.spec["input"]["options"] == {"sheet": "People"}
    assert preview.lane == "interactive"


def test_running_a_dataset_on_a_file(world, operator):
    response = run(operator, world.upload, action="dataset", dataset=str(world.dataset.pk))
    job = latest_job()
    assert (job.dataset, job.upload) == (world.dataset, world.upload)
    assert response["Location"] == job_url("job", job)


def test_the_service_layer_refuses_options_the_engine_does_not_know(world, operator):
    response = run(
        operator,
        world.upload,
        action="generate",
        more_options='{"delimiter": "too long"}',
    )
    assert response.status_code == 400
    assert b"delimiter" in response.content and response.context["form"].non_field_errors()


def test_deleting_a_file(world, operator):
    deleted = operator.post(upload_url("upload-delete", world.spare_upload), follow=True)
    assert b"was deleted from the store" in deleted.content
    world.spare_upload.refresh_from_db()
    assert world.spare_upload.status == UploadStatus.DELETED
    busy = operator.post(upload_url("upload-delete", world.upload))  # a queued job reads it
    assert busy.status_code == 409 and b"cancel them first" in busy.content


# --------------------------------------------------------------------------- jobs


def test_the_job_list_filters_and_answers_htmx_with_the_table(world, operator):
    everything = operator.get(reverse("ui:jobs"))
    assert {job.pk for job in everything.context["rows"]} >= {
        world.queued_job.pk,
        world.finished_job.pk,
    }
    queued = operator.get(reverse("ui:jobs"), {"status": "queued"})
    assert [job.pk for job in queued.context["rows"]] == [world.queued_job.pk]
    assert queued.context["rows"][0].can_cancel
    fragment = operator.get(reverse("ui:jobs"), {"kind": "preview", "mine": "on"}, **HTMX)
    content = fragment.content.decode()
    assert "<html" not in content and 'id="job-table"' in content
    assert "No jobs match." in content
    invalid = operator.get(reverse("ui:jobs"), {"status": "lost"})
    assert len(invalid.context["rows"]) >= 2  # an unknown filter value filters nothing


def test_a_queued_job_page_polls_and_a_finished_one_does_not(world, operator):
    queued = operator.get(job_url("job", world.queued_job)).content.decode()
    live = job_url("job-live", world.queued_job)
    assert f'hx-get="{live}?seen=queued"' in queued and 'hx-trigger="every 2s"' in queued
    assert "Waiting for a worker of the batch lane." in queued
    assert 'id="job-announce"' in queued and "Cancel the job" in queued
    finished = operator.get(job_url("job", world.finished_job)).content.decode()
    assert "hx-trigger" not in finished


def test_the_live_fragment_shows_progress_and_announces_a_change_once(world, operator):
    job = start(
        world.queued_job,
        worker_principal(),
        progress={"rows_read": 10, "rows_rejected": 1, "bytes_read": 12},
    )
    fragment = operator.get(job_url("job-live", job), {"seen": "queued"}, **HTMX)
    content = fragment.content.decode()
    assert "<html" not in content
    assert fragment.context["percent"] == 50  # 12 of the upload's 24 bytes
    assert '<progress id="job-progress" max="100" value="50">' in content
    assert "rows read" in content and "rows rejected" in content
    assert 'hx-swap-oob="innerHTML"' in content and "The job is now running." in content
    same = operator.get(job_url("job-live", job), {"seen": "running"}, **HTMX)
    assert b"hx-swap-oob" not in same.content


def test_a_failed_job_shows_its_error_code(world, operator):
    job = finish(
        world.queued_job,
        {"bad_rows.parquet": ("bad_rows", b"PAR1")},
        status="failed",
        error={
            "code": "BAD_ROWS_THRESHOLD_EXCEEDED",
            "message": "2 of 2 rows were rejected; at most 10% may be.",
            "retryable": True,
        },
        warnings=["Column 'x' is not in the schema"],
    )
    content = operator.get(job_url("job", job)).content.decode()
    assert "Why it failed" in content and "BAD_ROWS_THRESHOLD_EXCEEDED" in content
    assert (
        "at most 10% may be." in content and "Running the same job again may succeed." in content
    )
    assert "Column &#x27;x&#x27; is not in the schema" in content
    assert "rows rejected (bad_rows)" in content and "TYPE_MISMATCH:id" in content


def test_cancelled_jobs_say_they_stopped(world, operator):
    operator.post(job_url("job-cancel", world.queued_job))
    content = operator.get(job_url("job", world.queued_job)).content.decode()
    assert "Why it stopped" in content and "CANCELLED" in content


def test_downloads_and_viewers_follow_the_raw_rows_rules(world, client, make_user):
    sensitive = make_upload(world.operator, classification=Classification.SENSITIVE)
    job = make_job(world.operator, sensitive, kind=JobKind.PREVIEW)
    job = finish(
        job,
        {"preview.json": ("preview", as_json(PREVIEW)), "manifest.json": ("manifest", b"{}")},
    )
    preview = job.artifacts.get(kind=ArtifactKind.PREVIEW)
    manifest = job.artifacts.get(kind=ArtifactKind.MANIFEST)
    preview_link = reverse("ui:artifact-download", kwargs={"artifact_id": preview.pk})
    manifest_link = reverse("ui:artifact-download", kwargs={"artifact_id": manifest.pk})
    viewer_api = reverse("api-v1:download_artifact", kwargs={"artifact_id": preview.pk})

    client.force_login(world.viewer)  # no "view raw rows"
    content = client.get(job_url("job", job)).content.decode()
    assert manifest_link in content and preview_link not in content and viewer_api not in content
    assert "Needs the “view raw rows” permission" in content
    assert "showing its rows needs the “view raw rows” permission" in content
    assert client.get(preview_link).status_code == 403

    client.force_login(make_user(Role.VIEWER, raw_rows=True))
    content = client.get(job_url("job", job)).content.decode()
    assert preview_link in content and viewer_api in content
    assert "Show the rows" in content  # sensitive rows load on request, not by themselves
    assert 'data-kind="preview" data-autoload' not in content


def test_previews_of_other_data_load_by_themselves(world, operator):
    job = finish(
        make_job(world.operator, world.upload, kind=JobKind.PREVIEW),
        {"preview.json": ("preview", as_json(PREVIEW))},
    )
    assert b'data-kind="preview" data-autoload' in operator.get(job_url("job", job)).content


def test_a_generated_schema_can_be_saved_by_authors(world, client):
    job = finish(
        make_job(world.operator, world.upload, kind=JobKind.GENERATE_SCHEMA),
        {"schema.json": ("schema", as_json(GENERATED))},
    )
    artifact = job.artifacts.get()
    link = f"{reverse('ui:schema-new')}?from_artifact={artifact.pk}&amp;upload={world.upload.pk}"
    client.force_login(world.operator)
    assert link.encode() not in client.get(job_url("job", job)).content
    client.force_login(world.author)
    content = client.get(job_url("job", job)).content.decode()
    assert link in content and "an upload of" in content  # the author does not own the file


def test_deleted_artifacts_are_marked(world, operator):
    Artifact.objects.filter(pk=world.artifact.pk).update(deleted_at=timezone.now())
    content = operator.get(job_url("job", world.finished_job)).content.decode()
    assert "Deleted by retention" in content
    download = reverse("ui:artifact-download", kwargs={"artifact_id": world.artifact.pk})
    gone = operator.get(download)
    assert gone.status_code == 410 and b"was deleted by retention" in gone.content


def test_a_deleted_viewer_file_is_marked_too(world, operator):
    job = finish(
        make_job(world.operator, world.upload, kind=JobKind.PREVIEW),
        {"preview.json": ("preview", as_json(PREVIEW))},
    )
    Artifact.objects.filter(job=job).update(deleted_at=timezone.now())
    assert b"This file was deleted by retention." in operator.get(job_url("job", job)).content


def test_a_download_redirects_to_a_presigned_url_and_is_audited(world, operator):
    download = reverse("ui:artifact-download", kwargs={"artifact_id": world.artifact.pk})
    response = operator.get(download)
    target = urlsplit(response["Location"])
    store = urlsplit(settings.FORKLIFT_STORE["public_endpoint_url"])
    assert (target.scheme, target.netloc) == (store.scheme, store.netloc)
    assert "attachment" in parse_qs(target.query)["response-content-disposition"][0]
    entry = AuditLog.objects.get(action="artifact.download")
    assert entry.actor == world.operator and entry.object_id == str(world.artifact.pk)


def test_cancelling(world, client):
    client.force_login(world.operator)
    cancelled = client.post(job_url("job-cancel", world.queued_job), follow=True)
    assert b"The run job was cancelled." in cancelled.content
    running = start(make_job(world.operator, world.upload), worker_principal())
    back_to = reverse("ui:jobs") + "?status=running"
    requested = client.post(job_url("job-cancel", running), {"next": back_to})
    assert requested["Location"] == back_to
    running.refresh_from_db()
    assert running.cancel_requested_at is not None and running.status == JobStatus.RUNNING
    page = client.get(job_url("job", running)).content.decode()
    assert "cancellation requested" in page
    elsewhere = client.post(job_url("job-cancel", running), {"next": "https://evil.example/"})
    assert elsewhere["Location"] == job_url("job", running)  # not off the site
    done = client.post(job_url("job-cancel", world.finished_job))
    assert done.status_code == 409 and b"already finished" in done.content
    listed = client.get(reverse("ui:jobs"), {"status": "running"}).content.decode()
    assert "(cancelling)" in listed
