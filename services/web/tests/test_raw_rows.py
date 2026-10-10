"""Sensitive data: previewing it and downloading its rows need "view raw rows", which admins
always have and the other roles only when granted; downloads are presigned, short-lived and
audited."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest
from django.utils import timezone
from world import (
    SCHEMA_DOCUMENT,
    World,
    api_token,
    make_artifact,
    make_job,
    make_upload,
    make_user,
)

from forklift_web.core.choices import ArtifactKind, JobStatus, Role
from forklift_web.core.models import Artifact, AuditLog, Dataset
from forklift_web.errors import Gone, NotFound, PermissionDenied
from forklift_web.policy import Actor
from forklift_web.services import artifacts, jobs
from forklift_web.services.jobs import JobRequest

pytestmark = pytest.mark.django_db


@pytest.fixture
def sensitive_job():
    world = World.build()
    upload = make_upload(world.operator, classification="sensitive")
    job = make_job(world.operator, upload, status=JobStatus.SUCCEEDED)
    files = {
        kind: make_artifact(job, kind)
        for kind in (
            ArtifactKind.DATA,
            ArtifactKind.BAD_ROWS,
            ArtifactKind.PREVIEW,
            ArtifactKind.MANIFEST,
            ArtifactKind.METADATA,
            ArtifactKind.SCHEMA,
        )
    }
    return world, upload, job, files


@pytest.mark.parametrize("role", [Role.VIEWER, Role.OPERATOR, Role.AUTHOR])
def test_rows_of_sensitive_jobs_need_raw_rows(sensitive_job, as_user, role):
    _, _, _, files = sensitive_job
    without = as_user(make_user(role))
    with_raw = as_user(make_user(role, raw_rows=True))
    for kind, artifact in files.items():
        path = f"/api/v1/artifacts/{artifact.pk}/download"
        refused = kind in {"data", "bad_rows", "preview"}
        response = without.get(path)
        assert response.status_code == (403 if refused else 200), (kind, response.content)
        if refused:
            assert response.json()["code"] == "raw_rows_required"
        assert with_raw.get(path).status_code == 200
    # Seeing that the files exist (names, sizes, counts) needs no raw rows
    listed = without.get(f"/api/v1/jobs/{artifact.job_id}/artifacts").json()
    assert len(listed) == len(files)


def test_admins_download_rows_of_sensitive_jobs_without_the_grant(sensitive_job, as_user):
    _, _, _, files = sensitive_job
    admin = make_user(Role.ADMIN)
    caller = as_user(admin)
    for artifact in files.values():
        assert caller.get(f"/api/v1/artifacts/{artifact.pk}/download").status_code == 200
    assert AuditLog.objects.filter(actor=admin, action="artifact.download").count() == (len(files))
    # Demoted, the admin is back to what the grant says
    admin.role = Role.AUTHOR
    admin.save()
    path = f"/api/v1/artifacts/{files['data'].pk}/download"
    assert as_user(admin).get(path).status_code == 403


def test_a_token_carries_its_owner_s_raw_rows_permission(sensitive_job, as_token):
    _, _, _, files = sensitive_job
    _, raw = api_token(make_user(Role.VIEWER, raw_rows=True), ["artifacts:read"])
    _, plain = api_token(make_user(Role.VIEWER), ["artifacts:read"])
    path = f"/api/v1/artifacts/{files['bad_rows'].pk}/download"
    assert as_token(raw).get(path).status_code == 200
    assert as_token(plain).get(path).status_code == 403


def test_previewing_sensitive_inputs_needs_raw_rows(sensitive_job):
    world, upload, _, _ = sensitive_job
    operator = Actor.for_user(world.operator)
    with pytest.raises(PermissionDenied) as raised:
        jobs.create_job(operator, JobRequest(kind="preview", upload_id=upload.pk))
    assert raised.value.code == "raw_rows_required"
    # Running it, validating against it and generating a schema from it reveal no rows
    jobs.create_job(operator, JobRequest(kind="run", upload_id=upload.pk, schema=SCHEMA_DOCUMENT))
    jobs.create_job(operator, JobRequest(kind="generate_schema", upload_id=upload.pk))
    world.operator.can_view_raw_rows = True
    world.operator.save()
    preview, _ = jobs.create_job(
        Actor.for_user(world.operator), JobRequest(kind="preview", upload_id=upload.pk)
    )
    assert preview.classification == "sensitive"


def test_previewing_a_sensitive_dataset_needs_raw_rows(as_user):
    world = World.build()
    Dataset.objects.filter(pk=world.dataset.pk).update(classification="sensitive")
    caller = as_user(world.operator)
    body = {
        "kind": "preview",
        "dataset_id": str(world.dataset.pk),
        "upload_id": str(world.upload.pk),
    }
    assert caller.post("/api/v1/jobs", body).status_code == 403
    run = {"kind": "run", "dataset_id": str(world.dataset.pk), "upload_id": str(world.upload.pk)}
    created = caller.post("/api/v1/jobs", run)
    assert created.status_code == 201 and created.json()["classification"] == "sensitive"


def test_downloads_are_presigned_short_lived_and_audited(as_user):
    world = World.build()
    caller = as_user(world.viewer)
    response = caller.get(f"/api/v1/artifacts/{world.artifact.pk}/download")
    body = response.json()
    query = parse_qs(urlsplit(body["url"]).query)
    assert query["X-Amz-Expires"] == ["300"]
    assert "attachment" in query["response-content-disposition"][0]
    assert body["filename"] == f"{world.finished_job.pk}-data.parquet"
    entry = AuditLog.objects.get(action="artifact.download")
    assert entry.actor == world.viewer and entry.object_id == str(world.artifact.pk)
    assert entry.details == {
        "job_id": str(world.finished_job.pk),
        "kind": "data",
        "classification": "internal",
    }
    assert body["url"] not in str(entry.details)
    redirect = caller.get(f"/api/v1/artifacts/{world.artifact.pk}/download?redirect=true")
    assert redirect.status_code == 302 and redirect["Location"].startswith("http")
    assert AuditLog.objects.filter(action="artifact.download").count() == 2


def test_deleted_artifacts_are_gone(as_user):
    world = World.build()
    Artifact.objects.filter(pk=world.artifact.pk).update(deleted_at=timezone.now())
    viewer = Actor.for_user(world.viewer)
    with pytest.raises(Gone, match="deleted by retention"):
        artifacts.download(viewer, world.artifact.pk)
    assert (
        as_user(world.viewer).get(f"/api/v1/artifacts/{world.artifact.pk}/download").status_code
        == 410
    )
    with pytest.raises(NotFound):
        artifacts.get_artifact(viewer, "00000000-0000-0000-0000-000000000000")
    detail = as_user(world.viewer).get(f"/api/v1/artifacts/{world.artifact.pk}").json()
    assert detail["deleted_at"] is not None and detail["expires_at"] is None


def test_nested_artifact_names_make_flat_download_names():
    world = World.build()
    nested = make_artifact(world.finished_job, ArtifactKind.REPORT, name="reports/run report.json")
    download = artifacts.download(Actor.for_user(world.viewer), nested.pk)
    assert download.filename == f"{world.finished_job.pk}-reports-run report.json"
