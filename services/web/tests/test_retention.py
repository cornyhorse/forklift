"""Retention policies (installation, classification, dataset) and the sweeper."""

from __future__ import annotations

from datetime import timedelta

import pytest
from conftest import put_url
from django.utils import timezone
from world import World, make_artifact, make_job, make_upload, make_user

from forklift_web import storage
from forklift_web.core.choices import JobStatus, Role, UploadStatus
from forklift_web.core.models import Artifact, AuditLog, Dataset, Job, RetentionPolicy, Upload
from forklift_web.errors import InvalidRequest, NotFound, StoreUnavailable
from forklift_web.policy import Actor
from forklift_web.services import retention

pytestmark = pytest.mark.django_db

SYSTEM = Actor.for_system("sweep_retention")


def store_object(key: str, data: bytes = b"x") -> None:
    put_url(storage.store().presign_put(key, expires=60, audience=storage.Audience.GATEWAY), data)


def exists(key: str) -> bool:
    return storage.store().head(key) is not None


def age(model, pk, days: int, field: str = "created_at") -> None:
    model.objects.filter(pk=pk).update(**{field: timezone.now() - timedelta(days=days)})


def test_the_most_specific_level_wins(admin_actor):
    world = World.build()
    retention.set_policy(admin_actor, "installation", days={"data": 90, "bad_rows": 30})
    retention.set_policy(
        admin_actor, "classification", classification="internal", days={"data": 60}
    )
    retention.set_policy(admin_actor, "dataset", dataset_id=world.dataset.pk, days={"data": None})
    days = retention.Policies.load().days
    assert days("data", classification="sensitive") == 90
    assert days("data", classification="public") == 30  # the world's public policy
    assert days("data", classification="internal") == 60
    assert days("bad_rows", classification="internal") == 30
    assert days("data", classification="internal", dataset_id=world.dataset.pk) is None
    assert days("bad_rows", classification="internal", dataset_id=world.dataset.pk) == 30
    assert days("uploads", classification="sensitive") is None
    artifact = world.artifact  # data of a dataset job: kept until deleted
    assert retention.artifact_expires_at(artifact) is None
    bad_rows = make_artifact(world.finished_job, "bad_rows")
    assert retention.artifact_expires_at(bad_rows) == bad_rows.created_at + timedelta(days=30)
    assert retention.upload_expires_at(world.upload) is None
    retention.set_policy(admin_actor, "installation", days={"uploads": 7})
    expected = world.upload.completed_at + timedelta(days=7)
    assert retention.upload_expires_at(world.upload) == expected


def test_a_new_installation_warns_that_sensitive_data_never_expires(admin_actor):
    overview = retention.overview(admin_actor)
    assert overview["policies"] == [] and len(overview["warnings"]) == 3
    assert all("Sensitive" in warning for warning in overview["warnings"])
    assert overview["kinds"] == [
        "uploads",
        "data",
        "bad_rows",
        "previews",
        "metadata",
        "job_records",
    ]
    world = World.build()
    Dataset.objects.filter(pk=world.dataset.pk).update(classification="sensitive")
    retention.set_policy(
        admin_actor,
        "classification",
        classification="sensitive",
        days={"uploads": 30, "data": 30, "bad_rows": 7},
    )
    assert retention.overview(admin_actor)["warnings"] == []
    retention.set_policy(
        admin_actor, "dataset", dataset_id=world.dataset.pk, days={"bad_rows": None}
    )
    [warning] = retention.overview(admin_actor)["warnings"]
    assert f"Sensitive dataset {world.dataset.name!r} keeps its bad_rows" in warning


def test_setting_and_deleting_policies_is_audited(admin_actor):
    world = World.build()
    policy = retention.set_policy(
        admin_actor, "classification", classification="public", days={"data": 10}
    )
    assert policy.days == {"data": 10}  # replaces the world's policy of the same level
    entry = AuditLog.objects.get(action="retention.set")
    assert entry.details["from"] == {"data": 30} and entry.details["to"] == {"data": 10}
    retention.delete_policy(admin_actor, "dataset", dataset_id=world.spare_dataset.pk)
    assert not RetentionPolicy.objects.filter(dataset=world.spare_dataset).exists()
    assert AuditLog.objects.get(action="retention.delete_policy").details["days"] == {"data": 1}
    with pytest.raises(NotFound, match="no dataset retention policy"):
        retention.delete_policy(admin_actor, "dataset", dataset_id=world.spare_dataset.pk)


@pytest.mark.parametrize(
    "scope,fields,message",
    [
        ("installation", {"days": []}, "days must map retention kinds"),
        ("installation", {"days": {"videos": 1}}, "Unknown retention kinds: videos"),
        ("installation", {"days": {"data": -1}}, "whole number of days"),
        ("installation", {"days": {"data": True}}, "whole number of days"),
        ("installation", {"days": {"data": "7"}}, "whole number of days"),
        ("classification", {"days": {}, "classification": "secret"}, "Unknown classification"),
        (
            "dataset",
            {"days": {}, "dataset_id": "00000000-0000-0000-0000-000000000000"},
            "no dataset with id",
        ),
        ("planet", {"days": {}}, "Unknown retention scope 'planet'"),
    ],
)
def test_policy_validation(admin_actor, scope, fields, message):
    with pytest.raises((InvalidRequest, NotFound), match=message):
        retention.set_policy(admin_actor, scope, **fields)


def test_the_sweeper_deletes_expired_artifacts_and_records_each_deletion(admin_actor):
    world = World.build()
    old_data = world.artifact
    old_bad = make_artifact(world.finished_job, "bad_rows")
    fresh_bad = make_artifact(
        make_job(world.operator, world.upload, status=JobStatus.SUCCEEDED), "bad_rows"
    )
    for artifact in (old_data, old_bad, fresh_bad):
        store_object(artifact.key)
    age(Artifact, old_data.pk, 40)
    age(Artifact, old_bad.pk, 40)
    retention.set_policy(admin_actor, "installation", days={"bad_rows": 30, "data": 30})
    retention.set_policy(admin_actor, "dataset", dataset_id=world.dataset.pk, days={"data": None})

    dry = retention.sweep(admin_actor, dry_run=True)
    assert (dry.dry_run, dry.artifacts) == (True, 1) and exists(old_bad.key)
    report = retention.sweep(admin_actor)
    assert (report.artifacts, report.errors) == (1, [])
    assert not exists(old_bad.key) and exists(old_data.key) and exists(fresh_bad.key)
    assert Artifact.objects.get(pk=old_bad.pk).deleted_at is not None
    purge = AuditLog.objects.get(action="retention.purge")
    assert purge.details == {"kind": "bad_rows", "key": old_bad.key}
    assert purge.object_id == str(old_bad.pk) and purge.actor == admin_actor.user
    assert retention.sweep(admin_actor).artifacts == 0  # deleted artifacts are not swept again


def test_job_records_outlive_their_artifacts_unless_they_expire_too(admin_actor):
    world = World.build()
    age(Job, world.finished_job.pk, 100, "finished_at")
    retention.set_policy(admin_actor, "installation", days={"job_records": 60})
    assert retention.sweep(SYSTEM).jobs == 0  # its data artifact still exists
    Artifact.objects.filter(job=world.finished_job).update(deleted_at=timezone.now())
    other = make_job(world.operator, world.upload, status=JobStatus.FAILED, dataset=world.dataset)
    age(Job, other.pk, 100, "finished_at")  # the same context: counted once, not per row
    assert retention.sweep(SYSTEM, dry_run=True).jobs == 2
    assert retention.sweep(SYSTEM).jobs == 2
    assert not Job.objects.filter(pk=world.finished_job.pk).exists()
    assert Job.objects.filter(pk=world.queued_job.pk).exists()  # not finished
    entries = AuditLog.objects.filter(action="retention.purge", object_type="job")
    assert {e.object_id for e in entries} == {str(world.finished_job.pk), str(other.pk)}
    assert {(e.actor_label, e.actor) for e in entries} == {("system:sweep_retention", None)}


def test_uploads_expire_unless_a_job_still_needs_them(admin_actor):
    world = World.build()
    for upload in (world.upload, world.spare_upload):
        store_object(upload.key)
        age(Upload, upload.pk, 10, "completed_at")
    retention.set_policy(
        admin_actor, "classification", classification="internal", days={"uploads": 5}
    )
    assert retention.sweep(admin_actor, dry_run=True).uploads == 1
    report = retention.sweep(admin_actor)
    assert report.uploads == 1
    assert Upload.objects.get(pk=world.spare_upload.pk).status == UploadStatus.DELETED
    assert Upload.objects.get(pk=world.upload.pk).status == UploadStatus.COMPLETE  # queued job
    assert not exists(world.spare_upload.key) and exists(world.upload.key)


def test_pending_uploads_expire_with_their_urls(admin_actor):
    owner = make_user(Role.OPERATOR)
    single = make_upload(owner, status=UploadStatus.PENDING)
    multipart = make_upload(owner, status=UploadStatus.PENDING, size=20 * 1024 * 1024)
    multipart.part_size = 8 * 1024 * 1024
    multipart.multipart_upload_id = storage.store().create_multipart(multipart.key)
    multipart.save()
    fresh = make_upload(owner, status=UploadStatus.PENDING)
    Upload.objects.filter(pk__in=[single.pk, multipart.pk]).update(
        expires_at=timezone.now() - timedelta(minutes=1)
    )
    assert retention.sweep(admin_actor, dry_run=True).expired_uploads == 2
    assert retention.sweep(admin_actor).expired_uploads == 2
    assert Upload.objects.get(pk=multipart.pk).status == UploadStatus.EXPIRED
    assert Upload.objects.get(pk=single.pk).deleted_at is not None
    assert Upload.objects.get(pk=fresh.pk).status == UploadStatus.PENDING
    client = storage.store().client(storage.Purpose.DELETE)
    pending = client.list_multipart_uploads(Bucket=storage.store().bucket).get("Uploads", [])
    assert multipart.multipart_upload_id not in {u["UploadId"] for u in pending}


def test_store_failures_are_reported_and_the_records_kept(admin_actor, monkeypatch):
    world = World.build()
    age(Artifact, world.artifact.pk, 40)
    age(Upload, world.spare_upload.pk, 40, "completed_at")
    retention.set_policy(admin_actor, "installation", days={"data": 1, "uploads": 1})

    def refuse(self, key):
        raise StoreUnavailable(f"The object store refused or failed DELETE of {key} (denied).")

    monkeypatch.setattr(storage.Bucket, "delete", refuse)
    report = retention.sweep(admin_actor)
    assert (report.artifacts, report.uploads) == (1, 1) and len(report.errors) == 2
    assert Artifact.objects.get(pk=world.artifact.pk).deleted_at is None
    assert Upload.objects.get(pk=world.spare_upload.pk).status == UploadStatus.COMPLETE
    assert not AuditLog.objects.filter(action="retention.purge").exists()


def test_retention_api(as_user, admin):
    World.build()
    caller = as_user(admin)
    body = caller.get("/api/v1/admin/retention").json()
    assert {p["scope"] for p in body["policies"]} == {"installation", "classification", "dataset"}
    put = caller.put("/api/v1/admin/retention/installation", {"days": {"data": 3}})
    assert put.status_code == 200 and put.json()["days"] == {"data": 3}
    assert caller.post("/api/v1/admin/retention/sweep", {"dry_run": True}).json()["dry_run"]
    bad = caller.put("/api/v1/admin/retention/classifications/secret", {"days": {}})
    assert bad.status_code == 400


def test_pending_uploads_that_cannot_be_removed_stay_pending(admin_actor, monkeypatch):
    owner = make_user(Role.OPERATOR)
    pending = make_upload(owner, status=UploadStatus.PENDING)
    Upload.objects.filter(pk=pending.pk).update(expires_at=timezone.now() - timedelta(minutes=1))

    def refuse(upload):
        raise StoreUnavailable("The object store refused or failed DELETE of x (denied).")

    monkeypatch.setattr(retention, "remove_object", refuse)
    report = retention.sweep(admin_actor)
    assert report.expired_uploads == 1 and report.errors == [
        f"pending upload {pending.pk}: The object store refused or failed DELETE of x (denied)."
    ]
    assert Upload.objects.get(pk=pending.pk).status == UploadStatus.PENDING
