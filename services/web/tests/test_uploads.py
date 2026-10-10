"""Uploads: presigned PUTs and multipart uploads against RustFS, completed by HEAD."""

from __future__ import annotations

from datetime import timedelta

import pytest
from conftest import get_url, put_url
from django.utils import timezone
from world import World, make_job, make_upload, make_user

from forklift_web import storage
from forklift_web.core.choices import JobStatus, Role, UploadStatus
from forklift_web.core.models import AuditLog, Upload
from forklift_web.errors import Conflict, Gone, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Actor
from forklift_web.services import installation, uploads

pytestmark = pytest.mark.django_db

MIB = 1024 * 1024


@pytest.fixture
def operator():
    return Actor.for_user(make_user(Role.OPERATOR))


def test_single_upload(operator, s3):
    ticket = uploads.create_upload(
        operator,
        filename="Sales Q1 (final).csv",
        size=11,
        content_type="text/csv",
        sha256="a" * 64,
    )
    upload = ticket.upload
    assert ticket.url and ticket.method == "PUT" and not ticket.parts
    assert upload.key == f"uploads/{upload.id}/Sales_Q1_final_.csv"
    assert upload.filename == "Sales Q1 (final).csv" and upload.status == UploadStatus.PENDING
    with pytest.raises(Conflict, match="Nothing has been uploaded"):
        uploads.complete_upload(operator, upload.pk)
    put_url(ticket.url, b"0123456789")
    with pytest.raises(Conflict, match="has 10 bytes but 11 were declared"):
        uploads.complete_upload(operator, upload.pk)
    put_url(ticket.url, b"0123456789\n")
    done = uploads.complete_upload(operator, upload.pk)
    assert done.status == UploadStatus.COMPLETE and done.etag and done.completed_at
    assert uploads.complete_upload(operator, upload.pk).status == UploadStatus.COMPLETE
    head = s3.head_object(Bucket=storage.store().bucket, Key=upload.key)
    assert head["ContentLength"] == 11


def test_upload_urls_are_signed_for_the_public_endpoint(operator, settings):
    settings.FORKLIFT_STORE = {
        **settings.FORKLIFT_STORE,
        "public_endpoint_url": "https://files.example.org",
    }
    storage.reset_store()
    try:
        ticket = uploads.create_upload(operator, filename="a.csv", size=3)
        assert ticket.url.startswith(f"https://files.example.org/{storage.store().bucket}/")
    finally:
        storage.reset_store()


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"filename": "../etc/passwd"}, "not a path"),
        ({"filename": "a\\b.csv"}, "not a path"),
        ({"filename": "  "}, "not a path"),
        ({"filename": ".."}, "not a path"),
        ({"filename": "bell\x07.csv"}, "not a path"),
        ({"filename": "x" * 256}, "not a path"),
        ({"size": 0}, "an empty file has no rows"),
        ({"sha256": "ABC"}, "64 lower-case hexadecimal"),
        ({"classification": "secret"}, "Unknown classification"),
    ],
)
def test_upload_validation(operator, fields, message):
    with pytest.raises(InvalidRequest, match=message):
        uploads.create_upload(operator, **{"filename": "a.csv", "size": 10, **fields})


def test_upload_size_limit_and_default_classification(operator, admin_actor):
    installation.update(admin_actor, {"upload_max_bytes": 100, "default_classification": "public"})
    with pytest.raises(InvalidRequest, match="at most 100 bytes"):
        uploads.create_upload(operator, filename="a.csv", size=101)
    ticket = uploads.create_upload(operator, filename="...", size=5)
    assert ticket.upload.classification == "public"
    assert ticket.upload.key.endswith("/upload")


def test_multipart_upload(operator, admin_actor):
    installation.update(
        admin_actor, {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB}
    )
    data = b"x" * (5 * MIB) + b"tail\n"
    ticket = uploads.create_upload(operator, filename="big.csv", size=len(data))
    upload = ticket.upload
    assert ticket.url is None and upload.is_multipart
    assert (ticket.part_size, ticket.part_count) == (5 * MIB, 2)
    assert [part["part_number"] for part in ticket.parts] == [1, 2]
    Upload.objects.filter(pk=upload.pk).update(expires_at=timezone.now())
    fresh = uploads.part_urls(operator, upload.pk, [2])
    assert Upload.objects.get(pk=upload.pk).expires_at > timezone.now() + timedelta(minutes=50)
    etags = [
        put_url(ticket.parts[0]["url"], data[: 5 * MIB])["ETag"],
        put_url(fresh[0]["url"], data[5 * MIB :])["ETag"],
    ]
    with pytest.raises(InvalidRequest, match="needs the part numbers and ETags"):
        uploads.complete_upload(operator, upload.pk)
    with pytest.raises(InvalidRequest, match="has 2 parts"):
        uploads.complete_upload(operator, upload.pk, parts=[{"part_number": 1, "etag": etags[0]}])
    done = uploads.complete_upload(
        operator,
        upload.pk,
        parts=[{"part_number": 2, "etag": etags[1]}, {"part_number": 1, "etag": etags[0]}],
    )
    assert done.status == UploadStatus.COMPLETE
    url = storage.store().presign_get(upload.key, expires=60, audience=storage.Audience.GATEWAY)
    assert get_url(url)[-5:] == b"tail\n"
    with pytest.raises(Conflict, match="not a pending multipart upload"):
        uploads.part_urls(operator, upload.pk, [1])


def test_multipart_part_rules(operator, admin_actor):
    installation.update(
        admin_actor, {"multipart_threshold_bytes": 5 * MIB, "multipart_part_bytes": 5 * MIB}
    )
    ticket = uploads.create_upload(operator, filename="big.csv", size=12 * MIB)
    assert ticket.part_count == 3
    for numbers in ([], [0], [4], list(range(1, 1002))):
        with pytest.raises(InvalidRequest, match="part_numbers must name"):
            uploads.part_urls(operator, ticket.upload.pk, numbers)
    single = uploads.create_upload(operator, filename="small.csv", size=10)
    with pytest.raises(Conflict, match="not a pending multipart upload"):
        uploads.part_urls(operator, single.upload.pk, [1])


def test_huge_uploads_get_larger_parts(operator):
    ticket = uploads.create_upload(operator, filename="huge.csv", size=2 * 1024**4)
    assert ticket.part_count <= uploads.MAX_PARTS and ticket.part_size % MIB == 0
    assert len(ticket.parts) == uploads.PARTS_PER_RESPONSE
    uploads.delete_upload(operator, ticket.upload.pk)  # aborts the multipart upload


def test_uploads_belong_to_their_uploader():
    world = World.build()
    author = Actor.for_user(world.author)
    with pytest.raises(PermissionDenied, match="belongs to another user"):
        uploads.get_upload(author, world.upload.pk)
    with pytest.raises(PermissionDenied):
        uploads.complete_upload(author, world.upload.pk)
    assert list(uploads.list_uploads(author)) == []
    assert set(uploads.list_uploads(Actor.for_user(world.admin))) >= {world.upload}
    with pytest.raises(NotFound):
        uploads.get_upload(author, "00000000-0000-0000-0000-000000000000")
    with pytest.raises(NotFound):
        uploads.complete_upload(author, "00000000-0000-0000-0000-000000000000")


def test_deleting_uploads():
    world = World.build()
    operator = Actor.for_user(world.operator)
    with pytest.raises(Conflict, match="input of 1 queued or running jobs"):
        uploads.delete_upload(operator, world.upload.pk)
    deleted = uploads.delete_upload(operator, world.spare_upload.pk)
    assert deleted.status == UploadStatus.DELETED and deleted.deleted_at
    assert AuditLog.objects.get(action="upload.delete").object_id == str(world.spare_upload.pk)
    with pytest.raises(Gone, match="already deleted"):
        uploads.delete_upload(operator, world.spare_upload.pk)
    with pytest.raises(Gone, match="is deleted; start a new upload"):
        uploads.complete_upload(operator, world.spare_upload.pk)


def test_only_complete_own_uploads_are_job_inputs():
    world = World.build()
    operator = Actor.for_user(world.operator)
    pending = make_upload(world.operator, status=UploadStatus.PENDING)
    with pytest.raises(InvalidRequest, match="only complete uploads can be job inputs"):
        uploads.usable_upload(operator, pending.pk)
    with pytest.raises(InvalidRequest, match="There is no upload"):
        uploads.usable_upload(operator, "00000000-0000-0000-0000-000000000000")
    assert uploads.usable_upload(operator, world.upload.pk) == world.upload
    assert uploads.classification_for(world.upload, "sensitive") == "sensitive"
    assert uploads.classification_for(world.upload, "public") == "internal"
    assert uploads.classification_for(world.upload, None) == "internal"


def test_upload_api(as_user, make_user):
    caller = as_user(make_user(Role.OPERATOR))
    ticket = caller.post("/api/v1/uploads", {"filename": "a.csv", "size": 4}).json()
    assert ticket["upload"]["multipart"] is False and ticket["headers"] == {}
    put_url(ticket["url"], b"a,b\n")
    upload_id = ticket["upload"]["id"]
    assert caller.post(f"/api/v1/uploads/{upload_id}/complete").json()["status"] == "complete"
    assert caller.get(f"/api/v1/uploads/{upload_id}").json()["size"] == 4
    assert caller.get("/api/v1/uploads").json()["count"] == 1
    assert caller.delete(f"/api/v1/uploads/{upload_id}").status_code == 204
    gone = caller.post(f"/api/v1/uploads/{upload_id}/complete", {"parts": None})
    assert gone.status_code == 410 and gone.json()["code"] == "gone"


def test_finished_jobs_do_not_keep_an_upload(operator):
    upload = make_upload(operator.user)
    make_job(operator.user, upload, status=JobStatus.SUCCEEDED)
    assert uploads.delete_upload(operator, upload.pk).status == UploadStatus.DELETED


def test_store_errors_are_reported_without_secrets(operator, monkeypatch):
    upload = make_upload(operator.user, status=UploadStatus.PENDING)
    Upload.objects.filter(pk=upload.pk).update(expires_at=timezone.now() - timedelta(hours=1))
    monkeypatch.setitem(storage.store()._endpoints, storage.Audience.GATEWAY, "http://127.0.0.1:9")
    storage.store()._clients.clear()
    try:
        from forklift_web.errors import StoreUnavailable

        with pytest.raises(StoreUnavailable, match="failed HEAD of uploads/"):
            uploads.complete_upload(operator, upload.pk)
    finally:
        storage.reset_store()
