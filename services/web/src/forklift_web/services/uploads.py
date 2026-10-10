"""Uploads: files go from the browser or client straight into the store.

1. :func:`create_upload` records the upload and returns a presigned PUT URL, or, for files
   above ``multipart_threshold_bytes``, a multipart upload with one presigned URL per part
   (:func:`part_urls` gives fresh ones for a long upload).
2. The client PUTs the bytes (every part, for a multipart upload).
3. :func:`complete_upload` completes the multipart upload and checks with a HEAD that the
   object is there with the declared size. Only complete uploads can be the input of a job.

The gateway never sees the bytes.
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Optional

from django.db import transaction
from django.utils import timezone

from forklift_web import storage
from forklift_web.core.choices import Classification, JobStatus, UploadStatus, most_restrictive
from forklift_web.core.models import Upload
from forklift_web.errors import Conflict, Gone, InvalidRequest, NotFound
from forklift_web.policy import Action, Actor, check, visible_uploads
from forklift_web.services import audit, installation

MAX_PARTS = 10_000
PARTS_PER_RESPONSE = 1_000
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_KEY_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass
class UploadTicket:
    """What the client needs to upload: one PUT URL, or a multipart upload's part URLs."""

    upload: Upload
    expires_at: datetime
    method: str = "PUT"
    url: Optional[str] = None
    headers: dict = field(default_factory=dict)
    part_size: Optional[int] = None
    part_count: Optional[int] = None
    parts: list = field(default_factory=list)


def _clean_filename(filename: str) -> str:
    name = unicodedata.normalize("NFC", filename).strip()
    if (
        not name
        or name in {".", ".."}
        or len(name) > 255
        or any(ch in name for ch in "/\\")
        or any(unicodedata.category(ch).startswith("C") for ch in name)
    ):
        raise InvalidRequest(
            "filename must be a file name (1 to 255 characters, no '/', '\\' or control "
            "characters), not a path."
        )
    return name


def _key_name(filename: str) -> str:
    """The file name as used in the object key: safe ASCII characters only."""
    return _KEY_UNSAFE.sub("_", filename).strip("._") or "upload"


def _part_size(size: int, settings: dict) -> int:
    part = max(settings["multipart_part_bytes"], math.ceil(size / MAX_PARTS))
    return math.ceil(part / installation.MIB) * installation.MIB


def _part_urls(upload: Upload, numbers: Iterable[int], expires: int) -> list:
    bucket = storage.store()
    return [
        {
            "part_number": number,
            "url": bucket.presign_part(
                upload.key,
                upload.multipart_upload_id,
                number,
                expires=expires,
                audience=storage.Audience.PUBLIC,
            ),
        }
        for number in numbers
    ]


def create_upload(
    actor: Actor,
    *,
    filename: str,
    size: int,
    content_type: str = "",
    sha256: str = "",
    classification: Optional[str] = None,
) -> UploadTicket:
    check(actor, Action.UPLOAD_CREATE)
    settings = installation.current()
    filename = _clean_filename(filename)
    if size < 1:
        raise InvalidRequest("size must be the file's size in bytes (an empty file has no rows).")
    if size > settings["upload_max_bytes"]:
        raise InvalidRequest(
            f"The file is {size} bytes; this installation accepts uploads of at most "
            f"{settings['upload_max_bytes']} bytes (installation setting upload_max_bytes)."
        )
    if sha256 and not _SHA256.match(sha256):
        raise InvalidRequest("sha256 must be 64 lower-case hexadecimal characters.")
    classification = classification or settings["default_classification"]
    if classification not in Classification.values:
        raise InvalidRequest(
            f"Unknown classification {classification!r}; classifications: "
            f"{', '.join(Classification.values)}."
        )
    expires = settings["upload_url_seconds"]
    now = timezone.now()
    upload = Upload(
        filename=filename,
        content_type=content_type[:255],
        size=size,
        declared_sha256=sha256,
        classification=classification,
        uploaded_by=actor.user,
        created_at=now,
        expires_at=now + timedelta(seconds=expires),
    )
    upload.key = f"uploads/{upload.id}/{_key_name(filename)}"
    bucket = storage.store()
    ticket = UploadTicket(upload=upload, expires_at=upload.expires_at)
    if size > settings["multipart_threshold_bytes"]:
        upload.part_size = _part_size(size, settings)
        upload.multipart_upload_id = bucket.create_multipart(upload.key)
        upload.save()
        ticket.part_size = upload.part_size
        ticket.part_count = math.ceil(size / upload.part_size)
        ticket.parts = _part_urls(
            upload, range(1, min(ticket.part_count, PARTS_PER_RESPONSE) + 1), expires
        )
    else:
        upload.save()
        ticket.url = bucket.presign_put(
            upload.key, expires=expires, audience=storage.Audience.PUBLIC
        )
    return ticket


def list_uploads(actor: Actor):
    return visible_uploads(actor, Upload.objects.select_related("uploaded_by"))


def get_upload(actor: Actor, upload_id) -> Upload:
    check(actor, Action.UPLOAD_VIEW)
    upload = Upload.objects.select_related("uploaded_by").filter(pk=upload_id).first()
    if upload is None:
        raise NotFound(f"There is no upload with id {upload_id}.")
    check(actor, Action.UPLOAD_VIEW, upload)
    return upload


def _changeable(actor: Actor, upload_id) -> Upload:
    check(actor, Action.UPLOAD_CHANGE)
    upload = Upload.objects.select_for_update().filter(pk=upload_id).first()
    if upload is None:
        raise NotFound(f"There is no upload with id {upload_id}.")
    check(actor, Action.UPLOAD_CHANGE, upload)
    return upload


def part_urls(actor: Actor, upload_id, part_numbers: Iterable[int]) -> list:
    """Fresh presigned URLs for some parts of a pending multipart upload. The upload's own
    expiry moves with them, so the sweeper does not abort an upload that is still going on."""
    expires = installation.get("upload_url_seconds")
    with transaction.atomic():
        upload = _changeable(actor, upload_id)
        if not upload.is_multipart or upload.status != UploadStatus.PENDING:
            raise Conflict(f"Upload {upload.id} is not a pending multipart upload.")
        count = math.ceil(upload.size / upload.part_size)
        numbers = sorted(set(part_numbers))
        if (
            not numbers
            or len(numbers) > PARTS_PER_RESPONSE
            or numbers[0] < 1
            or numbers[-1] > count
        ):
            raise InvalidRequest(
                f"part_numbers must name 1 to {PARTS_PER_RESPONSE} parts between 1 and {count}."
            )
        upload.expires_at = timezone.now() + timedelta(seconds=expires)
        upload.save(update_fields=["expires_at"])
    return _part_urls(upload, numbers, expires)


def _check_parts(upload: Upload, parts: Optional[list]) -> list:
    count = math.ceil(upload.size / upload.part_size)
    if not parts:
        raise InvalidRequest(
            f"Upload {upload.id} is a multipart upload: completing it needs the part numbers "
            "and ETags the store returned for each part."
        )
    numbers = [part["part_number"] for part in parts]
    if sorted(numbers) != list(range(1, count + 1)):
        raise InvalidRequest(
            f"Upload {upload.id} has {count} parts; give each of the part numbers 1 to {count} "
            "exactly once."
        )
    return sorted(parts, key=lambda part: part["part_number"])


def complete_upload(actor: Actor, upload_id, *, parts: Optional[list] = None) -> Upload:
    with transaction.atomic():
        upload = _changeable(actor, upload_id)
        if upload.status == UploadStatus.COMPLETE:
            return upload
        if upload.status != UploadStatus.PENDING:
            raise Gone(f"Upload {upload.id} is {upload.status}; start a new upload.")
        bucket = storage.store()
        if upload.is_multipart:
            bucket.complete_multipart(
                upload.key, upload.multipart_upload_id, _check_parts(upload, parts)
            )
        info = bucket.head(upload.key)
        if info is None:
            raise Conflict(
                f"Nothing has been uploaded for upload {upload.id} yet: PUT the file to the "
                "upload URL, then complete it."
            )
        if info.size != upload.size:
            raise Conflict(
                f"The uploaded object has {info.size} bytes but {upload.size} were declared; "
                "upload the whole file again (to the same URL) and complete it."
            )
        upload.status = UploadStatus.COMPLETE
        upload.etag = info.etag
        upload.completed_at = timezone.now()
        upload.save(update_fields=["status", "etag", "completed_at"])
    return upload


def usable_upload(actor: Actor, upload_id) -> Upload:
    """An upload ``actor`` may use as the input of a job: their own (or any, for admins) and
    complete."""
    check(actor, Action.UPLOAD_USE)
    upload = Upload.objects.filter(pk=upload_id).first()
    if upload is None:
        raise InvalidRequest(f"There is no upload with id {upload_id}.")
    check(actor, Action.UPLOAD_USE, upload)
    if upload.status != UploadStatus.COMPLETE:
        raise InvalidRequest(
            f"Upload {upload.id} is {upload.status}; only complete uploads can be job inputs."
        )
    return upload


def classification_for(upload: Upload, requested: Optional[str]) -> str:
    """A job reading ``upload`` is at least as restricted as the upload."""
    return most_restrictive(requested or upload.classification, upload.classification)


def delete_upload(actor: Actor, upload_id) -> Upload:
    """Delete the object (or abort the multipart upload); the record stays as history."""
    with transaction.atomic():
        upload = _changeable(actor, upload_id)
        if upload.status in {UploadStatus.DELETED, UploadStatus.EXPIRED}:
            raise Gone(f"Upload {upload.id} is already {upload.status}.")
        active = upload.jobs.filter(status__in=[JobStatus.QUEUED, JobStatus.RUNNING])
        if active.exists():
            raise Conflict(
                f"Upload {upload.id} is the input of {active.count()} queued or running jobs; "
                "cancel them first."
            )
        remove_object(upload)
        upload.status = UploadStatus.DELETED
        upload.deleted_at = timezone.now()
        upload.save(update_fields=["status", "deleted_at"])
        audit.record(actor, "upload.delete", upload, {"size": upload.size})
    return upload


def remove_object(upload: Upload) -> None:
    bucket = storage.store()
    if upload.is_multipart and upload.status == UploadStatus.PENDING:
        bucket.abort_multipart(upload.key, upload.multipart_upload_id)
    bucket.delete(upload.key)
