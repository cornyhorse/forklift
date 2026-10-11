"""Publishing: copying a successful run's outputs to its dataset's s3 destination.

A run whose dataset has an s3 destination is not done when its worker completes it: complete
(services.queue) records the result and the artifacts and leaves the job ``publishing``. A
publisher (``forklift-web publish``; the Compose service ``publisher``) then copies the
attempt's artifacts, data first and ``manifest.json`` last, and marks the job succeeded, so
``succeeded`` means published and a job never goes from succeeded to failed. Its webhooks hear
about it once, at the end.

Publishers hold jobs with a lease, as workers do: :func:`publish_due` claims a publishing job
whose lease is free or has expired (``SELECT ... FOR UPDATE SKIP LOCKED``, so two publishers
never take the same job) with a token of its own for ``publish_lease_seconds``, and renews it
every third of that while it copies. Every change it makes to the job matches its token, so a
publisher that lost its lease (it was too slow, and another one took the job over) stops
without touching the job. A publisher that dies leaves its lease to expire, and the next pass
resumes the job by copying everything again: copies are idempotent (the same keys, the same
content). Each claim counts as an attempt. A store error releases the lease with a backoff
(:data:`BACKOFF_SECONDS`) until ``publish_max_attempts`` attempts have been made; any other
error (destination secrets that cannot be decrypted, anything unexpected) fails the job at once
with TARGET_WRITE_FAILED. The artifacts are kept either way.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import timedelta
from typing import Callable, Optional

from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone

from forklift_web import secret_backend, storage
from forklift_web.core.choices import ArtifactKind, ConnectionKind, EventType, JobKind, JobStatus
from forklift_web.core.models import Connection, Job, JobEvent
from forklift_web.errors import StoreUnavailable
from forklift_web.services import connections, installation, webhooks

logger = logging.getLogger(__name__)

BACKOFF_SECONDS = (60, 300, 1800, 7200)  # after the 1st, 2nd, 3rd and every later store error
TICK_SECONDS = 10.0  # how often a long copy touches the heartbeat (and renews when due)
STOPPED = "the publisher stopped before it finished (its lease ran out)."


class LeaseLost(Exception):
    """Another publisher holds the job now: stop without changing it."""


def _monotonic() -> float:
    return time.monotonic()


def destination_of(job: Job) -> Optional[Connection]:
    """The s3 connection a successful run of ``job`` is published to (None: nothing is)."""
    dataset = job.dataset if job.kind == JobKind.RUN else None
    destination = dataset.destination_connection if dataset is not None else None
    if destination is None or destination.kind != ConnectionKind.S3:
        return None
    return destination


def _event(job: Job, type_: str, **payload) -> None:
    JobEvent.objects.create(job=job, type=type_, attempt=job.attempt, payload=payload)


# --------------------------------------------------------------------------- the lease


class _Lease:
    """This publisher's lease on one job. :meth:`keep` runs on the publisher's thread (it uses
    the database); :meth:`check` is the copy's progress callback, on the copy's threads."""

    def __init__(self, job_id, token: uuid.UUID, seconds: int, heartbeat: Callable[[], None]):
        self.job_id, self.token, self.seconds, self.heartbeat = job_id, token, seconds, heartbeat
        self.renewed_at = _monotonic()
        self.lost = threading.Event()

    def keep(self) -> None:
        """Touch the heartbeat, and renew the lease once a third of it has passed; LeaseLost
        when the renewal finds the job no longer ours."""
        self.heartbeat()
        if _monotonic() - self.renewed_at < self.seconds / 3:
            return
        renewed = Job.objects.filter(
            pk=self.job_id, status=JobStatus.PUBLISHING, publish_lease_token=self.token
        ).update(publish_lease_expires_at=timezone.now() + timedelta(seconds=self.seconds))
        if not renewed:
            self.lost.set()
            raise LeaseLost()
        self.renewed_at = _monotonic()

    def check(self, transferred: int) -> None:
        if self.lost.is_set():
            raise LeaseLost()


def _copy(target: storage.Bucket, key: str, destination_key: str, lease: _Lease) -> None:
    """Copy one object on a thread of its own while this thread keeps the lease; a lost lease
    stops the copy at its next part (its progress callback raises)."""
    failures: list = []

    def run() -> None:
        try:
            target.copy_from(storage.store(), key, destination_key, progress=lease.check)
        except BaseException as error:  # handed to the publisher's thread
            failures.append(error)

    copier = threading.Thread(target=run, name="publish-copy", daemon=True)
    copier.start()
    try:
        while copier.is_alive():
            copier.join(min(TICK_SECONDS, lease.seconds / 3))
            if copier.is_alive():
                lease.keep()
    except BaseException:
        lease.lost.set()  # whatever stopped this thread stops the copy at its next part too
        copier.join()
        raise
    if failures:
        raise failures[0]


# --------------------------------------------------------------------------- one pass


def publish_due(
    *,
    now=None,
    heartbeat: Callable[[], None] = lambda: None,
    stop: Callable[[], bool] = lambda: False,
) -> dict:
    """Publish every job whose publishing is due, one after the other, each at most once;
    returns how many were published, will be tried again, failed and were lost to another
    publisher. ``heartbeat`` is called between objects and every few seconds of a long copy;
    once ``stop()`` is true no further job is claimed (the one being copied is finished)."""
    now = now or timezone.now()
    settings = installation.current()
    counts = {"published": 0, "retrying": 0, "failed": 0, "lost": 0}
    handled: list = []
    while not stop() and (claimed := _claim(now, settings, handled)) is not None:
        job, token = claimed
        outcome = "failed" if token is None else _publish(job, token, settings, now, heartbeat)
        counts[outcome] += 1
    return counts


def _claim(now, settings: dict, handled: list):
    """The next due job and a fresh lease token for it, or None when nothing is due. A job
    that has had all its attempts (its last publisher died) is failed instead (token None)."""
    # A pass publishes one job after another, so the clock it started with may be long past:
    # a lease counted from it could have run out already, and another publisher take the job
    now = max(now, timezone.now())
    with transaction.atomic():
        job = (
            Job.objects.select_for_update(skip_locked=True)
            .filter(status=JobStatus.PUBLISHING)
            .filter(
                Q(publish_lease_expires_at__isnull=True) | Q(publish_lease_expires_at__lte=now)
            )
            .exclude(pk__in=handled)
            .order_by(F("publish_lease_expires_at").asc(nulls_first=True), "created_at")
            .first()
        )
        if job is None:
            return None
        handled.append(job.pk)
        if job.publish_attempts >= settings["publish_max_attempts"]:
            reason = STOPPED if job.publish_lease_token else job.publish_error
            _fail(job, _exhausted(job, reason))
            return job, None
        token = uuid.uuid4()
        job.publish_attempts += 1
        job.publish_lease_token = token
        job.publish_lease_expires_at = now + timedelta(seconds=settings["publish_lease_seconds"])
        job.save(
            update_fields=["publish_attempts", "publish_lease_token", "publish_lease_expires_at"]
        )
    return job, token


def _publish(job: Job, token: uuid.UUID, settings: dict, now, heartbeat) -> str:
    """Copy the job's artifacts with the lease ``token``; returns the outcome."""
    lease = _Lease(job.pk, token, settings["publish_lease_seconds"], heartbeat)
    destination = destination_of(job)
    try:
        if destination is None:
            return _end(job, token, _failed(job, None, "Its dataset has no s3 destination now."))
        target = connections.bucket_of(destination)
        base = "/".join(
            part for part in (target.prefix, job.dataset.destination_prefix, str(job.id)) if part
        )
        artifacts = sorted(
            job.artifacts.filter(attempt=job.attempt),
            key=lambda artifact: (artifact.kind == ArtifactKind.MANIFEST, artifact.name),
        )
        for artifact in artifacts:
            lease.keep()
            _copy(target, artifact.key, f"{base}/{artifact.name}", lease)
    except LeaseLost:
        logger.warning("A publisher lost its lease and stopped", extra={"job_id": str(job.id)})
        return "lost"
    except StoreUnavailable as error:
        return _retry(job, token, destination, error.message, settings, now)
    except Exception as error:  # the job must end failed and announced, whatever this was
        logger.warning(
            "Publishing a run failed",
            extra={"job_id": str(job.id), "error": type(error).__name__},
        )
        return _end(job, token, _failed(job, destination, _reason(error)))
    return _succeed(job, token, destination, base, len(artifacts))


# --------------------------------------------------------------------------- endings


def _ours(job: Job, token: uuid.UUID) -> Optional[Job]:
    """The job, locked, while it is publishing under ``token``."""
    return (
        Job.objects.select_for_update()
        .filter(pk=job.pk, status=JobStatus.PUBLISHING, publish_lease_token=token)
        .first()
    )


def _release(job: Job) -> None:
    job.publish_lease_token = None
    job.publish_lease_expires_at = None


def _succeed(job: Job, token, destination: Connection, base: str, files: int) -> str:
    with transaction.atomic():
        job = _ours(job, token)
        if job is None:
            return "lost"
        _release(job)
        job.status = JobStatus.SUCCEEDED
        job.finished_at = timezone.now()
        job.publish_error = ""
        job.save()
        _event(job, EventType.LOG, published_to=destination.name, prefix=base, files=files)
        _event(job, EventType.STATE, status=JobStatus.SUCCEEDED)
        webhooks.job_finished(job)
    return "published"


def _retry(job: Job, token, destination: Connection, reason: str, settings: dict, now) -> str:
    """After a store error: try again after a backoff, or fail once the attempts are used up."""
    with transaction.atomic():
        job = _ours(job, token)
        if job is None:
            return "lost"
        if job.publish_attempts >= settings["publish_max_attempts"]:
            _fail(job, _exhausted(job, reason, destination))
            return "failed"
        delay = BACKOFF_SECONDS[min(job.publish_attempts, len(BACKOFF_SECONDS)) - 1]
        _release(job)
        # Not before the pass started (it may run on a clock of its own) nor before now
        job.publish_lease_expires_at = max(now, timezone.now()) + timedelta(seconds=delay)
        job.publish_error = reason
        job.save(
            update_fields=["publish_lease_token", "publish_lease_expires_at", "publish_error"]
        )
        _event(job, EventType.LOG, publish_attempt=job.publish_attempts, retry_in_seconds=delay)
    logger.warning(
        "Publishing a run is tried again later",
        extra={"job_id": str(job.id), "attempt": job.publish_attempts, "reason": reason},
    )
    return "retrying"


def _end(job: Job, token, message: str) -> str:
    """Fail the job with ``message`` if it is still ours."""
    with transaction.atomic():
        job_now = _ours(job, token)
        if job_now is None:
            return "lost"
        _fail(job_now, message)
    return "failed"


def _fail(job: Job, message: str) -> None:
    """Fail ``job`` (locked, publishing) with TARGET_WRITE_FAILED and announce it."""
    _release(job)
    job.status = JobStatus.FAILED
    job.error_code = "TARGET_WRITE_FAILED"
    job.error_message = message
    job.finished_at = timezone.now()
    job.save()
    _event(job, EventType.STATE, status=JobStatus.FAILED, error_code=job.error_code)
    webhooks.job_finished(job)


# --------------------------------------------------------------------------- messages


def _reason(error: Exception) -> str:
    if isinstance(error, secret_backend.SecretError):
        return str(error)  # ours: it names the setting, never the secret
    return f"An unexpected {type(error).__name__} stopped it."  # its text could hold anything


def _failed(job: Job, destination: Optional[Connection], reason: str) -> str:
    where = f" to {destination.name!r}" if destination is not None else ""
    return (
        f"The run succeeded but publishing{where} failed: {reason} The outputs are kept as job "
        "artifacts."
    )


def _exhausted(job: Job, reason: str, destination: Optional[Connection] = None) -> str:
    destination = destination or destination_of(job)
    where = f" to {destination.name!r}" if destination is not None else ""
    return (
        f"The run succeeded but publishing{where} did not finish after {job.publish_attempts} "
        f"attempt{'' if job.publish_attempts == 1 else 's'}: {reason} The outputs are kept as "
        "job artifacts."
    )
