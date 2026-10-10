"""Retention: admin-set lifetimes per kind of object, and the sweeper that applies them.

A policy at the installation, classification or dataset level maps retention kinds (uploads,
data, bad_rows, previews, metadata, job_records) to "keep n days" or null ("keep until
deleted"); the most specific level that names a kind wins, and with no policy at all objects are
kept (a new installation deletes nothing). The sweeper deletes expired objects from the store
and records each deletion in the audit log; job records outlive their artifacts unless
``job_records`` expires too (and only once no artifact of the job is left).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from django.db import transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from forklift_web import storage
from forklift_web.core.choices import (
    ARTIFACT_RETENTION_KIND,
    TERMINAL_STATUSES,
    Classification,
    JobStatus,
    RetentionKind,
    RetentionScope,
    UploadStatus,
)
from forklift_web.core.models import Artifact, Dataset, Job, RetentionPolicy, Upload
from forklift_web.errors import InvalidRequest, NotFound, StoreUnavailable
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit
from forklift_web.services.uploads import remove_object

_ARTIFACT_KINDS_BY_RETENTION: dict = {}
for _artifact_kind, _retention_kind in ARTIFACT_RETENTION_KIND.items():
    _ARTIFACT_KINDS_BY_RETENTION.setdefault(_retention_kind, []).append(_artifact_kind)


@dataclass
class Policies:
    """Every policy, loaded once, to resolve lifetimes without a query per object."""

    installation: dict = field(default_factory=dict)
    classifications: dict = field(default_factory=dict)
    datasets: dict = field(default_factory=dict)

    @classmethod
    def load(cls) -> "Policies":
        loaded = cls()
        for policy in RetentionPolicy.objects.all():
            if policy.scope == RetentionScope.INSTALLATION:
                loaded.installation = policy.days
            elif policy.scope == RetentionScope.CLASSIFICATION:
                loaded.classifications[policy.classification] = policy.days
            else:
                loaded.datasets[policy.dataset_id] = policy.days
        return loaded

    def days(self, kind: str, *, classification: str, dataset_id=None) -> Optional[int]:
        """Days to keep ``kind`` (None: until deleted), from the most specific level."""
        for level in (
            self.datasets.get(dataset_id, {}),
            self.classifications.get(classification, {}),
            self.installation,
        ):
            if kind in level:
                return level[kind]
        return None

    def expires_at(self, created_at: datetime, kind: str, **context) -> Optional[datetime]:
        days = self.days(kind, **context)
        return None if days is None else created_at + timedelta(days=days)


def artifact_expires_at(artifact: Artifact, policies: Optional[Policies] = None):
    """When retention will delete ``artifact`` (None: never)."""
    policies = policies or Policies.load()
    return policies.expires_at(
        artifact.created_at,
        ARTIFACT_RETENTION_KIND[artifact.kind],
        classification=artifact.job.classification,
        dataset_id=artifact.job.dataset_id,
    )


def upload_expires_at(upload: Upload, policies: Optional[Policies] = None):
    policies = policies or Policies.load()
    return policies.expires_at(
        upload.completed_at or upload.created_at,
        RetentionKind.UPLOADS,
        classification=upload.classification,
    )


# --------------------------------------------------------------------------- admin


def _check_days(days) -> dict:
    if not isinstance(days, dict):
        raise InvalidRequest("days must map retention kinds to a number of days or null.")
    unknown = sorted(set(days) - set(RetentionKind.values))
    if unknown:
        raise InvalidRequest(
            f"Unknown retention kinds: {', '.join(unknown)} (kinds: "
            f"{', '.join(RetentionKind.values)})."
        )
    for kind, value in days.items():
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise InvalidRequest(
                f"days.{kind} must be a whole number of days (0 or more) or null for 'keep "
                "until deleted'."
            )
    return days


def _selector(scope: str, classification: Optional[str], dataset_id) -> dict:
    if scope == RetentionScope.INSTALLATION:
        return {"scope": scope}
    if scope == RetentionScope.CLASSIFICATION:
        if classification not in Classification.values:
            raise InvalidRequest(
                f"Unknown classification {classification!r}; classifications: "
                f"{', '.join(Classification.values)}."
            )
        return {"scope": scope, "classification": classification}
    if scope == RetentionScope.DATASET:
        if not Dataset.objects.filter(pk=dataset_id).exists():
            raise NotFound(f"There is no dataset with id {dataset_id}.")
        return {"scope": scope, "dataset_id": dataset_id}
    raise InvalidRequest(
        f"Unknown retention scope {scope!r}; scopes: {', '.join(RetentionScope.values)}."
    )


def overview(actor: Actor) -> dict:
    """Every policy, and warnings while sensitive data has no expiry (design section 5.7)."""
    check(actor, Action.RETENTION_VIEW)
    policies = Policies.load()
    warnings = []
    for kind in (RetentionKind.UPLOADS, RetentionKind.DATA, RetentionKind.BAD_ROWS):
        if policies.days(kind, classification=Classification.SENSITIVE) is None:
            warnings.append(
                f"Sensitive {kind} have no expiry: set it for the sensitive classification or "
                "the installation."
            )
    for dataset in Dataset.objects.filter(classification=Classification.SENSITIVE):
        open_kinds = [
            kind
            for kind in (RetentionKind.DATA, RetentionKind.BAD_ROWS)
            if policies.days(kind, classification=dataset.classification, dataset_id=dataset.pk)
            is None
        ]
        if open_kinds:
            warnings.append(
                f"Sensitive dataset {dataset.name!r} keeps its {' and '.join(open_kinds)} until "
                "deleted."
            )
    return {
        "policies": list(RetentionPolicy.objects.select_related("dataset")),
        "warnings": warnings,
        "kinds": list(RetentionKind.values),
    }


def set_policy(
    actor: Actor, scope: str, *, days: dict, classification: Optional[str] = None, dataset_id=None
) -> RetentionPolicy:
    """Create or replace the policy of one level."""
    check(actor, Action.RETENTION_MANAGE)
    selector = _selector(scope, classification, dataset_id)
    days = _check_days(days)
    with transaction.atomic():
        policy, created = RetentionPolicy.objects.select_for_update().get_or_create(
            **selector, defaults={"days": days, "updated_by": actor.user}
        )
        previous = None if created else policy.days
        if not created:
            policy.days = days
            policy.updated_at = timezone.now()
            policy.updated_by = actor.user
            policy.save()
        audit.record(
            actor,
            "retention.set",
            policy,
            {
                "scope": scope,
                "classification": classification,
                "dataset_id": dataset_id,
                "from": previous,
                "to": days,
            },
        )
    return policy


def delete_policy(actor: Actor, scope: str, *, classification=None, dataset_id=None) -> None:
    check(actor, Action.RETENTION_MANAGE)
    selector = _selector(scope, classification, dataset_id)
    policy = RetentionPolicy.objects.filter(**selector).first()
    if policy is None:
        raise NotFound(f"There is no {scope} retention policy to delete.")
    with transaction.atomic():
        audit.record(
            actor,
            "retention.delete_policy",
            policy,
            {
                "scope": scope,
                "classification": classification,
                "dataset_id": dataset_id,
                "days": policy.days,
            },
        )
        policy.delete()


# --------------------------------------------------------------------------- the sweeper


@dataclass
class SweepReport:
    dry_run: bool
    expired_uploads: int = 0
    uploads: int = 0
    artifacts: int = 0
    jobs: int = 0
    errors: list = field(default_factory=list)


def _delete_object(actor: Actor, obj, key: str, kind: str, report: SweepReport, delete) -> bool:
    try:
        delete()
    except StoreUnavailable as error:
        report.errors.append(f"{kind} {obj.pk}: {error.message}")
        return False
    audit.record(actor, "retention.purge", obj, {"kind": kind, "key": key})
    return True


def _sweep_uploads(actor: Actor, policies: Policies, now, report: SweepReport) -> None:
    pending = Upload.objects.filter(status=UploadStatus.PENDING, expires_at__lt=now)
    for upload in pending:
        report.expired_uploads += 1
        if report.dry_run:
            continue
        if _delete_object(
            actor,
            upload,
            upload.key,
            "pending upload",
            report,
            lambda upload=upload: remove_object(upload),
        ):
            Upload.objects.filter(pk=upload.pk).update(status=UploadStatus.EXPIRED, deleted_at=now)
    for classification in Classification.values:
        days = policies.days(RetentionKind.UPLOADS, classification=classification)
        if days is None:
            continue
        expired = Upload.objects.filter(
            status=UploadStatus.COMPLETE,
            classification=classification,
            completed_at__lt=now - timedelta(days=days),
        ).exclude(
            Exists(
                Job.objects.filter(
                    upload=OuterRef("pk"), status__in=[JobStatus.QUEUED, JobStatus.RUNNING]
                )
            )
        )
        for upload in expired:
            report.uploads += 1
            if report.dry_run:
                continue
            if _delete_object(
                actor,
                upload,
                upload.key,
                "upload",
                report,
                lambda upload=upload: remove_object(upload),
            ):
                Upload.objects.filter(pk=upload.pk).update(
                    status=UploadStatus.DELETED, deleted_at=now
                )


def _sweep_artifacts(actor: Actor, policies: Policies, now, report: SweepReport) -> None:
    contexts = (
        Artifact.objects.filter(deleted_at=None)
        .order_by()  # the model's ordering would make DISTINCT see every row
        .values_list("job__dataset_id", "job__classification")
        .distinct()
    )
    bucket = storage.store()
    for dataset_id, classification in list(contexts):
        for kind, artifact_kinds in _ARTIFACT_KINDS_BY_RETENTION.items():
            days = policies.days(kind, classification=classification, dataset_id=dataset_id)
            if days is None:
                continue
            expired = Artifact.objects.filter(
                deleted_at=None,
                job__dataset_id=dataset_id,
                job__classification=classification,
                kind__in=artifact_kinds,
                created_at__lt=now - timedelta(days=days),
            )
            for artifact in expired:
                report.artifacts += 1
                if report.dry_run:
                    continue
                if _delete_object(
                    actor,
                    artifact,
                    artifact.key,
                    artifact.kind,
                    report,
                    lambda artifact=artifact: bucket.delete(artifact.key),
                ):
                    Artifact.objects.filter(pk=artifact.pk).update(deleted_at=now)


def _sweep_jobs(actor: Actor, policies: Policies, now, report: SweepReport) -> None:
    contexts = (
        Job.objects.filter(status__in=TERMINAL_STATUSES)
        .order_by()
        .values_list("dataset_id", "classification")
        .distinct()
    )
    for dataset_id, classification in list(contexts):
        days = policies.days(
            RetentionKind.JOB_RECORDS, classification=classification, dataset_id=dataset_id
        )
        if days is None:
            continue
        expired = Job.objects.filter(
            status__in=TERMINAL_STATUSES,
            dataset_id=dataset_id,
            classification=classification,
            finished_at__lt=now - timedelta(days=days),
        ).exclude(Exists(Artifact.objects.filter(job=OuterRef("pk"), deleted_at=None)))
        for job in expired:
            report.jobs += 1
            if report.dry_run:
                continue
            with transaction.atomic():
                audit.record(actor, "retention.purge", job, {"kind": "job_record"})
                job.delete()


def sweep(actor: Actor, *, dry_run: bool = False, now=None) -> SweepReport:
    """Delete what retention says has expired (``dry_run``: only count it)."""
    check(actor, Action.RETENTION_MANAGE)
    now = now or timezone.now()
    report = SweepReport(dry_run=dry_run)
    policies = Policies.load()
    _sweep_uploads(actor, policies, now, report)
    _sweep_artifacts(actor, policies, now, report)
    _sweep_jobs(actor, policies, now, report)
    return report
