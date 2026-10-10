"""Artifacts and their downloads: permission-checked, presigned, short-lived and audited."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from django.utils import timezone

from forklift_web import storage
from forklift_web.core.models import Artifact
from forklift_web.errors import Gone, NotFound
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, installation, jobs


@dataclass(frozen=True)
class Download:
    url: str
    expires_at: datetime
    filename: str


def list_artifacts(actor: Actor, job_id):
    check(actor, Action.ARTIFACT_VIEW)
    job = jobs.get_job(actor, job_id)
    return job.artifacts.select_related("job").order_by("attempt", "name")


def _find(artifact_id) -> Artifact:
    artifact = Artifact.objects.select_related("job").filter(pk=artifact_id).first()
    if artifact is None:
        raise NotFound(f"There is no artifact with id {artifact_id}.")
    return artifact


def get_artifact(actor: Actor, artifact_id) -> Artifact:
    check(actor, Action.ARTIFACT_VIEW)
    return _find(artifact_id)


def download(actor: Actor, artifact_id) -> Download:
    """A presigned GET for the artifact, signed for browsers and API clients; audited."""
    check(actor, Action.ARTIFACT_DOWNLOAD)
    artifact = _find(artifact_id)
    check(actor, Action.ARTIFACT_DOWNLOAD, artifact)
    if artifact.deleted_at is not None:
        raise Gone(
            f"Artifact {artifact.name!r} of job {artifact.job_id} was deleted by retention on "
            f"{artifact.deleted_at:%Y-%m-%d}."
        )
    seconds = installation.get("download_url_seconds")
    filename = f"{artifact.job_id}-{artifact.name.replace('/', '-')}"
    url = storage.store().presign_get(
        artifact.key, expires=seconds, audience=storage.Audience.PUBLIC, filename=filename
    )
    audit.record(
        actor,
        "artifact.download",
        artifact,
        {
            "job_id": str(artifact.job_id),
            "kind": artifact.kind,
            "classification": artifact.job.classification,
        },
    )
    return Download(
        url=url, expires_at=timezone.now() + timedelta(seconds=seconds), filename=filename
    )
