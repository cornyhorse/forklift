"""Model details: names shown in the audit log and the admin screens."""

from __future__ import annotations

import pytest
from world import World

pytestmark = pytest.mark.django_db


def test_model_names():
    world = World.build()
    assert str(world.version) == f"{world.version.schema.name} v1"
    assert str(world.worker) == "worker-1"
    assert str(world.upload) == f"people.csv ({world.upload.pk})"
    assert str(world.queued_job) == f"run job {world.queued_job.pk}"
    assert str(world.artifact) == f"data data.parquet of job {world.finished_job.pk}"
    assert str(world.tokens["viewer"]).startswith("token (fkl_")
    assert str(world.dataset) == world.dataset.name and str(world.connection)
    assert str(world.viewer) == world.viewer.username
    assert str(world.version.schema) == world.version.schema.name
    assert world.queued_job.attempt_prefix == f"jobs/{world.queued_job.pk}/attempt-0/"
