"""Run the engine from a declarative job spec: the job contract (``contracts/``).

``JobSpec`` says what a job reads, how it checks it and where the results go; ``run_job`` runs it
and returns a ``JobResult``. The same spec runs from Python, from ``forklift run-job`` and in the
service's workers. See ``forklift.jobs.readme.md`` next to this file.
"""

from ..engine.exceptions import ERROR_CODES
from ._model import ContractError
from .result import ARTIFACT_KINDS, STATUSES, Artifact, JobError, JobResult
from .runner import run_job
from .spec import (
    FORMATS,
    KINDS,
    SPEC_VERSION,
    FileLocation,
    FooterDetection,
    InputOptions,
    InputSpec,
    JobOptions,
    JobSpec,
    Limits,
    OutputSpec,
    PresignedUrlLocation,
    S3Location,
    SqlLocation,
    SqlTableLocation,
)

__all__ = [
    "run_job",
    "JobSpec",
    "JobResult",
    "ContractError",
    "SPEC_VERSION",
    "KINDS",
    "FORMATS",
    "STATUSES",
    "ARTIFACT_KINDS",
    "ERROR_CODES",
    "InputSpec",
    "InputOptions",
    "FooterDetection",
    "OutputSpec",
    "JobOptions",
    "Limits",
    "FileLocation",
    "S3Location",
    "PresignedUrlLocation",
    "SqlLocation",
    "SqlTableLocation",
    "Artifact",
    "JobError",
]
