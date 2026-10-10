"""``JobResult``: what a job did (contract v1).

A result holds counts, codes, column names and file names, never cell values: rows only ever
appear in artifacts (``preview.json``, the Parquet files), which are permission-checked separately.
``contracts/jobresult.schema.json`` is generated from these classes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from ..engine.exceptions import CANCELLED, ERROR_CODES
from ._model import Model, Problem, contract_field
from .spec import SPEC_VERSION

STATUSES = ("succeeded", "failed", "cancelled")
ARTIFACT_KINDS = ("data", "bad_rows", "manifest", "metadata", "preview", "schema", "report")


@dataclass
class Artifact(Model):
    """A file the job wrote."""

    kind: str = contract_field(
        "data: validated rows (Parquet); bad_rows: rejected rows with _rejection_reason; "
        "manifest / metadata: JSON files describing the run; preview, schema, report: the "
        "artifact of the preview, generate_schema and validate_schema kinds",
        enum=list(ARTIFACT_KINDS),
    )
    path: str = contract_field(
        "Path relative to the base directory ('/' separated), or the s3:// URI of an object "
        "written to an s3 output",
        minLength=1,
    )
    rows: Optional[int] = contract_field(
        "Rows in the file (Parquet files and previews); null for other files",
        default=None,
        required=True,
        minimum=0,
    )
    bytes: Optional[int] = contract_field("Size in bytes", default=None, required=True, minimum=0)
    sha256: Optional[str] = contract_field(
        "SHA-256 of the file, lowercase hex; null for objects written to an s3 output",
        default=None,
        required=True,
        pattern=r"^[0-9a-f]{64}$",
        **{"x-pattern-message": "must be 64 lowercase hexadecimal characters"},
    )


@dataclass
class JobError(Model):
    """Why a job failed or was cancelled."""

    code: str = contract_field("Stable error code", enum=list(ERROR_CODES))
    message: str = contract_field(
        "What went wrong and how to fix it; never contains cell values or secrets"
    )
    retryable: bool = contract_field(
        "True when running the same job again may succeed (a dropped connection, for example)",
        default=False,
        required=True,
    )


@dataclass
class JobResult(Model):
    """The outcome of a job (job contract v1)."""

    __contract_name__ = "job result"

    job_id: Optional[str] = contract_field(
        "job_id of the spec; null when the spec could not be read"
    )
    status: str = contract_field("Outcome of the job", enum=list(STATUSES))
    counts: Dict[str, int] = contract_field(
        "Row counts: total_rows (rows read; for preview the rows returned, for generate_schema "
        "the rows analysed), valid_rows, invalid_rows (sent to bad_rows), truncated_rows, and "
        "rows_written for sql_table outputs",
        default_factory=dict,
        required=True,
        additionalProperties={"minimum": 0},
    )
    schema_extensions: List[str] = contract_field(
        "Schema extensions that were applied", default_factory=list, required=True
    )
    validation_summary: Dict[str, int] = contract_field(
        "Findings of the schema extensions by CODE or CODE:column",
        default_factory=dict,
        required=True,
        additionalProperties={"minimum": 0},
    )
    warnings: List[str] = contract_field(
        "Notes that did not stop the job", default_factory=list, required=True
    )
    artifacts: List[Artifact] = contract_field(
        "Files the job wrote, data files first", default_factory=list, required=True
    )
    error: Optional[JobError] = contract_field(
        "Why the job failed or was cancelled; null when it succeeded",
        default=None,
        required=True,
    )
    spec_version: int = contract_field(
        "Version of the job contract",
        default=SPEC_VERSION,
        required=True,
        order=-1,
        const=SPEC_VERSION,
    )

    __schema_rules__ = [
        {
            "if": {"properties": {"status": {"const": "succeeded"}}},
            "then": {"properties": {"error": {"type": "null"}}},
            "else": {"properties": {"error": {"type": "object"}}},
        },
        {
            "if": {"properties": {"status": {"const": "cancelled"}}},
            "then": {"properties": {"error": {"properties": {"code": {"const": CANCELLED}}}}},
        },
    ]

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"

    def _check(self, path: str, problems: List[Problem]) -> None:
        if (self.status == "succeeded") != (self.error is None):
            problems.append(("error", "must be null when status is 'succeeded' and set otherwise"))
        elif self.status == "cancelled" and self.error.code != CANCELLED:
            problems.append(("error.code", f"must be {CANCELLED!r} when status is 'cancelled'"))
