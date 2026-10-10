"""The engine's JobResult, read as untrusted input, and the results the supervisor writes itself.

The engine process parses untrusted files, so the supervisor treats what it leaves behind as
untrusted too: ``result.json`` is opened without following symlinks and size-checked, its shape
is checked, and every artifact it lists must be a regular file inside the job's scratch
directory. The supervisor computes each artifact's size and sha256 itself.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import json
import os
import re
import signal
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

SPEC_VERSION = 1
RESULT_MAX_BYTES = 8 * 1024 * 1024  # result.json as the engine wrote it
REPORT_MAX_BYTES = 1024 * 1024  # the result as reported: the gateway refuses larger ones
MAX_WARNINGS = 100
MAX_TEXT = 2000  # characters of one warning
MAX_MESSAGE = 8000  # characters of the error message
MAX_ARTIFACTS = 100  # the gateway records at most 100 artifacts per job
STATUSES = ("succeeded", "failed", "cancelled")
ERROR_CODES = (
    "SPEC_INVALID",
    "SCHEMA_INVALID",
    "INPUT_UNREADABLE",
    "ENCODING_ERROR",
    "COLUMN_MISSING",
    "BAD_ROWS_THRESHOLD_EXCEEDED",
    "CONSTRAINT_VIOLATION",
    "LIMIT_EXCEEDED",
    "PERMISSION_DENIED",
    "TARGET_WRITE_FAILED",
    "CANCELLED",
    "INTERNAL",
)
ARTIFACT_KINDS = ("data", "bad_rows", "manifest", "metadata", "preview", "schema", "report")
_ARTIFACT_NAME = re.compile(r"^(?=.{1,200}$)[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_HASH_CHUNK = 1024 * 1024


class ResultInvalid(Exception):
    """The engine's result (or an artifact it lists) cannot be used."""


def open_beneath(root: Path, relative: str, what: str) -> int:
    """A read descriptor for ``root/relative``: a regular file with no other names, reached
    without following any symbolic link (each directory is opened relative to the last, so a
    link swapped in on the way cannot redirect the open). FileNotFoundError if it is missing."""
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts or "\\" in relative:
        raise ResultInvalid(f"{what} leaves the scratch directory")
    current = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in pure.parts[:-1]:
            try:
                inner = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=current,
                )
            except FileNotFoundError:
                raise
            except OSError:  # ELOOP or ENOTDIR: a symbolic link or a file on the way
                raise ResultInvalid(
                    f"{what} leaves the scratch directory (through a symbolic link)"
                ) from None
            os.close(current)
            current = inner
        try:
            descriptor = os.open(
                pure.parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                dir_fd=current,
            )
        except FileNotFoundError:
            raise
        except OSError as error:
            if error.errno == errno.ELOOP:
                raise ResultInvalid(
                    f"{what} leaves the scratch directory (it is a symbolic link)"
                ) from None
            raise ResultInvalid(f"{what} cannot be opened ({error.strerror})") from None
    finally:
        os.close(current)
    status = os.fstat(descriptor)
    problem = None
    if not stat.S_ISREG(status.st_mode):
        problem = f"{what} is not a regular file"
    elif status.st_nlink != 1:
        problem = f"{what} has other names (hard links), so it may be a file from elsewhere"
    if problem:
        os.close(descriptor)
        raise ResultInvalid(problem)
    return descriptor


def read_result(path: Path, job_id: str) -> dict[str, Any] | None:
    """The engine's JobResult, checked and normalised; None when the engine wrote none."""
    try:
        descriptor = open_beneath(path.parent, path.name, "result.json")
    except FileNotFoundError:
        return None
    with os.fdopen(descriptor, "rb") as handle:
        data = handle.read(RESULT_MAX_BYTES + 1)
    if len(data) > RESULT_MAX_BYTES:
        raise ResultInvalid(f"result.json is larger than {RESULT_MAX_BYTES} bytes")
    try:
        result = json.loads(data)
    except ValueError as error:
        raise ResultInvalid(f"result.json is not JSON ({error})") from None
    return normalised(result, job_id)


def _count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _error(result: dict[str, Any]) -> dict[str, Any] | None:
    status, error = result["status"], result.get("error")
    if status == "succeeded":
        if error is not None:
            raise ResultInvalid("result.json says the job succeeded but gives an error")
        return None
    if not isinstance(error, dict):
        raise ResultInvalid(f"result.json says the job {status} but gives no error")
    if error.get("code") not in ERROR_CODES:
        raise ResultInvalid("result.json has an error without a known code")
    if status == "cancelled" and error["code"] != "CANCELLED":
        raise ResultInvalid("result.json says the job was cancelled with another code")
    if not isinstance(error.get("message"), str):
        raise ResultInvalid("result.json has an error without a message")
    return {
        "code": error["code"],
        "message": error["message"],
        "retryable": error.get("retryable") is True,
    }


def _artifact(artifact: Any) -> dict[str, Any]:
    if not isinstance(artifact, dict) or artifact.get("kind") not in ARTIFACT_KINDS:
        raise ResultInvalid("result.json lists an artifact without a known kind")
    if not isinstance(artifact.get("path"), str) or not artifact["path"]:
        raise ResultInvalid("result.json lists an artifact without a path")
    rows = artifact.get("rows")
    if rows is not None and not _count(rows):
        raise ResultInvalid("result.json lists an artifact whose rows is not a count")
    size, digest = artifact.get("bytes"), artifact.get("sha256")
    return {
        "kind": artifact["kind"],
        "path": artifact["path"],
        "rows": rows,
        "bytes": size if _count(size) else None,
        "sha256": digest if isinstance(digest, str) and _SHA256.match(digest) else None,
    }


def normalised(result: Any, job_id: str) -> dict[str, Any]:
    """The engine's result as a JobResult that matches the contract, or ResultInvalid.

    The parts the supervisor acts on (job, status, error, artifacts) must be right; in the
    informational ones (counts, findings, extensions, warnings) entries of the wrong type are
    dropped. Fields the contract does not know are dropped too.
    """
    if not isinstance(result, dict):
        raise ResultInvalid("result.json is not a JSON object")
    if result.get("job_id") not in (job_id, None):
        raise ResultInvalid("result.json is for another job")
    if result.get("status") not in STATUSES:
        raise ResultInvalid(f"result.json has the status {result.get('status')!r}")
    artifacts = result.get("artifacts", [])
    if not isinstance(artifacts, list) or len(artifacts) > MAX_ARTIFACTS:
        raise ResultInvalid(f"result.json's artifacts are not a list of at most {MAX_ARTIFACTS}")

    def counts(value: Any) -> dict[str, int]:
        items = value.items() if isinstance(value, dict) else ()
        return {key: count for key, count in items if isinstance(key, str) and _count(count)}

    def texts(value: Any) -> list[str]:
        return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []

    return {
        "spec_version": SPEC_VERSION,
        "job_id": job_id,
        "status": result["status"],
        "counts": counts(result.get("counts")),
        "schema_extensions": texts(result.get("schema_extensions")),
        "validation_summary": counts(result.get("validation_summary")),
        "warnings": texts(result.get("warnings")),
        "artifacts": [_artifact(artifact) for artifact in artifacts],
        "error": _error(result),
    }


def synthesized(
    job_id: str,
    status: str,
    code: str | None,
    message: str = "",
    *,
    retryable: bool = False,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A JobResult the supervisor writes itself (the engine wrote none, or one it overrides).

    ``base`` keeps the engine's counts, findings and warnings; its artifacts are dropped.
    """
    base = base or {}
    return {
        "spec_version": SPEC_VERSION,
        "job_id": job_id,
        "status": status,
        "counts": base.get("counts") or {},
        "schema_extensions": base.get("schema_extensions") or [],
        "validation_summary": base.get("validation_summary") or {},
        "warnings": base.get("warnings") or [],
        "artifacts": [],
        "error": (
            None if code is None else {"code": code, "message": message, "retryable": retryable}
        ),
    }


def bounded(result: dict[str, Any]) -> dict[str, Any]:
    """``result`` itself when the gateway accepts its size; otherwise a copy with fewer and
    shorter warnings and a shorter error message, or, when even that is too large, a failed
    result that says so (with no artifacts)."""

    def size(document: dict[str, Any]) -> int:
        return len(json.dumps(document))

    warnings, error = result["warnings"], result["error"]
    if (
        len(warnings) <= MAX_WARNINGS
        and all(len(warning) <= MAX_TEXT for warning in warnings)
        and (error is None or len(error["message"]) <= MAX_MESSAGE)
        and size(result) <= REPORT_MAX_BYTES
    ):
        return result
    shortened = [warning[:MAX_TEXT] for warning in warnings[:MAX_WARNINGS]]
    if len(warnings) > MAX_WARNINGS:
        shortened.append(f"... and {len(warnings) - MAX_WARNINGS} more warnings")
    trimmed = {**result, "warnings": shortened}
    if error is not None:
        trimmed["error"] = {**error, "message": error["message"][:MAX_MESSAGE]}
    if size(trimmed) <= REPORT_MAX_BYTES:
        return trimmed
    return synthesized(
        result["job_id"],
        "failed",
        "INTERNAL",
        f"The job's result is larger than the {REPORT_MAX_BYTES} bytes the gateway accepts, "
        "even with its warnings shortened (its counts or findings are too large).",
    )


# --------------------------------------------------------------------------- engine exits


def _signal_name(number: int) -> str:
    try:
        return signal.Signals(number).name
    except ValueError:
        return f"signal {number}"


def with_tail(message: str, tail: str) -> str:
    return f"{message} The last lines it wrote to stderr:\n{tail}" if tail else message


def crash_message(returncode: int, tail: str, limits: dict[str, Sequence[int]]) -> tuple[str, str]:
    """(code, message) for an engine that exited without a result."""
    if returncode == 2:
        return "SPEC_INVALID", with_tail(
            "The engine refused the job spec (exit code 2) and wrote no result.", tail
        )
    if returncode >= 0:
        return "INTERNAL", with_tail(
            f"The engine exited with code {returncode} without writing a result.", tail
        )
    number = -returncode
    name = _signal_name(number)
    if number == signal.SIGXCPU:
        seconds = limits.get("cpu", [-1])[0]
        return "LIMIT_EXCEEDED", (
            f"The engine used up its CPU time limit of {seconds} seconds (RLIMIT_CPU) and was "
            "stopped."
        )
    if number == signal.SIGKILL:
        return "INTERNAL", with_tail(
            "The engine was killed by SIGKILL without writing a result. The usual causes are the "
            "kernel's out-of-memory killer (the container's memory limit) and the CPU time hard "
            "limit.",
            tail,
        )
    hint = ""
    space = limits.get("as", [-1])[0]
    if number in (signal.SIGSEGV, signal.SIGABRT, signal.SIGBUS) and space > 0:
        hint = (
            f" This can happen when it runs out of address space (RLIMIT_AS is {space} bytes; "
            "see --limit-address-space)."
        )
    return "INTERNAL", with_tail(
        f"The engine was killed by {name} without writing a result.{hint}", tail
    )


# --------------------------------------------------------------------------- artifacts


@dataclass(frozen=True)
class ArtifactFile:
    kind: str
    name: str
    root: Path  # the job's scratch directory
    relative: str  # the path the result gives, inside root
    rows: int | None
    bytes: int
    sha256: str
    md5_base64: str
    index: int  # position in the result's artifact list
    identity: tuple[int, int]  # (st_dev, st_ino) when it was hashed

    def entry(self, key: str) -> dict[str, Any]:
        """The artifact as ``complete`` reports it."""
        return {
            "kind": self.kind,
            "name": self.name,
            "key": key,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "rows": self.rows,
        }

    def open(self) -> int:
        """A descriptor for the same file that was hashed (ResultInvalid if it changed)."""
        what = f"the artifact {self.relative!r}"
        try:
            descriptor = open_beneath(self.root, self.relative, what)
        except FileNotFoundError:
            raise ResultInvalid(f"{what} disappeared after it was hashed") from None
        status = os.fstat(descriptor)
        if (status.st_dev, status.st_ino) != self.identity or status.st_size != self.bytes:
            os.close(descriptor)
            raise ResultInvalid(f"{what} changed after it was hashed")
        return descriptor


def artifact_name(path: str, output_dirs: Sequence[str]) -> str:
    """The upload name: the path within its output directory, or within scratch."""
    pure = PurePosixPath(path)
    for directory in output_dirs:
        root = PurePosixPath(directory)
        if root != PurePosixPath(".") and pure.is_relative_to(root) and pure != root:
            return str(pure.relative_to(root))
    return str(pure)


def collect_artifacts(
    result: dict[str, Any], workdir: Path, output_dirs: Sequence[str]
) -> list[ArtifactFile]:
    """The result's artifacts as files in scratch, hashed; data first and the manifest last."""
    files: list[ArtifactFile] = []
    names: set[str] = set()
    for index, entry in enumerate(result.get("artifacts", [])):
        relative = entry["path"]
        what = f"the artifact {relative!r}"
        try:
            descriptor = open_beneath(workdir, relative, what)
        except FileNotFoundError:
            raise ResultInvalid(
                f"the engine listed the artifact {relative!r} but did not write it"
            ) from None
        with os.fdopen(descriptor, "rb") as handle:
            name = artifact_name(relative, output_dirs)
            if not _ARTIFACT_NAME.match(name):
                raise ResultInvalid(f"{what} cannot be used as an upload name")
            if name in names:
                raise ResultInvalid(f"the artifact {name!r} is listed twice")
            names.add(name)
            status = os.fstat(handle.fileno())
            sha256, md5, size = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
            while chunk := handle.read(_HASH_CHUNK):
                sha256.update(chunk)
                md5.update(chunk)
                size += len(chunk)
        files.append(
            ArtifactFile(
                kind=entry["kind"],
                name=name,
                root=workdir,
                relative=relative,
                rows=entry.get("rows"),
                bytes=size,
                sha256=sha256.hexdigest(),
                md5_base64=base64.b64encode(md5.digest()).decode("ascii"),
                index=index,
                identity=(status.st_dev, status.st_ino),
            )
        )
    return sorted(files, key=lambda item: item.kind == "manifest")
