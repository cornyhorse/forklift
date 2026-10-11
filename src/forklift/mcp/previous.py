"""What ``run_job overwrite=true`` replaces: the files an earlier run listed as its artifacts.

Only a file that the earlier ``forklift-job.json`` in the output directory lists as one of its
artifacts is replaced, and only while it is still that file: a kind a job writes, directly in the
output directory, a regular file (not a link), not the input, with the size and SHA-256 the job
file recorded. Anything else it lists is left alone and reported. Nothing is deleted before the
engine has accepted the new job: the files are moved aside into a private directory inside the
output directory first, put back when the job is refused or the call fails, and deleted once the
new run has happened.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..jobs import ContractError, JobResult
from ..jobs.result import Artifact
from .workspace import ToolInputError

#: Kinds of artifact a job writes into its directory (``run`` and the interactive kinds)
REPLACEABLE_KINDS = ("data", "bad_rows", "manifest", "metadata", "report", "schema", "preview")
#: Name prefix of the directory the earlier run's files wait in while the new job runs
HOLDING_PREFIX = ".forklift-mcp-previous-"


class PreviousRun:
    """The earlier run in an output directory: what would be replaced and what is left alone."""

    def __init__(self, directory: Path, job_file: Optional[Path]):
        self.directory = directory
        #: The earlier job file (it is replaced by the new one), None when there is none
        self.job_file = job_file
        #: Files that are still the earlier run's artifacts
        self.replace: List[Path] = []
        #: ``(path, reason)`` of listed files that are left alone
        self.left: List[Tuple[Path, str]] = []

    def view(self, display: Callable[[Path], str], cap: int) -> Dict[str, Any]:
        replaced = [display(path) for path in self.replace]
        left = [{"path": display(path), "reason": reason} for path, reason in self.left]
        return {
            "job_file": display(self.job_file) if self.job_file is not None else None,
            "replaced": replaced[:cap],
            "replaced_total": len(replaced),
            "left_alone": left[:cap],
            "left_alone_total": len(left),
        }


def plan(
    directory: Path, job_file_name: str, source: Path, read_job: Callable[[Path], Any]
) -> PreviousRun:
    """Which files of the earlier run in ``directory`` would be replaced (nothing is changed).

    Args:
        directory: The output directory (resolved)
        job_file_name: The job file's name in it
        source: The new run's input (never replaced)
        read_job: Reads and parses a job file, within the server's size limit (raises
            ToolInputError)

    Raises:
        ToolInputError: The job file's name is taken by something that is not a file
    """
    job_file = directory / job_file_name
    try:
        mode = os.lstat(job_file).st_mode
    except FileNotFoundError:
        return PreviousRun(directory, None)
    if not (stat.S_ISREG(mode) or stat.S_ISLNK(mode)):
        raise ToolInputError(
            f"output_dir has a {job_file_name} that is not a file; move it away first"
        )
    previous = PreviousRun(directory, job_file)
    try:
        if stat.S_ISLNK(mode):
            raise ToolInputError("it is a symbolic link")
        artifacts = JobResult.from_dict(read_job(job_file)["result"]).artifacts
    except (ToolInputError, ContractError, KeyError, TypeError) as error:
        reason = str(error).splitlines()[0] if str(error) else type(error).__name__
        previous.left.append((job_file, f"not read as an earlier run's result: {reason}"))
        return previous
    for artifact in artifacts:
        # The path that is checked is the path that would be replaced
        path = Path(os.path.normpath(directory / artifact.path))
        reason = _why_kept(artifact, path, directory, source)
        if reason is None:
            previous.replace.append(path)
        else:
            previous.left.append((path, reason))
    return previous


def _why_kept(artifact: Artifact, path: Path, directory: Path, source: Path) -> Optional[str]:
    """Why a listed file is not replaced, or None when it is still the earlier run's file."""
    if artifact.kind not in REPLACEABLE_KINDS:
        return f"kind {artifact.kind!r} is not a file a job writes"
    real = Path(os.path.realpath(path))
    if real.parent != directory:
        return "it is not a file directly in output_dir (a job writes only there)"
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return "it no longer exists"
    except OSError as error:
        return f"it cannot be examined ({error.strerror})"
    if stat.S_ISLNK(info.st_mode):
        return "it is a symbolic link"
    if not stat.S_ISREG(info.st_mode):
        return "it is not a regular file"
    if _same_file(path, source):
        return "it is the input of this run"
    if artifact.sha256 is None or info.st_size != artifact.bytes:
        return "it changed since that run (its size or SHA-256 differ from the record)"
    try:
        digest = _sha256(path)
    except OSError as error:
        return f"it cannot be read ({error.strerror})"
    if digest != artifact.sha256:
        return "it changed since that run (its size or SHA-256 differ from the record)"
    return None


def _same_file(path: Path, source: Path) -> bool:
    try:
        return os.path.samefile(path, source)
    except OSError:  # the input vanished: it is not this file then
        return False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Holding:
    """The earlier run's files, moved aside into a private directory inside the output directory
    while the new job runs (a rename: nothing is copied)."""

    def __init__(self, previous: PreviousRun):
        self.directory: Optional[Path] = None
        self._moved: List[Tuple[Path, Path]] = []
        files = list(previous.replace)
        if previous.job_file is not None:
            files.append(previous.job_file)
        if not files:
            return
        try:
            self.directory = Path(tempfile.mkdtemp(prefix=HOLDING_PREFIX, dir=previous.directory))
            for number, path in enumerate(files):
                held = self.directory / f"{number}-{path.name}"
                os.rename(path, held)
                self._moved.append((path, held))
        except OSError as error:
            self.restore()
            raise ToolInputError(
                f"the earlier run's files in output_dir cannot be moved aside ({error.strerror}); "
                "nothing was changed"
            ) from None

    def restore(self) -> None:
        """Put every file back where it was (the new job was refused, or the call failed)."""
        for path, held in reversed(self._moved):
            os.replace(held, path)
        self._moved = []
        if self.directory is not None:
            shutil.rmtree(self.directory, ignore_errors=True)
            self.directory = None

    def discard(self) -> None:
        """The new run happened: the earlier run's files go."""
        self._moved = []
        if self.directory is not None:
            shutil.rmtree(self.directory, ignore_errors=True)
            self.directory = None
