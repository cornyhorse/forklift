"""Check a leased job spec and decide how its input reaches the engine (ADR 0006).

The gateway sends an input from the store as a ``presigned_url`` location. An input up to the
staging limit is *staged*: the supervisor downloads it into scratch and rewrites the location to
``file``, so the engine needs no network. A larger one is *streamed*: the engine reads the URL
itself and may reach only that URL's host (``--allow-url-host``). The no-network profile refuses
streamed inputs and SQL locations, which both need the network.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Iterator, Sequence
from urllib.parse import unquote, urlsplit

from .gateway import SPEC_VERSIONS, Lease
from .transport import host_of, port_of

KINDS = ("run", "preview", "validate_schema", "generate_schema")
LOCATION_TYPES = ("file", "s3", "presigned_url", "sql", "sql_table")
SQL_LOCATIONS = ("sql", "sql_table")
INPUT_DIR = "in"
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")


class SpecRefused(Exception):
    """The worker will not run this spec; ``code`` is the JobResult error code to report."""

    def __init__(self, code: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


@dataclass(frozen=True)
class StagedInput:
    url: str
    host: str
    size: int
    etag: str | None
    path: str  # relative to the job's scratch directory


@dataclass
class JobPlan:
    spec: dict[str, Any]  # the spec the engine gets (staged inputs rewritten to ``file``)
    staged: list[StagedInput] = field(default_factory=list)
    stream_hosts: list[str] = field(default_factory=list)  # passed as --allow-url-host
    stream_ports: list[int] = field(default_factory=list)
    uses_sql: bool = False
    output_dirs: list[str] = field(default_factory=list)
    max_seconds: float | None = None


def format_bytes(count: int) -> str:
    """``2147483648`` -> ``"2.0 GiB (2147483648 bytes)"``."""
    value, unit = float(count), "bytes"
    for next_unit in ("KiB", "MiB", "GiB", "TiB"):
        if value < 1024:
            break
        value, unit = value / 1024, next_unit
    return f"{count} bytes" if unit == "bytes" else f"{value:.1f} {unit} ({count} bytes)"


def _refuse(message: str) -> SpecRefused:
    return SpecRefused("SPEC_INVALID", message)


def _safe_relative(path: Any, where: str) -> str:
    """A path relative to scratch that cannot leave it."""
    if not isinstance(path, str) or not path or "\x00" in path or "\\" in path:
        raise _refuse(f"{where} has no usable path.")
    pure = PurePosixPath(path)
    if pure.is_absolute() or ".." in pure.parts:
        raise _refuse(
            f"{where} has the path {path!r}: paths must be relative to the job's scratch "
            "directory and must not contain '..'."
        )
    return path


def _locations(section: Any, where: str) -> Iterator[tuple[str, dict[str, Any]]]:
    """Location objects in a spec section: its ``location`` and any other location-shaped field."""
    if not isinstance(section, dict):
        return
    for key, value in section.items():
        if isinstance(value, dict) and value.get("type") in LOCATION_TYPES:
            yield f"{where}.{key}", value
        elif key == "location":
            raise _refuse(f"{where}.location is not a location object with a known type.")


def staged_name(url: str) -> str:
    """A safe file name for a staged input, from the last segment of the URL's path."""
    name = PurePosixPath(unquote(urlsplit(url).path)).name
    name = _UNSAFE_NAME.sub("_", name)[-100:].lstrip(".")
    return name or "input"


def _limits(spec: dict[str, Any]) -> tuple[float | None, int | None]:
    limits = {} if spec.get("limits") is None else spec["limits"]
    if not isinstance(limits, dict):
        raise _refuse("limits is not an object.")
    max_seconds = limits.get("max_seconds")
    max_input = limits.get("max_input_bytes")
    for name, value in (("max_seconds", max_seconds), ("max_input_bytes", max_input)):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0
        ):
            raise _refuse(f"limits.{name} is not a positive number.")
    return (None if max_seconds is None else float(max_seconds)), max_input


def plan_job(
    lease: Lease,
    *,
    stage_max_bytes: int,
    network_allowed: bool,
    store_hosts: Sequence[str] = (),
) -> JobPlan:
    """Check ``lease.spec`` and plan the job; raises SpecRefused with the code to report."""
    spec = copy.deepcopy(lease.spec)
    if spec.get("spec_version") not in SPEC_VERSIONS:
        raise _refuse(
            f"spec_version {spec.get('spec_version')!r} is not one this worker runs "
            f"({', '.join(map(str, SPEC_VERSIONS))})."
        )
    if spec.get("job_id") != lease.job_id:
        raise _refuse(f"The spec's job_id does not match the leased job {lease.job_id}.")
    if spec.get("kind") not in KINDS:
        raise _refuse(f"kind {spec.get('kind')!r} is not one of {', '.join(KINDS)}.")
    if not isinstance(spec.get("input"), dict):
        raise _refuse("The spec has no input object.")
    if spec.get("output") is not None and not isinstance(spec["output"], dict):
        raise _refuse("output is neither an object nor null.")
    max_seconds, max_input = _limits(spec)
    planner = _Planner(
        JobPlan(spec=spec, max_seconds=max_seconds),
        stage_max_bytes=stage_max_bytes,
        max_input=max_input,
        network_allowed=network_allowed,
        store_hosts=store_hosts,
    )
    for section in ("input", "output"):
        found = list(_locations(spec.get(section), section))
        if section == "input" and not any(where == "input.location" for where, _ in found):
            raise _refuse("input.location is missing.")
        for where, location in found:
            planner.check(location, where, section)
    if planner.plan.uses_sql and not network_allowed:
        raise _refuse(
            "This job needs the engine to reach a database (an sql or sql_table location), but "
            "this worker runs the no-network isolation profile. Run it on a standard-profile "
            "worker of the sql lane."
        )
    return planner.plan


@dataclass
class _Planner:
    plan: JobPlan
    stage_max_bytes: int
    max_input: int | None
    network_allowed: bool
    store_hosts: Sequence[str]

    def check(self, location: dict[str, Any], where: str, section: str) -> None:
        kind = location["type"]
        if kind == "s3":
            raise _refuse(
                f"{where} is an s3 location. Those are for library use with the caller's own "
                "credentials; workers hold none, so the gateway must send presigned_url "
                "locations."
            )
        if kind in SQL_LOCATIONS:
            self.plan.uses_sql = True
        elif kind == "file":
            path = _safe_relative(location.get("path"), where)
            if section == "output":
                self.plan.output_dirs.append(path.rstrip("/") or ".")
        elif section != "input":
            raise _refuse(f"{where} is a presigned_url location; those are for inputs only.")
        else:
            self.presigned(location, where)

    def presigned(self, location: dict[str, Any], where: str) -> None:
        url, size, etag = location.get("url"), location.get("size"), location.get("etag")
        try:
            host = host_of(url if isinstance(url, str) else "")
        except ValueError:
            raise _refuse(f"{where}.url is not an http:// or https:// URL.") from None
        if self.store_hosts and host not in self.store_hosts:
            raise _refuse(
                f"{where} points at {host}, which is not an object store this worker may read "
                f"(--store-host: {', '.join(self.store_hosts)})."
            )
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _refuse(f"{where}.size is not a whole number of bytes.")
        if etag is not None and not isinstance(etag, str):
            raise _refuse(f"{where}.etag is not a string.")
        if self.max_input is not None and size > self.max_input:
            raise SpecRefused(
                "LIMIT_EXCEEDED",
                f"The input is {format_bytes(size)}, more than the job's limit of "
                f"{format_bytes(int(self.max_input))} (limits.max_input_bytes).",
            )
        if size <= self.stage_max_bytes:
            path = f"{INPUT_DIR}/{staged_name(url)}"
            self.plan.staged.append(StagedInput(url, host, size, etag or None, path))
            location.clear()
            location.update({"type": "file", "path": path})
        elif not self.network_allowed:
            raise SpecRefused(
                "LIMIT_EXCEEDED",
                f"The input is {format_bytes(size)}, larger than this worker's staging limit of "
                f"{format_bytes(self.stage_max_bytes)}, and this worker runs the no-network "
                "isolation profile, which takes staged inputs only. Run the job on a "
                "standard-profile worker, or raise the staging limit (the gateway's "
                "stage_max_bytes and this worker's --stage-max-bytes).",
            )
        elif host not in self.plan.stream_hosts:
            self.plan.stream_hosts.append(host)
            self.plan.stream_ports.append(port_of(url))
