"""One job, from lease to completion (design §6.1).

    lease -> check the spec -> stage inputs -> write spec.json -> run the engine
          -> upload artifacts -> complete -> remove scratch

A heartbeat thread keeps the lease alive and carries progress the whole time. A cancel request
(in a heartbeat reply) stops the engine and the job completes as cancelled, without artifacts. A
lost lease (HTTP 409, or no heartbeat accepted for a whole lease period) and a worker shutdown
stop the engine and upload and report nothing: the lease expires and the gateway queues the job
again. Every failure the supervisor sees itself becomes a failed JobResult with a clear error,
and the scratch directory is removed in every case.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import stat
import threading
import time
from pathlib import Path
from typing import Any, Callable

from . import logs
from .engine import EngineRun, EngineRunner
from .gateway import (
    GatewayAuthError,
    GatewayClient,
    GatewayError,
    GatewayRejected,
    GatewayUnavailable,
    Lease,
    LeaseLost,
)
from .isolation import Isolation
from .redact import Redactor, secrets_of
from .results import (
    ResultInvalid,
    bounded,
    collect_artifacts,
    crash_message,
    read_result,
    synthesized,
    with_tail,
)
from .retry import Backoff, Interrupted, retry
from .sandbox import EXIT_SANDBOX, PREFIX
from .settings import Settings
from .spec import JobPlan, SpecRefused, format_bytes, plan_job
from .staging import StagingError, stage_input
from .transport import HttpClient
from .uploads import UploadError, upload_artifact

log = logs.logger("job")

MIN_HEARTBEAT_SECONDS = 0.05
PRESIGN_ATTEMPTS = 5
COMPLETE_ATTEMPTS = 8
_UNSAFE = re.compile(r"[^A-Za-z0-9_-]")


class JobControl:
    """What the job's thread, its heartbeat thread and the supervisor share."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.stopped = threading.Event()  # any reason to stop
        self.abandon = threading.Event()  # lost or shut down: report nothing
        self.cancelled = False
        self.lost: str | None = None
        self.shut_down = False
        self._progress: dict[str, int] = {"rows_read": 0, "rows_rejected": 0, "bytes_read": 0}

    def request_cancel(self) -> None:
        self.cancelled = True
        self.stopped.set()

    def mark_lost(self, why: str) -> None:
        with self._lock:
            self.lost = self.lost or why
        self.abandon.set()
        self.stopped.set()

    def shutdown(self) -> None:
        self.shut_down = True
        self.abandon.set()
        self.stopped.set()

    def stop_reason(self) -> str | None:
        if self.lost:
            return "lost"
        if self.shut_down:
            return "shutdown"
        return "cancel" if self.cancelled else None

    def update_progress(self, event: dict[str, int]) -> None:
        with self._lock:
            self._progress.update(event)

    def snapshot(self) -> dict[str, int]:
        """The progress a heartbeat carries (whole numbers only: the gateway takes counters)."""
        with self._lock:
            return dict(self._progress)


class _Abandon(Exception):
    """Stop without uploading or reporting anything."""


class Heartbeater:
    def __init__(
        self,
        gateway: GatewayClient,
        lease: Lease,
        control: JobControl,
        *,
        interval: float | None,
        on_fatal: Callable[[Exception], None],
        context: dict[str, Any],
        clock: Callable[[], float] = time.monotonic,
    ):
        self.gateway, self.lease, self.control = gateway, lease, control
        self.fixed_interval = interval
        self.on_fatal = on_fatal
        self.context = context
        self.clock = clock
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="heartbeat")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._done.set()
        self._thread.join(timeout=5)

    def interval(self, lease_seconds: float) -> float:
        return self.fixed_interval or max(MIN_HEARTBEAT_SECONDS, lease_seconds / 3)

    def _loop(self) -> None:
        lease_seconds = self.lease.lease_seconds
        last_accepted = self.clock()
        while not self._done.wait(self.interval(lease_seconds)):
            try:
                reply = self.gateway.heartbeat(
                    self.lease.job_id, self.lease.attempt, self.control.snapshot()
                )
            except LeaseLost as error:
                self.control.mark_lost(str(error))
                return
            except GatewayAuthError as error:
                self.control.mark_lost(str(error))
                self.on_fatal(error)
                return
            except GatewayError as error:
                silent = self.clock() - last_accepted
                if silent >= lease_seconds:
                    self.control.mark_lost(
                        f"No heartbeat was accepted for {silent:.1f} seconds, the whole "
                        f"{lease_seconds:g}-second lease (last error: {error})"
                    )
                    return
                log.warning("heartbeat failed: %s", error, extra=self.context)
                continue
            last_accepted = self.clock()
            lease_seconds = reply.lease_seconds or lease_seconds
            if reply.cancel and not self.control.cancelled:
                log.info("the gateway asked to cancel the job", extra=self.context)
                self.control.request_cancel()


def _make_writable_and_retry(function: Callable, target: str, _error: BaseException) -> None:
    """rmtree's error handler: the engine may have made a directory unwritable or unreadable."""
    os.chmod(os.path.dirname(target), stat.S_IRWXU)
    if os.path.isdir(target) and not os.path.islink(target):
        os.chmod(target, stat.S_IRWXU)
    function(target)


def remove_tree(path: Path) -> None:
    """``rm -rf``, also through directories the engine made unwritable."""
    shutil.rmtree(path, onexc=_make_writable_and_retry)


class JobRunner:
    def __init__(
        self,
        settings: Settings,
        gateway: GatewayClient,
        http: HttpClient,
        isolation: Isolation,
        *,
        engine: EngineRunner | None = None,
        on_fatal: Callable[[Exception], None] = lambda error: None,
    ):
        self.settings = settings
        self.gateway = gateway
        self.http = http
        self.isolation = isolation
        self.engine = engine or EngineRunner(isolation)
        self.on_fatal = on_fatal

    # ----------------------------------------------------------------------- entry point

    def run(self, lease: Lease, control: JobControl) -> str:
        """Run one leased job; returns its outcome (succeeded, failed, cancelled, abandoned or
        unreported)."""
        context = {"job_id": lease.job_id, "attempt": lease.attempt}
        spec_input = lease.spec.get("input") if isinstance(lease.spec.get("input"), dict) else {}
        log.info(
            "job leased",
            extra={
                **context,
                "kind": lease.spec.get("kind"),
                "input_format": spec_input.get("format"),
                "lease_seconds": lease.lease_seconds,
            },
        )
        heartbeat = Heartbeater(
            self.gateway,
            lease,
            control,
            interval=self.settings.heartbeat_seconds,
            on_fatal=self.on_fatal,
            context=context,
        )
        heartbeat.start()
        workdir: Path | None = None
        try:
            try:
                workdir = self._make_workdir(lease)
            except OSError as error:
                message = (
                    "The worker could not create the job's scratch directory in "
                    f"{self.settings.scratch} ({error.strerror or error})."
                )
                result = synthesized(lease.job_id, "failed", "INTERNAL", message, retryable=True)
                outcome = self._complete(lease, control, result, [], context)
            else:
                outcome = self._run(lease, control, workdir, context)
        finally:
            heartbeat.stop()
            if workdir is not None:
                try:
                    remove_tree(workdir)
                except OSError as error:
                    log.error(
                        "the job's scratch directory could not be removed: %s",
                        error,
                        extra={**context, "path": str(workdir)},
                    )
        log.info("job finished", extra={**context, "outcome": outcome})
        return outcome

    def _make_workdir(self, lease: Lease) -> Path:
        name = f"job-{_UNSAFE.sub('_', lease.job_id)[:64]}-a{lease.attempt}-{secrets.token_hex(4)}"
        workdir = self.settings.scratch / name
        workdir.mkdir(mode=0o700)
        return workdir

    def _run(self, lease: Lease, control: JobControl, workdir: Path, context: dict) -> str:
        redactor = Redactor(secrets_of(lease.spec))
        try:
            result, entries = self._produce(lease, control, workdir, redactor, context)
        except _Abandon as reason:
            log.warning("job abandoned; nothing uploaded or reported: %s", reason, extra=context)
            return "abandoned"
        return self._complete(lease, control, redactor.deep(result), entries, context)

    # ----------------------------------------------------------------------- phases

    def _interrupted(self, lease: Lease, control: JobControl, phase: str):
        if control.abandon.is_set():
            raise _Abandon(control.lost or "the worker is shutting down")
        return (
            synthesized(
                lease.job_id, "cancelled", "CANCELLED", f"The job was cancelled while {phase}."
            ),
            [],
        )

    def _produce(
        self, lease: Lease, control: JobControl, workdir: Path, redactor: Redactor, context
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        job_id = lease.job_id
        stage_max = lease.stage_max_bytes
        if self.settings.stage_max_bytes is not None:
            stage_max = min(stage_max, self.settings.stage_max_bytes)
        try:
            plan = plan_job(
                lease,
                stage_max_bytes=stage_max,
                network_allowed=self.isolation.network_allowed,
                store_hosts=self.settings.store_host,
            )
        except SpecRefused as refused:
            log.warning("spec refused: %s", refused.code, extra=context)
            return synthesized(job_id, "failed", refused.code, refused.message), []
        try:
            self._stage(plan, control, workdir, context)
        except StagingError as error:
            return (
                synthesized(
                    job_id, "failed", error.code, error.message, retryable=error.retryable
                ),
                [],
            )
        except Interrupted:
            return self._interrupted(lease, control, "its input was being staged")
        if control.stopped.is_set():
            return self._interrupted(lease, control, "its input was being staged")
        self._write_spec(workdir, plan)
        timeout = min(
            plan.max_seconds or self.settings.max_job_seconds, self.settings.max_job_seconds
        )
        run = self.engine.run(
            workdir,
            plan,
            timeout=timeout,
            stop_reason=control.stop_reason,
            update_progress=control.update_progress,
            redactor=redactor,
            context=context,
        )
        log.info(
            "engine exited",
            extra={**context, "returncode": run.returncode, "seconds": round(run.seconds, 3)},
        )
        if control.abandon.is_set():
            raise _Abandon(control.lost or "the worker is shutting down")
        result = self._interpret(run, workdir, job_id, timeout)
        if result["status"] == "cancelled":
            return result, []
        try:
            return self._upload(lease, control, result, workdir, plan, context)
        except Interrupted:
            return self._interrupted(lease, control, "its artifacts were being uploaded")

    def _stage(self, plan: JobPlan, control: JobControl, workdir: Path, context) -> None:
        for item in plan.staged:
            started = time.monotonic()
            stage_input(
                self.http,
                item,
                workdir,
                stopped=control.stopped.is_set,
                wait=control.stopped.wait,
                on_progress=lambda received: control.update_progress({"bytes_staged": received}),
            )
            log.info(
                "input staged",
                extra={
                    **context,
                    "bytes": item.size,
                    "host": item.host,
                    "seconds": round(time.monotonic() - started, 3),
                },
            )
        for directory in plan.output_dirs:
            (workdir / directory).mkdir(mode=0o700, parents=True, exist_ok=True)
        if plan.stream_hosts:
            log.info(
                "input streamed by the engine",
                extra={**context, "hosts": plan.stream_hosts},
            )

    @staticmethod
    def _write_spec(workdir: Path, plan: JobPlan) -> None:
        """spec.json, readable by this user only (SQL jobs carry connection strings in it)."""
        descriptor = os.open(
            workdir / "spec.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(plan.spec, handle)

    def _interpret(self, run: EngineRun, workdir: Path, job_id: str, timeout: float) -> dict:
        """The JobResult to report for an engine run (one that was not abandoned)."""
        killed = " It ignored SIGTERM and was killed." if run.killed else ""
        if run.stop_reason == "timeout":
            return synthesized(
                job_id,
                "failed",
                "LIMIT_EXCEEDED",
                f"The engine did not finish within {timeout:g} seconds, the job's wall-clock "
                f"limit (limits.max_seconds, at most --max-job-seconds).{killed}",
            )
        problem = None
        try:
            result = read_result(workdir / "result.json", job_id)
        except ResultInvalid as error:
            result, problem = None, str(error)
        if run.stop_reason == "cancel":
            message = f"The job was cancelled while the engine was running.{killed}"
            return synthesized(job_id, "cancelled", "CANCELLED", message, base=result)
        if result is not None:
            return result
        if problem is not None:
            message = (
                f"The engine's result cannot be used: {problem} (exit code {run.returncode})."
            )
            return synthesized(job_id, "failed", "INTERNAL", with_tail(message, run.stderr_tail))
        last_line = run.stderr_tail.rsplit("\n", 1)[-1]
        if run.returncode == EXIT_SANDBOX and last_line.startswith(PREFIX):
            return synthesized(
                job_id,
                "failed",
                "INTERNAL",
                "The engine's sandbox could not be set up on this worker: "
                f"{last_line[len(PREFIX):]}.",
                retryable=True,
            )
        code, message = crash_message(run.returncode, run.stderr_tail, run.limits)
        return synthesized(job_id, "failed", code, message)

    def _upload(
        self,
        lease: Lease,
        control: JobControl,
        result: dict[str, Any],
        workdir: Path,
        plan: JobPlan,
        context,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        job_id = lease.job_id
        try:
            files = collect_artifacts(result, workdir, plan.output_dirs)
        except ResultInvalid as error:
            return (
                synthesized(
                    job_id,
                    "failed",
                    "INTERNAL",
                    f"The engine's result cannot be used: {error}.",
                    base=result,
                ),
                [],
            )
        if not files:
            return result, []
        try:
            targets = retry(
                lambda: self.gateway.presign(
                    job_id, lease.attempt, [{"name": f.name, "bytes": f.bytes} for f in files]
                ),
                attempts=PRESIGN_ATTEMPTS,
                retryable=lambda error: isinstance(error, GatewayUnavailable),
                backoff=Backoff(0.5, 10.0),
                wait=control.stopped.wait,
            )
        except LeaseLost as error:
            raise _Abandon(str(error)) from None
        except GatewayAuthError as error:
            self.on_fatal(error)
            raise _Abandon(str(error)) from None
        except GatewayRejected as error:
            return (
                synthesized(
                    job_id,
                    "failed",
                    "INTERNAL",
                    f"The gateway would not presign the job's artifacts: {error}",
                    base=result,
                ),
                [],
            )
        except GatewayUnavailable as error:
            raise _Abandon(f"the artifacts could not be presigned: {error}") from None
        entries = []
        for item in files:
            target = targets[item.name]
            try:
                upload_artifact(
                    self.http,
                    target,
                    item,
                    stopped=control.stopped.is_set,
                    wait=control.stopped.wait,
                )
            except UploadError as error:
                return (
                    synthesized(
                        job_id,
                        "failed",
                        "INTERNAL",
                        error.message,
                        retryable=error.retryable,
                        base=result,
                    ),
                    [],
                )
            entries.append(item.entry(target.key))
            reported = result["artifacts"][item.index]
            reported["bytes"], reported["sha256"] = item.bytes, item.sha256
        log.info(
            "artifacts uploaded",
            extra={
                **context,
                "artifacts": len(entries),
                "bytes": sum(item.bytes for item in files),
                "size": format_bytes(sum(item.bytes for item in files)),
            },
        )
        return result, entries

    def _complete(
        self,
        lease: Lease,
        control: JobControl,
        result: dict[str, Any],
        entries: list[dict[str, Any]],
        context,
    ) -> str:
        reportable = bounded(result)
        if reportable is not result:
            log.warning("the result was too large to report", extra=context)
            result, entries = reportable, []
        error = result.get("error") or {}
        try:
            retry(
                lambda: self.gateway.complete(lease.job_id, lease.attempt, result, entries),
                attempts=COMPLETE_ATTEMPTS,
                retryable=lambda failure: isinstance(failure, GatewayUnavailable),
                backoff=Backoff(0.5, 15.0),
                wait=control.abandon.wait,
            )
        except Interrupted:
            log.warning("job abandoned before its result was reported", extra=context)
            return "abandoned"
        except LeaseLost as failure:
            log.warning("the result was not accepted: %s", failure, extra=context)
            return "abandoned"
        except GatewayError as failure:
            if isinstance(failure, GatewayAuthError):
                self.on_fatal(failure)
            log.error("the result could not be reported: %s", failure, extra=context)
            return "unreported"
        log.info(
            "job completed",
            extra={**context, "status": result["status"], "error_code": error.get("code")},
        )
        return result["status"]
