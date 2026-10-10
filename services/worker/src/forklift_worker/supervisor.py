"""The supervisor loop: lease a job, run it, repeat; back off while idle; drain on SIGTERM.

With ``--concurrency N``, N slots each lease and run one job at a time. Stopping (SIGTERM or
SIGINT): the first signal stops leasing, and running jobs may finish for up to
``--drain-seconds``; then their engines are stopped and the jobs handed back (nothing is uploaded
or reported, their leases expire and the gateway queues them again). A second signal stops them
at once. A worker token the gateway refuses, or a gateway that does not speak this worker's
internal API, stops the worker with exit code 3; an internal error with exit code 1.
"""

from __future__ import annotations

import fcntl
import os
import threading
import time
from importlib import metadata
from typing import Callable

from . import __version__, linux, logs
from .gateway import GatewayAuthError, GatewayClient, GatewayRejected, GatewayUnavailable
from .isolation import Isolation, IsolationError, Platform
from .job import JobControl, JobRunner, remove_tree
from .retry import Backoff
from .settings import Settings
from .transport import HttpClient

log = logs.logger("supervisor")

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFIG = 2
EXIT_GATEWAY = 3
LOCK_FILE = ".forklift-worker.lock"


class StartupError(Exception):
    """The worker cannot start with these settings on this platform."""


def installed_engine_version() -> str:
    try:
        return metadata.version("forklift-etl")
    except metadata.PackageNotFoundError:
        return "unknown"


class Supervisor:
    def __init__(
        self,
        settings: Settings,
        *,
        isolation: Isolation | None = None,
        http: HttpClient | None = None,
        engine_version: str | None = None,
        runner_factory: Callable[["Supervisor", GatewayClient], JobRunner] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.settings = settings
        self.http = http or HttpClient(
            timeout=settings.http_timeout, user_agent=settings.user_agent, ca_file=settings.ca_file
        )
        self.isolation = isolation or Isolation(settings, Platform.probe(settings.isolation))
        self.engine_version = engine_version or installed_engine_version()
        self.runner_factory = runner_factory or (
            lambda supervisor, gateway: JobRunner(
                supervisor.settings,
                gateway,
                supervisor.http,
                supervisor.isolation,
                on_fatal=supervisor.fatal,
            )
        )
        self.clock = clock
        self.draining = threading.Event()
        self.halted = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.RLock()  # also taken from signal handlers in the main thread
        self._controls: set[JobControl] = set()
        self._reserved = 0
        self._stop_requests = 0
        self._drain_deadline: float | None = None
        self._lock_descriptor: int | None = None
        self.error: Exception | None = None
        self.crashed = False
        self.outcomes: list[str] = []

    # ----------------------------------------------------------------------- start and stop

    def worker_ids(self) -> list[str]:
        base = self.settings.worker_id
        if self.settings.concurrency == 1:
            return [base]
        return [f"{base}-{n}" for n in range(1, self.settings.concurrency + 1)]

    def prepare(self) -> list[str]:
        """Check the isolation profile and take the scratch directory; returns warnings."""
        try:
            warnings = self.isolation.check()
        except IsolationError as error:
            raise StartupError(str(error)) from None
        try:
            linux.set_child_subreaper()
        except OSError as error:  # Linux 3.4 and later have it; a filter could refuse it
            warnings.append(
                f"The supervisor cannot become its engines' subreaper ({error.strerror}): "
                "processes an engine leaves running outside its process group are not killed."
            )
        scratch = self.settings.scratch
        try:
            scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(scratch / LOCK_FILE, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        except OSError as error:
            raise StartupError(
                f"The scratch directory {scratch} cannot be used ({error.strerror or error})."
            ) from None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise StartupError(
                f"Another forklift-worker is using the scratch directory {scratch}; give each "
                "worker process its own."
            ) from None
        self._lock_descriptor = descriptor
        leftovers = [p for p in scratch.iterdir() if p.name.startswith("job-") and p.is_dir()]
        for leftover in leftovers:
            remove_tree(leftover)
        if leftovers:
            log.info(
                "removed scratch directories a previous worker left behind",
                extra={"count": len(leftovers)},
            )
        return warnings

    def release(self) -> None:
        if self._lock_descriptor is not None:
            os.close(self._lock_descriptor)
            self._lock_descriptor = None

    def request_stop(self) -> None:
        """SIGTERM/SIGINT: the first one drains, the second one stops running jobs now."""
        with self._lock:
            self._stop_requests += 1
            if self._stop_requests == 1:
                log.info(
                    "stopping: no new leases; running jobs may finish",
                    extra={"drain_seconds": self.settings.drain_seconds},
                )
                self._drain_deadline = self.clock() + self.settings.drain_seconds
                self.draining.set()
            else:
                self.halt("a second stop signal")
        self._wake.set()

    def halt(self, why: str) -> None:
        """Stop every running engine and hand its job back."""
        with self._lock:
            if not self.halted.is_set():
                log.warning("stopping running jobs: %s", why, extra={"jobs": len(self._controls)})
            self.halted.set()
            self.draining.set()
            for control in self._controls:
                control.shutdown()
        self._wake.set()

    def fatal(self, error: Exception) -> None:
        """The gateway refused this worker: stop everything, exit with EXIT_GATEWAY."""
        with self._lock:
            if self.error is None:
                self.error = error
                log.error("%s", error)
        self.halt("the gateway refused this worker")

    # ----------------------------------------------------------------------- running

    def run(self) -> int:
        warnings = self.prepare()
        try:
            for warning in warnings:
                log.warning("%s", warning)
            log.info(
                "forklift-worker started",
                extra={
                    "worker_version": __version__,
                    "engine_version": self.engine_version,
                    "worker_ids": self.worker_ids(),
                    "lanes": self.settings.lanes,
                    "gateway": self.settings.gateway,
                    "isolation": self.isolation.describe(),
                },
            )
            slots = [
                threading.Thread(target=self._slot, args=(worker_id,), name=f"slot-{n}")
                for n, worker_id in enumerate(self.worker_ids(), start=1)
            ]
            for slot in slots:
                slot.start()
            while any(slot.is_alive() for slot in slots):
                self._wake.wait(0.2)
                self._wake.clear()
                with self._lock:
                    deadline = self._drain_deadline
                if deadline is not None and self.clock() >= deadline and not self.halted.is_set():
                    self.halt(f"--drain-seconds ({self.settings.drain_seconds:g}) is over")
            for slot in slots:
                slot.join()
        finally:
            self.release()
        log.info("forklift-worker stopped", extra={"jobs": len(self.outcomes)})
        if self.crashed:
            return EXIT_INTERNAL
        return EXIT_GATEWAY if self.error else EXIT_OK

    def _reserve(self) -> bool:
        """Claim the right to lease one more job (--max-jobs)."""
        with self._lock:
            if self.draining.is_set():
                return False
            if self.settings.max_jobs and self._reserved >= self.settings.max_jobs:
                return False
            self._reserved += 1
            return True

    def _unreserve(self) -> None:
        with self._lock:
            self._reserved -= 1

    def _slot(self, worker_id: str) -> None:
        gateway = GatewayClient(
            self.http,
            self.settings.gateway,
            self.settings.token_file,
            worker_id=worker_id,
            lanes=self.settings.lanes,
            engine_version=self.engine_version,
        )
        runner = self.runner_factory(self, gateway)
        idle = Backoff(self.settings.idle_min_seconds, self.settings.idle_max_seconds)
        context = {"worker_id": worker_id}
        unreachable = False
        while self._reserve():
            try:
                lease = gateway.lease()
            except (GatewayAuthError, GatewayRejected) as error:
                self._unreserve()
                self.fatal(error)
                return
            except GatewayUnavailable as error:
                self._unreserve()
                if not unreachable:
                    log.warning("lease request failed: %s", error, extra=context)
                unreachable = True
                self.draining.wait(idle.next())
                continue
            if unreachable:
                log.info("the gateway is reachable again", extra=context)
                unreachable = False
            if lease is None:
                self._unreserve()
                self.draining.wait(idle.next())
                continue
            idle.reset()
            self._run_job(runner, lease)

    def _run_job(self, runner: JobRunner, lease) -> None:
        control = JobControl()
        with self._lock:
            self._controls.add(control)
            if self.halted.is_set():
                control.shutdown()
        try:
            outcome = runner.run(lease, control)
        except Exception as error:  # a bug in this worker: stop rather than lose jobs quietly
            log.exception("internal error while running a job; stopping the worker")
            with self._lock:
                self.error = self.error or error
                self.crashed = True
            self.halt("an internal error")
            outcome = "crashed"
        finally:
            with self._lock:
                self._controls.discard(control)
        with self._lock:
            self.outcomes.append(outcome)
            if self.settings.max_jobs and len(self.outcomes) >= self.settings.max_jobs:
                self.draining.set()
        self._wake.set()
