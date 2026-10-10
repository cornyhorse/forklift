"""Run ``forklift run-job`` for one job in its sandbox, and watch it until it exits.

The engine starts as ``python -I -m forklift_worker.sandbox CONFIG -- <engine command> run-job
spec.json --base-dir <scratch> --result result.json [--allow-url-host HOST] --progress-jsonl``,
in its own session (so the whole process group can be signalled), with the scratch directory as
working directory, stdin from /dev/null and the environment from ``Isolation.environment``.

Progress arrives as JSON lines on stdout; only small non-negative whole numbers under simple
names are kept, and over-long lines are dropped. stderr is kept as a bounded, redacted tail for
error messages (and logged at debug level). The engine is stopped with SIGTERM (it then writes a
cancelled result) and, after the grace period, SIGKILL.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Callable, Iterator

from . import logs
from .isolation import Isolation
from .redact import Redactor
from .spec import JobPlan

log = logs.logger("engine")

POLL_SECONDS = 0.05
MAX_LINE_BYTES = 64 * 1024
MAX_PROGRESS_KEYS = 16  # the gateway keeps 20 counters, and the supervisor adds its own
STDERR_TAIL_LINES = 20
STDERR_TAIL_CHARS = 4000
_PROGRESS_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True)
class EngineRun:
    returncode: int
    stop_reason: str | None  # None, "cancel", "timeout", "lost" or "shutdown"
    killed: bool  # SIGKILL was needed after the grace period
    stderr_tail: str  # redacted
    seconds: float
    limits: dict[str, list[int]]


def bounded_lines(stream: IO[bytes], limit: int = MAX_LINE_BYTES) -> Iterator[bytes]:
    """Lines of at most ``limit`` bytes; longer ones are skipped whole."""
    while True:
        line = stream.readline(limit + 1)
        if not line:
            return
        if len(line) > limit and not line.endswith(b"\n"):
            while line and not line.endswith(b"\n"):
                line = stream.readline(limit + 1)
            continue
        yield line


def progress_event(line: bytes) -> dict[str, int] | None:
    """The numbers in one progress line, or None if it has none worth passing on."""
    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    kept = {
        key: value
        for key, value in event.items()
        if isinstance(key, str)
        and _PROGRESS_KEY.match(key)
        and isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value < 2**63
    }
    return dict(list(kept.items())[:MAX_PROGRESS_KEYS]) or None


class StderrTail:
    """The last lines of the engine's stderr, redacted."""

    def __init__(self, redactor: Redactor):
        self.redactor = redactor
        self.lines: deque[str] = deque(maxlen=STDERR_TAIL_LINES)

    def add(self, line: bytes) -> str:
        text = self.redactor(line.decode("utf-8", "replace").rstrip("\r\n"))[:STDERR_TAIL_CHARS]
        self.lines.append(text)
        return text

    def text(self) -> str:
        return "\n".join(self.lines)[-STDERR_TAIL_CHARS:]


def _read_progress(stream: IO[bytes], update: Callable[[dict[str, int]], None]) -> None:
    try:
        for line in bounded_lines(stream):
            event = progress_event(line)
            if event:
                update(event)
    except (OSError, ValueError):  # the pipe was closed under us (see EngineRunner.run)
        pass


def _read_stderr(stream: IO[bytes], tail: StderrTail, context: dict[str, Any]) -> None:
    try:
        for line in bounded_lines(stream):
            text = tail.add(line)
            log.debug("engine: %s", text, extra=context)
    except (OSError, ValueError):  # the pipe was closed under us (see EngineRunner.run)
        pass


# Engines that are running (their pids), so that what they leave behind can be told from them.
_engines: set[int] = set()
_engines_lock = threading.Lock()


def _children() -> list[tuple[int, int]]:
    """(pid, session id) of this process's children, from /proc."""
    me, found = os.getpid(), []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", encoding="ascii", errors="replace") as handle:
                fields = handle.read().rsplit(")", 1)[1].split()
        except (OSError, IndexError):
            continue  # gone, or not readable
        if int(fields[1]) == me:
            found.append((int(entry), int(fields[3])))
    return found


def kill_orphans() -> list[int]:
    """Kill and reap what engines left running (the supervisor is their subreaper).

    An engine's descendants that outlive it, even after leaving its process group with setsid,
    are reparented to the supervisor. Any child that is not a running engine and not in the
    supervisor's own session is one of them. Returns their pids.
    """
    own_session = os.getsid(0)
    with _engines_lock:
        orphans = [
            pid for pid, session in _children() if pid not in _engines and session != own_session
        ]
        for pid in orphans:
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except ChildProcessError:  # reaped between the scan and the wait
                pass
    return orphans


def _signal_group(process: subprocess.Popen, number: int) -> None:
    try:
        os.killpg(process.pid, number)
    except (ProcessLookupError, PermissionError):
        pass


class EngineRunner:
    def __init__(self, isolation: Isolation, *, clock: Callable[[], float] = time.monotonic):
        self.isolation = isolation
        self.settings = isolation.settings
        self.clock = clock

    def command(self, workdir: Path, plan: JobPlan, config: dict[str, Any]) -> list[str]:
        engine = [
            *self.settings.engine_command,
            "run-job",
            str(workdir / "spec.json"),
            "--base-dir",
            str(workdir),
            "--result",
            str(workdir / "result.json"),
        ]
        for host in plan.stream_hosts:
            engine += ["--allow-url-host", host]
        engine.append("--progress-jsonl")
        sandbox = [sys.executable, "-I", "-m", "forklift_worker.sandbox"]
        return [*sandbox, json.dumps(config), "--", *engine]

    def run(
        self,
        workdir: Path,
        plan: JobPlan,
        *,
        timeout: float,
        stop_reason: Callable[[], str | None],
        update_progress: Callable[[dict[str, int]], None],
        redactor: Redactor,
        context: dict[str, Any],
    ) -> EngineRun:
        """Run the engine until it exits; ``stop_reason()`` returning a reason stops it."""
        env = self.isolation.environment(workdir, plan)
        config = self.isolation.sandbox_config(workdir, plan, env, timeout)
        (workdir / "tmp").mkdir(mode=0o700, exist_ok=True)
        started = self.clock()
        with _engines_lock:
            process = subprocess.Popen(
                self.command(workdir, plan, config),
                cwd=workdir,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            _engines.add(process.pid)
        log.info("engine started", extra={**context, "pid": process.pid})
        tail = StderrTail(redactor)
        readers = [
            threading.Thread(
                target=_read_progress, args=(process.stdout, update_progress), daemon=True
            ),
            threading.Thread(
                target=_read_stderr, args=(process.stderr, tail, context), daemon=True
            ),
        ]
        for reader in readers:
            reader.start()
        reason, killed, stopped_at = None, False, 0.0
        # Wait for the exit without reaping (WNOWAIT): until it is reaped, the engine's pid, which
        # is also its process group's id, cannot be given to another process.
        exited = os.WEXITED | os.WNOHANG | os.WNOWAIT
        while os.waitid(os.P_PID, process.pid, exited) is None:
            time.sleep(POLL_SECONDS)
            now = self.clock()
            if reason is None:
                reason = stop_reason() or ("timeout" if now - started >= timeout else None)
                if reason:
                    log.info("stopping the engine", extra={**context, "reason": reason})
                    _signal_group(process, signal.SIGTERM)
                    stopped_at = now
            elif not killed and now - stopped_at >= self.settings.kill_grace_seconds:
                log.warning(
                    "the engine ignored SIGTERM; killing it",
                    extra={**context, "grace_seconds": self.settings.kill_grace_seconds},
                )
                _signal_group(process, signal.SIGKILL)
                killed = True
        _signal_group(process, signal.SIGKILL)  # anything it left behind in its process group
        returncode = process.wait()
        with _engines_lock:
            _engines.discard(process.pid)
        orphans = kill_orphans()
        if orphans:
            log.warning(
                "killed processes the engine left running", extra={**context, "pids": orphans}
            )
        # A process that left the group (setsid) could still hold the pipes open: do not wait
        # for it forever.
        for reader in readers:
            reader.join(timeout=5)
        for stream in (process.stdout, process.stderr):
            stream.close()
        seconds = self.clock() - started
        return EngineRun(returncode, reason, killed, tail.text(), seconds, config["rlimits"])
