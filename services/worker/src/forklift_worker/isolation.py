"""Isolation profiles (design §6.2): what the engine process gets on this worker.

``standard``: a scrubbed environment, resource limits, its own scratch directory as working
directory and TMPDIR, no core dumps, no new privileges, killed with the supervisor; with Landlock,
also no file-system access outside the Python installation, system directories and its scratch
directory, and no TCP connections except to the object store (streamed inputs) or none at all
(staged inputs). It expects the worker to run as a non-root user in a container with a read-only
root file system and egress limited to the gateway and the store.

``no-network``: as standard, plus the engine runs in its own empty network namespace, so it has
no network at all; streamed inputs and SQL locations are refused. It needs unprivileged user
namespaces, and the worker refuses to start without them.

README.md has the full table, including what is *not* enforced.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from . import linux, logs
from .settings import AUTO, Settings
from .spec import JobPlan

log = logs.logger("isolation")

SYSTEM_READ_PATHS = (
    "/usr",
    "/lib",
    "/lib32",
    "/lib64",
    "/bin",
    "/sbin",
    "/etc",
    "/opt",
    "/proc",
    "/sys",
)
DEVICES = ("/dev/null", "/dev/zero", "/dev/random", "/dev/urandom")
# Passed to the engine when set: locale, time zone and CA bundle locations (paths, not secrets).
PASSED_THROUGH = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR")
# Passed only to engines that stream an input, and only without user info in the proxy URL.
PROXY_URL_VARIABLES = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")
PROXY_VARIABLES = (*PROXY_URL_VARIABLES, "NO_PROXY", "no_proxy")


class IsolationError(Exception):
    """This worker cannot provide the isolation its settings ask for."""


@dataclass(frozen=True)
class Platform:
    """What the kernel and the process offer, probed once at startup."""

    landlock_abi: int
    network_namespace: bool
    network_namespace_problem: str
    euid: int

    @classmethod
    def probe(cls, isolation: str) -> "Platform":
        if isolation == "no-network":
            available, problem = probe_network_namespace()
        else:
            available, problem = False, "not probed (standard profile)"
        return cls(linux.landlock_abi(), available, problem, os.geteuid())


def probe_network_namespace() -> tuple[bool, str]:
    """Ask a fresh process (namespaces need a single-threaded one) whether it can unshare."""
    try:
        completed = subprocess.run(
            [sys.executable, "-I", "-m", "forklift_worker.sandbox", "--probe"],
            capture_output=True,
            text=True,
            timeout=30,
            env={"PATH": os.environ.get("PATH", os.defpath)},
        )
        answer = json.loads(completed.stdout)
        return bool(answer["network_namespace"]), str(answer["problem"])
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as error:
        return False, f"the probe failed ({type(error).__name__}: {error})"


def _proxy_port(value: str) -> int | None:
    parts = urlsplit(value if "://" in value else f"http://{value}")
    try:
        return parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        return None


def _within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


class Isolation:
    def __init__(self, settings: Settings, platform: Platform):
        self.settings = settings
        self.platform = platform
        self.landlock_abi = platform.landlock_abi if settings.landlock != "off" else 0
        self.read_paths = self._read_paths()

    @property
    def profile(self) -> str:
        return self.settings.isolation

    @property
    def network_allowed(self) -> bool:
        return self.profile != "no-network"

    def _read_paths(self) -> list[str]:
        candidates = [*SYSTEM_READ_PATHS, sys.prefix, sys.base_prefix, sys.exec_prefix]
        candidates += [
            entry for entry in sys.path if os.path.isabs(entry) and os.path.isdir(entry)
        ]
        executable = shutil.which(self.settings.engine_command[0])
        if executable:
            candidates.append(os.path.dirname(os.path.realpath(executable)))
        candidates += [str(path) for path in self.settings.engine_read_path]
        unique: list[str] = []
        for candidate in candidates:
            if candidate not in unique:
                unique.append(candidate)
        return unique

    def check(self) -> list[str]:
        """Refuse settings this platform cannot honour; returns warnings worth logging."""
        settings, platform = self.settings, self.platform
        if platform.euid == 0 and not settings.allow_root:
            raise IsolationError(
                "forklift-worker is running as root. The isolation profiles expect a non-root "
                "user (the image runs as uid 10001); pass --allow-root (FORKLIFT_WORKER_ALLOW_ROOT"
                "=1) only for development."
            )
        if self.profile == "no-network" and not platform.network_namespace:
            raise IsolationError(
                "The no-network isolation profile runs the engine in its own network namespace, "
                f"and this platform does not allow one: {platform.network_namespace_problem}. "
                "Use --isolation standard, or let the worker create unprivileged user and network "
                "namespaces (services/worker/README.md, 'The no-network profile')."
            )
        if settings.landlock == "required" and not platform.landlock_abi:
            raise IsolationError(
                "--landlock required, but this kernel has no Landlock support (Linux 5.13 or "
                "later with landlock in the lsm= boot parameter; container runtimes must allow "
                "the landlock_* system calls)."
            )
        warnings = []
        if settings.landlock == "auto" and not platform.landlock_abi:
            warnings.append(
                "This kernel has no Landlock support: the engine can read every file this "
                "worker's user can read and connect anywhere the container's network allows. "
                "Use --landlock required to refuse to run without it."
            )
        if self.landlock_abi:
            token = Path(os.path.realpath(settings.token_file))
            scratch = Path(os.path.realpath(settings.scratch))
            for readable in self.read_paths:
                root = Path(os.path.realpath(readable))
                if _within(token, root):
                    warnings.append(
                        f"The worker token file {settings.token_file} is inside {readable}, "
                        "which the engine may read; mount it outside the engine's readable "
                        "paths (for example under /run/secrets)."
                    )
                if _within(scratch, root):
                    warnings.append(
                        f"The scratch directory {settings.scratch} is inside {readable}, which "
                        "every engine may read: concurrent jobs could read each other's files."
                    )
        return warnings

    def describe(self) -> dict[str, Any]:
        """What the engine gets here, for the startup log."""
        abi = self.landlock_abi
        return {
            "profile": self.profile,
            "non_root": self.platform.euid != 0,
            "network_namespace": self.profile == "no-network",
            "landlock_abi": abi,
            "filesystem_sandbox": abi > 0,
            "tcp_restricted": abi >= 4,
            "signal_scoped": abi >= 6,
        }

    # ----------------------------------------------------------------------- per job

    def environment(
        self, workdir: Path, plan: JobPlan, environ: Mapping[str, str] | None = None
    ) -> dict[str, str]:
        """The engine's environment: an allow-list, never credentials or the gateway's address."""
        source = os.environ if environ is None else environ
        env = {
            "PATH": source.get("PATH", os.defpath),
            "HOME": str(workdir),
            "TMPDIR": str(workdir / "tmp"),
            "LANG": "C.UTF-8",
            "PYTHONUNBUFFERED": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
        }
        for name in (*PASSED_THROUGH, *self.settings.engine_env):
            if name in source:
                env[name] = source[name]
        if plan.stream_hosts:
            for name in PROXY_VARIABLES:
                value = source.get(name)
                if value and "@" not in urlsplit(value if "://" in value else f"//{value}").netloc:
                    env[name] = value
        return env

    def _tcp_ports(self, plan: JobPlan, env: Mapping[str, str]) -> list[int] | None:
        """TCP ports the engine may connect to; None leaves TCP alone (no ABI 4, or SQL)."""
        if self.landlock_abi < 4 or plan.uses_sql:
            return None
        ports = set(plan.stream_ports)
        if plan.stream_hosts:
            for name in PROXY_URL_VARIABLES:
                port = _proxy_port(env[name]) if name in env else None
                if port:
                    ports.add(port)
        return sorted(ports)

    def rlimits(self, timeout: float) -> dict[str, list[int]]:
        """Resource limits for one engine run whose wall-clock limit is ``timeout`` seconds."""
        settings = self.settings
        address_space = settings.limit_address_space
        if address_space == AUTO:
            address_space = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        cpu = settings.limit_cpu_seconds
        if cpu == AUTO:
            cpu = timeout * len(os.sched_getaffinity(0))
        file_size = settings.limit_file_size
        if file_size == AUTO:
            stats = os.statvfs(settings.scratch)
            file_size = stats.f_blocks * stats.f_frsize
        limits = {"core": [0, 0]}
        limits["as"] = [-1, -1] if address_space is None else [int(address_space)] * 2
        if cpu is None:
            limits["cpu"] = [-1, -1]
        else:
            soft = math.ceil(cpu)
            # SIGXCPU at the soft limit, SIGKILL at the hard one.
            limits["cpu"] = [soft, soft + math.ceil(settings.kill_grace_seconds)]
        limits["fsize"] = [-1, -1] if file_size is None else [int(file_size)] * 2
        open_files = settings.limit_open_files
        limits["nofile"] = [-1, -1] if open_files is None else [int(open_files)] * 2
        return limits

    def sandbox_config(
        self, workdir: Path, plan: JobPlan, env: Mapping[str, str], timeout: float
    ) -> dict[str, Any]:
        config: dict[str, Any] = {
            "parent_pid": os.getpid(),
            "rlimits": self.rlimits(timeout),
            "new_network": self.profile == "no-network",
            "landlock": None,
        }
        if self.landlock_abi:
            config["landlock"] = {
                "abi": self.landlock_abi,
                "read": self.read_paths,
                "devices": list(DEVICES),
                "write": [str(workdir)],
                "tcp_ports": self._tcp_ports(plan, env),
                "scope": self.landlock_abi >= 6,
            }
        return config
