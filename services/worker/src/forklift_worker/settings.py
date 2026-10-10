"""Settings: command-line flags with ``FORKLIFT_WORKER_*`` environment variables as fallbacks.

Every setting is one row of ``OPTIONS``: its flag, its variable, its default and what it means. A
flag wins over the variable and the variable over the default, so Compose and Helm can set
everything through the environment while a developer overrides one flag. README.md lists the same
table, and a test keeps the two in step.
"""

from __future__ import annotations

import argparse
import re
import secrets
import shlex
import socket
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from . import __version__

AUTO = "auto"
UNLIMITED = "unlimited"
INTERNAL_API_PATH = "/internal/v1"
ISOLATION_PROFILES = ("standard", "no-network")
LANDLOCK_MODES = ("auto", "required", "off")
LOG_LEVELS = ("debug", "info", "warning", "error")
LOG_FORMATS = ("json", "text")

# Environment variables that are never passed to the engine, even when --engine-env names them:
# the engine process holds no credentials of any kind.
_NEVER_PASSED = re.compile(r"^(AWS_|FORKLIFT_)|TOKEN|SECRET|PASSW|CREDENTIAL|KEY", re.IGNORECASE)
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LANE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
# The gateway's rule (at most 200 characters), with room for the slot suffix of --concurrency.
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,189}$")
_HOST = re.compile(r"^[a-z0-9.-]+(:[0-9]{1,5})?$|^\[[0-9a-f:.]+\](:[0-9]{1,5})?$")
_SIZE = re.compile(r"^(\d+(?:\.\d+)?)\s*([kmgtpe]?)(i?)b?$", re.IGNORECASE)
_DURATION = re.compile(r"^(\d+(?:\.\d+)?)\s*([smh]?)$", re.IGNORECASE)
_SIZE_POWERS = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}
_DURATION_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600}


class SettingsError(ValueError):
    """A setting is missing or invalid; the message names the flag and the variable to fix."""


# --------------------------------------------------------------------------- value parsers


def parse_size(text: str) -> int:
    """``"2GiB"``, ``"2Gi"`` (2**31), ``"2G"`` (2 * 10**9), ``"512"`` (bytes) -> bytes."""
    match = _SIZE.match(text.strip())
    if not match:
        raise ValueError(f"{text!r} is not a size (examples: 1048576, 512MiB, 2GiB, 10G)")
    number, prefix, binary = match.groups()
    base = 1024 if binary else 1000
    if binary and not prefix:
        raise ValueError(f"{text!r} is not a size (examples: 1048576, 512MiB, 2GiB, 10G)")
    return int(float(number) * base ** _SIZE_POWERS[prefix.lower()])


def parse_duration(text: str) -> float:
    """``"90"`` or ``"90s"`` (seconds), ``"5m"``, ``"1.5h"`` -> seconds."""
    match = _DURATION.match(text.strip())
    if not match:
        raise ValueError(f"{text!r} is not a duration (examples: 30, 2.5, 90s, 5m, 1h)")
    number, unit = match.groups()
    return float(number) * _DURATION_UNITS[unit.lower()]


def _positive_duration(text: str) -> float:
    value = parse_duration(text)
    if value <= 0:
        raise ValueError(f"{text!r} must be more than zero")
    return value


def _positive_int(text: str) -> int:
    try:
        value = int(text.strip())
    except ValueError:
        raise ValueError(f"{text!r} is not a whole number") from None
    if value < 1:
        raise ValueError(f"{text!r} must be 1 or more")
    return value


def _non_negative_int(text: str) -> int:
    try:
        value = int(text.strip())
    except ValueError:
        raise ValueError(f"{text!r} is not a whole number") from None
    if value < 0:
        raise ValueError(f"{text!r} must be 0 or more")
    return value


def _limit(parse: Callable[[str], Any], *, allow_auto: bool) -> Callable[[str], Any]:
    """A resource limit: a value, ``unlimited`` (None) or, where allowed, ``auto``."""

    def parse_limit(text: str) -> Any:
        word = text.strip().lower()
        if word == UNLIMITED:
            return None
        if word == AUTO and allow_auto:
            return AUTO
        value = parse(text)
        if value <= 0:
            raise ValueError(f"{text!r} must be more than zero (or {UNLIMITED!r})")
        return value

    return parse_limit


def _choice(choices: Sequence[str]) -> Callable[[str], str]:
    def parse_choice(text: str) -> str:
        word = text.strip().lower()
        if word not in choices:
            raise ValueError(f"{text!r} is not one of {', '.join(choices)}")
        return word

    return parse_choice


def _bool(text: str) -> bool:
    word = text.strip().lower()
    if word in ("1", "true", "yes", "on"):
        return True
    if word in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"{text!r} is not a boolean (use 1/0, true/false, yes/no or on/off)")


def _gateway_url(text: str) -> str:
    """The internal API's base URL; ``/internal/v1`` is appended unless it is already there."""
    url = text.strip().rstrip("/")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"{text!r} is not an http:// or https:// URL with a host")
    if parts.query or parts.fragment or parts.username or parts.password:
        raise ValueError(f"{text!r} must not carry a query, a fragment or credentials")
    return url if url.endswith(INTERNAL_API_PATH) else url + INTERNAL_API_PATH


def _comma_list(text: str) -> list[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def _lanes(text: str) -> list[str]:
    lanes = _comma_list(text)
    if not lanes:
        raise ValueError("at least one lane is needed (for example batch,interactive)")
    for lane in lanes:
        if not _LANE.match(lane):
            raise ValueError(
                f"{lane!r} is not a lane name (lower-case letters, digits, '-' and '_')"
            )
    if len(set(lanes)) != len(lanes):
        raise ValueError(f"{text!r} names a lane more than once")
    return lanes


def _host(text: str) -> str:
    host = text.strip().lower()
    if not _HOST.match(host):
        raise ValueError(f"{text!r} is not a host name or host:port")
    return host


def _env_name(text: str) -> str:
    name = text.strip()
    if not _ENV_NAME.match(name):
        raise ValueError(f"{text!r} is not an environment variable name")
    if _NEVER_PASSED.search(name):
        raise ValueError(
            f"{name} is never passed to the engine: the engine process holds no credentials, "
            "and AWS_*, FORKLIFT_* and names containing TOKEN, SECRET, PASSW, CREDENTIAL or KEY "
            "are always removed from its environment"
        )
    return name


def _worker_id(text: str) -> str:
    name = text.strip()
    if not _WORKER_ID.match(name):
        raise ValueError(
            f"{text!r} is not a worker id (1 to 190 letters, digits and . _ : @ -, starting with "
            "a letter or digit)"
        )
    return name


def _command(text: str) -> list[str]:
    try:
        words = shlex.split(text)
    except ValueError as error:
        raise ValueError(f"{text!r} is not a valid command line ({error})") from None
    if not words:
        raise ValueError("the engine command is empty")
    return [sys.executable if word == "{python}" else word for word in words]


def _path(text: str) -> Path:
    if not text.strip():
        raise ValueError("the path is empty")
    return Path(text.strip())


def _absolute_path(text: str) -> Path:
    path = _path(text)
    if not path.is_absolute():
        raise ValueError(f"{text!r} is not an absolute path")
    return path


# --------------------------------------------------------------------------- the table


@dataclass(frozen=True)
class Option:
    name: str
    flag: str
    help: str
    parse: Callable[[str], Any] = str
    default: Any = None
    required: bool = False
    repeatable: bool = False  # the flag may repeat; the variable holds a comma-separated list
    is_flag: bool = False  # a switch: present means true
    variable: str = ""  # FORKLIFT_WORKER_<NAME> unless set

    @property
    def env(self) -> str:
        return self.variable or "FORKLIFT_WORKER_" + self.name.upper()

    def default_text(self) -> str:
        if self.required:
            return "(required)"
        if self.name == "worker_id":
            return "host name + random suffix"
        if self.name == "engine_command":
            return "`{python} -I -m forklift`"
        if self.default is None or self.default == []:
            return "(none)"
        if isinstance(self.default, bool):
            return "on" if self.default else "off"
        return f"`{self.default}`"


OPTIONS: tuple[Option, ...] = (
    Option(
        "gateway",
        "--gateway",
        "URL of the gateway's internal port; /internal/v1 is appended unless it ends with that.",
        _gateway_url,
        required=True,
    ),
    Option(
        "token_file",
        "--token-file",
        "File holding the worker token. It is read again for every request, so it can be rotated, "
        "and it should live outside the engine's readable paths (for example /run/secrets).",
        _absolute_path,
        required=True,
    ),
    Option(
        "lanes",
        "--lanes",
        "Comma-separated lanes to lease jobs from (batch, interactive, sql).",
        _lanes,
        default="batch",
    ),
    Option(
        "scratch",
        "--scratch",
        "Directory for the per-job scratch directories (created if missing, mode 0700).",
        _absolute_path,
        required=True,
    ),
    Option(
        "isolation",
        "--isolation",
        "Isolation profile: standard or no-network (see README.md for what each enforces).",
        _choice(ISOLATION_PROFILES),
        default="standard",
    ),
    Option(
        "landlock",
        "--landlock",
        "Landlock file-system (and TCP) sandbox for the engine: auto (use it when the kernel "
        "has it), required (refuse to start without it) or off.",
        _choice(LANDLOCK_MODES),
        default="auto",
    ),
    Option(
        "concurrency",
        "--concurrency",
        "Jobs run at the same time, each in its own engine process and scratch directory.",
        _positive_int,
        default="1",
    ),
    Option(
        "worker_id",
        "--worker-id",
        "Name the gateway knows this worker by (with --concurrency above 1, each slot appends "
        "-1, -2, ...).",
        _worker_id,
        variable="FORKLIFT_WORKER_ID",
    ),
    Option(
        "max_jobs",
        "--max-jobs",
        "Exit after this many jobs (0: never). --max-jobs 1 runs one job per process.",
        _non_negative_int,
        default="0",
    ),
    Option(
        "engine_command",
        "--engine-command",
        "Command that starts the engine; `run-job SPEC --base-dir ... --result ...` is appended. "
        "{python} stands for the worker's own interpreter.",
        _command,
        default="{python} -I -m forklift",
    ),
    Option(
        "engine_env",
        "--engine-env",
        "Extra environment variables passed to the engine, by name (for example ODBCSYSINI). "
        "Credentials never are: AWS_*, FORKLIFT_* and names with TOKEN, SECRET, PASSW, "
        "CREDENTIAL or KEY are refused.",
        _env_name,
        default=[],
        repeatable=True,
    ),
    Option(
        "engine_read_path",
        "--engine-read-path",
        "Extra paths the engine may read under Landlock (the Python installation, /usr, /lib*, "
        "/etc, /opt, /proc and /sys are always readable).",
        _absolute_path,
        default=[],
        repeatable=True,
        variable="FORKLIFT_WORKER_ENGINE_READ_PATHS",
    ),
    Option(
        "store_host",
        "--store-host",
        "Object store host (or host:port) that presigned URLs must point at; repeat for "
        "several. Without it, each URL's own host is the only one its engine may reach.",
        _host,
        default=[],
        repeatable=True,
        variable="FORKLIFT_WORKER_STORE_HOSTS",
    ),
    Option(
        "ca_file",
        "--ca-file",
        "CA bundle for HTTPS to the gateway and the object store (default: the system's).",
        _absolute_path,
    ),
    Option(
        "stage_max_bytes",
        "--stage-max-bytes",
        "Ceiling on the gateway's stage_max_bytes: larger inputs are streamed (standard "
        "profile) or refused (no-network).",
        _limit(parse_size, allow_auto=False),
    ),
    Option(
        "max_job_seconds",
        "--max-job-seconds",
        "Wall-clock ceiling for one engine run (a job's limits.max_seconds applies when lower).",
        _positive_duration,
        default="24h",
    ),
    Option(
        "kill_grace_seconds",
        "--kill-grace-seconds",
        "Time a stopped engine gets between SIGTERM (it writes a cancelled result) and SIGKILL.",
        _positive_duration,
        default="10",
    ),
    Option(
        "drain_seconds",
        "--drain-seconds",
        "After SIGTERM, how long running jobs may continue before their engines are stopped and "
        "the jobs handed back (their leases expire and the gateway queues them again).",
        parse_duration,
        default="20",
    ),
    Option(
        "idle_min_seconds",
        "--idle-min-seconds",
        "First wait after an empty lease; it doubles up to --idle-max-seconds.",
        _positive_duration,
        default="0.5",
    ),
    Option(
        "idle_max_seconds",
        "--idle-max-seconds",
        "Longest wait between lease requests while there is nothing to do or the gateway is down.",
        _positive_duration,
        default="10",
    ),
    Option(
        "heartbeat_seconds",
        "--heartbeat-seconds",
        "Time between heartbeats (default: a third of the lease the gateway grants).",
        _positive_duration,
    ),
    Option(
        "http_timeout",
        "--http-timeout",
        "Socket timeout for requests to the gateway and the object store.",
        _positive_duration,
        default="60",
    ),
    Option(
        "limit_address_space",
        "--limit-address-space",
        "Engine RLIMIT_AS: a size, unlimited, or auto (the machine's physical memory).",
        _limit(parse_size, allow_auto=True),
        default=AUTO,
    ),
    Option(
        "limit_cpu_seconds",
        "--limit-cpu-seconds",
        "Engine RLIMIT_CPU: a duration, unlimited, or auto (the job's wall-clock limit times the "
        "CPUs this worker may use).",
        _limit(parse_duration, allow_auto=True),
        default=AUTO,
    ),
    Option(
        "limit_file_size",
        "--limit-file-size",
        "Engine RLIMIT_FSIZE (largest file it may write): a size, unlimited, or auto (the size "
        "of the scratch file system).",
        _limit(parse_size, allow_auto=True),
        default=AUTO,
    ),
    Option(
        "limit_open_files",
        "--limit-open-files",
        "Engine RLIMIT_NOFILE: a number or unlimited (the worker's own hard limit).",
        _limit(_positive_int, allow_auto=False),
        default="1024",
    ),
    Option(
        "allow_root",
        "--allow-root",
        "Run even as root (development only: the profiles expect a non-root worker).",
        _bool,
        default=False,
        is_flag=True,
    ),
    Option(
        "log_level",
        "--log-level",
        "debug, info, warning or error (debug includes the engine's own log lines).",
        _choice(LOG_LEVELS),
        default="info",
    ),
    Option(
        "log_format",
        "--log-format",
        "json (one object per line) or text.",
        _choice(LOG_FORMATS),
        default="json",
    ),
)


@dataclass(frozen=True)
class Settings:
    gateway: str
    token_file: Path
    scratch: Path
    lanes: list[str] = field(default_factory=lambda: ["batch"])
    isolation: str = "standard"
    landlock: str = "auto"
    concurrency: int = 1
    worker_id: str = ""
    max_jobs: int = 0
    engine_command: list[str] = field(default_factory=lambda: _command("{python} -I -m forklift"))
    engine_env: list[str] = field(default_factory=list)
    engine_read_path: list[Path] = field(default_factory=list)
    store_host: list[str] = field(default_factory=list)
    ca_file: Path | None = None
    stage_max_bytes: int | None = None
    max_job_seconds: float = 86400.0
    kill_grace_seconds: float = 10.0
    drain_seconds: float = 20.0
    idle_min_seconds: float = 0.5
    idle_max_seconds: float = 10.0
    heartbeat_seconds: float | None = None
    http_timeout: float = 60.0
    limit_address_space: int | str | None = AUTO
    limit_cpu_seconds: float | str | None = AUTO
    limit_file_size: int | str | None = AUTO
    limit_open_files: int | None = 1024
    allow_root: bool = False
    log_level: str = "info"
    log_format: str = "json"

    def __post_init__(self) -> None:
        if not self.worker_id:
            object.__setattr__(self, "worker_id", default_worker_id())
        if self.idle_min_seconds > self.idle_max_seconds:
            raise SettingsError(
                f"--idle-min-seconds ({self.idle_min_seconds:g}) is more than --idle-max-seconds "
                f"({self.idle_max_seconds:g})"
            )
        if self.isolation == "no-network" and "sql" in self.lanes:
            raise SettingsError(
                "The no-network isolation profile cannot serve the sql lane: SQL sources and "
                "targets need the engine to reach the database. Run a standard-profile worker "
                "for --lanes sql."
            )

    @property
    def user_agent(self) -> str:
        return f"forklift-worker/{__version__}"


def default_worker_id() -> str:
    host = re.sub(r"[^A-Za-z0-9._-]", "-", socket.gethostname())[:180].lstrip("._-")
    return f"{host or 'worker'}-{secrets.token_hex(3)}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="forklift-worker",
        description="Lease jobs from the forklift gateway and run them in a sandboxed engine "
        "process. Every flag can also be set with the environment variable shown.",
    )
    parser.add_argument("--version", action="version", version=f"forklift-worker {__version__}")
    for option in OPTIONS:
        help_text = f"{option.help} [{option.env}; default: {option.default_text()}]"
        if option.is_flag:
            parser.add_argument(
                option.flag, dest=option.name, action="store_const", const="1", help=help_text
            )
        elif option.repeatable:
            parser.add_argument(option.flag, dest=option.name, action="append", help=help_text)
        else:
            parser.add_argument(option.flag, dest=option.name, help=help_text)
    return parser


def _value(option: Option, raw: Any, source: str) -> Any:
    try:
        if option.repeatable:
            items = raw if isinstance(raw, list) else _comma_list(raw)
            return [option.parse(item) for item in items]
        return option.parse(raw)
    except ValueError as error:
        raise SettingsError(f"{source}: {error}") from None


def load_settings(argv: Sequence[str], environ: Mapping[str, str]) -> Settings:
    """Settings from ``argv`` (flags) over ``environ`` (variables) over the defaults.

    Raises SettingsError naming the flag and the variable; ``--help`` and ``--version`` exit.
    """
    arguments = vars(_parser().parse_args(list(argv)))
    values: dict[str, Any] = {}
    for option in OPTIONS:
        source = f"{option.flag} / {option.env}"
        if arguments[option.name] is not None:
            values[option.name] = _value(option, arguments[option.name], option.flag)
        elif environ.get(option.env, "").strip():
            values[option.name] = _value(option, environ[option.env], option.env)
        elif option.required:
            raise SettingsError(f"{source} is required: {option.help}")
        elif isinstance(option.default, str):
            values[option.name] = _value(option, option.default, source)
        elif option.default is not None:
            values[option.name] = option.default
    return Settings(**values)
