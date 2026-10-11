"""The proxy's settings, read from the environment (``FORKLIFT_MCP_...``).

Every problem is collected and reported at once, so an operator fixes a deployment in one go.
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from typing import Callable, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
DEFAULT_PORT = 8000
DEFAULT_API_TIMEOUT = 30.0
DEFAULT_MAX_WAIT_SECONDS = 50.0
MAX_WAIT_SECONDS = 600.0


class SettingsError(ValueError):
    """The environment does not make a usable configuration; ``problems`` lists why."""

    def __init__(self, problems: List[str]):
        super().__init__("\n".join(f"- {problem}" for problem in problems))
        self.problems = problems


@dataclass(frozen=True)
class Settings:
    """What the proxy needs to know (see services/mcp/README.md)."""

    #: The gateway's public base URL (``http://gateway:8080``), without ``/api/v1``
    api_url: str
    #: ``Host`` header values accepted on /mcp: ``name``, ``name:port`` or ``name:*``
    allowed_hosts: Tuple[str, ...]
    #: ``Origin`` header values accepted on /mcp (requests without one are always accepted)
    allowed_origins: Tuple[str, ...] = ()
    #: A PEM bundle to verify an https gateway (and the store) with; default: the system's
    ca_file: Optional[str] = None
    bind_address: str = "0.0.0.0"
    port: int = DEFAULT_PORT
    log_level: str = "INFO"
    #: Socket timeout of one request to the gateway or the store, in seconds
    api_timeout: float = DEFAULT_API_TIMEOUT
    #: The longest a tool waits for a job before it answers with the job's id to poll
    max_wait_seconds: float = DEFAULT_MAX_WAIT_SECONDS

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "Settings":
        environ = os.environ if environ is None else environ
        problems: List[str] = []

        def get(name: str) -> str:
            return environ.get(name, "").strip()

        def number(name: str, default: float, low: float, high: float, kind: Callable) -> float:
            text = get(name)
            if not text:
                return default
            try:
                value = kind(text)
            except ValueError:
                value = None
            if value is None or not low <= value <= high:
                problems.append(f"{name} must be a number from {low:g} to {high:g} (got {text!r})")
                return default
            return value

        api_url = _api_url(get("FORKLIFT_MCP_API_URL"), problems)
        allowed_hosts = _host_list(get("FORKLIFT_MCP_ALLOWED_HOSTS"), problems)
        allowed_origins = tuple(
            item.strip() for item in get("FORKLIFT_MCP_ALLOWED_ORIGINS").split(",") if item.strip()
        )
        ca_file = get("FORKLIFT_MCP_CA_FILE") or None
        if ca_file is not None:
            try:
                ssl.create_default_context(cafile=ca_file)
            except OSError as error:  # ssl.SSLError is an OSError too
                problems.append(
                    f"FORKLIFT_MCP_CA_FILE {ca_file!r} is not a readable PEM bundle of CA "
                    f"certificates ({type(error).__name__}: {error.strerror or error})"
                )
        port_text = get("FORKLIFT_MCP_PORT")
        if port_text.startswith("tcp://"):
            problems.append(
                f"FORKLIFT_MCP_PORT is {port_text!r}, a Kubernetes service link (a Service named "
                "...-mcp-... sets it): set the pod's enableServiceLinks to false, or set "
                "FORKLIFT_MCP_PORT to the port number"
            )
            port = DEFAULT_PORT
        else:
            port = int(number("FORKLIFT_MCP_PORT", DEFAULT_PORT, 1, 65535, int))
        level = (get("FORKLIFT_MCP_LOG_LEVEL") or "INFO").upper()
        if level not in LOG_LEVELS:
            problems.append(
                f"FORKLIFT_MCP_LOG_LEVEL must be one of {', '.join(LOG_LEVELS)} (got {level!r})"
            )
            level = "INFO"
        api_timeout = number("FORKLIFT_MCP_API_TIMEOUT", DEFAULT_API_TIMEOUT, 1, 300, float)
        max_wait = number(
            "FORKLIFT_MCP_MAX_WAIT_SECONDS", DEFAULT_MAX_WAIT_SECONDS, 0, MAX_WAIT_SECONDS, float
        )
        if problems:
            raise SettingsError(problems)
        return cls(
            api_url=api_url,
            allowed_hosts=allowed_hosts,
            allowed_origins=allowed_origins,
            ca_file=ca_file,
            bind_address=get("FORKLIFT_MCP_BIND_ADDRESS") or "0.0.0.0",
            port=port,
            log_level=level,
            api_timeout=api_timeout,
            max_wait_seconds=max_wait,
        )


def _api_url(text: str, problems: List[str]) -> str:
    if not text:
        problems.append(
            "FORKLIFT_MCP_API_URL is required: the base URL of the gateway's public port, such "
            "as http://gateway:8080"
        )
        return ""
    parts = urlsplit(text)
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or parts.username is not None
        or parts.query
        or parts.fragment
    ):
        problems.append(
            "FORKLIFT_MCP_API_URL must be an http:// or https:// URL without credentials, query "
            "or fragment, such as http://gateway:8080 (the gateway's public port)"
        )
        return ""
    path = parts.path.rstrip("/")
    if path.endswith("/api/v1"):
        path = path[: -len("/api/v1")]
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _host_list(text: str, problems: List[str]) -> Tuple[str, ...]:
    hosts = tuple(item.strip() for item in text.split(",") if item.strip())
    if not hosts:
        problems.append(
            "FORKLIFT_MCP_ALLOWED_HOSTS is required: the Host header values clients use to reach "
            "this server, comma-separated (name, name:port, or name:* for any port), such as "
            "forklift.example.org,localhost:8000; it guards against DNS rebinding"
        )
        return ()
    for host in hosts:
        name = host[:-2] if host.endswith(":*") else host
        if "*" in name or "/" in host or any(ch.isspace() for ch in host):
            problems.append(
                f"FORKLIFT_MCP_ALLOWED_HOSTS entry {host!r} is not a host name with an optional "
                "port (name, name:port or name:*); wildcards in names are not supported"
            )
    return hosts
