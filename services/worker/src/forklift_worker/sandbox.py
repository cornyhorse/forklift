"""Start the engine in its sandbox: ``python -I -m forklift_worker.sandbox CONFIG -- COMMAND``.

This runs in the child process, between the supervisor and the engine, so the supervisor never
needs ``preexec_fn`` (which is unsafe in a process with threads). In order, it:

1. arranges to die with the supervisor (PR_SET_PDEATHSIG) and to gain no privileges through
   execve (PR_SET_NO_NEW_PRIVS);
2. no-network profile: moves into a new user and network namespace, whose only interface is a
   loopback that is down, and checks that no other interface is there;
3. Landlock, when configured: the engine may read only the listed paths, write only its scratch
   directory, make TCP connections only to the listed ports (none for staged inputs), and may
   not signal or reach abstract unix sockets of processes outside its sandbox;
4. sets the resource limits (RLIMIT_AS, CPU, FSIZE, NOFILE, and CORE 0 so a crash never writes
   data to a core file);
5. replaces itself with the engine command (same pid, so the supervisor's signals reach the
   engine itself).

If any step fails it prints one line starting with ``forklift-worker sandbox:`` to stderr and
exits with 125: the engine never runs without the protections it was given.

``--probe`` checks whether this platform lets an unprivileged process create the namespaces the
no-network profile needs, prints ``{"network_namespace": bool, "problem": str}`` and exits 0.
"""

from __future__ import annotations

import json
import os
import resource
import signal
import socket
import sys
from typing import Any, NoReturn, Sequence

from . import linux

EXIT_SANDBOX = 125
PREFIX = "forklift-worker sandbox: "

RLIMITS = {
    "as": resource.RLIMIT_AS,
    "cpu": resource.RLIMIT_CPU,
    "fsize": resource.RLIMIT_FSIZE,
    "nofile": resource.RLIMIT_NOFILE,
    "core": resource.RLIMIT_CORE,
}


class SandboxError(Exception):
    pass


def _write(path: str, text: str) -> None:
    with open(path, "w", encoding="ascii") as handle:
        handle.write(text)


def enter_network_namespace() -> None:
    """A new user namespace (mapping our own uid and gid) and an empty network namespace."""
    uid, gid = os.getuid(), os.getgid()
    try:
        os.unshare(os.CLONE_NEWUSER | os.CLONE_NEWNET)
    except OSError as error:
        raise SandboxError(
            f"cannot create a user and network namespace ({error.strerror})"
        ) from None
    try:
        _write("/proc/self/setgroups", "deny")
        _write("/proc/self/uid_map", f"{uid} {uid} 1")
        _write("/proc/self/gid_map", f"{gid} {gid} 1")
    except OSError as error:
        raise SandboxError(f"cannot map the user into its namespace ({error.strerror})") from None
    interfaces = sorted(name for _, name in socket.if_nameindex())
    if interfaces != ["lo"]:
        raise SandboxError(f"the new network namespace is not empty (interfaces: {interfaces})")


def apply_landlock(config: dict[str, Any]) -> None:
    abi = int(config["abi"])
    ports = config.get("tcp_ports")
    ruleset = linux.Ruleset(abi, tcp=ports is not None, scope=bool(config.get("scope")))
    for path in config.get("read", []):
        ruleset.allow_path(path, linux.READ)
    for path in config.get("devices", []):
        ruleset.allow_path(path, linux.FS_READ_FILE | linux.FS_WRITE_FILE | linux.FS_TRUNCATE)
    for path in config["write"]:
        if not ruleset.allow_path(path, linux.fs_rights(abi)):
            raise SandboxError(f"the writable path {path} does not exist")
    for port in ports or ():
        ruleset.allow_port(int(port))
    ruleset.restrict()


def _clamp(value: int, current_hard: int) -> int:
    """A requested limit (-1: unlimited) that an unprivileged process may set."""
    if value < 0 or (current_hard != resource.RLIM_INFINITY and value > current_hard):
        return current_hard
    return value


def apply_rlimits(limits: dict[str, Sequence[int]]) -> None:
    """``{"as": [soft, hard], ...}`` (-1: unlimited), clamped to the current hard limits."""
    for name, (soft, hard) in limits.items():
        which = RLIMITS[name]
        current_hard = resource.getrlimit(which)[1]
        new_hard = _clamp(hard, current_hard)
        new_soft = _clamp(soft, new_hard)
        resource.setrlimit(which, (new_soft, new_hard))


def apply(config: dict[str, Any]) -> None:
    """Steps 1-4 of the module docstring; raises SandboxError or OSError."""
    linux.set_parent_death_signal(signal.SIGKILL)
    if os.getppid() != config["parent_pid"]:
        raise SandboxError("the supervisor exited before the engine started")
    linux.set_no_new_privs()
    if config.get("new_network"):
        enter_network_namespace()
    if config.get("landlock"):
        apply_landlock(config["landlock"])
    apply_rlimits(config.get("rlimits", {}))


def probe() -> dict[str, Any]:
    """Can this process create the no-network profile's namespaces? (Run in a fresh process.)"""
    try:
        enter_network_namespace()
    except SandboxError as error:
        return {"network_namespace": False, "problem": str(error)}
    return {"network_namespace": True, "problem": ""}


def _fail(message: str) -> NoReturn:
    print(PREFIX + message, file=sys.stderr, flush=True)
    sys.exit(EXIT_SANDBOX)


def main(argv: Sequence[str] | None = None) -> NoReturn:
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--probe"]:
        print(json.dumps(probe()), flush=True)
        sys.exit(0)
    if len(args) < 3 or args[1] != "--":
        _fail("usage: python -I -m forklift_worker.sandbox CONFIG_JSON -- COMMAND [ARGS...]")
    try:
        config = json.loads(args[0])
    except ValueError as error:
        _fail(f"the sandbox configuration is not JSON ({error})")
    command = args[2:]
    try:
        apply(config)
    except SandboxError as error:
        _fail(str(error))
    except (OSError, KeyError, TypeError, ValueError) as error:
        _fail(f"setting up the sandbox failed ({type(error).__name__}: {error})")
    try:
        os.execvp(command[0], command)
    except OSError as error:
        _fail(f"cannot start the engine command {command[0]!r} ({error.strerror or error})")


if __name__ == "__main__":
    main()
