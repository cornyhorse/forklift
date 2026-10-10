"""Linux process hardening through ctypes: prctl flags and Landlock, with no compiled modules.

Landlock (Linux 5.13 and later) lets an unprivileged process give up access for itself and its
descendants: after ``Ruleset.restrict()`` the process may open only the paths it allowed, may make
TCP connections only to the ports it allowed (ABI 4, Linux 6.7), and may not signal processes or
reach abstract unix sockets outside its domain (ABI 6, Linux 6.12). It also cannot ptrace, or
read the memory of, any process outside its domain.
See https://docs.kernel.org/userspace-api/landlock.html.
"""

from __future__ import annotations

import ctypes
import os

PR_SET_PDEATHSIG = 1
PR_SET_DUMPABLE = 4
PR_SET_CHILD_SUBREAPER = 36
PR_SET_NO_NEW_PRIVS = 38

# The landlock_* system calls have the same numbers on every architecture (they are newer than
# the unified system call table of Linux 5.1).
SYS_LANDLOCK_CREATE_RULESET = 444
SYS_LANDLOCK_ADD_RULE = 445
SYS_LANDLOCK_RESTRICT_SELF = 446
_CREATE_RULESET_VERSION = 1
_RULE_PATH_BENEATH = 1
_RULE_NET_PORT = 2

FS_EXECUTE = 1 << 0
FS_WRITE_FILE = 1 << 1
FS_READ_FILE = 1 << 2
FS_READ_DIR = 1 << 3
FS_REFER = 1 << 13  # ABI 2
FS_TRUNCATE = 1 << 14  # ABI 3
NET_BIND_TCP = 1 << 0  # ABI 4
NET_CONNECT_TCP = 1 << 1  # ABI 4
SCOPE_ABSTRACT_UNIX_SOCKET = 1 << 0  # ABI 6
SCOPE_SIGNAL = 1 << 1  # ABI 6

READ = FS_EXECUTE | FS_READ_FILE | FS_READ_DIR
# Rights that may be granted on a file rather than a directory.
_FILE_RIGHTS = FS_EXECUTE | FS_WRITE_FILE | FS_READ_FILE | FS_TRUNCATE


def fs_rights(abi: int) -> int:
    """Every file-system right the kernel's Landlock ABI knows (bits 0-12, then REFER, TRUNCATE).

    IOCTL_DEV (ABI 5) is left out: the engine's descriptors are pipes and regular files, and
    restricting device ioctls buys nothing here.
    """
    rights = (1 << 13) - 1
    if abi >= 2:
        rights |= FS_REFER
    if abi >= 3:
        rights |= FS_TRUNCATE
    return rights


class _RulesetAttr(ctypes.Structure):
    _fields_ = [
        ("handled_access_fs", ctypes.c_uint64),
        ("handled_access_net", ctypes.c_uint64),
        ("scoped", ctypes.c_uint64),
    ]


class _PathBeneathAttr(ctypes.Structure):
    # The kernel's struct is packed (12 bytes) and read through a pointer: these fields have the
    # same offsets, and the 4 bytes of padding after them are never read.
    _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]


class _NetPortAttr(ctypes.Structure):
    _fields_ = [("allowed_access", ctypes.c_uint64), ("port", ctypes.c_uint64)]


_libc = ctypes.CDLL(None, use_errno=True)
_libc.syscall.restype = ctypes.c_long
_libc.prctl.restype = ctypes.c_int


def _check(result: int) -> int:
    if result < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return result


def prctl(option: int, value: int) -> None:
    _check(
        _libc.prctl(
            ctypes.c_int(option),
            ctypes.c_ulong(value),
            ctypes.c_ulong(0),
            ctypes.c_ulong(0),
            ctypes.c_ulong(0),
        )
    )


def set_no_new_privs() -> None:
    """execve can no longer grant privileges (setuid bits, file capabilities)."""
    prctl(PR_SET_NO_NEW_PRIVS, 1)


def set_parent_death_signal(signal_number: int) -> None:
    """Receive ``signal_number`` when the parent (the supervisor's thread) exits."""
    prctl(PR_SET_PDEATHSIG, signal_number)


def set_dumpable(dumpable: bool) -> None:
    """A non-dumpable process writes no core dump, and processes of the same user cannot read its
    memory, environment or file descriptors through /proc or ptrace."""
    prctl(PR_SET_DUMPABLE, 1 if dumpable else 0)


def set_child_subreaper() -> None:
    """Orphaned descendants are reparented to this process instead of init, so the supervisor
    can find (and kill) whatever an engine left running."""
    prctl(PR_SET_CHILD_SUBREAPER, 1)


def landlock_abi() -> int:
    """The kernel's Landlock ABI version; 0 when Landlock is missing or disabled."""
    try:
        return _check(
            _libc.syscall(
                ctypes.c_long(SYS_LANDLOCK_CREATE_RULESET),
                None,
                ctypes.c_size_t(0),
                ctypes.c_uint32(_CREATE_RULESET_VERSION),
            )
        )
    except OSError:
        return 0


class Ruleset:
    """A Landlock ruleset: what the process may still do once ``restrict()`` is called.

    ``tcp`` True handles TCP bind and connect (ABI 4): only the ports passed to ``allow_port``
    can then be connected to, and nothing can be bound. ``scope`` True (ABI 6) stops signals and
    abstract unix socket connections to processes outside the sandbox.
    """

    def __init__(self, abi: int, *, tcp: bool, scope: bool):
        self.abi = abi
        self.handled_fs = fs_rights(abi)
        attr = _RulesetAttr(
            self.handled_fs,
            (NET_BIND_TCP | NET_CONNECT_TCP) if tcp else 0,
            (SCOPE_ABSTRACT_UNIX_SOCKET | SCOPE_SIGNAL) if scope else 0,
        )
        self.fd = _check(
            _libc.syscall(
                ctypes.c_long(SYS_LANDLOCK_CREATE_RULESET),
                ctypes.byref(attr),
                ctypes.c_size_t(ctypes.sizeof(attr)),
                ctypes.c_uint32(0),
            )
        )

    def allow_path(self, path: str, access: int) -> bool:
        """Allow ``access`` beneath ``path``; False (and nothing allowed) if it does not exist."""
        try:
            descriptor = os.open(path, os.O_PATH | os.O_CLOEXEC)
        except FileNotFoundError:
            return False
        try:
            allowed = access & self.handled_fs
            if not os.path.isdir(f"/proc/self/fd/{descriptor}"):
                allowed &= _FILE_RIGHTS
            rule = _PathBeneathAttr(allowed, descriptor)
            _check(
                _libc.syscall(
                    ctypes.c_long(SYS_LANDLOCK_ADD_RULE),
                    ctypes.c_int(self.fd),
                    ctypes.c_int(_RULE_PATH_BENEATH),
                    ctypes.byref(rule),
                    ctypes.c_uint32(0),
                )
            )
        finally:
            os.close(descriptor)
        return True

    def allow_port(self, port: int) -> None:
        rule = _NetPortAttr(NET_CONNECT_TCP, port)
        _check(
            _libc.syscall(
                ctypes.c_long(SYS_LANDLOCK_ADD_RULE),
                ctypes.c_int(self.fd),
                ctypes.c_int(_RULE_NET_PORT),
                ctypes.byref(rule),
                ctypes.c_uint32(0),
            )
        )

    def restrict(self) -> None:
        """Enforce the ruleset on this process (needs no_new_privs) and close it."""
        try:
            _check(
                _libc.syscall(
                    ctypes.c_long(SYS_LANDLOCK_RESTRICT_SELF),
                    ctypes.c_int(self.fd),
                    ctypes.c_uint32(0),
                )
            )
        finally:
            os.close(self.fd)
