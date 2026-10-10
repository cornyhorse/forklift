"""prctl and Landlock through ctypes."""

from __future__ import annotations

import os

import pytest

from forklift_worker import linux

ABI = linux.landlock_abi()


class FakeLibc:
    """Records system calls; ``fail`` makes the named ones return -1."""

    def __init__(self, fail=()):
        self.calls = []
        self.fail = set(fail)
        self.next_fd = 1000

    def syscall(self, number, *args):
        number = number.value
        self.calls.append((number, args))
        if number in self.fail:
            return -1
        if number == linux.SYS_LANDLOCK_CREATE_RULESET:
            return 7 if args[0] is None else os.open("/dev/null", os.O_RDONLY)
        return 0

    def prctl(self, option, value, *rest):
        self.calls.append(("prctl", option.value, value.value))
        return -1 if "prctl" in self.fail else 0


@pytest.fixture
def libc(monkeypatch):
    fake = FakeLibc()
    monkeypatch.setattr(linux, "_libc", fake)
    return fake


def test_prctl_flags(libc):
    linux.set_no_new_privs()
    linux.set_parent_death_signal(9)
    linux.set_dumpable(False)
    linux.set_dumpable(True)
    assert libc.calls == [
        ("prctl", linux.PR_SET_NO_NEW_PRIVS, 1),
        ("prctl", linux.PR_SET_PDEATHSIG, 9),
        ("prctl", linux.PR_SET_DUMPABLE, 0),
        ("prctl", linux.PR_SET_DUMPABLE, 1),
    ]
    libc.fail.add("prctl")
    with pytest.raises(OSError):
        linux.set_dumpable(False)


def test_a_real_prctl_error():
    with pytest.raises(OSError):
        linux.prctl(123456, 0)
    linux.set_dumpable(True)  # what every process starts with


def test_the_landlock_abi(libc):
    assert linux.landlock_abi() == 7
    libc.fail.add(linux.SYS_LANDLOCK_CREATE_RULESET)
    assert linux.landlock_abi() == 0


def test_rights_by_abi():
    assert linux.fs_rights(1) == (1 << 13) - 1
    assert linux.fs_rights(2) & linux.FS_REFER and not linux.fs_rights(2) & linux.FS_TRUNCATE
    assert linux.fs_rights(5) & linux.FS_TRUNCATE


def test_a_ruleset(libc, tmp_path):
    ruleset = linux.Ruleset(4, tcp=True, scope=False)
    attr = libc.calls[0][1][0]._obj
    assert attr.handled_access_fs == linux.fs_rights(4)
    assert attr.handled_access_net == linux.NET_BIND_TCP | linux.NET_CONNECT_TCP
    assert attr.scoped == 0
    file = tmp_path / "file"
    file.write_text("x")
    assert ruleset.allow_path(str(tmp_path), linux.fs_rights(4)) is True
    assert ruleset.allow_path(str(file), linux.fs_rights(4)) is True
    assert ruleset.allow_path(str(tmp_path / "missing"), linux.READ) is False
    directory_rule = libc.calls[1][1][2]._obj
    file_rule = libc.calls[2][1][2]._obj
    assert directory_rule.allowed_access == linux.fs_rights(4)
    assert (
        file_rule.allowed_access
        == linux.FS_EXECUTE | linux.FS_WRITE_FILE | linux.FS_READ_FILE | linux.FS_TRUNCATE
    )
    ruleset.allow_port(9000)
    assert libc.calls[-1][1][2]._obj.port == 9000
    ruleset.restrict()
    assert libc.calls[-1][0] == linux.SYS_LANDLOCK_RESTRICT_SELF
    scoped = linux.Ruleset(6, tcp=False, scope=True)
    attr = libc.calls[-1][1][0]._obj
    assert attr.scoped == linux.SCOPE_SIGNAL | linux.SCOPE_ABSTRACT_UNIX_SOCKET
    assert attr.handled_access_net == 0
    os.close(scoped.fd)


def test_ruleset_errors(libc, tmp_path):
    libc.fail.add(linux.SYS_LANDLOCK_ADD_RULE)
    ruleset = linux.Ruleset(1, tcp=False, scope=False)
    with pytest.raises(OSError):
        ruleset.allow_path(str(tmp_path), linux.READ)
    libc.fail.add(linux.SYS_LANDLOCK_RESTRICT_SELF)
    with pytest.raises(OSError):
        ruleset.restrict()
    with pytest.raises(OSError):
        os.fstat(ruleset.fd)  # closed even though restrict failed


@pytest.mark.skipif(not ABI, reason="this kernel has no Landlock")
def test_a_real_ruleset_takes_rules(tmp_path):
    ruleset = linux.Ruleset(ABI, tcp=ABI >= 4, scope=ABI >= 6)
    assert ruleset.allow_path(str(tmp_path), linux.fs_rights(ABI))
    assert ruleset.allow_path("/dev/null", linux.FS_READ_FILE | linux.FS_WRITE_FILE)
    if ABI >= 4:
        ruleset.allow_port(443)
    os.close(ruleset.fd)  # not restricted: this is the test process
    assert linux._PathBeneathAttr.allowed_access.offset == 0
    assert linux._PathBeneathAttr.parent_fd.offset == 8
