"""The sandbox launcher that runs in the engine's process before the engine itself."""

from __future__ import annotations

import json
import os
import resource
import runpy
import subprocess
import sys

import pytest

from forklift_worker import linux, sandbox
from forklift_worker.sandbox import EXIT_SANDBOX, PREFIX, SandboxError

LANDLOCK = linux.landlock_abi()


def launch(config: dict, *command: str, cwd=None) -> subprocess.CompletedProcess:
    """Run the real launcher in a child process."""
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "forklift_worker.sandbox",
            json.dumps(config),
            "--",
            *command,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=cwd,
    )


def base_config(**changes) -> dict:
    config = {"parent_pid": os.getpid(), "rlimits": {}, "new_network": False, "landlock": None}
    config.update(changes)
    return config


# --------------------------------------------------------------------------- for real


def test_the_launcher_sets_limits_and_execs_the_command():
    limits = {"nofile": [64, 64], "core": [0, 0], "as": [-1, -1], "fsize": [1 << 30, -1]}
    script = (
        "import resource, json; print(json.dumps({n: resource.getrlimit(getattr(resource, "
        "'RLIMIT_' + n.upper())) for n in ('nofile', 'core', 'fsize')}))"
    )
    done = launch(base_config(rlimits=limits), sys.executable, "-c", script)
    assert done.returncode == 0, done.stderr
    seen = json.loads(done.stdout)
    assert seen["nofile"] == [64, 64] and seen["core"] == [0, 0]
    assert seen["fsize"][0] == 1 << 30


def test_a_missing_engine_command_is_a_sandbox_failure():
    done = launch(base_config(), "/no/such/engine")
    assert done.returncode == EXIT_SANDBOX
    assert done.stderr.startswith(PREFIX + "cannot start the engine command '/no/such/engine'")


def test_a_launcher_whose_supervisor_is_gone_does_not_start_the_engine():
    done = launch(base_config(parent_pid=1), sys.executable, "-c", "print('ran')")
    assert done.returncode == EXIT_SANDBOX
    assert "the supervisor exited" in done.stderr and "ran" not in done.stdout


@pytest.mark.skipif(not LANDLOCK, reason="this kernel has no Landlock")
def test_landlock_for_real(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    secret = tmp_path / "secret"
    secret.write_text("s")
    landlock = {
        "abi": LANDLOCK,
        "read": ["/usr", "/lib", "/lib64", "/etc", sys.prefix, sys.base_prefix, *sys.path[1:]],
        "devices": ["/dev/null", "/dev/urandom"],
        "write": [str(work)],
        "tcp_ports": [],
        "scope": LANDLOCK >= 6,
    }
    script = (
        "import sys\n"
        "open('inside.txt', 'w').write('ok')\n"
        "open('/dev/null', 'w').write('x')\n"
        "try:\n    open(sys.argv[1]).read(); print('read')\n"
        "except PermissionError:\n    print('denied')\n"
    )
    done = launch(
        base_config(landlock=landlock), sys.executable, "-c", script, str(secret), cwd=work
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "denied"
    assert (work / "inside.txt").read_text() == "ok"


@pytest.mark.skipif(not LANDLOCK, reason="this kernel has no Landlock")
def test_a_missing_writable_path_stops_the_launch(tmp_path):
    landlock = {"abi": LANDLOCK, "read": [], "write": [str(tmp_path / "gone")], "tcp_ports": None}
    done = launch(base_config(landlock=landlock), sys.executable, "-c", "print('ran')")
    assert done.returncode == EXIT_SANDBOX
    assert "the writable path" in done.stderr and "ran" not in done.stdout


# --------------------------------------------------------------------------- in this process


class Recorder:
    def __init__(self):
        self.calls: list = []

    def __call__(self, name):
        def record(*args, **kwargs):
            self.calls.append((name, *args))

        return record


@pytest.fixture
def recorder(monkeypatch):
    calls = Recorder()
    monkeypatch.setattr(sandbox.linux, "set_parent_death_signal", calls("pdeathsig"))
    monkeypatch.setattr(sandbox.linux, "set_no_new_privs", calls("no_new_privs"))
    monkeypatch.setattr(sandbox.os, "unshare", calls("unshare"))
    monkeypatch.setattr(sandbox, "_write", calls("write"))
    monkeypatch.setattr(sandbox.resource, "setrlimit", calls("setrlimit"))
    monkeypatch.setattr(sandbox.socket, "if_nameindex", lambda: [(1, "lo")])
    return calls


def test_apply_runs_every_step_in_order(recorder, monkeypatch):
    rulesets = []

    class FakeRuleset:
        def __init__(self, abi, *, tcp, scope):
            rulesets.append(self)
            self.abi, self.tcp, self.scope, self.rules, self.restricted = (
                abi,
                tcp,
                scope,
                [],
                False,
            )

        def allow_path(self, path, access):
            self.rules.append((path, access))
            return True

        def allow_port(self, port):
            self.rules.append(("port", port))

        def restrict(self):
            self.restricted = True

    monkeypatch.setattr(sandbox.linux, "Ruleset", FakeRuleset)
    config = base_config(
        parent_pid=os.getppid(),
        new_network=True,
        rlimits={"core": [0, 0]},
        landlock={
            "abi": 6,
            "read": ["/usr"],
            "devices": ["/dev/null", "/dev/urandom"],
            "write": ["/scratch/job"],
            "tcp_ports": [9000],
            "scope": True,
        },
    )
    sandbox.apply(config)
    names = [call[0] for call in recorder.calls]
    assert names[:3] == ["pdeathsig", "no_new_privs", "unshare"]
    assert names.count("write") == 3 and names[-1] == "setrlimit"
    (ruleset,) = rulesets
    assert ruleset.tcp and ruleset.scope and ruleset.restricted
    assert ("port", 9000) in ruleset.rules
    assert ("/scratch/job", linux.fs_rights(6)) in ruleset.rules


def test_namespace_failures(recorder, monkeypatch):
    def refuse(flags):
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(sandbox.os, "unshare", refuse)
    with pytest.raises(SandboxError, match="cannot create a user and network namespace"):
        sandbox.enter_network_namespace()
    monkeypatch.setattr(sandbox.os, "unshare", lambda flags: None)

    def unwritable(path, text):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(sandbox, "_write", unwritable)
    with pytest.raises(SandboxError, match="cannot map the user"):
        sandbox.enter_network_namespace()
    monkeypatch.setattr(sandbox, "_write", lambda path, text: None)
    monkeypatch.setattr(sandbox.socket, "if_nameindex", lambda: [(1, "lo"), (2, "eth0")])
    with pytest.raises(SandboxError, match="not empty"):
        sandbox.enter_network_namespace()


def test_the_probe(monkeypatch):
    monkeypatch.setattr(sandbox, "enter_network_namespace", lambda: None)
    assert sandbox.probe() == {"network_namespace": True, "problem": ""}

    def unavailable():
        raise SandboxError("no user namespaces")

    monkeypatch.setattr(sandbox, "enter_network_namespace", unavailable)
    assert sandbox.probe() == {"network_namespace": False, "problem": "no user namespaces"}


def test_rlimits_are_clamped_to_the_hard_limit(monkeypatch):
    applied = {}
    monkeypatch.setattr(sandbox.resource, "getrlimit", lambda which: (100, 200))
    monkeypatch.setattr(
        sandbox.resource, "setrlimit", lambda which, value: applied.update({which: value})
    )
    sandbox.apply_rlimits({"nofile": [500, 900], "as": [-1, -1], "cpu": [50, -1]})
    assert applied[resource.RLIMIT_NOFILE] == (200, 200)
    assert applied[resource.RLIMIT_AS] == (200, 200)
    assert applied[resource.RLIMIT_CPU] == (50, 200)
    monkeypatch.setattr(sandbox.resource, "getrlimit", lambda which: (100, resource.RLIM_INFINITY))
    sandbox.apply_rlimits({"fsize": [10, -1]})
    assert applied[resource.RLIMIT_FSIZE] == (10, resource.RLIM_INFINITY)


def run_main(argv, monkeypatch, apply=None, execvp=None):
    if apply is not None:
        monkeypatch.setattr(sandbox, "apply", apply)
    if execvp is not None:
        monkeypatch.setattr(sandbox.os, "execvp", execvp)
    with pytest.raises(SystemExit) as caught:
        sandbox.main(argv)
    return caught.value.code


def test_main_reports_every_kind_of_failure(monkeypatch, capsys):
    assert run_main([], monkeypatch) == EXIT_SANDBOX
    assert "usage" in capsys.readouterr().err
    assert run_main(["{bad", "--", "x"], monkeypatch) == EXIT_SANDBOX
    assert "not JSON" in capsys.readouterr().err

    def sandbox_error(config):
        raise SandboxError("no namespace")

    assert run_main(["{}", "--", "x"], monkeypatch, apply=sandbox_error) == EXIT_SANDBOX
    assert capsys.readouterr().err == PREFIX + "no namespace\n"

    def os_error(config):
        raise OSError(1, "Operation not permitted")

    assert run_main(["{}", "--", "x"], monkeypatch, apply=os_error) == EXIT_SANDBOX
    assert "setting up the sandbox failed (PermissionError" in capsys.readouterr().err

    def exec_error(file, args):
        raise FileNotFoundError(2, "No such file or directory")

    code = run_main(["{}", "--", "x"], monkeypatch, apply=lambda c: None, execvp=exec_error)
    assert code == EXIT_SANDBOX
    assert "cannot start the engine command 'x'" in capsys.readouterr().err


def test_main_execs_the_command(monkeypatch):
    started = []

    def execvp(file, args):
        started.append(args)
        raise SystemExit(0)

    assert run_main(["{}", "--", "engine", "run-job"], monkeypatch, lambda c: None, execvp) == 0
    assert started == [["engine", "run-job"]]


def test_main_probe(monkeypatch, capsys):
    monkeypatch.setattr(sandbox, "probe", lambda: {"network_namespace": False, "problem": "x"})
    monkeypatch.setattr(sys, "argv", ["sandbox", "--probe"])
    with pytest.raises(SystemExit) as caught:
        sandbox.main()
    assert caught.value.code == 0
    assert json.loads(capsys.readouterr().out) == {"network_namespace": False, "problem": "x"}


def test_running_the_module(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sandbox"])
    monkeypatch.delitem(sys.modules, "forklift_worker.sandbox")
    with pytest.raises(SystemExit) as caught:
        runpy.run_module("forklift_worker.sandbox", run_name="__main__")
    assert caught.value.code == EXIT_SANDBOX


def test_landlock_needs_its_writable_path(monkeypatch):
    class MissingPaths:
        def __init__(self, abi, *, tcp, scope):
            assert tcp is False and scope is False

        def allow_path(self, path, access):
            return False

    monkeypatch.setattr(sandbox.linux, "Ruleset", MissingPaths)
    with pytest.raises(SandboxError, match="the writable path /gone does not exist"):
        sandbox.apply_landlock({"abi": 1, "write": ["/gone"], "tcp_ports": None})


def test_write(tmp_path):
    target = tmp_path / "uid_map"
    sandbox._write(str(target), "1000 1000 1")
    assert target.read_text() == "1000 1000 1"


def test_apply_without_namespaces_or_landlock(recorder):
    sandbox.apply({"parent_pid": os.getppid()})
    assert [call[0] for call in recorder.calls] == ["pdeathsig", "no_new_privs"]


def test_apply_refuses_when_the_supervisor_is_gone(recorder):
    with pytest.raises(SandboxError, match="the supervisor exited"):
        sandbox.apply({"parent_pid": -1})
    assert [call[0] for call in recorder.calls] == ["pdeathsig"]
