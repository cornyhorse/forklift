"""``forklift-mcp`` / ``python -m forklift.mcp``: arguments, and what happens without the SDK.

Importing forklift never imports the MCP SDK, and the command says how to install it (and that
it needs Python 3.10+) instead of failing with a traceback.
"""

from __future__ import annotations

import os
import runpy
import subprocess
import sys
from importlib import metadata
from pathlib import Path

import pytest

import forklift.mcp
from forklift.mcp import EXIT_UNAVAILABLE, INSTALL_HINT, main, sdk_problem

SRC = Path(__file__).resolve().parents[2] / "src"
try:
    import mcp.server.mcpserver  # noqa: F401

    HAVE_SDK = True
except ImportError:
    HAVE_SDK = False
needs_sdk = pytest.mark.skipif(not HAVE_SDK, reason="needs the MCP Python SDK")


def _python(code, **kwargs):
    environment = dict(os.environ, PYTHONPATH=str(SRC))
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=environment,
        timeout=120,
        **kwargs,
    )


def test_importing_forklift_never_imports_the_sdk():
    done = _python(
        "import sys, forklift, forklift.mcp, forklift.mcp.tools, forklift.mcp.workspace\n"
        "loaded = [m for m in sys.modules if m == 'mcp' or m.startswith(('mcp.', 'mcp_types'))]\n"
        "assert not loaded, loaded\n"
        "print('ok')"
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "ok"


def test_help_and_version(capsys):
    with pytest.raises(SystemExit) as stop:
        main(["--help"])
    assert stop.value.code == 0
    out = capsys.readouterr().out
    assert "--root DIR" in out and "--read-only" in out and INSTALL_HINT in out
    with pytest.raises(SystemExit):
        main(["--version"])
    assert capsys.readouterr().out.strip() == f"forklift-mcp {forklift.__version__}"


def test_python_older_than_3_10(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(forklift.mcp, "python_version", lambda: (3, 9))
    assert main(["--root", str(tmp_path)]) == EXIT_UNAVAILABLE
    err = capsys.readouterr().err
    assert "forklift-mcp needs Python 3.10 or newer (this is Python 3.9)" in err
    assert INSTALL_HINT in err


def test_without_the_sdk(monkeypatch, capsys, tmp_path):
    monkeypatch.setitem(sys.modules, "mcp", None)
    assert sdk_problem().startswith("forklift-mcp needs the MCP Python SDK")
    assert main(["--root", str(tmp_path)]) == EXIT_UNAVAILABLE
    assert INSTALL_HINT in capsys.readouterr().err


def test_without_the_sdk_in_a_real_process(tmp_path):
    done = _python(
        "import sys\n"
        "sys.modules['mcp'] = None  # as if the SDK were not installed\n"
        "from forklift.mcp import main\n"
        f"sys.exit(main(['--root', {str(tmp_path)!r}]))"
    )
    assert done.returncode == EXIT_UNAVAILABLE
    assert "pip install 'forklift-etl[mcp]'" in done.stderr
    assert "Traceback" not in done.stderr and done.stdout == ""


def test_python_version():
    assert forklift.mcp.python_version() == sys.version_info[:2]


def test_python_dash_m(monkeypatch):
    monkeypatch.setattr(forklift.mcp, "main", lambda: 7)
    with pytest.raises(SystemExit) as stop:
        runpy.run_module("forklift.mcp", run_name="__main__")
    assert stop.value.code == 7


def test_the_console_script_is_declared():
    try:
        distribution = metadata.distribution("forklift-etl")
    except metadata.PackageNotFoundError:
        pytest.skip("forklift-etl is not installed")
    scripts = {e.name: e.value for e in distribution.entry_points if e.group == "console_scripts"}
    assert scripts["forklift-mcp"] == "forklift.mcp:main"


@needs_sdk
class TestStart:
    @pytest.fixture
    def served(self, monkeypatch):
        calls = []
        from forklift.mcp import server

        monkeypatch.setattr(
            server, "serve", lambda workspace, log_level: calls.append((workspace, log_level))
        )
        return calls

    def test_serves_the_roots(self, served, tmp_path):
        (tmp_path / "b").mkdir()
        assert main(["--root", str(tmp_path), "--root", str(tmp_path / "b"), "--read-only"]) == 0
        ((workspace, level),) = served
        assert workspace.roots == [tmp_path.resolve(), (tmp_path / "b").resolve()]
        assert workspace.read_only is True and level == "WARNING"

    def test_the_working_directory_is_the_default_root(self, served, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert main(["--log-level", "DEBUG"]) == 0
        ((workspace, level),) = served
        assert workspace.roots == [tmp_path.resolve()] and level == "DEBUG"
        assert workspace.read_only is False

    def test_the_file_system_root_needs_an_explicit_root(self, served, monkeypatch, capsys):
        monkeypatch.chdir("/")
        assert main([]) == EXIT_UNAVAILABLE
        assert "the working directory is the file system root" in capsys.readouterr().err
        assert served == []
        assert main(["--root", "/"]) == 0

    def test_a_root_that_is_not_a_directory(self, served, tmp_path, capsys):
        assert main(["--root", str(tmp_path / "missing")]) == EXIT_UNAVAILABLE
        assert "missing is not a directory" in capsys.readouterr().err
        assert served == []
