"""forklift-mcp: the --root confinement, file checks, read-only and output rules (no SDK)."""

from __future__ import annotations

import os
import threading

import pytest

from forklift.mcp.workspace import ToolInputError, Workspace


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "root"
    (root / "in").mkdir(parents=True)
    (root / "in" / "people.csv").write_text("id\n1\n")
    return root


@pytest.fixture
def workspace(root):
    return Workspace([root])


class TestRoots:
    def test_roots_are_resolved_and_deduplicated(self, tmp_path, root):
        link = tmp_path / "link"
        link.symlink_to(root)
        workspace = Workspace([link, root, str(root)])
        assert workspace.roots == [root.resolve()]
        assert workspace.read_only is False
        assert workspace.describe_roots() == str(root.resolve())

    def test_a_root_must_be_a_directory(self, tmp_path):
        with pytest.raises(ValueError, match="--root .*missing is not a directory"):
            Workspace([tmp_path / "missing"])
        (tmp_path / "file").write_text("x")
        with pytest.raises(ValueError, match="is not a directory"):
            Workspace([tmp_path / "file"])

    def test_at_least_one_root(self):
        with pytest.raises(ValueError, match="at least one --root"):
            Workspace([])

    def test_root_of_picks_the_deepest_root(self, tmp_path):
        outer = tmp_path / "outer"
        inner = outer / "inner"
        inner.mkdir(parents=True)
        workspace = Workspace([outer, inner])
        assert workspace.root_of(inner / "x.csv") == inner
        assert workspace.root_of(outer / "x.csv") == outer
        assert workspace.root_of(outer) == outer
        assert workspace.root_of(tmp_path) is None


class TestResolve:
    def test_relative_paths_are_relative_to_the_first_root(self, workspace, root):
        assert workspace.resolve("in/people.csv", "input_path") == root / "in" / "people.csv"
        assert workspace.resolve(str(root / "in"), "input_path") == root / "in"

    def test_home_directory_is_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        workspace = Workspace([tmp_path])
        assert workspace.resolve("~/x.csv", "input_path") == tmp_path / "x.csv"

    @pytest.mark.parametrize("path", ["../outside.csv", "/etc/passwd", "in/../../x"])
    def test_paths_outside_the_roots_are_refused(self, workspace, path):
        with pytest.raises(ToolInputError, match="is outside the directories this server may use"):
            workspace.resolve(path, "input_path")

    def test_a_symbolic_link_out_of_the_root_is_refused(self, workspace, root, tmp_path):
        secret = tmp_path / "secret.csv"
        secret.write_text("password\nhunter2\n")
        (root / "in" / "innocent.csv").symlink_to(secret)
        with pytest.raises(ToolInputError, match="symbolic links are followed before the check"):
            workspace.resolve("in/innocent.csv", "input_path")
        (root / "escape").symlink_to(tmp_path)
        with pytest.raises(ToolInputError, match="outside"):
            workspace.resolve("escape/secret.csv", "input_path")

    def test_a_symbolic_link_inside_the_root_is_followed(self, workspace, root):
        (root / "alias.csv").symlink_to(root / "in" / "people.csv")
        assert workspace.resolve("alias.csv", "input_path") == root / "in" / "people.csv"

    @pytest.mark.parametrize("path", ["", "   ", None])
    def test_empty_paths(self, workspace, path):
        with pytest.raises(ToolInputError, match="input_path is empty: give a path inside"):
            workspace.resolve(path, "input_path")

    def test_nul_characters(self, workspace):
        with pytest.raises(ToolInputError, match="contains a NUL character"):
            workspace.resolve("in/\x00x", "input_path")

    def test_display(self, tmp_path, root):
        other = tmp_path / "other"
        other.mkdir()
        workspace = Workspace([root, other])
        assert workspace.display(root) == "."
        assert workspace.display(root / "in" / "people.csv") == "in/people.csv"
        assert workspace.display(other / "x") == str(other / "x")


class TestInputFiles:
    def test_a_regular_file(self, workspace, root):
        assert workspace.input_file("in/people.csv", "input_path") == root / "in" / "people.csv"

    def test_missing(self, workspace):
        with pytest.raises(ToolInputError, match="input_path 'in/nope.csv' does not exist"):
            workspace.input_file("in/nope.csv", "input_path")

    def test_a_directory(self, workspace):
        with pytest.raises(ToolInputError, match="is a directory, not a file"):
            workspace.input_file("in", "input_path")

    def test_a_pipe_is_never_opened(self, workspace, root):
        os.mkfifo(root / "in" / "pipe.csv")
        with pytest.raises(ToolInputError, match="not a regular file .*a device, pipe or socket"):
            workspace.input_file("in/pipe.csv", "input_path")

    def test_a_device_is_never_opened(self, tmp_path):
        workspace = Workspace(["/dev"])
        with pytest.raises(ToolInputError, match="not a regular file"):
            workspace.input_file("/dev/zero", "input_path")

    def test_a_file_that_cannot_be_examined(self, workspace, root):
        loop = root / "in" / "loop.csv"
        loop.symlink_to(loop)
        with pytest.raises(ToolInputError, match="cannot be read: Too many levels of symbolic"):
            workspace.input_file("in/loop.csv", "input_path")

    def test_check_regular_file_returns_the_size(self, root):
        assert Workspace.check_regular_file(root / "in" / "people.csv", "path", "x") == 5


class TestOutputs:
    def test_read_only_refuses_writes(self, root):
        workspace = Workspace([root], read_only=True)
        with pytest.raises(ToolInputError, match="run_job would write files.*--read-only"):
            workspace.require_writable("run_job")
        Workspace([root]).require_writable("run_job")

    def test_output_dir_new_or_empty(self, workspace, root):
        assert workspace.output_dir("out/new", "output_dir", False) == root / "out" / "new"
        (root / "empty").mkdir()
        assert workspace.output_dir("empty", "output_dir", False) == root / "empty"

    def test_output_dir_not_empty(self, workspace, root):
        with pytest.raises(ToolInputError, match=r"'in' is not empty \(1 entry\).*overwrite=true"):
            workspace.output_dir("in", "output_dir", False)
        (root / "in" / "more.csv").write_text("x")
        with pytest.raises(ToolInputError, match=r"\(2 entries\)"):
            workspace.output_dir("in", "output_dir", False)
        assert workspace.output_dir("in", "output_dir", True) == root / "in"

    def test_output_dir_that_is_a_file(self, workspace):
        with pytest.raises(ToolInputError, match="exists and is not a directory"):
            workspace.output_dir("in/people.csv", "output_dir", True)

    def test_output_below_a_file(self, workspace):
        with pytest.raises(ToolInputError, match="cannot be created: .*people.csv is not a dir"):
            workspace.output_dir("in/people.csv/out", "output_dir", False)
        with pytest.raises(ToolInputError, match="cannot be created"):
            workspace.output_file("in/people.csv/schema.json", "output_path", False)

    def test_output_file(self, workspace, root):
        assert workspace.output_file("a/b/schema.json", "output_path", False) == (
            root / "a" / "b" / "schema.json"
        )
        with pytest.raises(ToolInputError, match="already exists.*overwrite=true"):
            workspace.output_file("in/people.csv", "output_path", False)
        assert workspace.output_file("in/people.csv", "output_path", True)
        with pytest.raises(ToolInputError, match="exists and is not a regular file"):
            workspace.output_file("in", "output_path", True)

    def test_claim_refuses_a_second_writer(self, workspace, root):
        target = root / "out"
        with workspace.claim(target, "output_dir"):
            with pytest.raises(ToolInputError, match="'out' is being written by another call"):
                with workspace.claim(target, "output_dir"):
                    pass
            errors = []

            def other():
                try:
                    with workspace.claim(target, "output_dir"):
                        pass
                except ToolInputError as error:
                    errors.append(error)

            thread = threading.Thread(target=other)
            thread.start()
            thread.join()
            assert errors
        with workspace.claim(target, "output_dir"):
            pass

    def test_claim_is_released_when_the_call_fails(self, workspace, root):
        with pytest.raises(RuntimeError):
            with workspace.claim(root, "output_dir"):
                raise RuntimeError("boom")
        with workspace.claim(root, "output_dir"):
            pass
