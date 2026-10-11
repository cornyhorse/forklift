"""Where the local MCP server may read and write: the ``--root`` directories.

Every path a tool is given goes through :class:`Workspace`: relative paths are taken relative to
the first root, symbolic links are resolved, and the result must lie inside one of the roots.
Inputs must be regular files (never a device, a pipe or a socket, which could block or never
end), outputs directories or files inside a root, and with ``--read-only`` nothing is written at
all. Messages name paths and never contain anything read from a file.
"""

from __future__ import annotations

import contextlib
import os
import stat
import threading
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Set, Union

PathLike = Union[str, "os.PathLike[str]"]


class ToolInputError(Exception):
    """A tool call the server refuses; the message says why and what to do instead."""


class Workspace:
    """The directories the tools may use, and whether they may write there.

    Args:
        roots: Directories every path must stay inside (symbolic links are resolved first);
            relative tool paths are relative to the first one
        read_only: Refuse every write (dry runs still work)

    Raises:
        ValueError: No root, or a root that is not a directory
    """

    def __init__(self, roots: Sequence[PathLike], read_only: bool = False):
        resolved: List[Path] = []
        for root in roots:
            real = Path(os.path.realpath(os.fspath(root)))
            if not real.is_dir():
                raise ValueError(f"--root {os.fspath(root)} is not a directory")
            if real not in resolved:
                resolved.append(real)
        if not resolved:
            raise ValueError("at least one --root directory is needed")
        self.roots = resolved
        self.read_only = read_only
        self._busy: Set[Path] = set()
        self._busy_lock = threading.Lock()

    # ------------------------------------------------------------------ paths

    def describe_roots(self) -> str:
        return ", ".join(str(root) for root in self.roots)

    def root_of(self, real: Path) -> Optional[Path]:
        """The deepest root that contains ``real`` (an absolute, resolved path), if any."""
        found = None
        for root in self.roots:
            if real == root or root in real.parents:
                if found is None or len(root.parts) > len(found.parts):
                    found = root
        return found

    def resolve(self, path: str, what: str) -> Path:
        """The absolute, resolved path of a tool argument; it must lie inside a root."""
        if not isinstance(path, str) or not path.strip():
            raise ToolInputError(f"{what} is empty: give a path inside {self.describe_roots()}")
        if "\x00" in path:
            raise ToolInputError(f"{what} contains a NUL character")
        candidate = Path(os.path.expanduser(path))
        if not candidate.is_absolute():
            candidate = self.roots[0] / candidate
        real = Path(os.path.realpath(candidate))
        if self.root_of(real) is None:
            raise ToolInputError(
                f"{what} {path!r} is outside the directories this server may use "
                f"({self.describe_roots()}; symbolic links are followed before the check). "
                "Start forklift-mcp with --root for each directory it should reach."
            )
        return real

    def display(self, real: Path) -> str:
        """How results show a path: relative to the first root when inside it, else absolute."""
        first = self.roots[0]
        if real == first:
            return "."
        if first in real.parents:
            return real.relative_to(first).as_posix()
        return str(real)

    # ------------------------------------------------------------------ reading

    def input_file(self, path: str, what: str) -> Path:
        """A regular file to read."""
        real = self.resolve(path, what)
        self.check_regular_file(real, what, path)
        return real

    @staticmethod
    def check_regular_file(real: Path, what: str, shown: str) -> int:
        """Refuse anything but an existing regular file; returns its size."""
        try:
            info = os.stat(real)
        except FileNotFoundError:
            raise ToolInputError(f"{what} {shown!r} does not exist") from None
        except OSError as error:
            raise ToolInputError(f"{what} {shown!r} cannot be read: {error.strerror}") from None
        if stat.S_ISDIR(info.st_mode):
            raise ToolInputError(f"{what} {shown!r} is a directory, not a file")
        if not stat.S_ISREG(info.st_mode):
            raise ToolInputError(
                f"{what} {shown!r} is not a regular file (a device, pipe or socket); only "
                "regular files are read"
            )
        return info.st_size

    # ------------------------------------------------------------------ writing

    def require_writable(self, what: str) -> None:
        if self.read_only:
            raise ToolInputError(
                f"{what} would write files, and this server runs with --read-only, which refuses "
                "every write; dry_run=true shows what would be written"
            )

    def output_dir(self, path: str, what: str, overwrite: bool) -> Path:
        """A directory to write into: missing, empty, or (with ``overwrite``) any directory."""
        real = self.resolve(path, what)
        self._check_parents(real, what, path)
        if os.path.lexists(real):
            if not real.is_dir():
                raise ToolInputError(f"{what} {path!r} exists and is not a directory")
            entries = len(os.listdir(real))
            if entries and not overwrite:
                noun = "entry" if entries == 1 else "entries"
                raise ToolInputError(
                    f"{what} {path!r} is not empty ({entries} {noun}): choose a new or empty "
                    "directory, or pass overwrite=true to write into it (the files of an "
                    "earlier run there are replaced)"
                )
        return real

    def output_file(self, path: str, what: str, overwrite: bool) -> Path:
        """A file to write: missing, or (with ``overwrite``) an existing regular file."""
        real = self.resolve(path, what)
        self._check_parents(real, what, path)
        if os.path.lexists(real):
            if not real.is_file():
                raise ToolInputError(f"{what} {path!r} exists and is not a regular file")
            if not overwrite:
                raise ToolInputError(
                    f"{what} {path!r} already exists: choose another path, or pass "
                    "overwrite=true to replace it"
                )
        return real

    @staticmethod
    def _check_parents(real: Path, what: str, path: str) -> None:
        """The closest existing parent must be a directory (missing ones are created)."""
        parent = real.parent
        while not os.path.lexists(parent):  # ends at the file system root at the latest
            parent = parent.parent
        if not parent.is_dir():
            raise ToolInputError(f"{what} {path!r} cannot be created: {parent} is not a directory")

    @contextlib.contextmanager
    def claim(self, real: Path, what: str) -> Iterator[None]:
        """Hold ``real`` while one tool call writes there; a second call is refused."""
        with self._busy_lock:
            if real in self._busy:
                raise ToolInputError(
                    f"{what} {self.display(real)!r} is being written by another call that has "
                    "not finished; wait for it or choose another directory"
                )
            self._busy.add(real)
        try:
            yield
        finally:
            with self._busy_lock:
                self._busy.discard(real)
