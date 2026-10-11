"""``forklift-mcp``: the local MCP server (stdio) of forklift.

An agent on a machine that has the data connects over stdio and can preview a local file,
generate and validate a schema, apply it (run the import with :func:`forklift.jobs.run_job`) and
explain a failed run. Every path the tools touch is confined to the ``--root`` directories, and
``--read-only`` refuses every write. See ``docs/platform/mcp.md``.

The server is built on the MCP Python SDK (``pip install 'forklift-etl[mcp]'``, Python 3.10+).
Importing this package never imports the SDK: :func:`main` checks for it first and says how to
install it when it is missing, so the engine itself keeps supporting Python 3.8.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Sequence, Tuple

#: The MCP Python SDK supports Python 3.10 and newer
MIN_PYTHON = (3, 10)
INSTALL_HINT = "pip install 'forklift-etl[mcp]'"
#: Exit code when the server cannot start (no SDK, old Python, a bad --root)
EXIT_UNAVAILABLE = 2

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def build_parser() -> argparse.ArgumentParser:
    from .. import __version__

    parser = argparse.ArgumentParser(
        "forklift-mcp",
        description=(
            "The local forklift MCP server: an MCP client (Claude Code, Claude Desktop, ...) "
            "starts it and talks to it over stdin and stdout. Its tools preview, generate and "
            "validate schemas for, and import, the files inside the --root directories."
        ),
        epilog=f"Needs the MCP Python SDK and Python 3.10+: {INSTALL_HINT}",
    )
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        metavar="DIR",
        help=(
            "A directory the tools may read and write in; repeat for several. Every path must "
            "be inside one (symbolic links are resolved first); relative paths are relative to "
            "the first. Default: the working directory"
        ),
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Refuse every write: no run_job, no generate_schema output_path (dry runs work)",
    )
    parser.add_argument(
        "--log-level",
        choices=_LOG_LEVELS,
        default="WARNING",
        help="Log level of the messages on stderr (default: WARNING)",
    )
    parser.add_argument("--version", action="version", version=f"forklift-mcp {__version__}")
    return parser


def python_version() -> Tuple[int, int]:
    return sys.version_info[0], sys.version_info[1]


def sdk_problem() -> Optional[str]:
    """Why the server cannot run here (old Python, no MCP SDK), or None."""
    major, minor = python_version()
    if (major, minor) < MIN_PYTHON:
        return (
            f"forklift-mcp needs Python 3.10 or newer (this is Python {major}.{minor}): the MCP "
            "Python SDK it is built on does not support older versions. Install it into a "
            f"Python 3.10+ environment: {INSTALL_HINT}"
        )
    try:
        import mcp.server.mcpserver  # noqa: F401
    except ImportError as error:
        return (
            "forklift-mcp needs the MCP Python SDK (the 'mcp' package, version 2), which "
            f"cannot be imported here ({error}). Install it with: {INSTALL_HINT}"
        )
    return None


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run ``forklift-mcp``; returns the exit code (0 when the client disconnected)."""
    args = build_parser().parse_args(argv)
    problem = sdk_problem()
    if problem is not None:
        print(problem, file=sys.stderr)
        return EXIT_UNAVAILABLE
    roots = args.root or [os.getcwd()]
    if not args.root and Path(os.getcwd()).resolve() == Path(os.getcwd()).resolve().parent:
        # Started from "/" (as some desktop clients do): every file would be reachable
        print(
            "forklift-mcp: the working directory is the file system root; pass --root DIR for "
            "the directories the tools may use (--root / to really allow every file)",
            file=sys.stderr,
        )
        return EXIT_UNAVAILABLE
    from .workspace import Workspace

    try:
        workspace = Workspace(roots, read_only=args.read_only)
    except ValueError as error:
        print(f"forklift-mcp: {error}", file=sys.stderr)
        return EXIT_UNAVAILABLE
    from .server import serve

    serve(workspace, log_level=args.log_level)
    return 0
