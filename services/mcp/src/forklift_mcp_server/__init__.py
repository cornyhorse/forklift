"""forklift-mcp-server: forklift's remote MCP server (image ``forklift-mcp``).

The MCP tools of design §9.1 over Streamable HTTP at ``/mcp``, as a client of the gateway's
``/api/v1`` with each caller's own API token. It has no database and no storage credentials and
keeps nothing between requests. Settings come from the environment (``FORKLIFT_MCP_...``, see
services/mcp/README.md); ``forklift-mcp-server`` serves until SIGTERM.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

__version__ = "0.1.0"

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def main(argv: Optional[List[str]] = None) -> int:
    """The ``forklift-mcp-server`` command: 0 after a clean shutdown, 2 for bad settings."""
    parser = argparse.ArgumentParser(
        prog="forklift-mcp-server",
        description="Forklift's remote MCP server (Streamable HTTP at /mcp). Settings come from "
        "the environment: FORKLIFT_MCP_API_URL and FORKLIFT_MCP_ALLOWED_HOSTS are required; see "
        "services/mcp/README.md.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--check",
        action="store_true",
        help="check the settings and the tools, print them and exit (0 when they are usable)",
    )
    arguments = parser.parse_args(argv)

    from .settings import Settings, SettingsError

    try:
        settings = Settings.from_env()
    except SettingsError as error:
        print(f"forklift-mcp-server cannot start:\n{error}", file=sys.stderr)
        return 2
    logging.basicConfig(level=settings.log_level, format=LOG_FORMAT, stream=sys.stderr)

    from . import app

    if arguments.check:
        from .docs import schema_pages
        from .tools import tool_names

        app.create_app(settings)  # builds the tools' schemas from their signatures
        print(
            f"forklift-mcp-server {__version__}: gateway {settings.api_url}, "
            f"port {settings.port}, "
            f"hosts {', '.join(settings.allowed_hosts)}; {len(tool_names())} tools, "
            f"{len(schema_pages())} documentation pages"
        )
        return 0
    app.serve(settings)
    return 0
