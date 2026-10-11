"""The MCP server: the tools of :mod:`forklift_mcp_server.tools` and the schema pages.

Built on the official MCP Python SDK (``mcp`` 2.x, ``MCPServer``); :mod:`forklift_mcp_server.app`
serves it over Streamable HTTP.
"""

from __future__ import annotations

from typing import Dict, Tuple

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.resources import FileResource
from mcp.server.mcpserver.tools import Tool
from mcp.types import ToolAnnotations

from . import __version__
from .docs import schema_pages
from .gateway import Gateway
from .tools import MAX_UPLOAD_BYTES, Toolbox, tool_names

SERVER_NAME = "forklift"

# Every tool talks to the gateway (open world); the ones that enqueue jobs or create uploads are
# not read-only, and none deletes or replaces anything.
_READ = ToolAnnotations(read_only_hint=True, open_world_hint=True)
_SUBMIT = ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True)

#: name -> (title, description, annotations)
TOOLS: Dict[str, Tuple[str, str, ToolAnnotations]] = {
    "list_connections": (
        "List connections",
        "The service's connections (S3-compatible stores, mounted volumes, databases) that this "
        "token's role may use: kind, non-secret configuration and the names of the secrets that "
        "are set (never their values). Paged with limit and offset.",
        _READ,
    ),
    "list_datasets": (
        "List datasets",
        "The datasets this token can see: each a source (a connection and path, or uploads), a "
        "stored schema version and a destination, run together. Paged with limit and offset. "
        "Give a dataset's name or id to the other tools as dataset.",
        _READ,
    ),
    "get_schema": (
        "Read a stored schema",
        "A stored schema version: a summary (columns with type, format, required, the x-... "
        "extensions and constraint keywords they use; the schema's extensions) and the document, "
        "or its first max_bytes bytes (truncated: true). By schema (name or id, and version; "
        "default the latest) or dataset (the version it runs). Without either: the list of "
        "stored schemas.",
        _READ,
    ),
    "generate_schema": (
        "Generate a schema",
        "Infer a forklift schema from the first sample_rows rows of an upload or a dataset's "
        "source (column types, required columns, optionally a primary key; never cell values), "
        "with a job on the service's interactive lane. Returns a summary and the document (cut "
        "at max_bytes); the schema is not stored. dry_run=true submits nothing.",
        _SUBMIT,
    ),
    "validate_schema": (
        "Check a schema against an input",
        "Check a schema (inline, or a stored one) against an upload (CSV) or a dataset's source "
        "as the import would: the header against the schema's columns, the extensions' "
        "configuration, and an import of the first sample_rows rows. Returns valid, the error "
        "(code and message, what it means and what to try), the column comparison, findings by "
        "rule, warnings and, when the sample failed its bad-rows threshold, the first rejected "
        "rows. dry_run=true submits nothing.",
        _SUBMIT,
    ),
    "preview": (
        "Preview the first rows",
        "The first rows of an upload or a dataset's source as text, as the import reads them "
        "(header detection, comment rows, footer), from a job on the interactive lane. Bounded "
        "by rows and max_bytes; long cells are cut. Previewing sensitive data needs the 'view "
        "raw rows' permission. dry_run=true submits nothing.",
        _SUBMIT,
    ),
    "run_job": (
        "Run an import",
        "Apply a schema: import an upload (with an inline or stored schema) or run a dataset on "
        "the service's workers, into validated Parquet artifacts (data, bad_rows with the "
        "rejected rows, manifest, metadata); a dataset with an s3 destination is published "
        "there. Waits up to wait_seconds, then returns the job (still queued, running or "
        "publishing: follow it with get_job). The same idempotency_key returns the job an "
        "earlier call started. A failed run is a result (status failed, error code), not a "
        "tool error. dry_run=true submits nothing.",
        _SUBMIT,
    ),
    "get_job": (
        "Read a job",
        "A job by id, optionally waiting up to wait_seconds for it to end: status, error, "
        "counts, schema extensions applied, findings by rule, warnings and its files; for a "
        "finished preview, validate_schema or generate_schema job also what that tool returns "
        "(rows, report, schema).",
        _READ,
    ),
    "explain_failure": (
        "Explain a job",
        "Explain a job (normally a failed one): the error code and message with what it means "
        "and what to try, findings by rule, the reasons rows were rejected (counted over its "
        "bad_rows artifact), the first rejected rows and the schema documentation pages that "
        "apply. Rows of sensitive data need the 'view raw rows' permission; without it the "
        "rest is still returned.",
        _READ,
    ),
    "get_bad_rows": (
        "Read rejected rows",
        "Rows a job rejected (its bad_rows artifact: the input's text plus _rejection_reason), "
        "from offset on, at most limit rows and max_bytes bytes; next_offset continues. Read "
        "with range requests: only the row groups shown are fetched.",
        _READ,
    ),
    "upload_csv": (
        "Upload CSV text",
        f"Upload a small text file (CSV or another delimited text, at most {MAX_UPLOAD_BYTES} "
        "bytes) given inline, as an upload of yours: its upload_id is the input of preview, "
        "generate_schema, validate_schema and run_job. dry_run=true uploads nothing.",
        _SUBMIT,
    ),
}


def instructions(max_wait_seconds: float) -> str:
    """What the server tells the agent when it connects."""
    return (
        "Forklift cleans tabular files (CSV, Excel) into validated Parquet, driven by a JSON "
        "schema with x-... extensions. This server works on the forklift service with your API "
        "token: its uploads, datasets, stored schemas and jobs, within your role's permissions. "
        "A typical flow: upload_csv (or an existing upload_id, or a dataset from list_datasets); "
        "preview; generate_schema; adjust the schema; validate_schema until valid; run_job; then "
        "get_job, explain_failure and get_bad_rows with the job's id. Jobs run on the service's "
        f"workers: tools wait for them at most {max_wait_seconds:g} seconds and otherwise return "
        "the job's id and status to follow with get_job (publishing has not ended). Tools that "
        "submit a job or upload take dry_run. Results are bounded; each says which caps "
        "applied. The schema documentation is available as resources "
        "forklift://docs/schemas/<page>.md (README.md says what each part of a schema does); "
        "read the page of an extension before writing one."
    )


def create_server(gateway: Gateway, toolbox: Toolbox | None = None) -> MCPServer:
    """The server: the tools (bound to ``gateway``) and the schema pages as resources."""
    toolbox = toolbox or Toolbox(gateway)
    tools = []
    for name in tool_names():
        title, description, annotations = TOOLS[name]
        tools.append(
            Tool.from_function(
                getattr(toolbox, name),
                name=name,
                title=title,
                description=description,
                annotations=annotations,
            )
        )
    server = MCPServer(
        SERVER_NAME,
        title="Forklift",
        description="Generate, validate and apply forklift schemas with the forklift service",
        instructions=instructions(gateway.settings.max_wait_seconds),
        version=__version__,
        website_url="https://github.com/cornyhorse/forklift",
        tools=tools,
        log_level=gateway.settings.log_level,
    )
    for page in schema_pages():
        server.add_resource(
            FileResource(
                uri=page.uri,
                name=page.name,
                title=page.title,
                description=f"Forklift schema documentation: {page.title}",
                mime_type="text/markdown",
                path=page.path,
            )
        )
    return server
