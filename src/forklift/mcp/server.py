"""The MCP server: the tools of :mod:`forklift.mcp.tools` and the schema pages, over stdio.

Built on the official MCP Python SDK (``mcp`` 2.x, ``MCPServer``). This module is imported only
after :func:`forklift.mcp.main` has checked that the SDK can be imported.
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resources import FileResource
from mcp.server.mcpserver.tools import Tool
from mcp.types import ToolAnnotations

from .. import __version__
from . import tools as caps
from .docs import schema_pages
from .tools import JOB_FILE, Toolbox
from .workspace import ToolInputError, Workspace

SERVER_NAME = "forklift"

_READ = ToolAnnotations(read_only_hint=True, open_world_hint=False)
_WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=True, open_world_hint=False)

#: name -> (title, description, annotations)
TOOLS: Dict[str, Any] = {
    "get_schema": (
        "Read a schema file",
        "Read a forklift JSON schema file: a summary (columns with type, format, required, "
        "the x-... extensions and constraint keywords they use; the schema's extensions) and "
        "the document itself, or its first max_bytes bytes when it is larger (truncated: "
        "true).",
        _READ,
    ),
    "generate_schema": (
        "Generate a schema from a file",
        "Infer a forklift schema from the first sample_rows rows of a local CSV or Excel file "
        "(column types, required columns, optionally a primary key; never cell values). "
        "Returns a summary and the document (cut at max_bytes). With output_path the schema "
        "is also written there (refused if the file exists unless overwrite=true; dry_run=true "
        "writes nothing).",
        _WRITE,
    ),
    "validate_schema": (
        "Check a schema against a file",
        "Check a schema (inline or schema_path) against a local CSV file as the import would: "
        "the header against the schema's columns, the extensions' configuration, and an import "
        "of the first sample_rows rows. Returns valid, the engine's error (code and message), "
        "the column comparison, the findings by rule (validation_summary) and warnings.",
        _READ,
    ),
    "preview": (
        "Preview the first rows",
        "Show the first rows of a local CSV or Excel file. Without a schema the rows are text "
        "as the import reads them (header detection, comment rows, footer). With a schema they "
        "are the typed rows the import would write, with the rows the schema would reject "
        "counted by reason (sample). Bounded by rows and max_bytes; long cells are cut.",
        _READ,
    ),
    "run_job": (
        "Run an import",
        "Apply a schema: import a local CSV or Excel file into validated Parquet "
        "(data.parquet, bad_rows.parquet with the rejected rows, manifest and metadata) in "
        f"output_dir, and write the job result there ({JOB_FILE}) for get_job, explain_failure "
        "and get_bad_rows. The input and output_dir must be inside the same root. An existing, "
        "non-empty output_dir is refused unless overwrite=true. dry_run=true checks everything "
        "and returns the job it would run without writing anything. A failed run is a result "
        "(status failed, error code), not a tool error. The run cannot be cancelled: bound it "
        "with limits.max_seconds (checked between batches of rows).",
        _WRITE,
    ),
    "get_job": (
        "Read a past run",
        f"The result of a run_job call, read from {JOB_FILE} in its output directory: status, "
        "error, counts, schema extensions applied, findings by rule, warnings and the files "
        "written.",
        _READ,
    ),
    "explain_failure": (
        "Explain a run",
        "Explain a run (normally a failed one) from its output directory: the error code and "
        "message with what it means and what to try, findings by rule, the reasons rows were "
        "rejected (counted over bad_rows.parquet), the first rejected rows and the schema "
        "documentation pages that apply.",
        _READ,
    ),
    "get_bad_rows": (
        "Read rejected rows",
        "Rows a run rejected (bad_rows.parquet: the input's text plus _rejection_reason), from "
        "offset on, at most limit rows and max_bytes bytes; next_offset continues.",
        _READ,
    ),
}

_ROOT_NOTE = "relative paths are relative to the first --root"
_INPUT_OPTIONS = (
    "How to read the input, as the job contract's input.options: CSV encoding, delimiter, "
    "quote_char, escape_char, header_mode (present|absent|auto), header_search_rows, "
    "skip_blank_lines, comment_rows, footer_detection, excess_column_mode; Excel sheet, "
    "values_only, engine, date_system"
)
_SCHEMA = "The forklift JSON schema as an object (or give schema_path instead)"
_SCHEMA_PATH = f"Path of a schema JSON file ({_ROOT_NOTE}); instead of schema"
_FORMAT = {
    "description": "Input format; default: from the extension (.csv/.tsv/.tab/.txt csv, "
    ".xlsx/.xlsm/.xls excel)",
    "enum": [*caps.FORMATS, None],
}
_OUTPUT_DIR = f"Output directory of a run_job call, or its {JOB_FILE} ({_ROOT_NOTE})"


def _bytes(maximum: int, what: str) -> Dict[str, Any]:
    return {
        "description": f"Largest {what} in bytes (compact JSON), {caps.MIN_BYTES} to {maximum}",
        "minimum": caps.MIN_BYTES,
        "maximum": maximum,
    }


_DOCUMENT_BYTES = _bytes(caps.MAX_DOCUMENT_BYTES, "schema document in the result")
_ROWS_BYTES = _bytes(caps.MAX_ROWS_BYTES, "rows in the result")
_SAMPLE_ROWS = {
    "description": f"Rows of the input to analyse, 1 to {caps.MAX_SAMPLE_ROWS}",
    "minimum": 1,
    "maximum": caps.MAX_SAMPLE_ROWS,
}
_INPUT_PATH = {"description": f"The CSV or Excel file to read ({_ROOT_NOTE})"}
_DRY_RUN = {"description": "Check everything and say what would be written; write nothing"}

#: tool -> parameter -> extra JSON Schema (descriptions and bounds) for its input schema
PARAMETERS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "get_schema": {
        "path": {"description": f"Path of the schema JSON file ({_ROOT_NOTE})"},
        "max_bytes": _DOCUMENT_BYTES,
    },
    "generate_schema": {
        "input_path": _INPUT_PATH,
        "format": _FORMAT,
        "input_options": {
            "description": _INPUT_OPTIONS + " (generate_schema uses encoding, "
            "delimiter and sheet)"
        },
        "sample_rows": _SAMPLE_ROWS,
        "infer_primary_key": {
            "description": "Propose x-primaryKey from columns that are unique in the sample"
        },
        "output_path": {"description": f"Also write the schema to this file ({_ROOT_NOTE})"},
        "overwrite": {"description": "Replace output_path if it exists"},
        "dry_run": _DRY_RUN,
        "max_bytes": _DOCUMENT_BYTES,
    },
    "validate_schema": {
        "input_path": {"description": f"The CSV file to check against ({_ROOT_NOTE})"},
        "schema": {"description": _SCHEMA},
        "schema_path": {"description": _SCHEMA_PATH},
        "format": _FORMAT,
        "input_options": {"description": _INPUT_OPTIONS},
        "sample_rows": _SAMPLE_ROWS,
        "apply_schema_extensions": {
            "description": "Apply the schema's x-... extensions (false: only types, nulls and "
            "required)"
        },
    },
    "preview": {
        "input_path": _INPUT_PATH,
        "rows": {
            "description": f"Rows to show, 1 to {caps.MAX_PREVIEW_ROWS}",
            "minimum": 1,
            "maximum": caps.MAX_PREVIEW_ROWS,
        },
        "schema": {"description": _SCHEMA + "; with one the rows are typed"},
        "schema_path": {"description": _SCHEMA_PATH},
        "format": _FORMAT,
        "input_options": {"description": _INPUT_OPTIONS},
        "max_bytes": _ROWS_BYTES,
    },
    "run_job": {
        "input_path": _INPUT_PATH,
        "output_dir": {
            "description": f"Directory for the outputs; new or empty unless overwrite "
            f"({_ROOT_NOTE}; inside the same root as input_path)"
        },
        "schema": {"description": _SCHEMA},
        "schema_path": {"description": _SCHEMA_PATH},
        "format": _FORMAT,
        "input_options": {"description": _INPUT_OPTIONS},
        "options": {
            "description": "The job contract's options: apply_schema_extensions (true), "
            "batch_size (10000), include_value_statistics (false: the output metadata holds no "
            "cell values)"
        },
        "limits": {
            "description": "The job contract's limits, each optional: max_rows, "
            "max_input_bytes, max_seconds (exceeding one fails the run with LIMIT_EXCEEDED)"
        },
        "job_id": {
            "description": "Id of the run (letters, digits, '.', '_', '-'); default: generated"
        },
        "overwrite": {
            "description": "Write into a non-empty output_dir. The files an earlier run_job "
            f"listed in its {JOB_FILE} there, and that are still those files, are replaced once "
            "the engine accepted the job; the run's own files replace files of the same name; "
            "nothing else is deleted (previous_run in the result says what was replaced and "
            "what was left alone)"
        },
        "dry_run": _DRY_RUN,
    },
    "get_job": {"output_dir": {"description": _OUTPUT_DIR}},
    "explain_failure": {
        "output_dir": {"description": _OUTPUT_DIR},
        "rows": {
            "description": f"Rejected rows to show, 1 to {caps.MAX_EXPLAIN_ROWS}",
            "minimum": 1,
            "maximum": caps.MAX_EXPLAIN_ROWS,
        },
        "max_bytes": _ROWS_BYTES,
    },
    "get_bad_rows": {
        "output_dir": {"description": _OUTPUT_DIR},
        "offset": {"description": "Index of the first row to show (0-based)", "minimum": 0},
        "limit": {
            "description": f"Rows to show, 1 to {caps.MAX_BAD_ROWS}",
            "minimum": 1,
            "maximum": caps.MAX_BAD_ROWS,
        },
        "columns": {
            "description": f"Columns to show (default: the first {caps.MAX_COLUMNS}, with "
            "_rejection_reason); at most that many"
        },
        "max_bytes": _ROWS_BYTES,
    },
}


def instructions(workspace: Workspace) -> str:
    """What the server tells the agent when it connects."""
    roots = workspace.describe_roots()
    access = (
        "The server is read-only: run_job and writing a generated schema are refused (dry_run "
        "still works)."
        if workspace.read_only
        else "It writes inside the roots only where a tool is asked to (run_job's output_dir, "
        "plus a temporary .forklift-job-* directory next to it while the import runs; "
        "generate_schema's output_path) and never replaces files unless overwrite=true."
    )
    return (
        "Forklift cleans tabular files (CSV, Excel) into validated Parquet, driven by a JSON "
        "schema with x-... extensions. A typical flow: preview the file; generate_schema (with "
        "output_path to keep it); adjust it; validate_schema; preview with the schema to see "
        "typed rows and what it rejects; run_job into a new output directory; then get_job, "
        "explain_failure and get_bad_rows with that directory. "
        f"Paths must be inside {roots}; relative paths are relative to {workspace.roots[0]}. "
        f"{access} Results are bounded; each says which caps applied. The schema documentation "
        "is available as resources forklift://docs/schemas/<page>.md (README.md says what each "
        "part of a schema does); read the page of an extension before writing one."
    )


class StrictTool(Tool):
    """A tool that refuses arguments its input schema does not have (the SDK drops them), and
    true or false given for an integer (the SDK would take them as 1 or 0)."""

    async def run(
        self, arguments: Dict[str, Any], context: Any, convert_result: bool = False
    ) -> Any:
        properties = self.parameters["properties"]
        unknown = sorted(set(arguments) - set(properties))
        if unknown:
            names = ", ".join(repr(key[:64]) for key in unknown[:10])
            raise ToolError(
                f"Error executing tool {self.name}: unknown argument"
                f"{'s' if len(unknown) > 1 else ''} {names}; {self.name} takes "
                f"{', '.join(properties)}"
            )
        for key, value in arguments.items():
            if isinstance(value, bool) and properties[key].get("type") == "integer":
                raise ToolError(
                    f"Error executing tool {self.name}: {key} must be an integer, not "
                    f"{str(value).lower()}"
                )
        return await super().run(arguments, context, convert_result=convert_result)


def _tool(toolbox: Toolbox, name: str) -> Tool:
    method: Callable[..., Dict[str, Any]] = getattr(toolbox, name)

    @functools.wraps(method)
    def call(**arguments: Any) -> Dict[str, Any]:
        try:
            return method(**arguments)
        except ToolInputError as error:
            raise ToolError(str(error)) from None

    title, description, annotations = TOOLS[name]
    tool = StrictTool.from_function(
        call, name=name, title=title, description=description, annotations=annotations
    )
    tool.parameters["additionalProperties"] = False
    properties = tool.parameters["properties"]
    for parameter, extra in PARAMETERS[name].items():
        properties[parameter].update(extra)
    return tool


def create_server(workspace: Workspace, log_level: str = "WARNING") -> MCPServer:
    """The server for ``workspace``: the tools, and the schema pages as resources."""
    toolbox = Toolbox(workspace)
    server = MCPServer(
        SERVER_NAME,
        title="Forklift",
        description="Generate, validate and apply forklift schemas to local files",
        instructions=instructions(workspace),
        version=__version__,
        website_url="https://github.com/cornyhorse/forklift",
        tools=[_tool(toolbox, name) for name in TOOLS],
        log_level=log_level,
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


def serve(workspace: Workspace, log_level: str = "WARNING") -> None:
    """Run the server over stdin and stdout until the client disconnects."""
    create_server(workspace, log_level).run("stdio")
