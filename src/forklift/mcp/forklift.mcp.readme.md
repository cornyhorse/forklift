# Forklift MCP Package

`forklift.mcp` is `forklift-mcp`, the local MCP server: an agent on a machine that has the data
(Claude Code, Claude Desktop, any MCP client) starts it over stdio and can preview a local file,
generate and validate a schema, apply it and explain a failed run. User guide:
[docs/platform/mcp.md](../../../docs/platform/mcp.md); design: `docs/design/platform.md` §9.1.

```bash
pip install 'forklift-etl[mcp]'                # the MCP Python SDK 2.x, Python 3.10+
claude mcp add forklift -- forklift-mcp --root "$PWD/data"
```

Importing `forklift` (or this package) never imports the SDK, so the engine keeps supporting
Python 3.8: `main()` checks for the SDK and the Python version first and exits with code 2 and
an install hint when either is missing.

## Modules

| Module | What it holds |
|---|---|
| `__init__.py` | `main()`: the `forklift-mcp` command (`--root`, `--read-only`, `--log-level`), the SDK and Python checks |
| `__main__.py` | `python -m forklift.mcp` |
| `server.py` | The MCP glue (SDK `MCPServer`): the tools with their descriptions, parameter documentation and annotations, the schema pages as resources, the instructions, `serve()` over stdio. The only module that imports the SDK |
| `tools.py` | `Toolbox`: the eight tools as plain methods (`get_schema`, `generate_schema`, `validate_schema`, `preview`, `run_job`, `get_job`, `explain_failure`, `get_bad_rows`) and their caps |
| `workspace.py` | `Workspace`: the `--root` confinement (symbolic links resolved first), regular-file checks, output rules, `--read-only`, one writer per output directory; `ToolInputError` |
| `values.py` | Cells as JSON values, bounded row collection, Parquet reads by offset, counting `_rejection_reason` |
| `explain.py` | What each error code and rejection rule means, what to try, and which schema page explains it |
| `docs.py` | The `docs/schemas` pages: inside the package in a wheel, `docs/schemas` of the checkout otherwise |

## How the tools run the engine

Everything goes through `forklift.jobs.run_job`, the job contract the service's workers use:

- `preview`, `validate_schema` and `generate_schema` copy the beginning of the input (the first
  records of a CSV file, a whole workbook up to 256 MiB) into a private temporary directory and
  run the `preview`, `validate_schema` or `generate_schema` kind there (a typed preview runs the
  `run` kind on the copy). Nothing is written inside the roots.
- `run_job` runs the `run` kind with the root that holds both the input and the output
  directory as `base_dir`, and writes `forklift-job.json` (the `JobResult`, artifact paths
  relative to the output directory) next to the outputs. `get_job`, `explain_failure` and
  `get_bad_rows` read it.

A refused call raises `ToolInputError`, which the server turns into an MCP tool error with the
message; a job that ran and failed is a result (`status: failed`, the engine's error code).
Messages name paths, columns, rules and counts, never cell values; only previews and rejected
rows show rows, within the caps each result states.

## Tests

`tests/unit-tests/test_mcp_*.py`: the tools on real files and the real engine (`test_mcp_tools`,
no SDK needed), the protocol in process (`test_mcp_server`, the SDK's in-memory streams), a real
`forklift-mcp` process over stdio driving the M2 flow (`test_mcp_stdio`), the command and the
missing-SDK path (`test_mcp_entry`), and the workspace, values, docs and packaging. The tests
that need the SDK skip where it is not installed.
