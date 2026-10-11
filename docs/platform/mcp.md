# MCP: forklift for agents

Forklift offers its engine to agents (Claude Code, Claude Desktop and any other
[Model Context Protocol](https://modelcontextprotocol.io) client) through two servers with one
tool set:

| Server | Transport | Runs the engine | For |
|---|---|---|---|
| `forklift-mcp`, the **local server** (an extra of `forklift-etl`) | stdio: the client starts it as a child process | In the server's own process, with [`forklift.run_job`](../../src/forklift/jobs/forklift.jobs.readme.md) | An agent on a machine that has the data |
| The `forklift-mcp` image, the **remote server** | Streamable HTTP at `/mcp` | On the service's workers, through `/api/v1` with the caller's API token | Agents that should use the service's connections, permissions and audit |

Both have the same tools with the same names and meanings (design: `docs/design/platform.md`
§9.1). The local server works on paths where the remote one takes upload, dataset and job ids,
and it has no `list_connections` or `list_datasets`: there is no service behind it.

| Tool | Local | Remote | What it does |
|---|---|---|---|
| `list_connections`, `list_datasets` | - | yes | The service's connections and datasets |
| `get_schema` | a schema file | a stored schema | Summary (columns, types, required, extensions) and the document |
| `generate_schema` | yes | yes | Infer a schema from the first rows of an input |
| `validate_schema` | yes | yes | Check a schema against an input as the import would |
| `preview` | yes | yes | The first rows, as text or typed by a schema |
| `run_job` | yes | yes | Apply a schema: run the import |
| `get_job` | an output directory | a job id | A run's result |
| `explain_failure` | yes | yes | Why a run failed: error, findings by rule, the first rejected rows |
| `get_bad_rows` | yes | yes | The rejected rows, page by page |

Resources: the schema documentation pages (`docs/schemas/*.md`) as
`forklift://docs/schemas/<file>.md`, so an agent can read how an extension works before it
writes one.

Every tool returns a JSON object (MCP structured content, also sent as text) whose size is
bounded: rows, bytes, list lengths and cell lengths are capped, and each result names the caps
that applied in its `caps`. Results never carry secrets, and only the tools that are meant to show
rows (`preview`, `explain_failure`, `get_bad_rows`) carry cell values.

## Local server (stdio)

### Install

`forklift-mcp` is part of `forklift-etl`; the `mcp` extra installs the
[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) (`mcp` 2.x), which needs
**Python 3.10 or newer** (the engine itself still supports 3.8):

```bash
pip install 'forklift-etl[mcp]'          # or [all], which includes it
forklift-mcp --version
```

Excel inputs need the `excel` extra as well (`pip install 'forklift-etl[mcp,excel]'`).

Without the SDK, or on an older Python, `forklift-mcp` exits with code 2 and says what to install
(`pip install 'forklift-etl[mcp]'`, Python 3.10+). Importing `forklift` never imports the SDK.

### Start it from a client

The client starts `forklift-mcp` and talks to it over stdin and stdout; you never run it by
hand. Always give it the directories it may use with `--root` (see
[below](#where-the-tools-may-read-and-write)).

```
forklift-mcp [--root DIR]... [--read-only] [--log-level {DEBUG,INFO,WARNING,ERROR}]
```

| Option | Meaning |
|---|---|
| `--root DIR` | A directory the tools may read and write in; repeat it for several. Relative paths in tool calls are relative to the first. Default: the working directory, except that a working directory of `/` is refused (some desktop clients start servers there) |
| `--read-only` | Refuse every write: `run_job` and `generate_schema`'s `output_path` are refused (their `dry_run` still works) |
| `--log-level` | Messages on stderr (default `WARNING`); the client usually keeps them in its MCP log |

Exit codes: 0 when the client disconnects, 2 when the server cannot start (no SDK, Python older
than 3.10, a `--root` that is not a directory, the file system root as the default root).

#### Claude Code

```bash
# For yourself, in this project
claude mcp add forklift -- forklift-mcp --root "$PWD/data"

# For everyone working on the project: writes .mcp.json (commit it)
claude mcp add --scope project forklift -- forklift-mcp --root ./data --read-only
```

`.mcp.json` written by hand looks the same as Claude Desktop's configuration below. `claude mcp
list` shows whether the server started; `/mcp` inside Claude Code shows its tools.

#### Claude Desktop

Edit `claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/`, Windows:
`%APPDATA%\Claude\`) and restart Claude Desktop. Desktop does not read your shell's `PATH`, so
give the full path of the command (for a virtual environment, its `bin/forklift-mcp`), and
absolute `--root` paths:

```json
{
  "mcpServers": {
    "forklift": {
      "command": "/Users/me/.venvs/forklift/bin/forklift-mcp",
      "args": ["--root", "/Users/me/data/exports", "--root", "/Users/me/data/clean"]
    }
  }
}
```

#### Other clients

Any MCP client that starts stdio servers takes a command and its arguments: the command is
`forklift-mcp` (or `python -m forklift.mcp` with the Python of the environment it is installed
in), the arguments `--root DIR ...`. The server speaks both the initialize handshake and the newer
`server/discover` negotiation of the SDK. Nothing needs a network connection.

### Where the tools may read and write

- **Roots.** Every path a tool is given must be inside a `--root` directory. Symbolic links are
  resolved before the check, so a link inside a root that points elsewhere is refused. A path
  outside is refused with a message that names the roots ("... is outside the directories this
  server may use (...); start forklift-mcp with --root for each directory it should reach").
  Paths in results are relative to the first root when they are inside it, absolute otherwise.
- **Regular files only.** Inputs, schema files and job files must be regular files: a device, a
  pipe or a socket (which could block or never end) is refused before it is opened.
- **Writes only where asked.** Only `run_job` (its `output_dir`) and `generate_schema` (its
  `output_path`) write inside the roots. An existing, non-empty `output_dir` and an existing
  `output_path` are refused unless the call says `overwrite=true`; `dry_run=true` checks
  everything a real call would and writes nothing. Two calls never write into the same output
  directory at once (the second is refused while the first runs).
- **Read-only.** With `--read-only` every write is refused, and the server tells the agent so
  when it connects.
- **Temporary copies.** `preview`, `validate_schema` and `generate_schema` copy the beginning of
  the input (a CSV file's first records, an Excel workbook whole, up to 256 MiB) into a private
  directory in the system's temporary directory (`TMPDIR`), run the job there and delete the
  directory when the call ends. Nothing is written inside the roots, so they work with
  `--read-only` too.
- **No cell values in messages.** Error messages and logs name files, columns, rules and
  counts, never the content of a cell (the engine's rule). Rows appear only in the results of
  `preview`, `explain_failure` and `get_bad_rows`, bounded as listed below.

The server is meant for one user's agent on that user's machine: it runs with the user's
permissions, and the roots are a guard against an agent wandering off, not a sandbox against
someone who can change the files inside them.

### Conventions of the tools

- **Inputs** are CSV or Excel files. `format` is `csv` or `excel`; by default it follows the
  extension (`.csv`, `.tsv`, `.tab`, `.txt`: csv, with a tab delimiter for `.tsv` and `.tab`;
  `.xlsx`, `.xlsm`, `.xls`: excel).
- **`input_options`** are the job contract's `input.options` (see the
  [jobs readme](../../src/forklift/jobs/forklift.jobs.readme.md)): CSV `encoding`, `delimiter`,
  `quote_char`, `escape_char`, `header_mode` (`present`, `absent`, `auto`),
  `header_search_rows`, `skip_blank_lines`, `comment_rows`, `footer_detection`,
  `excess_column_mode`; Excel `sheet`, `values_only`, `engine`, `date_system`. An unknown or
  invalid option is refused with the field it is about.
- **Schemas** are given inline (`schema`, a JSON object) or as a file (`schema_path`), never
  both.
- **Refusals and failures.** A call the server refuses (a path outside the roots, a bad
  argument, `--read-only`) is an MCP tool error whose text says why. A job that ran and failed is
  not a tool error: its result has `status: "failed"` and `error: {code, message, retryable}`,
  with the engine's stable error codes (`SCHEMA_INVALID`, `INPUT_UNREADABLE`, `ENCODING_ERROR`,
  `COLUMN_MISSING`, `BAD_ROWS_THRESHOLD_EXCEEDED`, `CONSTRAINT_VIOLATION`, `LIMIT_EXCEEDED`,
  `PERMISSION_DENIED`, `CANCELLED`, `INTERNAL`). An unexpected crash shows only "Error executing
  tool <name>"; the details stay in the server's log.
- **Input description.** The interactive tools return `input: {path, format, bytes, sampled}`;
  `sampled: true` means the job saw a copy of the input's first records only.

### Tools

#### `get_schema`

Reads a schema file.

| Input | Default | |
|---|---|---|
| `path` | required | The schema JSON file |
| `max_bytes` | 65536 | Largest document in the result, 1024 to 1048576 |

Output: `path`, `bytes`; `summary`: `title`, `column_count`, `columns` (`name`, `type`,
`required`, `format`, the `x-...` `extensions` and the constraint keywords of the column),
`columns_truncated`, `required`, `extensions` (the schema's top-level `x-...` keys);
`document` (the schema) or, when its compact JSON is larger than `max_bytes`, `document: null`,
`document_text` (its first `max_bytes` bytes) and `truncated: true`; `document_bytes`.
Caps: files up to 8 MiB are read, 500 columns are listed.

#### `generate_schema`

Infers a schema from the first rows of a CSV or Excel file (column types, required columns and,
with `infer_primary_key`, a primary key; never cell values).

| Input | Default | |
|---|---|---|
| `input_path` | required | |
| `format`, `input_options` | from the extension | the generator uses `encoding`, `delimiter` and `sheet` |
| `sample_rows` | 1000 | Rows analysed, 1 to 100000 |
| `infer_primary_key` | false | Propose `x-primaryKey` from columns unique in the sample |
| `output_path` | none | Also write the schema there (refused if it exists unless `overwrite`) |
| `overwrite`, `dry_run` | false | |
| `max_bytes` | 65536 | As for `get_schema` |

Output: `status`, `error`, `input`, `rows_analyzed`, `warnings`, `summary` and the document (as
for `get_schema`), `output_path`, `written`, `dry_run`. A schema that cannot be generated (an
undecodable file, say) is `status: "failed"` with the error and writes nothing.

#### `validate_schema`

Checks a schema against a CSV file as the import would: the header against the schema's
columns, the extensions' configuration, and an import of the first `sample_rows` rows into a
temporary directory.

| Input | Default | |
|---|---|---|
| `input_path` | required | A CSV file (for a workbook, use `preview` with the schema) |
| `schema` or `schema_path` | required | |
| `format`, `input_options` | from the extension | |
| `sample_rows` | 1000 | 1 to 100000 |
| `apply_schema_extensions` | true | false: only types, null markers and `required` |

Output: `valid`, `status`, `error`; the column comparison `columns` (the input's),
`schema_columns`, `required_columns`, `columns_not_in_schema`, `schema_columns_not_in_input`;
`sample_rows`, `counts`, `schema_extensions`, `validation_summary` (findings by rule,
`CODE:column`), `warnings`, `notes`; and `rejected_rows` when the sample failed its bad-rows
threshold: the first rows it rejected, in the shape `get_bad_rows` returns (`total_rows`,
`columns`, `rows` with `_rejection_reason`, `row_count`, `truncated`, ...), with `reasons` counted
by reason (null otherwise). The check keeps no files, so the error message points at
`rejected_rows` instead of a `bad_rows.parquet`; `run_job` keeps the whole file. Caps: 500 names
per list, the 100 most frequent findings, 50 warnings, 10 rejected rows within 65536 bytes, 100
columns, 20 reasons, 2000 characters per cell.

#### `preview`

The first rows of a CSV or Excel file.

| Input | Default | |
|---|---|---|
| `input_path` | required | |
| `rows` | 20 | 1 to 500 |
| `schema` or `schema_path` | none | With a schema the rows are typed |
| `format`, `input_options` | from the extension | |
| `max_bytes` | 65536 | Largest rows in the result, 1024 to 1048576 |

Without a schema (`mode: "raw"`): the rows as text, as the import reads them (header
detection, comment rows, footer): `columns`, `rows`, `row_count`, `truncated`, `limit`
(`rows`, `bytes` or null), `truncated_cells`, and `sheet` for a workbook.

With a schema (`mode: "typed"`): the import runs on the first rows and the result shows the
rows it would write, typed (`columns` with their Arrow types, `columns_total`, `rows`), and what
it would reject: `sample: {rows_read, rejected_rows, rejection_reasons}`. `x-validation`'s
bad-rows threshold is not applied to a preview (a share of rejected rows is judged on a whole
input; `notes` says so). Also `schema_extensions`, `validation_summary`, `warnings`.

Caps: cells longer than 2000 characters are cut (and counted in `truncated_cells`), at most 100
items of a list or map inside a cell, 100 columns of a typed preview.

#### `run_job`

Applies a schema: imports a CSV or Excel file into validated Parquet in `output_dir`
(`data.parquet`, `bad_rows.parquet` with the rejected rows and their `_rejection_reason`,
`manifest.json`, metadata files) with `forklift.run_job`, the job contract the service's
workers use, and writes the result next to the outputs (`forklift-job.json`).

| Input | Default | |
|---|---|---|
| `input_path` | required | |
| `output_dir` | required | New or empty; inside the same root as `input_path` |
| `schema` or `schema_path` | none | |
| `format`, `input_options` | from the extension | |
| `options` | `{}` | The contract's `options`: `apply_schema_extensions` (true), `batch_size` (10000), `include_value_statistics` (false: the output metadata holds no cell values) |
| `limits` | `{}` | The contract's `limits`: `max_rows`, `max_input_bytes`, `max_seconds`; exceeding one fails the run with `LIMIT_EXCEEDED` |
| `job_id` | generated | Letters, digits, `.`, `_`, `-` |
| `overwrite` | false | Write into a non-empty `output_dir`: the files an earlier `run_job` listed in its `forklift-job.json` there are deleted first; other files are left alone |
| `dry_run` | false | Check everything and return the job (`would_run`, `spec` without the schema, `writable`) |

Output: the run's result as `get_job` returns it, plus `next` (the tool to call next). If the
result file cannot be written (a full disk, say), `result_file` is null and
`result_file_error` says why.

The run happens in the server's process and cannot be cancelled from the client: a cancelled
call lets the run finish, and its result is in `output_dir`. Bound long runs with
`limits.max_seconds`.

#### `get_job`

A past run's result from its output directory (`output_dir`, or the path of its
`forklift-job.json`): `job_id`, `status`, `error`, `output_dir`, `result_file`, `input`
(`path`, `format`, `bytes`), `schema` (its path, or `inline`), `created_at`, `counts`,
`schema_extensions`, `validation_summary`, `warnings`, `artifacts` (`kind`, `path`, `rows`,
`bytes`, `exists`). Caps: the 100 most frequent findings, 50 warnings, 50 artifacts (with
`..._total` counts).

#### `explain_failure`

Explains a run, normally a failed one.

| Input | Default | |
|---|---|---|
| `output_dir` | required | |
| `rows` | 10 | Rejected rows to show, 1 to 50 |
| `max_bytes` | 65536 | Largest rows in the result |

Output: `summary` (one sentence), `error` with `meaning` and `next_steps` (what the code means
and what to try), `counts`, `findings` (`rule`, `column`, `count` from `validation_summary`),
`rejected` (`path`, `total_rows`, `reasons`: each reason with its rule, column, number of rows
and, for `type_conversion_failed`, `required_value_missing` and `too_many_fields`, its meaning;
`first_rows`: the first rejected rows with `_rejection_reason`), `warnings`, and `docs`: the
documentation resources that explain the error and the rules involved. Caps: the 20 most frequent
reasons (counted over at most 5,000,000 rejected rows; a row with several reasons counts for
each), 100 columns, 2000 characters per cell.

#### `get_bad_rows`

The rows a run rejected (`bad_rows.parquet`: the input's text and `_rejection_reason`).

| Input | Default | |
|---|---|---|
| `output_dir` | required | |
| `offset` | 0 | First row (0-based) |
| `limit` | 20 | 1 to 200 |
| `columns` | the first 100, with `_rejection_reason` | At most 100 |
| `max_bytes` | 65536 | |

Output: `total_rows`, `offset`, `columns`, `columns_total`, `columns_truncated`, `rows`,
`row_count`, `truncated`, `limit`, `truncated_cells`, `next_offset` (null on the last page). A run
that rejected nothing returns `path: null` and no rows.

### Resources

Every page of `docs/schemas` is listed (`resources/list`) and read (`resources/read`) as
`forklift://docs/schemas/<file>.md`, `text/markdown`, titled by its first heading; `README.md`
says what each part of a schema does. A wheel carries the pages inside the package; an editable
install serves `docs/schemas` of the checkout.

### `forklift-job.json`

`run_job` writes it into the output directory of every run it starts, failed ones included:

```json
{
  "format": "forklift-mcp-job/1",
  "created_at": "2026-10-11T09:30:00+00:00",
  "input": {"path": "/home/me/data/in/orders.csv", "format": "csv", "bytes": 1840},
  "schema": "schemas/orders.json",
  "result": {"spec_version": 1, "job_id": "mcp-run-20261011T093000Z-3fa2c1", "status": "failed", "...": "..."}
}
```

`result` is a [JobResult](../../contracts/jobresult.schema.json) whose artifact paths are
relative to the output directory. `get_job`, `explain_failure` and `get_bad_rows` read only
files in this format, and refuse one that names files outside its directory.

### Example

An agent asked to "load `in/orders.csv` and tell me what is wrong with it" typically calls:

1. `preview {"input_path": "in/orders.csv"}`: the header and the first rows as text.
2. `generate_schema {"input_path": "in/orders.csv", "output_path": "schemas/orders.json"}`.
3. `resources/read forklift://docs/schemas/X_VALIDATION_DOCUMENTATION.md`, then adds rules.
4. `validate_schema {"input_path": "in/orders.csv", "schema": {...}}` until `valid` is true.
5. `preview {"input_path": "in/orders.csv", "schema": {...}}`: typed rows, and what is rejected.
6. `run_job {"input_path": "in/orders.csv", "output_dir": "out/orders", "schema": {...}}`.
7. If it failed: `explain_failure {"output_dir": "out/orders"}`, for example

```json
{
  "status": "failed",
  "summary": "The run failed with BAD_ROWS_THRESHOLD_EXCEEDED after reading 8 rows. 1 rejected row is kept; most common reasons: VALIDATION_ERROR:amount (1).",
  "error": {
    "code": "BAD_ROWS_THRESHOLD_EXCEEDED",
    "message": "Bad rows (12.5%) exceed threshold (5.0%): 1 of 8 rows that were validated by x-validation were rejected. ...",
    "retryable": false,
    "meaning": "x-validation rejected a larger share of the rows than x-validation.badRowsHandling.maxBadRowsPercent allows, ...",
    "next_steps": ["Look at the rejected rows and their _rejection_reason (get_bad_rows) ...", "..."]
  },
  "rejected": {
    "path": "out/orders/bad_rows.parquet",
    "total_rows": 1,
    "reasons": [{"rule": "VALIDATION_ERROR", "column": "amount", "reason": "VALIDATION_ERROR:amount", "rows": 1}],
    "first_rows": {"columns": ["order_id", "customer", "amount", "ordered_on", "_rejection_reason"],
                   "rows": [["3", "Cy", "-3.25", "2026-01-04", "VALIDATION_ERROR:amount"]], "...": "..."}
  },
  "docs": ["forklift://docs/schemas/X_VALIDATION_DOCUMENTATION.md"]
}
```

8. `get_bad_rows {"output_dir": "out/orders", "offset": 20}` for more rejected rows.

### Troubleshooting

| Symptom | Cause |
|---|---|
| The client says the server exited at once | Run the client's command by hand: `forklift-mcp --root DIR` prints why it cannot start (no SDK, Python < 3.10, a missing root) and exits with 2 |
| "... is outside the directories this server may use" | Add a `--root` for that directory, or use a path inside one; symbolic links count where they point |
| "... is not empty ..." | Choose a new `output_dir`, or pass `overwrite=true` |
| "... runs with --read-only ..." | The server was started with `--read-only`; use `dry_run=true`, or start it without |
| Excel inputs fail with `INTERNAL`: "ModuleNotFoundError: No module named 'openpyxl'" | Install the `excel` extra: `pip install 'forklift-etl[mcp,excel]'` |

## Remote server (Streamable HTTP)

_To be written: the `forklift-mcp` image (`services/mcp`), Streamable HTTP at `/mcp` with a
Forklift API token._
