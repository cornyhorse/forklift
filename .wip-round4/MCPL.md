# MCPL report: `forklift-mcp` over stdio (milestone M2)

Done: an agent connected over stdio can generate a schema for a local file, validate it, apply it
(run the import) and explain a failed run. `tests/unit-tests/test_mcp_stdio.py::
test_m2_generate_validate_apply_and_explain_a_failed_run` drives exactly that flow through the
MCP protocol against a real `forklift-mcp` subprocess (the SDK's own stdio client, initialize
handshake as Claude Code / Claude Desktop use it), then checks the Parquet on disk.

## What I built, and where (all new unless marked)

| Path | What |
|---|---|
| `src/forklift/mcp/__init__.py` | `main()` = the `forklift-mcp` command: `--root DIR` (repeatable, default cwd), `--read-only`, `--log-level`, `--version`. Parses arguments, then checks Python >= 3.10 and that `mcp.server.mcpserver` (SDK 2.x) imports; otherwise prints the reason + `pip install 'forklift-etl[mcp]'` on stderr and returns 2. Refuses a default root of `/` (desktop clients start servers there). Imports neither the SDK nor `server.py` at import time |
| `src/forklift/mcp/__main__.py` | `python -m forklift.mcp` |
| `src/forklift/mcp/server.py` | The only module importing the SDK: `MCPServer` (mcp 2.3.0 API, `Tool.from_function`, `FileResource`, `ToolAnnotations`, `ToolError`). Tool titles/descriptions, per-parameter descriptions and bounds in the input schemas (table `PARAMETERS`), read-only/destructive annotations, instructions (roots, read-only state, the flow, the docs resources), the 17 `docs/schemas` pages as resources, `serve()` = `run("stdio")` |
| `src/forklift/mcp/tools.py` | `Toolbox`: the 8 tools as plain methods (no SDK needed), all through `forklift.jobs.run_job` |
| `src/forklift/mcp/workspace.py` | `Workspace`: root confinement (realpath first), regular-file checks, output dir/file rules, `--read-only`, one writer per output dir; `ToolInputError` |
| `src/forklift/mcp/values.py` | JSON-safe cells (cut at 2000 chars, 100 items), byte/row-bounded row collection, Parquet reads from an offset (skips row groups), `_rejection_reason` counting |
| `src/forklift/mcp/explain.py` | Meaning + next steps + doc pages per error code (all 12 `ERROR_CODES`, test-enforced) and per rejection rule |
| `src/forklift/mcp/docs.py` | Finds the pages: `forklift/mcp/docs/schemas` in a wheel, else `docs/schemas` of the checkout |
| `src/forklift/mcp/forklift.mcp.readme.md` | Package readme (like the other packages) |
| `pyproject.toml` (modified, only the agreed parts) | `mcp = ["mcp>=2.3,<3; python_version >= '3.10'"]`, the same line appended to `all`, script `forklift-mcp = "forklift.mcp:main"`, `[tool.hatch.build.targets.wheel.force-include] "docs/schemas" = "forklift/mcp/docs/schemas"` |
| `tests/unit-tests/test_mcp_{tools,workspace,values,docs,entry,packaging}.py` | Run without the SDK (the tools are plain methods); entry tests that need the SDK are marked |
| `tests/unit-tests/test_mcp_server.py` | In process through the SDK's in-memory client/server streams (`Client(server, mode="legacy")`); skips without the SDK |
| `tests/unit-tests/test_mcp_stdio.py` | Real `forklift-mcp` subprocesses over stdio: the M2 flow + failed run explained + doc resource followed; read-only server; automatic protocol negotiation (`server/discover`); a server that cannot start; skips without the SDK |
| `docs/platform/mcp.md` | Overview of the two servers, full "Local server (stdio)" section, "Remote server (Streamable HTTP)" heading with a one-line placeholder for you to fill from MCPR |

### The tools (local meanings; MCPR should mirror names, result keys and caps)

| Tool | Inputs (defaults) | Result (main keys) | Caps |
|---|---|---|---|
| `get_schema` | `path`, `max_bytes` (65536) | `path`, `bytes`, `summary` {title, column_count, columns[{name,type,required,format?,extensions?,constraints?}], columns_truncated, required, extensions}, `document` or `document_text`+`truncated`, `document_bytes` | 1024..1 MiB document, 8 MiB file, 500 columns listed |
| `generate_schema` | `input_path`, `format`, `input_options`, `sample_rows` (1000), `infer_primary_key`, `output_path`, `overwrite`, `dry_run`, `max_bytes` | `status`, `error`, `input`, `rows_analyzed`, `warnings`, `summary`, document, `output_path`, `written`, `dry_run` | sample_rows 1..100000 |
| `validate_schema` | `input_path` (CSV), `schema` or `schema_path`, `format`, `input_options`, `sample_rows`, `apply_schema_extensions` | `valid`, `status`, `error`, `columns`, `schema_columns`, `required_columns`, `columns_not_in_schema`, `schema_columns_not_in_input`, `sample_rows`, `counts`, `schema_extensions`, `validation_summary`(+`_total`), `warnings`(+`_total`), `rejected_rows` (sample failed its threshold: the first rejected rows in `get_bad_rows`' shape + `reasons`; else null), `notes` | 500 names, 100 findings, 50 warnings, 10 rejected rows / 65536 bytes / 100 columns / 20 reasons |
| `preview` | `input_path`, `rows` (20), `schema`/`schema_path` (typed when given), `format`, `input_options`, `max_bytes` | raw: `columns`, `rows`, `row_count`, `truncated`, `limit` (rows/bytes), `truncated_cells`, `sheet`; typed: + `columns[{name,type}]`, `columns_total`, `sample` {rows_read, rejected_rows, rejection_reasons}, `notes` | rows 1..500, 1024..1 MiB, 100 columns, 2000 chars/cell |
| `run_job` | `input_path`, `output_dir`, `schema`/`schema_path`, `format`, `input_options`, `options`, `limits`, `job_id`, `overwrite`, `dry_run` | as `get_job` + `next`; dry run: `would_run`, `writable`, `spec` (no schema) | - |
| `get_job` | `output_dir` (or its `forklift-job.json`) | `job_id`, `status`, `error`, `output_dir`, `result_file`, `input`, `schema`, `created_at`, `counts`, `schema_extensions`, `validation_summary`, `warnings`, `artifacts[{kind,path,rows,bytes,exists}]` | 100 findings, 50 warnings, 50 artifacts |
| `explain_failure` | `output_dir`, `rows` (10), `max_bytes` | `summary`, `error` {code,message,retryable,meaning,next_steps}, `counts`, `findings[{rule,column,key,count}]`, `rejected` {path,total_rows,reasons[{rule,column,reason,rows,meaning?}],first_rows{...}}, `warnings`, `docs` (resource URIs) | rows 1..50, 20 reasons over <= 5M rows |
| `get_bad_rows` | `output_dir`, `offset` (0), `limit` (20), `columns`, `max_bytes` | `total_rows`, `offset`, `columns`, `columns_total`, `columns_truncated`, `rows`, `row_count`, `truncated`, `limit`, `next_offset` | limit 1..200, 100 columns |

Every result has `caps` naming the bounds that applied. Refusals are MCP tool errors
(`is_error`, "Error executing tool X: <reason>"); jobs that ran and failed are results with
`status: failed` and the engine's `error`. Crashes show only "Error executing tool X".

## Decisions and why

- **Interactive tools copy the start of the input into a private temp dir.** `run_job(spec,
  base_dir=...)` writes its artifact (`preview.json`, `report.json`, `schema.json`) and its
  `.forklift-job-*` scratch inside `base_dir`, and inputs must be inside `base_dir` (realpath,
  so no symlink tricks). Using a root as `base_dir` would write into the user's tree, which
  breaks `--read-only` and read-only mounts. So `preview`/`validate_schema`/`generate_schema`
  copy the first records of a CSV file (counted with Python's csv module using the given
  delimiter/quote/escape/encoding: header_search_rows + n + 1 records; bytes copied as-is if the
  copy cannot be decoded/parsed, so the engine reports the encoding error itself) or a whole
  workbook (<= 256 MiB) into `tempfile.mkdtemp()` (0700) and run the job there; the dir is removed
  after the call. Results say `input.sampled` when the job saw only a prefix. Engine messages that
  name the copy are rewritten to the caller's path; `.forklift-job-*/` becomes
  `<temporary directory>/`.
- **Typed preview = the `run` kind on that copy**, then the first rows of `data.parquet` and the
  `_rejection_reason` counts of `bad_rows.parquet`. x-validation's threshold is switched off for
  the preview only (`failOnExceedThreshold: false` in a copy of the schema; said in `notes`):
  a share of rejected rows is meaningless on 20 rows.
- **`run_job` runs in place** with the root that holds both input and output dir as `base_dir`
  (the deepest such root; different roots are refused with a clear message). It writes
  `forklift-job.json` = `{"format": "forklift-mcp-job/1", "created_at", "input": {path (absolute),
  format, bytes}, "schema": <path or "inline">, "result": <JobResult, artifact paths relative to
  the output dir>}` atomically, also for failed runs. Readers validate it with
  `JobResult.from_dict` and refuse artifact paths that resolve outside the directory. If it
  cannot be written, the run's result is still returned with `result_file_error`.
- **Overwrite** never deletes anything but the files the previous `forklift-job.json` in that
  directory listed (inside it) and that file; other files stay. A non-empty dir without
  `overwrite` is refused, also in a dry run (dry run = every check, no write).
- **`SPEC_INVALID` from the engine is a tool error** (the caller's argument is wrong, e.g. an
  unknown encoding), nothing is written; all other failures are results. Arguments are
  pre-checked with `JobSpec.from_dict`/`InputOptions.from_dict` so errors name the field.
- **No cancellation/progress** of a running `run_job`: tools are sync functions the SDK runs in a
  worker thread; cooperative cancel would need `anyio.from_thread` in src (anyio is not a
  declared dependency, and the "every import is declared" test would need it added). A
  cancelled call lets the run finish and its result lands in the output dir; documented, with
  `limits.max_seconds` as the bound.
- **Return annotation `dict[str, Any]`** (not `typing.Dict`): with the SDK, `typing.Dict` output is
  wrapped as `{"result": ...}`; the builtin generic gives plain structured content. Files keep
  `from __future__ import annotations`, so nothing is evaluated on 3.8.
- **Editable installs** also get a copy of the pages in `site-packages/forklift/mcp/docs/schemas`
  (hatch applies force-include to editable wheels); it is unused (the package resolves to
  `src/`), and the server serves the checkout's `docs/schemas` instead. Harmless.

## Follow-up after the lead's engine fixes (round 2)

The lead fixed both engine findings below (`runner._keep_sample_bad_rows` now moves a failed
validate sample's `bad_rows.parquet` to the artifact directory and lists it; `check_schema_shape`
gives `SCHEMA_INVALID`). That made `validate_schema`'s message name `out/bad_rows.parquet`
inside my private copy, which the caller cannot open. Changed in `src/forklift/mcp/tools.py`:

- `validate_schema` reads the sample's `bad_rows` artifact before the copy is removed and
  returns `rejected_rows`: the first 10 rejected rows within 65536 bytes (100 columns, 2000
  characters per cell), in the shape `get_bad_rows` returns (`total_rows`, `offset`, `columns`,
  `columns_total`, `columns_truncated`, `rows`, `row_count`, `truncated`, `limit`,
  `truncated_cells`; no `next_offset`, the file is not kept), plus `reasons` (the 20 most frequent
  `_rejection_reason`s); null when the sample kept no bad rows. The caps are in `caps`.
- Its messages (error and warnings) replace the artifact's path with "rejected_rows of this
  result (the first of them; run_job keeps the file)", e.g. "All rows rejected by the checks are
  in rejected_rows of this result (the first of them; run_job keeps the file) (the
  _rejection_reason column says why)." `_VALIDATE_NOTE` and `docs/platform/mcp.md`
  (validate_schema output and caps) say so.
- Tests: `test_a_schema_that_rejects_too_many_rows` asserts the rows, reasons, caps and message;
  new `test_rejected_rows_of_the_sample_are_bounded`; the passing case asserts
  `rejected_rows is None`.
- Verified: MCP tests 188 passed (with stdio), **100% line+branch of `src/forklift/mcp`** (919
  statements, 284 branches); `$S/mcpl-venv` `test_jobs_run_job.py` + `test_mcp_*.py`: 285 passed;
  `$S/venv/bin/python $S/run_tree.py tests/unit-tests/test_jobs_run_job.py
  tests/unit-tests/test_mcp_*.py` (run_tree always runs all of `tests/`): 6362 passed, 316
  skipped, 2 failed, the same two as before and neither in my area: `test_extras` (patch below)
  and `test_only_expected_jobs_have_their_own_permissions` (HELM's `helm.yml`). Log:
  `$S/mcpl/lead_run.log`.

## Engine findings (not my area; for you) — both fixed by the lead since

1. **`validate_schema` threshold failures list a dangling artifact.** On
   `BAD_ROWS_THRESHOLD_EXCEEDED`, `runner._validate_schema` re-raises, `_failed` appends the kept
   `bad_rows` (`.forklift-job-XXXX/validate-out/bad_rows.parquet`, with sha256) to the result's
   artifacts, and then `run()`'s `finally: self._remove_scratch()` deletes that directory. The
   result (and the message) names a file that no longer exists. The worker uploads listed
   artifacts; worth checking how it handles a missing one for validate jobs. Reproduce:
   `run_job({... "kind": "validate_schema", schema with x-validation maxBadRowsPercent 10 ...})`
   on a CSV with 2 bad rows of 4. (The MCP tool hides it: it does not expose validate artifacts
   and rewrites the path to `<temporary directory>/`.)
2. **A schema whose `properties` is not an object** (`{"properties": "x"}`) fails a run or a
   validate_schema with `INTERNAL` `AttributeError: 'str' object has no attribute 'items'`
   instead of `SCHEMA_INVALID`.

## Could not do / verify (plainly)

- No real Claude Code or Claude Desktop session (no GUI/CLI login here); verified with the MCP
  SDK's stdio client in legacy (initialize) and auto (`server/discover`) modes, which is the
  protocol those hosts speak. The `claude mcp add` / `claude_desktop_config.json` snippets in the
  doc follow those products' documented formats but were not executed.
- Not run on Windows or macOS, nor on Python 3.10 (ran 3.11, 3.12, 3.13, 3.14).
- The CHANGELOG, design doc, index, README and USAGE texts below are not applied (your files).
- `tests/unit-tests/test_wp6_review_fixes.py::TestPackagingMetadata::test_extras` FAILS with my
  `pyproject.toml` change until you apply the patch below (it pins the set of extras).

## Exact text for the lead's files

### `tests/unit-tests/test_wp6_review_fixes.py` (required, else `test_extras` fails)

Verified: the patched test (and `test_every_third_party_import_in_src_is_declared`) pass
against the new `pyproject.toml`.

```diff
--- a/tests/unit-tests/test_wp6_review_fixes.py
+++ b/tests/unit-tests/test_wp6_review_fixes.py
@@ -1218,16 +1218,28 @@
 
     def test_extras(self, pyproject):
         extras = pyproject["project"]["optional-dependencies"]
-        assert set(extras) == {"excel", "sql", "pandas", "polars", "clipboard", "all", "dev"}
+        assert set(extras) == {
+            "excel",
+            "sql",
+            "pandas",
+            "polars",
+            "clipboard",
+            "mcp",
+            "all",
+            "dev",
+        }
         assert _requirement_names(extras["excel"]) == {"openpyxl", "xlrd"}
         assert _requirement_names(extras["sql"]) == {"pyodbc"}
         assert _requirement_names(extras["clipboard"]) == {"pyperclip"}
+        # forklift-mcp: the MCP Python SDK 2.x, only where it installs (Python 3.10+)
+        assert extras["mcp"] == ["mcp>=2.3,<3; python_version >= '3.10'"]
         assert _requirement_names(extras["all"]) == (
             _requirement_names(extras["excel"])
             | _requirement_names(extras["sql"])
             | _requirement_names(extras["pandas"])
             | _requirement_names(extras["polars"])
             | _requirement_names(extras["clipboard"])
+            | _requirement_names(extras["mcp"])
         )
         assert "pyspark" not in " ".join(sum(extras.values(), []))  # nothing imports it
         assert {"pytest", "pytest-cov", "black", "isort", "flake8", "mypy", "pre-commit"} <= (
```

(The same diff is in `$S/mcpl/test_extras.diff`.)

### `CHANGELOG.md`, under `## [Unreleased]` / `### Added`

```markdown
- **`forklift-mcp`, the local MCP server (stdio; milestone M2).** An agent on a machine that has
  the data (Claude Code, Claude Desktop, any MCP client) can preview a local CSV or Excel file,
  generate a schema and write it, validate it, apply it and explain a failed run (user guide:
  `docs/platform/mcp.md`). Install with `pip install 'forklift-etl[mcp]'` (the MCP Python SDK
  2.x, so Python 3.10+; also part of `all`) and register `forklift-mcp --root DIR` with the
  client. Tools `get_schema`, `generate_schema`, `validate_schema`, `preview` (as text, or typed
  by a schema with the rows it would reject counted by reason), `run_job`, `get_job`,
  `explain_failure` (the error with what it means and what to try, findings by rule, rejection
  reasons counted over `bad_rows.parquet`, the first rejected rows and the documentation pages
  that apply) and `get_bad_rows`: the names and meanings of the remote server's, with paths
  where it takes ids. The `docs/schemas` pages are resources (`forklift://docs/schemas/<file>.md`;
  the wheel carries them). Every tool runs the engine through `forklift.run_job`, and `run_job`
  writes the result (`forklift-job.json`) next to the outputs for the tools that read a run
  later. Results are structured JSON whose rows, bytes, lists and cells are bounded, with the caps
  stated in each result; a refused call is a tool error that says why, a failed job is a result
  with the engine's error code. Every path must be inside a `--root` directory (symbolic links are
  resolved first) and only regular files are read; `--read-only` refuses every write (dry runs
  still work); an existing non-empty output directory or an existing file is replaced only with
  `overwrite=true`; `preview`, `validate_schema` and `generate_schema` work on a private temporary
  copy of the input's first records and write nothing inside the roots. Messages never contain
  cell values. Importing `forklift` never imports the SDK: without it, or on Python older than
  3.10, `forklift-mcp` says what to install and exits with 2.
```

### `docs/design/platform.md`

§9.1, append after the paragraph that ends "...supports a dry run where it would change
something.":

```markdown
As built (M2, the local server): `src/forklift/mcp`, the command `forklift-mcp` of the `mcp`
extra of `forklift-etl` (MCP Python SDK 2.x, so Python 3.10+; importing `forklift` never
imports it). It has every tool except `list_connections` and `list_datasets`, takes paths where
the remote server takes ids, and runs each through `run_job`: `preview`, `validate_schema` and
`generate_schema` on a private temporary copy of the input's first records (a workbook whole),
so they write nothing where the data is; `run_job` in place, writing `forklift-job.json` (the
`JobResult`) next to the outputs, which `get_job`, `explain_failure` and `get_bad_rows` read.
Paths are confined to the `--root` directories (symbolic links resolved first) and to regular
files, `--read-only` refuses every write, and outputs never replace files unless the call says
`overwrite`. User guide: `docs/platform/mcp.md`.
```

§11, the layout block's first line:

```
pyproject.toml, src/forklift/, tests/   engine + CLI + local MCP server (forklift-mcp, extra mcp) → PyPI forklift-etl (unchanged), tags v*
```

§13, the M2 row:

```markdown
| M2 | `forklift-mcp` (stdio) | An agent can generate, validate and apply a schema to a local file and explain a failed run. Built: an end-to-end test drives that flow through a real `forklift-mcp` process over stdio |
```

### `docs/DOCUMENTATION_INDEX.md`

Under `## Platform (platform/)`:

```markdown
- [mcp.md](platform/mcp.md): MCP for agents: the local server `forklift-mcp` (stdio; install, Claude Code and Claude Desktop configuration, every tool with its inputs, outputs and caps, the documentation resources, `--root` and `--read-only`) and the remote server
```

In the "Package readmes" table, after the `jobs` row:

```markdown
| `mcp` (`forklift-mcp`, the local MCP server) | [forklift.mcp.readme.md](../src/forklift/mcp/forklift.mcp.readme.md) |
```

### `README.md`

In "Optional Dependencies (extras)", before `# Several at once, or everything`:

```bash
# forklift-mcp, the local MCP server for agents (Python 3.10+)
pip install "forklift-etl[mcp]"
```

In "CLI Usage", a new subsection after "Schema Generation":

````markdown
### MCP Server for Agents

```bash
# Let Claude Code (or any MCP client) preview, check and import the files in ./data
claude mcp add forklift -- forklift-mcp --root "$PWD/data"
```

`forklift-mcp` (extra `mcp`, Python 3.10+) is started by the MCP client over stdio. Its tools
preview a file, generate and validate a schema, run the import and explain a failed run, on files
inside the `--root` directories only (`--read-only` refuses every write). See
[docs/platform/mcp.md](docs/platform/mcp.md).
````

### `docs/guides/USAGE.md` (where the CLI is documented: end of `## Command Line`)

````markdown
### MCP Server for Agents

`forklift-mcp` (install the `mcp` extra; Python 3.10+) is the local MCP server: an MCP client
such as Claude Code or Claude Desktop starts it over stdio, and an agent can then preview a
file, generate and validate a schema, run the import and explain a failed run, with the same
tools as the service's remote MCP server.

```bash
pip install "forklift-etl[mcp]"
claude mcp add forklift -- forklift-mcp --root "$PWD/data"      # Claude Code
forklift-mcp --root ./data --read-only                          # what the client runs
```

Every path must be inside a `--root` directory, `--read-only` refuses every write, and outputs
never replace files unless the agent asks. Exit code 2 when it cannot start (no SDK, Python older
than 3.10, a missing root). See [MCP](../platform/mcp.md) for the configuration of each client and
every tool.
````

### CI (`.github/workflows/test.yml`)

Nothing is required: the `test` job installs `.[all,dev]` on 3.12/3.13/3.14, which now includes
the SDK, so every MCP test (in-process and stdio) runs and the coverage gate covers
`src/forklift/mcp` (100%). `minimum-versions` (pyarrow 16.1.0, numpy<2) also installs it; I ran
the MCP tests against pyarrow 16.1.0 + numpy 1.26.4 on 3.12: 187 passed.

Recommended: in the `core-install` job (installs the wheel without extras), after "Import the
package from outside the source tree", so the missing-SDK path and the wheel's pages are checked
on a real non-editable install:

```yaml
    - name: forklift-mcp without its extra, and the pages the wheel carries
      working-directory: ${{ runner.temp }}
      run: |
        python -c "import sys, forklift.mcp; assert 'mcp' not in sys.modules"
        forklift-mcp --version
        set +e; forklift-mcp --root . 2> err.txt; code=$?; set -e
        cat err.txt
        test "$code" = 2
        grep -q "pip install 'forklift-etl\[mcp\]'" err.txt
        python -c "from forklift.mcp import docs; assert docs.docs_directory() == docs.PACKAGED_DOCS; assert len(docs.schema_pages()) >= 17"
```

## How I verified (commands and results)

`S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad`. Own venv
`$S/mcpl-venv` (Python 3.13, `pip install -e '.[all,dev]' mcp==2.3.0`).

- MCP tests with branch coverage of `forklift.mcp` (`$S/mcpl/cov_mcp.py`, which is pytest with
  `--cov=forklift.mcp --cov-branch`): in-process only: 183 passed, **100% line+branch** (902
  statements, 276 branches); with the stdio tests as well: 187 passed. (After round 2: 188
  passed, 100%, 919 statements, 284 branches.)
- Same on Python 3.14 (`$S/mcpl-py3.14`), 3.12 (`$S/mcpl-py3.12`, also with pyarrow 16.1.0) and
  3.11: all passed, 100%.
- Whole engine suite in `$S/mcpl-venv` with `--cov=src/forklift/ --cov-branch` (as CI):
  6367 passed, 309 skipped, 2 failed; coverage **100% line+branch of all of `src/forklift`**
  (17679 statements, 7050 branches). The 2 failures are not caused by MCP code:
  `test_wp6_review_fixes.py::TestPackagingMetadata::test_extras` (needs the patch above) and
  `TestWorkflows::test_only_expected_jobs_have_their_own_permissions` (HELM's new
  `.github/workflows/helm.yml` has a `publish` job with `packages: write`; that test's allow-list
  needs it, which is for you/HELM). Log: `$S/mcpl/full_cov2.log`.
- Engine suite without the SDK (`$S/venv/bin/python $S/run_tree.py`, Python 3.13, no `mcp`):
  6345 passed, 316 skipped, the same 2 failures; the SDK tests skip cleanly
  ("needs the MCP Python SDK: pip install 'forklift-etl[mcp]'": `test_mcp_server.py`,
  `test_mcp_stdio.py`, 4 entry tests), every SDK-free MCP test (tools, workspace, values, docs,
  missing-SDK path) runs and passes. Log: `$S/mcpl/tree_nosdk.log`.
- Wheel: `python -m build --sdist --wheel` (wheel built from the sdist): the wheel has
  `forklift/mcp/docs/schemas/*.md` (17 pages), `forklift-mcp = forklift.mcp:main`, and
  `Requires-Dist: mcp<3,>=2.3; (python_version >= '3.10') and extra == 'mcp'` (also for `all`).
  Installed into a clean venv with `[mcp]` and driven from `/tmp` over stdio: 17 resources from
  `site-packages/forklift/mcp/docs/schemas`, a page read, a `run_job` that wrote its outputs and
  `forklift-job.json`. Installed into a clean venv without the extra: `forklift-mcp --root /tmp`
  prints "forklift-mcp needs the MCP Python SDK (the 'mcp' package, version 2), which cannot be
  imported here (No module named 'mcp'). Install it with: pip install 'forklift-etl[mcp]'" and
  exits 2; `import forklift, forklift.mcp` loads no `mcp` module.
- Style: `black --line-length 99 --check src/ tests/` clean; `isort --profile black
  --line-length 99 --check-only` clean for my files; `flake8 --max-line-length=99
  --extend-ignore=E203,W503 src/forklift/mcp tests/unit-tests/test_mcp_*.py` clean.

## Clean-up

Deleted my extra venvs at the end (`$S/mcpl-py3.11`, `$S/mcpl-py3.12`, `$S/mcpl-py3.14`,
`$S/mcpl-clean`, `$S/mcpl-bare`); kept `$S/mcpl-venv` (the SDK venv for the coverage run) and
`$S/mcpl/` (probes, logs, the diff). No Docker containers or Compose stacks were used.
