# Forklift Jobs Package

`forklift.jobs` runs the engine from a declarative **job spec** and describes the outcome as a
**job result**: the job contract of the platform design (`docs/design/platform.md` §4,
ADR 0002). The same spec runs from Python (`forklift.run_job`), from the command line
(`forklift run-job`) and in the service's workers, which start exactly that command.

The contract is defined once, here, as plain dataclasses. Their JSON Schemas are generated into
`contracts/jobspec.schema.json` and `contracts/jobresult.schema.json` at the root of the
repository; those files are normative for every other component (the gateway validates specs and
results with them and never imports the engine).

## Modules

| Module | What it holds |
|---|---|
| `spec.py` | `JobSpec` and its parts: `InputSpec`, `InputOptions`, `FooterDetection`, `OutputSpec`, `JobOptions`, `Limits`, the locations |
| `result.py` | `JobResult`, `Artifact`, `JobError` |
| `runner.py` | `run_job()`: runs a spec with the engine |
| `http_input.py` | `PresignedUrlSource`: a presigned URL read as a stream (with Range requests), replaced by a fresh one when it expires |
| `pipe.py` | `JobPipe`: the JSON lines `forklift run-job` writes on stdout and the answers it reads on stdin |
| `errors.py` | `classify_error()`: an exception as an error code, a clean message and `retryable` |
| `contract.py` | Generates the JSON Schemas; `python -m forklift.jobs.contract [--check] [DIR]` |
| `_model.py` | The small field system the dataclasses, their validation and the schemas share |

## Running a job

```python
from forklift import run_job

result = run_job(
    {
        "spec_version": 1,
        "job_id": "people-2026-10-10",
        "kind": "run",
        "input": {
            "format": "csv",
            "location": {"type": "file", "path": "in/people.csv"},
            "options": {"delimiter": ";"},
        },
        "schema": {"properties": {"id": {"type": "integer"}}, "required": ["id"]},
        "output": {"location": {"type": "file", "path": "out/"}},
    },
    base_dir="/scratch/job-17",
    progress=print,
)
print(result.status, result.counts, [a.path for a in result.artifacts])
```

`run_job(spec, *, base_dir, allowed_url_hosts=(), progress=None, cancel=None, s3_client=None,
refresh_input_url=None)`

- `spec`: a `JobSpec` or its JSON form as a dict. An invalid dict gives a `failed` result with
  code `SPEC_INVALID` whose message lists every problem with its field
  (`input.location.path: required field is missing (...)`).
- `base_dir`: every `file` location is relative to it, and no `file` location may lead out of it
  (not even through a symbolic link). The inline schema is written into a private directory
  inside it (`.forklift-job-*`, removed afterwards); schemas are never read from caller paths.
- `allowed_url_hosts`: hosts (or `host:port`) a `presigned_url` input may point at. Empty: no
  presigned URL is accepted. The worker passes the object store's host.
- `progress(event)`: called at every batch boundary with
  `{"rows_read", "rows_rejected", "bytes_read"}` (plus `"rows_written"` while a `sql_table`
  output is loaded).
- `cancel()`: asked after every batch; `True` stops the job at that point with
  `status: "cancelled"` and code `CANCELLED`.
- `s3_client`: a `forklift.io.S3StreamingClient` for `s3` locations (library use; default: boto3's
  credential chain).
- `refresh_input_url()`: returns a fresh presigned URL for the `presigned_url` input (the same
  object), when its URL is about to expire or the store refused it (see below). Without it the
  URL is used as it is until the store refuses it.

A job never raises for its own failures: the result says what happened. `run_job` raises only
for wrong arguments (`ValueError` when `base_dir` is not a directory, `TypeError` for a callback
that is not callable).

## The spec (`JobSpec`, `spec_version` 1)

| Field | Type | Notes |
|---|---|---|
| `spec_version` | `1` | Required in documents |
| `job_id` | string | 1-128 letters, digits, `.`, `_`, `-`; names the staging table of a `sql_table` load |
| `kind` | `run` \| `preview` \| `validate_schema` \| `generate_schema` | |
| `input` | `{format, location, options}` | `format`: `csv` \| `excel` \| `fwf` \| `sql` |
| `schema` | object or null | Inline forklift schema; required for `validate_schema` and `sql` inputs |
| `output` | `{location, compression, artifacts}` or null | Required for `run` |
| `options` | `JobOptions` | `apply_schema_extensions` (true), `batch_size` (10000), `include_value_statistics` (false), `preview_rows` (100), `preview_max_bytes` (1 MiB), `sample_rows` (1000), `infer_primary_key` (false) |
| `limits` | `{max_input_bytes, max_seconds, max_rows}` | Each optional; exceeding one fails the job with `LIMIT_EXCEEDED` |

### Locations (`type` key)

| `type` | Fields | Use |
|---|---|---|
| `file` | `path` | Relative to `base_dir` (`/` separated, no `..`, no leading `/`); a directory for outputs |
| `s3` | `uri` | Library use: read and written with the caller's credentials |
| `presigned_url` | `url`, `size`, `etag` | Input only, CSV only, host must be allowed; streamed (see below) |
| `sql` | `connection_string` | SQL input; the tables come from the schema's `x-sql` |
| `sql_table` | `connection_string`, `table`, `schema_name`, `mode` (`append`), `key_columns`, `staging` (`table`) | Output: the validated rows are loaded with `forklift.outputs.sql.write_table`. `mode`: `create`, `append`, `replace`, `upsert` (needs `key_columns`); `staging`: `table` or `none` |

For a `sql_table` output the data and `bad_rows` Parquet files are still written, to
`output.artifacts` (a `file` location, default `out/`), and stay job artifacts. The import runs
first; a threshold failure therefore loads nothing and keeps `bad_rows.parquet`. A load needs
exactly one data file (one sheet, one `x-sql` table).

Connection strings and presigned URLs are secrets: `repr()` hides them, and they never appear in
results, logs or messages (messages are scrubbed even when an underlying library repeats them).

### Input options

`input.options` holds the reader options, all optional (`null`/absent keeps the engine's
default). CSV: `encoding`, `delimiter`, `quote_char`, `escape_char`, `header_mode`,
`header_search_rows`, `skip_blank_lines`, `comment_rows`, `footer_detection`
(`{stop_on_blank, column_index, patterns}`), `excess_column_mode`. Excel: `sheet` (name or
0-based index), `values_only`, `engine`, `date_system`. SQL: `query_timeout`,
`connection_timeout`, `schema_name`, `null_values`, `enable_streaming`. An option for another
format, or a job option for another kind, is ignored with a warning in `result.warnings`.

## Kinds

| Kind | Formats | What it does | Artifact |
|---|---|---|---|
| `run` | csv, excel, sql | The full import (`import_csv` / `import_excel` / `import_sql`), then the table load for `sql_table` outputs | the import's files (below) |
| `preview` | csv, excel | The first `preview_rows` rows as text, as the import would see them (header detection, comment rows, footer); `preview.json` is at most `preview_max_bytes` bytes, cells longer than 2000 characters are cut | `preview.json`: `columns`, `rows`, `row_count`, `truncated`, `limit` (`rows`/`bytes`/null), `truncated_cells` (and `sheet` for Excel) |
| `validate_schema` | csv | The engine's pre-write checks (schema, header, required columns, extension configuration) and a real import of the header plus `sample_rows` rows into scratch | `report.json`: `valid`, `columns`, `schema_columns`, `required_columns`, `columns_not_in_schema`, `schema_columns_not_in_input`, `sample_rows`, `counts`, `schema_extensions`, `validation_summary`, `warnings`, `error` |
| `generate_schema` | csv, excel | `SchemaGenerator` on the first `sample_rows` rows (no cell values unless `include_value_statistics`) | `schema.json` |

`fwf` inputs fail with `SPEC_INVALID`: fixed-width import is not implemented
(`forklift.import_fwf` raises `NotImplementedError`). The interactive kinds write their artifact
to `output.location` (a `file` directory) or `out/`. A `validate_schema` job whose schema fails a
check is `failed` with that check's code, and still lists `report.json`.

## The result (`JobResult`)

| Field | Notes |
|---|---|
| `spec_version`, `job_id` | `job_id` is null only when the spec could not be read |
| `status` | `succeeded` \| `failed` \| `cancelled` |
| `counts` | `total_rows`, `valid_rows`, `invalid_rows`, `truncated_rows`; `rows_written` for `sql_table`; previews and generated schemas report `total_rows` only; a failed run reports what it had read |
| `schema_extensions`, `validation_summary`, `warnings` | As in `ProcessingResults`; never cell values |
| `artifacts` | `[{kind, path, rows, bytes, sha256}]`, data files first and the manifest last. `kind`: `data`, `bad_rows`, `manifest`, `metadata`, `preview`, `schema`, `report`. `path` is relative to `base_dir` (or the `s3://` URI of an object written to an `s3` output, which has no `bytes`/`sha256`) |
| `error` | `{code, message, retryable}`; null exactly when `status` is `succeeded` |

### Error codes

| Code | When |
|---|---|
| `SPEC_INVALID` | The spec does not match the contract, or names something that cannot work (unknown encoding, a path out of `base_dir`, a host that is not allowed, `fwf`) |
| `SCHEMA_INVALID` | The schema cannot be read or one of its extensions is configured wrongly |
| `INPUT_UNREADABLE` | Missing file or object, no header, malformed rows, a dropped stream that could not be resumed, a database that cannot be read |
| `ENCODING_ERROR` | Bytes that are not valid for the configured encoding |
| `COLUMN_MISSING` | A column the schema needs is not in the input |
| `BAD_ROWS_THRESHOLD_EXCEEDED` | `x-validation` rejected too many rows; the kept `bad_rows` artifact is listed |
| `CONSTRAINT_VIOLATION` | Constraints with `errorMode` `fail_fast` / `fail_complete` were violated |
| `LIMIT_EXCEEDED` | A `limits` value was exceeded (a known input size is checked before anything is read) |
| `PERMISSION_DENIED` | The store, the file system or the database refused access (an expired presigned URL is HTTP 403) |
| `TARGET_WRITE_FAILED` | Loading the `sql_table` output failed (the Parquet artifacts are kept) |
| `CANCELLED` | `cancel()` returned True (SIGTERM for `forklift run-job`) |
| `INTERNAL` | Anything else; the message starts with the exception type |

The engine marks the errors it understands with an `error_code` attribute
(`forklift.engine.exceptions`); `classify_error()` falls back to the exception type. Messages are
the engine's own, cleaned: Arrow messages lose the row content they quote, database errors are
described by SQLSTATE (never the driver's text), secrets are removed and paths inside `base_dir`
are shown relative to it. `retryable` is true for transient trouble (a stream that kept dropping,
connection errors, throttling, database deadlocks and lost connections).

## Streamed inputs (`presigned_url`)

A `presigned_url` input is never copied (ADR 0006): `PresignedUrlSource` reads it as one
forward-only HTTP stream (`Range: bytes=0-`) and, when the connection drops, resumes with
`Range: bytes=<offset>-` and `If-Match: <etag>` (up to 5 attempts in a row without progress,
waiting 0.5 s, 1 s, 2 s, ...). Header detection reads the start in 64 KiB range requests. A row
reader that has to start again (rows with too many or too few fields) opens a second stream.
Footer detection works without a copy: the row reader stops at the footer row.

Only http(s) URLs without user information, on an allowed host, are contacted; a redirect to
another host is refused. The object must not change during the job (`If-Match`, and the size
from `Content-Range` must match `size` when it is given). Proxy settings of the environment
(`HTTPS_PROXY`, `NO_PROXY`) are honoured. The URL never appears in output files: the input is
called by its URL without the query string there.

### Fresh URLs

A presigned URL stops working when it expires, and earlier when the credentials it was signed
with were temporary ones that have ended (an IAM role session lasts 1 to 12 hours, whatever
`X-Amz-Expires` says). With `refresh_input_url`:

- **Before a request** (the first one, a resume after a dropped connection, a range read of the
  header), a SigV4 URL that expires within 5 minutes, or has used up half its lifetime if that
  is shorter, is replaced (`X-Amz-Date` plus `X-Amz-Expires` say when it expires; a URL without
  them is never replaced in advance). If no fresh URL can be had, the current one is used and a
  warning is logged.
- **After a refusal**: a request answered with HTTP 403 (an expired or invalid signature), or
  with HTTP 400 `ExpiredToken` (S3's answer once temporary credentials have ended), is sent
  once more with a fresh URL. If there is none, the job fails with `PERMISSION_DENIED`: "The
  input URL expired and a fresh one could not be obtained: ...".

A request gets at most one fresh URL, and an input at most 100. A fresh URL must name the same
object: the same scheme, host, port and path, and no user information; anything else fails the
job (`INPUT_UNREADABLE`) without being contacted. The ETag sent as `If-Match` keeps guaranteeing
that the bytes did not change. Fresh URLs are secrets like the first one: they never appear in
messages or results.

## Command line

```
forklift run-job SPEC.json --base-dir DIR --result RESULT.json [--allow-url-host HOST]...
                 [--progress-jsonl] [--input-url-requests [--input-url-timeout SECONDS]]
```

Exit code 0: succeeded; 1: failed or cancelled; 2: invalid spec (including a spec file that
cannot be read and a `--base-dir` that is not a directory). The result is written atomically to
`--result` in every case. Logs go to stderr; stdout carries JSON lines only (below). SIGTERM
cancels the job: it stops at the next batch boundary and the result says `cancelled`.

stdout and stdin carry a line protocol (`pipe.py`), one JSON object per line:

- `--progress-jsonl`: every progress event, `{"rows_read": 30, "rows_rejected": 1,
  "bytes_read": 4096}` (no `type` key).
- `--input-url-requests`: when the `presigned_url` input needs a fresh URL (above), the engine
  writes `{"type": "input_url"}` and reads exactly one line from stdin: `{"url": "https://..."}`
  or `{"error": "why there is none"}`, within `--input-url-timeout` seconds (default 120).
  Requests are sent one at a time. An answer that does not come in time, or is neither of those
  objects (or longer than 64 KiB), ends the conversation: a late line could not be matched to
  its request, so every later request fails at once. Without the flag stdin is never read.

The worker's supervisor starts the engine with `--input-url-requests` for streamed inputs and
answers each request from the gateway's `POST /internal/v1/jobs/{id}/input-url`.

The engine writes temporary files (S3 upload spools, the filtered copy that footer detection
makes of a local CSV) in the system temporary directory; a sandboxed engine process should get
`TMPDIR` inside its scratch directory.

## Changing the contract

Edit the dataclasses in `spec.py` / `result.py` (fields are declared with `contract_field`, whose
keyword arguments are JSON Schema constraints), then regenerate the schemas:

```bash
python -m forklift.jobs.contract            # writes contracts/*.schema.json
python -m forklift.jobs.contract --check    # exit 1 when they are out of date
```

`tests/unit-tests/test_jobs_contract.py` fails when the checked-in files differ from the generated
ones, and checks that `from_dict` and the JSON Schema accept and refuse the same documents. A
change that is not additive needs a new `spec_version`.
