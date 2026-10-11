# REVIEW-MCPL: independent review of `forklift-mcp` (local MCP server, stdio)

Reviewer: REVIEW-MCPL. Scope: `src/forklift/mcp/**`, `tests/unit-tests/test_mcp_*.py`, the `mcp` extra /
script / force-include in `pyproject.toml`, `docs/platform/mcp.md`, the package readme. No repository file
was edited. Every probe is a standalone script in `$S/review-mcpl/` (data under `$S/review-mcpl/pN/`).

Run any probe from the repo root:

```bash
S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad
PY=$S/mcpl-venv/bin/python; R=$S/review-mcpl; cd /home/user/forklift
```

Baseline: `test_mcp_{tools,workspace,values,server,entry,docs,packaging}.py` 184 passed, `test_mcp_stdio.py`
4 passed; black / isort / flake8 clean on the MCP files.

## Summary

No blocking findings. 14 should-fix, 10 nits, plus 6 engine observations for the lead (not MCPL's area).
The ones to fix first:

| # | Severity | Finding |
|---|---|---|
| 1 | should-fix (high) | Typed `preview` still applies the 10% bad-rows threshold when `x-validation` has no `badRowsHandling` (the engine's default), so it shows no rows. The docs say the threshold is not applied |
| 2 | should-fix (high) | `run_job overwrite=true` deletes the previous run's files *before* the engine refuses the job (`SPEC_INVALID`). The call is refused and the directory is left empty, with no `forklift-job.json` |
| 3 | should-fix (high) | `overwrite=true` deletes every regular file a `forklift-job.json` lists inside `output_dir`, the **input file** included. Nothing checks kind, sha256 or the input. If the input is deleted, the call then crashes |
| 4 | should-fix (high) | Results are not bounded: raw `preview` `columns` (2,000,001 names, a 26 MB result for `max_bytes=1024`), the `generate_schema` summary, and the `validate_schema` name lists |
| 5 | should-fix (high) | `get_bad_rows` / `explain_failure` turn whole 1024-row batches into Python objects. A 673-byte Parquet raised peak RSS by 600 MiB for `limit=1` |
| 6 | should-fix | `get_bad_rows`: a row larger than `max_bytes` returns 0 rows with `next_offset == offset`, so a client that follows `next_offset` loops forever |

---

## Findings

### 1. should-fix (high): typed preview does not switch off the default bad-rows threshold

`src/forklift/mcp/tools.py:339`: the threshold is switched off only when `x-validation.badRowsHandling`
is already a dict. The engine applies `maxBadRowsPercent: 10, failOnExceedThreshold: true` whenever
`x-validation` exists without `badRowsHandling` (`src/forklift/processors/schema_extensions.py:705`). That is
the usual shape of a schema an agent writes from the docs (`fieldValidations` only).

```
$PY $R/p1_threshold.py $R/p1
no badRowsHandling -> {"status": "failed", "row_count": 0, "sample": {"rows_read": 20, "rejected_rows": 6, ...}, "notes": []} BAD_ROWS_THRESHOLD_EXCEEDED
badRowsHandling {} -> {"status": "succeeded", "row_count": 14, ..., "notes": ["x-validation's bad-rows threshold is not applied to a preview: ..."]} None
```

Why it matters: "preview with the schema: typed rows, and what is rejected" is step 5 of the documented
flow (docs `mcp.md:242-246`). With the most common schema shape it fails and shows nothing, which contradicts
the docs.

Fix: when `x-validation` is a dict (and enabled), set `badRowsHandling = {**(validation.get("badRowsHandling")
or {}), "failOnExceedThreshold": False}` whenever the existing value is a dict or None/missing. Leave a non-dict
value alone so the engine still reports `SCHEMA_INVALID`. Add a test with `fieldValidations` only.

### 2. should-fix (high): overwrite deletes the previous run, then the job is refused

`tools.py:435-440`: `_remove_previous_run(target)` runs before `run_job(spec)`. The engine refuses some specs
only at run time, for example an unknown encoding (`runner._check_options`). The MCP pre-check
`InputOptions.from_dict` does not look encodings up.

```
$PY $R/p16_refused_overwrite.py $R/p16
first run: succeeded ['data.parquet', 'forklift-job.json', 'manifest.json', 'metadata.json', 'output_data_metadata.json']
second call refused: The job was refused: Invalid job spec (1 problem):
- input.options.encoding: unknown encoding 'no-such-encoding'
out now: []
get_job: 'out' has no forklift-job.json: run_job writes one next to the outputs of every run it sta
```

Why it matters: the call is refused, and the docs say a refused job writes nothing (report: "SPEC_INVALID ... nothing is written"),
yet the earlier run's outputs and result are gone. Any exception after the deletion has the same effect.
Example: `source.stat()` at `tools.py:437` raising (see #3).

Fix: run every check the engine would make before deleting anything (`codecs.lookup` of the encoding, or a
dry engine check), or move the old files aside and restore them when the job is refused or crashes. At the
least, delete only after `run_job` returned something other than `SPEC_INVALID`. Doing that needs the engine
to write into a fresh directory, then a swap.

### 3. should-fix (high): a `forklift-job.json` decides what `overwrite` deletes, the input included

`tools.py:1011-1028` (`_remove_previous_run`) deletes every regular file that the job file in `output_dir`
lists and that resolves inside the directory: subdirectories, any kind, the input file. It does not use the
recorded `sha256`, the kind, or whether the path is the input. The job file is trusted as if this server
wrote it, but a dataset copied or unpacked from elsewhere can carry one.

```
$PY $R/p10_overwrite.py $R/p10
before: ['forklift-job.json', 'notes.txt', 'orders.csv', 'sub', 'sub/keep.xlsx']
run_job raised FileNotFoundError (over MCP: Error executing tool run_job)
after: ['sub']
```

The input `orders.csv`, `notes.txt` and `sub/keep.xlsx` were deleted. The call then crashed at
`tools.py:437` (`source.stat()`), and the agent saw only "Error executing tool run_job". The existing test
`test_overwrite_never_deletes_outside_the_directory` covers `../`, not this case.

Why it matters: this is irreversible loss of user data (the input), through a documented path ("the files an
earlier run_job listed ... are deleted first").

Fix: delete a listed file only if all of these hold:
- its kind is one the run kind writes (`data`, `bad_rows`, `metadata`, `manifest`);
- it is a direct child of the directory;
- it is not the input (compare `os.path.samefile`);
- its size and `sha256` match the job file.

Refuse the call with a clear message when the job file lists anything else. Never let a `FileNotFoundError`
escape as a crash.

### 4. should-fix (high): results are not bounded by their caps

The contract in BRIEF.md says "Every tool has bounded output (rows, bytes, list lengths, all capped with the
cap stated in the result)":
- `tools.py:321`: raw `preview` passes the engine's `columns` through with no cap. The engine's `_PreviewBuilder` does
  not cap columns either, and counts them against `preview_max_bytes`, so a wide file gets 0 rows.
- `tools.py:858-888`: `summarize_schema` caps the number of columns (500) but not the length of names, `title`, the
  per-column `extensions` list, or the top-level `extensions` list. `max_bytes` bounds only `document_text`.
- `tools.py:222-231`: the `validate_schema` lists are capped at 500 names of any length, with no `_total`.

```
$PY $R/p4_wide.py $R/p4        # 2,000 columns with 1 KB names (2 MB header)
preview raw max_bytes=1024: result 2022372 bytes; columns 2000 rows 0 bytes 0.1s
generate_schema max_bytes=1024: result 531196 bytes; summary 529698 document_text 1049 0.9s
validate_schema: result 1517585 bytes; columns listed 500 0.3s
$PY $R/p4b_wide_short.py $R/p4  # 10,000 ordinary columns (col_00001 ...)
raw default max_bytes: 130372 bytes, rows 0 bytes
raw 1 MiB: 380380 bytes, rows 5 None
$PY $R/p5_oneline.py $R/p5 20 fields   # a 20 MB CSV without a newline (2M fields)
20 MB fields: status succeeded code None columns 2000001 result 26000387 bytes sampled False; 2.1s maxrss 444 MiB
```

Why it matters: a 26 MB tool result for `max_bytes=1024` will be cut or rejected by the client (Claude
Code limits tool output). A wide but ordinary file (10k columns) can never be raw-previewed at the default,
and the result is 2× its stated cap.

Fix:
- Raw preview: cap `columns` like the typed preview does (`MAX_COLUMNS` with `columns_total`), and give the
  engine `preview_rows`/`max_bytes` for rows only. That needs an engine option, or MCP can slice the rows to the
  shown columns.
- Cut every name to a bounded length, for example `MAX_CELL_CHARS`, and count the cuts.
- Add `<list>_total` to every capped list.
- Make `summarize_schema` respect `max_bytes`, or put its own byte cap in `caps`.

### 5. should-fix (high): bad-row reads load 1024-row batches into memory

`values.py:25,143-153`: `iter_parquet_rows` reads `batch_size=1024` rows and calls `batch.slice(skip).to_pylist()`
on the whole batch, then cuts cells to 2000 characters. That happens even for `limit=1` or a 64 KiB result.
Peak memory is therefore about 1024 × the row size, twice over (Arrow plus Python). A highly compressible bad_rows file is a
decompression bomb. Legitimate rows with large text or JSON cells hit the same path.

```
$PY $R/p2_bigcells.py $R/p2 setup   # real run: 200 KB cells kept in bad_rows; then 1100 such rows (zstd)
parquet on disk bytes: 673
$PY $R/p2_bigcells.py $R/p2 bad
bad row_count 0 result bytes 412 maxrss before 129 MiB after 731 MiB
$PY $R/p2_bigcells.py $R/p2 explain
explain row_count 0 result bytes 134 maxrss before 129 MiB after 731 MiB
```

That is +600 MiB to return no row. With 2 MB cells it is about 4.5 GB, enough to OOM a laptop.

Fix:
- Use `batch_size=min(limit, 1024)` (at most 64 for wide files).
- Slice to `limit - len(rows)` before `to_pylist()`.
- Cut strings and binaries in Arrow first (`pc.utf8_slice_codeunits(col, 0, MAX_CELL_CHARS)`, `binary_slice`). Then only bounded
  values reach Python.

### 6. should-fix: `next_offset` does not advance when a row does not fit `max_bytes`

`values.py:86-99` and `tools.py:626-634`: a first row bigger than `max_bytes` is not added
(`limit: "bytes"`, `row_count: 0`), and `next_offset = offset + 0`.

```
$PY $R/p3_pagination.py $R/p3      # 3 rejected rows of 40 cells x 2500 chars
{'total_rows': 3, 'offset': 0, 'row_count': 0, 'truncated': True, 'limit': 'bytes', 'next_offset': 0}
max_bytes=1 MiB: {'row_count': 3, 'limit': None, 'next_offset': None}
explain first_rows: {'row_count': 0, 'limit': 'bytes'}
```

Why it matters: an agent that follows `next_offset` (as the tool description says) repeats the same call
forever. 100 columns × 2000 escaped characters can exceed even the 1 MiB maximum, so such a row can never be
shown without `columns`.

Fix: always return at least one row, cutting cells further and counting them in `truncated_cells`. Or set
`next_offset = offset + 1` with a `row_too_large` note. Never return `next_offset == offset`.

### 7. should-fix: `validate_schema` says `valid: true` when the copy is empty

`tools.py:1082-1106`: when the first line (or first record) of a CSV is longer than
`MAX_SAMPLE_COPY_CHARS`, nothing is copied and `whole=False`. The engine then validates an empty file. It
answers valid, even with required columns missing (see engine observation E2).

```
$PY $R/p5c_empty_copy.py $R/p5      # cap lowered to 1M chars to make the case small
preview: {"status": "succeeded", "error": null, "input": {..., "bytes": 2000007, "sampled": true}, "columns": [], "row_count": 0}
generate_schema: failed INPUT_UNREADABLE Failed to read CSV sample: file is empty
validate_schema: True succeeded None
```

Why it matters: with the real cap this is a file whose first record exceeds 256 Mi characters. For that file
`validate_schema` says valid while `run_job` fails. It also reads that whole line into memory first: the
cap is checked only after `for line in text` returned a complete line. Measured with the cap lowered:

```
$PY $R/p5b_cap.py <19 MiB one-line file>  -> maxrss 167 MiB
$PY $R/p5b_cap.py <76 MiB one-line file>  -> maxrss 281 MiB
```

A 2 GB single-line file costs at least 2 GB of RAM before the cap applies.

Fix:
- Read with `text.readline(limit)` or in chunks, so the cap bounds memory.
- When the copy holds no complete record but the source is not empty, refuse with a specific message
  ("the first record is longer than 256 MiB ...").

### 8. should-fix: ordinary bad arguments and I/O errors become "Error executing tool X"

`tools.py:1107` catches only `UnicodeError, LookupError, csv.Error`. `csv.reader(...)` raises `ValueError` for
dialects it refuses, and file operations raise `OSError`. Neither is a `ToolInputError`, so the SDK reports
a crash with no reason (mcp `tools/base.py:213`). The same happens for a `RecursionError` from `json.load` (`tools.py:642-648`),
and for `FileNotFoundError` at `tools.py:437` (#3).

```
$PY $R/p14_dialect.py $R/p14
preview {"delimiter": "\""} -> True Error executing tool preview
run_job {"delimiter": "\""} -> False {"status": "failed", "error": {"code": "INPUT_UNREADABLE", "message": "bad delimiter or quotechar value", ...}}
preview {"delimiter": "\n"} -> True Error executing tool preview
preview {"quote_char": ","} -> True Error executing tool preview
preview {"escape_char": "\n"} -> True Error executing tool preview
$PY $R/p9_full.py $R/p9            # RLIMIT_FSIZE 4 KiB: writes fail like a full disk / quota
preview -> True Error executing tool preview
generate_schema -> True Error executing tool generate_schema
$PY $R/p8_nesting.py $R/p8         # 100,000 nested '[' in a schema file / job file
get_schema -> True Error executing tool get_schema
preview -> True Error executing tool preview
get_job -> True Error executing tool get_job
```

Why it matters: the quality bar asks for specific messages. `preview` is the tool the `INPUT_UNREADABLE` help
sends the agent to for checking delimiter and quoting, and it gives no reason. Disk full or permission denied
on the input (the reviewer runs as root, so this was checked in code only: `open()` at `tools.py:1077/1108` and
`shutil.copyfile` at `:721` are unguarded) also give no reason.

Fix:
- Catch `ValueError` from the dialect: refuse with "input_options ...: <csv message>".
- In `_stage` / `_scratch`, catch `OSError` and say "cannot read input_path ... (<strerror>)" or "cannot write the
  temporary copy in <tmpdir> (<strerror>)".
- Catch `RecursionError` in `_read_json` ("nested too deeply").

### 9. should-fix: unknown arguments are dropped silently, including a misspelled `dry_run`

The SDK's argument model ignores extra keys, and the published input schemas have no
`additionalProperties: false` (`server.py:258-275`).

```
$PY $R/p18_unknown_args.py $R/p18
additionalProperties in run_job input schema: None
is_error False False succeeded
written: ['data.parquet', 'forklift-job.json', 'manifest.json', 'metadata.json', 'output_data_metadata.json']
```

`{"dryRun": true}` ran the import for real. The same happens to `{"sheet": ...}` passed top level instead of in
`input_options`, `{"limit": 5}` on `preview`, and so on. `dry_run` is the documented safety switch, and LLMs often
camel-case it.

Fix: set `additionalProperties: false` on every tool's `input_schema` in `_tool()`, and reject unknown keys.
For example, build the tool with an `extra="forbid"` argument model, or check
`set(arguments) - set(properties)` in a thin async wrapper before validation. Add a protocol test.

### 10. should-fix: typed preview messages name `out/bad_rows.parquet` in the deleted copy

`tools.py:348`, cf. `:268-273`: `validate_schema` rewrites the sample's artifact path. The typed preview does not.

```
$PY $R/p1b_message.py $R/p1
...set x-validation.badRowsHandling.thresholdMode to 'early'. All rows rejected by the checks are in out/bad_rows.parquet (the _rejection_reason column says why).
out/ exists in root? False
```

`out/bad_rows.parquet` read relative to the first root names a file that does not exist. Or, worse, it names
another run's real `out/bad_rows.parquet`, which the agent would then read as this preview's rejects.

Fix: give the typed preview the same replacement, pointing at `sample.rejection_reasons`. Add a test. Doing #1
reduces, but does not remove, the cases: constraint `fail_*` modes can still fail.

### 11. should-fix: typed preview work is not bounded by `rows` (Excel always; CSV after a long cell)

- Excel: `_stage` copies the whole workbook and the `run` kind imports **every row of every sheet**.
- CSV: one cell longer than the csv module's 131,072-character field limit makes `_copy_csv_head` fall back
  to copying the first 256 MiB of raw bytes (`tools.py:1107-1110`). The typed preview then imports all of
  it.

```
$PY $R/p21_excel_typed.py $R/p21 50000
50000 rows, 1356362 bytes: raw preview 0.5s rows=5; typed preview 4.0s rows=5 sample.rows_read=50000
$PY $R/p25_fieldlimit.py $R/p25     # 41 MB CSV, one 200 KB cell in row 1
file 41088885 bytes; typed preview rows=20: 1.5s status=succeeded row_count=20 sample.rows_read=2999999 sampled=False
```

At that rate a 256 MiB workbook takes about 10 minutes and writes a full `data.parquet` to TMPDIR. Interactive
tools pass no `limits` and cannot be cancelled. The docs say "the import runs on the first rows" (`mcp.md:242`).

Fix:
- Excel: limit the sheet read in the copy of the schema (`x-excel` `dataEndRow`, or the engine's
  `data_end_row`) to header + `rows` + 1.
- CSV fallback: raise `csv.field_size_limit` for the copy (to `MAX_SAMPLE_COPY_CHARS`) so it still stops after
  N records, or copy by lines.

### 12. should-fix (docs): the Claude Desktop example cannot run an import

`docs/platform/mcp.md:99` shows `--root /Users/me/data/exports --root /Users/me/data/clean`, which suggests
reading from one and writing to the other. `run_job` requires the input and `output_dir` to be in the same root
(`tools.py:990-999`, test `test_input_and_output_in_different_roots`): "input_path and output_dir must be inside
the same --root directory".

Fix: use one root (`/Users/me/data`) in the example and state the rule next to it. Better: allow different
roots by using the input's directory and the output dir with a common real ancestor as `base_dir` when both
are inside roots.

### 13. should-fix (docs): `run_job` writes at the top of the root, not only in `output_dir`

The engine's scratch `.forklift-job-XXXX/` (holding `schema.json`; every MCP run passes the schema inline) is
created in `base_dir`, which is the root (`tools.py:404,438`; `runner.py:377,446`).

```
$PY $R/p15_scratch_in_root.py $R/p15
engine scratch: .forklift-job-2ar6v5r2 (inside root: True )
```

Contradicts:
- `mcp.md:121-122`: "Only run_job (its output_dir) and generate_schema (its output_path) write inside the roots".
- `server.py:242-243` (instructions).

A root that is not writable but has a writable `out/` fails every run with a schema. A killed server leaves
`.forklift-job-*/schema.json` in the root.

Fix: document it, or pass the deepest common ancestor of input and output as `base_dir`. That ancestor is still
inside the root, and usually the output's parent.

### 14. should-fix (docs and messages): accuracy of what the docs promise

- `mcp.md:34-35`, `:133-135`, `values.py:3-4` and the readme (`forklift.mcp.readme.md:45-46`) say only `preview`,
  `explain_failure` and `get_bad_rows` carry cell values. `validate_schema.rejected_rows` carries them too, by
  design since round 2. Operators read this as a data-exposure statement.
- `mcp.md:267`, `server.py:203-205`: "other files are left alone / kept" under `overwrite`. In fact the engine
  replaces any same-named file the user has in that directory (`data.parquet`, `bad_rows.parquet`,
  `metadata.json`, `manifest.json`, `output_data_metadata.json`, `<stem>_<sheet>.parquet` for Excel; see
  p12/p16 listings).
- `explain.py:70-73` says for `BAD_ROWS_THRESHOLD_EXCEEDED` "the rejected rows are kept in bad_rows.parquet".
  With `thresholdMode: early` nothing is kept. Then `get_bad_rows` says "This run kept no bad_rows.parquet:
  it rejected no rows" (`tools.py:582`), which is false. The same false sentence appears after
  `LIMIT_EXCEEDED` / `CONSTRAINT_VIOLATION` failures.

```
$PY $R/p22_fail_sweep.py $R/p22   # threshold early -> arts=[] bad_rows=0
explain: The run failed with BAD_ROWS_THRESHOLD_EXCEEDED.  meaning: ... the rejected rows are kept in bad_rows.parquet.
get_bad_rows: This run kept no bad_rows.parquet: it rejected no rows 0
```

- `mcp.md:275-276`: "Bound long runs with limits.max_seconds". It is checked only at batch boundaries:
  `run_job` with `max_seconds: 0.5` ran 6.0 s before `LIMIT_EXCEEDED`. The interactive tools take no
  limits at all.

```
$PY $R/p26_regex_time.py $R/p26 27   # comment_rows ["^(a+)+$"], first cell 27 'a' + '!'
n=27 preview: 6.0s succeeded
n=27 run_job max_seconds=0.5: 6.0s failed LIMIT_EXCEEDED
```

  Each further 2 characters cost about 3× the time. Calls cannot be cancelled, so a hung call keeps a worker
  thread and a CPU busy until the server exits.

Fix:
- Correct the texts.
- Change the `get_bad_rows` message to "kept no bad_rows.parquet (no rows were rejected, or the run stopped
  before keeping them; see counts and error)".
- Give interactive jobs a default `limits.max_seconds`, and say in the docs it is checked between batches. Or
  run the engine in a child process that can be killed on timeout or cancel.

---

## Nits

| # | Where | Finding | Repro / fix |
|---|---|---|---|
| N1 | `tools.py:761`, `:1017-1021` | `forklift-job.json` inside an `output_dir` is not resolved. A symlink pointing outside every root is read (get_job/explain/get_bad_rows), and `_remove_previous_run` reads it with no size cap. The docs (`mcp.md:114-116`) say links that point elsewhere are refused | `$PY $R/p11_jobfile_symlink.py $R/p11`: "through the link: {... "input": {"path": ".../outside/secret.csv" ...}". Fix: `self.workspace.resolve(str(job_file), ...)` and reuse `_read_json` (with its cap) in `_remove_previous_run` |
| N2 | `tools.py:1034` | Files written via `mkstemp` get mode 0600. An overwritten 0644 schema becomes 0600, while `data.parquet` stays 0644 | `$PY $R/p17_modes.py $R/p17`: `schema.json 0o600 ... out/data.parquet 0o644`. Fix: `os.chmod(temporary, 0o666 & ~umask)`, or keep the replaced file's mode |
| N3 | `tools.py:911-913` | `rows: true` is accepted as 1: pydantic turns the bool into an int before `_check_range`'s bool check | p7 output: `{"rows": true} -> ok rows=1`. Strict int fields, or drop the dead check |
| N4 | `tools.py:575-584` | A run without bad rows returns a different result shape (no `offset`, `columns`, `next_offset`, `truncated`, `limit`, `truncated_cells`) | Return the same keys with empty values |
| N5 | `tools.py:222-231`, `:886` | Capped lists (`columns`, `columns_not_in_schema`, ..., `summary.required`) have no `_total` / truncated flag, so an agent cannot tell that 500 of 1,500 were shown | Add `<name>_total` |
| N6 | `mcp.md:157-159` | The error-code list leaves out `SPEC_INVALID` and `TARGET_WRITE_FAILED` | Name all 12 |
| N7 | `server.py:108-109` | The `format` description omits `.tab` | Add it |
| N8 | `tools.py:1143` | "The run cancelled with CANCELLED ..." (status used as a verb) and `next` "get_bad_rows shows the rejected rows" when nothing was rejected | Wording |
| N9 | `tools.py:96` / `mcp.md:129` | The copy cap counts characters (up to about 1 GiB of UTF-8), but the docs say "up to 256 MiB" | Say characters, or cap bytes |
| N10 | `mcp.md:68`, `__init__.py:56` | "`--read-only`: Refuse every write". Interactive tools still write private temporary copies (inside a root when TMPDIR is under it, e.g. `--root /`) | "every write inside the roots; temporary copies go to TMPDIR" |

---

## Engine observations (not MCPL's area; for the lead)

- **E1** The engine's `preview` and `validate_schema` kinds use Python `csv` with its 131,072-character field limit
  (`HeaderDetector`, `_csv_sample`). The `run` kind uses pyarrow. So `validate_schema` fails where `run_job`
  succeeds, with the unspecific message "field larger than field limit (131072)":
  `validate: False failed {'code': 'INPUT_UNREADABLE', 'message': 'field larger than field limit (131072)'}` /
  `run_job: succeeded` (p25 inline probe, 200 KB cell). `ERROR_HELP["INPUT_UNREADABLE"]` then points at
  delimiter and quoting.
- **E2** `validate_schema` on an empty file returns `valid: true` although a required column is missing
  (`schema_columns_not_in_input: ['b']`) (`$PY $R/p5d_empty.py $R/p5`).
- **E3** With `thresholdMode: early`, a `BAD_ROWS_THRESHOLD_EXCEEDED` run keeps no `bad_rows.parquet` and reports
  `counts: {}` (p22).
- **E4** An output write failure (EFBIG) is classified `INPUT_UNREADABLE`, not `TARGET_WRITE_FAILED` (p9 run_job line).
- **E5** `limits.max_seconds` is checked only at batch boundaries. Header detection and regex rules before the
  first batch are unbounded (p26).
- **E6** A schema property with `"type": "nonsense"` runs without `SCHEMA_INVALID` (p20).

---

## Checked and found sound

- **Root confinement** (`workspace.py:67-83`): all of these are refused:
  - `../`, absolute paths outside, `in/../../x`;
  - a symlink to a file or directory outside, a parent-component symlink, a dangling symlink pointing outside;
  - a symlink loop (as "cannot be read");
  - NUL characters, empty paths;
  - `~` expansion that lands outside.

  `//`-prefixed paths and case or Unicode-normalisation mismatches fail toward refusal, never toward escape.
  Relative paths resolve against the first root, and nested roots pick the deepest.
- **Special files**: FIFOs, devices and sockets are refused by `stat` before any open, for inputs, schema files
  and job files. A FIFO or device at an artifact path is skipped (`is_file()`), never opened. Time-of-check
  races and hard links are possible only for someone who can already write inside a root, which the docs exclude.
- **Overwrite stays inside `output_dir`**: `../x`, symlinks that point out, and absolute paths are not deleted.
  A symlinked `forklift-job.json` is unlinked, not its target. `_load_job` refuses artifacts that resolve
  outside the directory. Job files are capped at 16 MiB and need the format marker.
- **`--read-only`**: `run_job` and `generate_schema output_path` are refused, while dry runs and every read
  work. Interactive tools use `mkdtemp` (0700) as `base_dir`, so the engine writes nothing inside the roots
  (`_csv_config` output = the scratch). Schema extensions cannot name output paths (`outputPath` is ignored).
- **The copy stops early** on a large normal CSV (150 MB): preview 0.01 s, typed 0.24 s, validate 0.02 s,
  generate (100k rows) 0.37 s, all `sampled: true` (`$R/p23_big_csv.py`). Header detection on the copy matches
  the full file: the copy holds ≥ `header_search_rows` + n + 1 non-empty records, and the engine counts
  header-search rows including blank ones.
- **Excel zip bombs**: a 147 KB xlsx with 3M sharedStrings entries is refused by the engine's ratio guard
  ("compression ratio exceeds max_compression_ratio=200.0") (`$R/p6_xlsx_bomb.py`). The Excel flow
  (preview with sheet, generate, run with two sheets, explain) works (`$R/p12_excel.py`).
- **No cell values in messages**: probes for pk fail_fast, pattern and enum+threshold found none in
  errors/warnings (`$R/p19_values_in_messages.py`). Generated schemas carry no values (`$R/p13_values_in_schema.py`).
  No engine path on these tools writes to stdout: the generator's `print` is only in `output_schema`, which is
  not called. Logging goes to stderr.
- **Failure sweep**: threshold, early threshold, `fail_fast`, `fail_complete`, `bad_rows` and `max_rows`
  limits produce no crash in `_rebased_result`, and explain/get_bad_rows work (`$R/p22_fail_sweep.py`). Rule
  codes the engine emits map to `RULE_PAGES` and to docs (`$R/p24_rules.py`). All 12 error codes are
  explained (test-enforced).
- **MCP SDK 2.3.0**:
  - Refusals arrive as `is_error` with the reason, and crashes show the generic text (SDK `tools/base.py`).
  - Sync tools run on worker threads.
  - Input schemas carry the bounds and descriptions.
  - Annotations: read-only/destructive, `open_world_hint: false`.
  - Structured output is a plain object.
  - Resources: 17 pages, `text/markdown`; a traversal URI is refused ("Unknown resource").
  - Pydantic messages for wrong types are specific.
  - Startup imports in about 1.2 s.
  - stdio tests pass, in legacy and discover modes.
- **Entry**: argument parsing comes before the SDK and Python checks, so `--version` works without the SDK.
  Exit code 2 and the install hint without the SDK. A cwd of `/` is refused. `import forklift.mcp` does not import `mcp`.
- **Packaging**: the wheel (`$S/mcpl/dist`) has the 17 pages under `forklift/mcp/docs/schemas`, entry point
  `forklift-mcp = forklift.mcp:main`, and `Requires-Dist: mcp<3,>=2.3; (python_version >= '3.10') and extra ==
  'mcp'` (also for `all`). The sdist includes `/docs`. The module `docs.py` takes precedence over the
  namespace directory `docs/` in the wheel. An editable install falls back to `docs/schemas` of the checkout.
  The SDK needs Python ≥ 3.10, as the marker says.
- **Docs vs code** for each tool's inputs, defaults and caps (`mcp.md:166-320` against `tools.py` /
  `server.py` / `runner.py` constants): these match apart from the items above. Checked: rows 1–500,
  `max_bytes` 1024–1 MiB, `sample_rows` 1–100000, explain rows 1–50, bad rows 1–200, 100 columns,
  2000 characters (the engine's `PREVIEW_MAX_CELL_CHARS` is also 2000), 50 warnings, 100 findings,
  20 reasons / 5M rows, 8 / 16 MiB files, 256 MiB workbooks, `options`/`limits` defaults.
- **Style**: black, isort and flake8 are clean on `src/forklift/mcp` and `tests/unit-tests/test_mcp_*.py`.

Not verified: Windows and macOS behaviour (case-insensitive roots, `\\?\` paths), a real Claude Code / Desktop
session, and permission-denied paths (the reviewer runs as root, so these were checked by reading the code
only).

Clean-up: large probe inputs were deleted. `$S/review-mcpl/` holds only the scripts and small outputs (about 9 MB).
