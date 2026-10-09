# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release is the result of a security and correctness review. It contains **behaviour changes
that can alter output** (marked **Breaking**); please read "Changed" before upgrading.

### Security

- **Calculated-column expressions no longer use `eval`.** A schema-supplied expression could run
  arbitrary code (`abs.__globals__['__builtins__']['__import__']('os')...`). Expressions are now
  parsed and run by a whitelist interpreter with length, node, exponent and result-size limits.
  Any caller feeding `forklift.processors.CalculatedColumnsProcessor` an untrusted schema was
  exposed, and `import_csv` now runs calculated columns (see "Changed"). A `ConstantColumn` value
  can no longer be turned into code either.
- **Database passwords are no longer written to disk.** The SQL importer wrote the full ODBC
  connection string, including `PWD=`, to `metadata.json`. Secrets are redacted there and in logs;
  `SqlInputConfig.__repr__` redacts them as well.
- **SQL injection via schema files closed.** Table and schema names are validated when the schema
  is loaded, resolved against the database catalog before any SQL is built, and always quoted
  (embedded quotes escaped). Connections are read-only by default. Connection parameters are
  brace-escaped.
- **Path traversal via `outputName`** (`../../x`, absolute paths) is rejected at schema load and
  again when writing; output files must stay inside the output directory.
- **Cell values no longer leak into metadata by default.** `output_data_metadata.json` and
  generated schemas contained real values (`top_values`, min/max, quantiles, enum suggestions,
  sample rows) such as SSNs and salaries. These are now opt-in via `include_value_statistics`.
  Source file paths are recorded as base names.
- **S3 writers no longer publish truncated objects** when the `with` body raises; the multipart
  upload is aborted. S3 keys containing `?` or `#` are no longer cut short.
- **Excel resource limits.** Workbooks are streamed read-only; `max_rows`, `max_cells`,
  `max_uncompressed_bytes` and `max_compression_ratio` bound what is parsed (a 4.8 KB file with
  one stray cell used to take 11 s and 839 MB).
- **ReDoS guard** for user-supplied patterns in the processors (compile at configuration time,
  length cap, nested-quantifier check, `allow_unsafe_regex` opt-in); regexes in input and
  transformation configs are compiled when the configuration is created.
- **Schema generation rejects URL-like inputs** (`http://`, `ftp://`, `file://`, ...); only local
  paths and `s3://` are accepted.
- Row hashes use an injective encoding (see Changed); bad-row CSV output neutralises spreadsheet
  formula prefixes; validation errors no longer contain cell values by default.
- CI: least-privilege workflow permissions, fork PRs never get write access, publishing verifies
  that the release tag equals the package version and runs the unit tests first.

### Changed

- **Breaking - `import_csv` applies the schema extensions.** Until now the CSV engine ignored every
  `x-...` block except `x-csv`; the processors in `forklift.processors` were library code that
  nothing called. Each batch now goes through: `x-csv` null markers and `x-transformations`
  (including the automatic `x-special-type` formatting) on the text of the file, type conversion,
  `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality`, `x-validation`, the constraints
  (`x-primaryKey`, `x-uniqueConstraints`, per-property `minimum`/`maximum`/`minLength`/`maxLength`/
  `pattern`/`enum`/`x-unique`, `x-constraintHandling.errorMode`) and `x-rowHash`. What this means
  for existing schemas:
  - Rows that break a validation rule or a constraint now go to `bad_rows.parquet`, which then has a
    last column `_rejection_reason` (`CODE` or `CODE:column`, never a cell value). `errorMode`
    `fail_fast` / `fail_complete` raise instead and leave no output behind.
  - Output columns change: renamed (`x-columnMapping`), appended (`x-calculatedColumns`,
    `x-rowHash`), reformatted (`x-special-type` columns such as SSN, ZIP, phone) or set to NULL
    when invalid.
  - Configuration is checked before anything is written: invalid options, a key or unique
    constraint on a column that is not in the file, a name no `properties` entry declares in
    `x-validation`/`x-dataQuality`, a calculated or hash column that would overwrite a data column,
    or a header starting with `__forklift_` raise `ValueError`. A calculated column whose listed
    `dependencies` the file lacks is left out with a warning when `properties` declares them.
  - `x-validation.badRowsHandling.maxBadRowsPercent` is judged once, after the whole input was
    checked (`thresholdMode: end_of_file`, the default): the verdict no longer depends on where the
    bad rows are or on `batch_size`, and the error lists the findings by rule and the settings that
    change the outcome. `thresholdMode: early` keeps the per-batch check, which stops a hopeless
    input sooner. When the import stops on this threshold it discards `data.parquet` but keeps
    `bad_rows.parquet` (finished and readable) and names it in the error, so the rejected rows can
    be inspected (`BadRowsThresholdExceededError.bad_rows_file`); every other failure still leaves
    no output.
  - Content that no processor reads (for example `x-pii`, or `x-transformations.stringCleaning`)
    is reported in `ProcessingResults.warnings`, logged and printed by the CLI instead of failing.
    `x-pii` masking is not implemented.
  - `ImportConfig(apply_schema_extensions=False)` / `--no-schema-extensions` restores the old
    behaviour (types, null markers and `required` still apply). CSV only: the Excel, SQL and
    fixed-width importers do not apply extensions yet.
  - `schema-standards/20250826-csv.json` was rewritten to what is applied: expressions in the
    supported syntax (not `CASE WHEN`), `x-transformations.column_transformations`, `email_address`
    and `phone_number` instead of the misspelt `email` / `phone`, and every key no processor reads
    was removed (`x-dataQuality.completeness` ..., `x-validation.crossFieldValidations`, ...). It
    now runs without warnings except `x-pii`.
- **Breaking - pandas is no longer a dependency.** Nothing in processing uses pandas or polars.
  They are optional output formats: `pip install forklift-etl[pandas]` /
  `forklift-etl[polars]` for `DataFrameReader.as_pandas()` / `as_polars()`. The install name is
  `forklift-etl` (the README said `forklift`). Runtime dependencies now include `pytz`,
  `chardet` and `charset-normalizer`, which were imported but undeclared. `pyarrow` has no upper
  bound (`<18` has no Python 3.13 wheels) and its lower bound is now 16: the repository's own tests
  build batches with `pa.record_batch(dict)`, so the suite already failed on 15. The unit suite was
  run on pyarrow 16.1, 17.0, 18.0 and 25.0, and CI has a "minimum versions" job.
- **Breaking - schema types are enforced.** The JSON schema's types now drive the Parquet schema
  (a `string` column keeps `00123`; before, Arrow inferred `int64` and wrote `123`). Values that
  do not convert, and empty values in `required` columns, go to `bad_rows.parquet` instead of
  being accepted or aborting the run. `required` is matched by column name, not position, and a
  required column missing from the file raises `ValueError`. Bad-row files have all-string
  columns in header shape.
- **Breaking - metadata content.** Default output and schema metadata omit value-bearing fields
  (see Security). String min/max became `min_length`/`max_length`. Distinct/uniqueness figures are
  exact up to a cap and then flagged (`distinct_count_is_lower_bound`, ratios `null`); quantiles
  are estimated from a seeded reservoir (`quantiles_are_estimated`).
- **Breaking - failures are loud.** `import_sql` raises `ProcessingError` when a table fails
  (partial results on `error.results`; `continue_on_error=True` restores continuing), and no
  longer reports failed tables as invalid rows. CLI failures exit non-zero (1 failure, 2 usage or
  not implemented). Header detection raises when no header is found. `ImportConfig` accepts enum
  names as strings and raises on unknown values. `fail_on_exceed_threshold` now defaults to
  actually raising once more than 10 % of rows are bad. Unknown keys in transformation configs
  raise `ValueError`.
- **Breaking - row hash.** `RowHashProcessor` defaults to an injective version-2 encoding, so
  hashes differ from earlier releases. `legacy_encoding=True` (`hashVersion: 1`) reproduces the old
  bytes; `md5`/`sha1` require `allow_weak_hash=True`.
- Header handling: a row is a comment only if it is a single `#...` cell (`comment_rows=[]`
  disables comments); `#,name,amount` is a header. Absent header without a schema generates
  `col_1..col_N`. Blank lines are skipped on every read path (files with trailing blank lines now
  report fewer rows).
- `DataFrameReader` reads only data files (rejected rows are no longer included) and cleans up via
  `close()`/context manager and at exit instead of `__del__`.
- Schema generation infers types from an all-string sample, so leading zeros survive and `"NA"` is
  no longer null; dates and timestamps infer as `date32`/`timestamp`; `nrows` must be a positive
  int or `None` (whole file) and gives the same result either way; inferred primary keys must be
  100 % unique within the sample; unsupported input locations raise.
- Excel: `header.row` is 0-based, `data_start_row`/`data_end_row` are 1-based and inclusive;
  `keep_default_na` now means only empty/whitespace is null (not "NA"); the Excel schema accepts
  `x-excel.sheet` (singular), and `schema-standards/20250826-excel.json` defines `x-excel` (it
  contained a copy of the CSV block).
- SQL types: `FLOAT` is `float64`, identity suffixes are stripped, `TINYINT` is `int16`, `money`,
  `datetime2`, `uniqueidentifier` and exact decimal precision are mapped.
- Transformations: `decimal_separator=","` now pairs `"."` as the thousands separator (before,
  `"12,50"` became 1250.0); integer targets use exact decimal parsing (non-integral values become
  NULL); HTML/XML cleaning strips tags first and decodes entities once; it is text extraction, not
  a sanitizer; mojibake repair uses a round-trip instead of a replacement table that deleted valid
  letters; `dayfirst=True` is an explicit keyword shared by `coerce_date`/`coerce_datetime`
  (previously they disagreed); epoch detection is skipped when a format is given; timezone names
  are validated when the configuration is created; `zero_pad` for SSN, zip-9 and zip-permissive
  applies only with `validate=False` (documented; validation still rejects short values).
- Calculated columns: arithmetic and ordering comparisons with NULL give NULL regardless of column
  names (a substring heuristic made `price - cost` fail); duplicate output names, unknown
  `dataType` values and unsafe expressions raise when the processor is created; `now()`/`today()`
  are one snapshot per batch.
- Validation processors fail closed (internal errors raise instead of returning unvalidated data),
  uniqueness strategies `last_wins`/`mark_all_duplicates` are implemented, required rules on
  missing columns raise, `allow_empty` is honoured, empty strings are validated, and pattern
  rules use unanchored (JSON-Schema) search with `$` meaning end of string.
- Unused shim modules (`processors/calculated_columns.py`, `schema_validator.py`,
  `transformations.py`, `data_validation.py`, `utils/date_parser.py`) were removed; the packages of
  the same names are unchanged.

### Added

- `ImportConfig.apply_schema_extensions` (default on) and CLI flag `--no-schema-extensions`;
  `ProcessingResults.warnings`, `validation_summary` (counts per `CODE` / `CODE:column`) and
  `schema_extensions`; the CLI prints them. Run metadata (S3) records them as well.
- `forklift.processors.schema_extensions`: `build_column_mapper`, `build_constraint_validator`,
  `build_data_validator`, `build_quality_processor`, `referenced_columns` and
  `unsupported_extension_keys` turn the schema blocks into processors (documented formats, clear
  errors, no cell values in messages). `ColumnMapper.output_names`, `row_hash_output_columns`,
  `RowHashProcessor.compute_input_hash` and the `input_hash=` / `source_row_numbers=` arguments
  let a pipeline that drops rows keep hashes and row numbers aligned with the source file.
- `ConstraintConfig.max_retained_violations` / `include_values`: violations kept in memory are
  bounded (counts stay exact), and messages carry no values by default.
- `docs/schemas/X_VALIDATION_DOCUMENTATION.md`; the other `X_*` pages now say what is applied.
- `include_value_statistics` option (default off) for schema generation, `ImportConfig` and the
  metadata collector; CLI flag `--include-value-stats`.
- `ProcessingResults.bad_rows_file` and `truncated_rows`; `DataFrameReader.close()` and context
  manager; header-only inputs produce an empty `data.parquet` carrying the schema.
- Excel: `get_sheet_info`/`process_sheets` (the importer called methods that did not exist, so
  `import_excel`, `read_excel` and `--input-kind excel` always failed), `.xls` support via xlrd,
  `ExcelInputConfig` limits, header modes, `nulls`, `columns`, `name_override`.
- `SqlInputConfig.read_only`, `import_sql(..., continue_on_error=, s3_client=)`, S3 output for SQL
  and Excel imports, `redact_connection_string`.
- `dayfirst` option for date parsing and transformations; FWF `errors` and `rejected_lines`;
  shared strict Parquet type grammar for all schema importers; `snake_case`/`camelCase` column
  name styles (accepted but ignored before).
- `CHANGELOG.md`, `requirements-dev.txt`, `.github/dependabot.yml`.
- Python 3.14 support: classifier and CI/publish test matrix. All dependencies and extras have 3.14
  wheels (pyarrow 22 is the first release that does); the unit suite passes on 3.14.6 with pyarrow
  22.0 and 26.0, also with deprecation warnings treated as errors.

### Fixed

- **Wrong data for sliced string columns on pyarrow 16 - 22.** `pc.if_else(mask, <null scalar>,
  array)` returns `'\x00'` strings for a sliced array on these versions, and the engine cuts
  Arrow's blocks into `batch_size` rows. With a schema or null markers, an input with more rows than
  `batch_size` per block lost values silently (strings) or had whole rows sent to `bad_rows`
  (numbers); pyarrow 25 was not affected. `set_null_where` (`forklift.utils.arrow_compat`) is used
  by the converter, schema inference and the schema validator; tests cover several batch sizes and
  run in the minimum-versions CI job.
- Errors in calculated-column expressions say what to write instead: `CASE WHEN` gets the
  equivalent conditional expression for that expression, `=`, `<>`, `IS NULL`, upper-case
  `AND`/`OR`/`NOT` and `||` get a hint, unknown functions and columns get "did you mean" and the
  available names, all checked before anything is written (they used to fail at row 0 of the first
  batch, or with "invalid syntax").
- Calculated-column constants of a date or timestamp type accept ISO text (`"2024-08-26"`, also with `Z`);
  building the result no longer triggers pyarrow's `names=` deprecation warning.
- `ColumnMapper` ignored `allowUnmapped: false`; `DataValidationProcessor` results did not carry
  the column; `EnhancedDataProcessor` miscounted violations once the validator bounded them.
- `read_csv(...).as_pandas()/as_polars()` returned rejected rows; `read_sql` always raised
  `TypeError`; `as_polars(lazy=True)` returned frames whose files were already deleted.
- Mid-file ragged rows could emit earlier rows twice and then crash on a schema mismatch; partial
  outputs were left behind (and S3 temp files leaked) when a run failed.
- `dedupe_column_names` looped forever for three or more empty names; deduped Postgres names could
  exceed 63 characters.
- Nullable string columns crashed string and HTML cleaning under pandas 3 (NaN reached Arrow);
  trimming/regex/padding returned `large_string`, which later string transforms silently skipped;
  invalid timezones nulled every row; naive datetimes used the machine time zone for epochs.
- Date parser: schema-token formats such as `YYYY-MM-DD HH:mm:ss.SSS` raised `re.error`; the
  dateutil fallback turned `"12"`, `"Mon"` or `"2024"` into dates based on today; `%z` rejected
  `Z`/`+05:00`.
- FWF: swallowed per-line exceptions (a bad regex returned an empty file with success), BOM shifted
  columns, unknown bool/int values became `False`/NULL silently, simple specs skipped validation,
  conditional schemas never populated the flag column and crashed on missing methods.
- Encoding detection returned `None` for empty or undetectable input and sampled only 10 KB.
- Schema importers crashed with `TypeError`/`AttributeError` (about 1,500 crashes in a fuzz of
  well-formed-but-wrong values) instead of `SchemaValidationError`; nullable `type` arrays were
  rejected; `required: "id"` was treated as `['i', 'd']`; `encodingPriority` rejected valid codecs.
- Metadata statistics were wrong for large columns (uniqueness ratio inverted, biased quantile
  sample, `Infinity`/`NaN` in JSON, mislabelled quantile keys).
- Column mapper output-name collisions, `drop_unmapped`, camel/Pascal case; bad-row counters and
  row indexes across batches; `max_bad_rows=0`; write-time validator double counting.
- `__version__` disagreed with `pyproject.toml`; FWF standard-schema tests silently skipped on
  every machine except the author's; hard-coded home-directory paths in scripts and examples.

### Known limitations

- Schema extensions are applied by the CSV engine only; the Excel, SQL and fixed-width importers
  ignore them. `x-pii` is documentation (no masking). Not implemented, and reported as warnings:
  cross-field and global validations, `x-dataQuality` completeness/uniqueness/consistency/accuracy
  blocks, `x-uniqueConstraints` `condition` / `ignoreNulls: false` / case-insensitive keys, partition
  and index columns, `x-columnMapping.standardizationRules`.
- `bad_rows.parquet` shows the rows as the input file had them (all strings, under the input's
  column names), whichever stage rejected them; a column the schema does not list keeps Arrow's
  inferred type, so it is shown as that type prints. Its row order depends on `batch_size` (type
  failures of a batch are written before the batch's other rejects).
- `properties` (types, `required`, constraints, `x-csv.nulls`) are matched by the column names of
  the file, before `x-columnMapping` renames; a property declared under a rename's new name is not
  applied to the renamed column (the import warns).
- `max_validation_errors` is reserved and not enforced; foreign-key constraints have no format.
- `ColumnTransformer` still reports a failing transform as an error result and passes the batch
  through; `ColumnMapper` returns an error result with the original batch if building the output
  fails.
- The Excel zip-bomb check trusts the sizes declared in the archive; `max_rows`/`max_cells` bound
  what is actually parsed.
- The CSV schema importer's `standardize_column_names` returns early when `standardizeNames` is
  unset, so `dedupeNames` alone does nothing.
