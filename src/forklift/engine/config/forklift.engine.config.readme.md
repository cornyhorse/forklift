# Forklift Engine Configuration Module

The configuration module provides essential classes and enums for configuring data import operations in the Forklift engine. It handles CSV processing settings, validation options, and output preferences.

## Overview

This module contains the core configuration components:

- **ImportConfig**: Main configuration class for data import operations
- **ProcessingResults**: Results tracking for completed operations
- **HeaderMode**: Enumeration for header detection strategies
- **ExcessColumnMode**: Enumeration for handling extra columns

## Components

### ImportConfig

The primary configuration class that controls all aspects of data import processing.

**Key Features:**
- File path configuration (input/output)
- CSV parsing options (delimiter, encoding, quotes)
- Header detection and processing
- Schema validation settings (`validate_schema`, and `apply_schema_extensions` for the schema's `x-...` extensions)
- Output file generation options
- Error handling preferences

### ProcessingResults

Tracks the outcomes of data processing operations, including:
- Row counts: `total_rows`, `valid_rows`, `invalid_rows` and `truncated_rows` (rows cut to the header width)
- Generated file paths: `output_files` (the data file and, when rows were rejected, the bad rows file), plus `bad_rows_file` to tell the rejected-rows file apart (`None` when nothing was rejected), `manifest_file` and `metadata_file`
- Execution metrics (`execution_time`)
- Error collection (`errors`): a failed run appends the message here and re-raises; a failure to write the output-metadata file (the data files are already complete by then) is recorded here without raising
- Schema extension reporting (CSV imports; empty for Excel and SQL):
  - `schema_extensions`: names of the extensions that were applied (for example `['x-transformations', 'x-primaryKey/x-uniqueConstraints/constraints']`)
  - `validation_summary`: `{CODE or CODE:column: count}` for rows rejected by validation and constraints (`UNIQUE_VIOLATION:id`), values nulled by `x-special-type` (`INVALID_SPECIAL_VALUE:ssn`), `x-dataQuality` findings and so on; it never holds cell values
  - `warnings`: notes that do not stop the import, such as schema content that no processor reads (`x-pii`, `x-transformations.stringCleaning`, ...) or rules skipped because the file lacks the column
  - all three are also written to `metadata.json` and printed by the CLI

### Enums

#### HeaderMode
Controls header detection behavior:
- `PRESENT`: The first row that is not blank or a comment is the header (a header must appear within `header_search_rows` rows, otherwise `ValueError`)
- `ABSENT`: No header row. Column names come from the schema, or are generated as `col_1`..`col_N` from the width of the first data row when there is no schema
- `AUTO`: Pick the row within `header_search_rows` that looks most like a header (mostly text rather than numbers); falls back to the first row

`header_mode` and `excess_column_mode` accept the enum member or its string value, case-insensitively (`"absent"`, `"ABSENT"`); anything else raises a `ValueError` listing the valid values.

#### ExcessColumnMode
Handles data rows that have more fields than the header (or, in `ABSENT` mode, than the schema's columns):
- `TRUNCATE`: Cut the row to the header width and keep it (default). The number of cut rows is reported in `ProcessingResults.truncated_rows` and logged as a warning
- `REJECT`: Write the whole row to `bad_rows.parquet` (cut to the header width, so fields beyond it are not kept) and count it in `invalid_rows`
- `PASSTHROUGH`: Keep every field and name the extra columns `col_N` (N is the 1-based position). The output schema is fixed by the first batch, so a wider row that appears after data has already been written raises a `ValueError` (put the widest row first, or use `TRUNCATE`/`REJECT`)

Rows with *fewer* fields than the header are padded with empty strings in every mode.

**Important**: the width that counts is the width of the header found in the file (in `ABSENT` mode: the schema's columns). A schema never adds or removes columns by itself: it only supplies types, null markers and `required` rules for the columns that exist in the file, matched by name. Schema properties that are not in the file are ignored (a *required* one that is missing from the file raises a `ValueError` before any row is read).

**Example**: A file with the header `Name,Age,City` and a data row `Ann,41,Paris,France,555-1234`:
- `TRUNCATE`: the row is kept as `Ann,41,Paris`; `truncated_rows` is incremented
- `REJECT`: the row is written to `bad_rows.parquet` as `Ann,41,Paris`
- `PASSTHROUGH`: the output columns are `Name,Age,City,col_4,col_5` (only if this row is reached before the first batch of data is written)

**Implementation**: This functionality is implemented in the `BatchProcessor` class located at `src/forklift/engine/processors/batch_processor.py`. The local Arrow reader requires every row to have exactly the header's field count; on the first row that does not, the rest of the file is read by a row-by-row reader (the S3 reader always works this way) which applies the mode:
- For `TRUNCATE` mode: Excess columns are removed using `row[:expected_columns]` and counted in `truncated_rows`
- For `REJECT` mode: The row is handed to the bad rows writer
- For `PASSTHROUGH` mode: Extra columns get auto-generated names
- Short rows are padded with empty strings regardless of mode

**Testing**: The functionality is validated by unit tests in `tests/unit-tests/test_batch_processor.py`:
- `test_create_s3_csv_batches_excess_columns_truncate()` - Tests that excess columns are properly removed while preserving the row
- `test_create_s3_csv_batches_excess_columns_reject()` - Tests that rows with excess columns are rejected
- `test_create_s3_csv_batches_excess_columns_passthrough()` - Tests that all columns are preserved with auto-generated names for extras

## Usage Examples

### Basic Configuration

```python
from forklift.engine.config import ImportConfig, HeaderMode

config = ImportConfig(
    input_path="data/input.csv",
    output_path="data/output/",
    header_mode=HeaderMode.PRESENT,
    batch_size=5000
)
```

### Advanced Configuration with Schema Validation

```python
from forklift.engine.config import ExcessColumnMode, HeaderMode, ImportConfig

config = ImportConfig(
    input_path="data/complex.csv",
    output_path="data/processed/",
    schema_file="schemas/data_schema.json",
    delimiter="|",
    encoding="utf-8",
    header_mode=HeaderMode.AUTO,        # or the string "auto"
    header_search_rows=5,
    excess_column_mode=ExcessColumnMode.REJECT,  # or "reject"
    validate_schema=True,
    create_manifest=True,
    compression="gzip"
)
```

### Processing Results Usage

```python
from forklift import import_csv

results = import_csv("data/input.csv", "data/output/", schema_file="schemas/data_schema.json")

print(f"Success rate: {results.valid_rows / results.total_rows * 100:.2f}%")
if results.bad_rows_file:
    print(f"{results.invalid_rows} rejected rows are in {results.bad_rows_file}")
print(f"{results.truncated_rows} rows were cut to the header width")
for message in results.errors:  # e.g. the output metadata file could not be written
    print("Warning:", message)
print(results.schema_extensions)   # extensions that were applied, e.g. ['x-transformations']
print(results.validation_summary)  # findings per CODE / CODE:column, e.g. {'UNIQUE_VIOLATION:id': 1}
for note in results.warnings:      # schema content that nothing reads, rules skipped for absent columns
    print("Note:", note)
```

## Configuration Parameters

### File Handling
- `input_path`: Source file location (local path or `s3://` URI)
- `output_path`: Destination directory (local path or `s3://` prefix). `data.parquet` and `bad_rows.parquet` of an earlier run in that location are removed when processing starts, so a re-run never leaves stale outputs next to the new ones
- `schema_file`: Optional JSON schema (local or `s3://`). Its types are applied to the output: the `x-csv.parquetTypeMapping` entry first, otherwise the JSON `type`/`format`. A column typed `string` keeps its text exactly (`00123` stays `00123`). A value that cannot be converted sends its row to `bad_rows.parquet`. Columns that are not in the schema keep Arrow's type inference on the local path and stay strings on the S3 path. The schema's `x-...` extensions (transformations, column mapping, calculated columns, validation, keys, constraints, row hash) are applied too unless `apply_schema_extensions` is false; they use the header names of the file for `properties`, `required`, `x-csv` and `x-transformations` and the output names after `x-columnMapping`
- `encoding`: Text encoding (default: utf-8); a UTF-8 byte order mark is ignored

### CSV Processing
- `delimiter`: Field separator (default: comma)
- `quote_char`: Quote character (default: double quote)
- `escape_char`: Escape character for special chars
- `skip_blank_lines`: Only used while looking for the header (blank rows above it are skipped). Completely empty lines in the data section are always skipped

### Header Processing
- `header_mode`: Header detection strategy (enum member or case-insensitive string such as `"absent"`)
- `header_search_rows`: Max rows to scan for headers (default: 10); no header inside the window is an error
- `comment_rows`: Regex patterns for comment rows. Only applied while looking for the header (matching rows above it are skipped; lines below the header are never comments). With the default `None`, only a row that is a single `#...` cell counts as a comment (so `# Generated: 2025-01-01` is skipped but a header such as `#,name,amount` is a header); `comment_rows=[]` turns comment detection off
- `footer_detection`: Dictionary, for example `{"stop_on_blank": True}` or `{"column_index": 0, "patterns": ["^Total"]}`; processing stops at the first matching row

### Validation & Error Handling
- `validate_schema`: Enforce the schema's `required` columns (default: True). Required columns are matched by column *name*; a null or an empty string in a required column sends the row to `bad_rows.parquet`. A required column that is missing from the input raises `ValueError`
- `max_validation_errors`: Reserved, not enforced: every invalid row goes to `bad_rows.parquet` and processing continues
- `apply_schema_extensions`: Run the schema's `x-...` extensions on every batch of a CSV import (default: True): `x-transformations` and `x-special-type` formatting, `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality` (findings only), `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, per-property constraints (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique`), `x-constraintHandling.errorMode` and `x-rowHash`. Rows rejected by validation or constraints go to `bad_rows.parquet`, which then has a last `_rejection_reason` column. With False the extensions are ignored; types, null markers and `required` still apply (CLI: `--no-schema-extensions`). Excel and SQL imports never apply them
- `excess_column_mode`: Strategy for extra columns (enum member or string)

### Output Options
- `batch_size`: Upper bound on rows per batch written (default: 10000). The local reader produces batches of roughly 1 MiB, which are only split down to this size, so smaller batches can occur; the S3 reader buffers exactly this many rows
- `include_value_statistics`: Allow value-bearing statistics (top values, min/max, median, mode, quantiles) in `output_data_metadata.json` (default: False, because cell values can be personal data)
- `create_manifest`: Generate manifest file (default: True)
- `create_metadata`: Generate the metadata files (default: True)
- `compression`: Output compression type (default: snappy)

## Error Handling

The module provides error handling through:
- Row-level isolation: rows with an unconvertible value, an empty/null required column, (with `REJECT`) excess fields or a violation of `x-validation` / a key / a constraint are written to `bad_rows.parquet` instead of aborting the run
- Run-level failures (unreadable input, undecodable bytes, invalid schema, no header found, a misconfigured schema extension, `x-constraintHandling.errorMode` `fail_fast` / `fail_complete` with a violation, `x-validation` over its `maxBadRowsPercent`) raise, are appended to `ProcessingResults.errors`, and leave no partial `data.parquet`/`bad_rows.parquet` behind: local partial files are removed and S3 uploads are not completed. The one exception is `x-validation` over its threshold: `data.parquet` is discarded but a finished `bad_rows.parquet` is kept and named in the error, so the rejected rows can be inspected
- Clear `ValueError`s for invalid enum values and missing required columns

## Performance Considerations

- **Batch Size**: Larger batches improve throughput but use more memory
- **Header Search**: Limit `header_search_rows` for large files
- **Validation**: Disable schema validation for trusted data sources
- **Compression**: Choose appropriate compression for your use case

## Integration

This configuration module integrates with the broader Forklift engine:

```python
from forklift.engine.config import ImportConfig
from forklift.engine.forklift_core import ForkliftCore

config = ImportConfig(input_path="data.csv", output_path="output/")
results = ForkliftCore(config).process_csv()
```
