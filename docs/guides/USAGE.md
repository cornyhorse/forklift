# Forklift Usage Guide

This guide provides comprehensive examples and workflows for using Forklift effectively.

## Table of Contents

- [Installation](#installation)
- [Data Import Workflows](#data-import-workflows)
  - [Applying Schema Extensions](#applying-schema-extensions)
- [Schema Generation](#schema-generation)
- [Data Reading and Analysis](#data-reading-and-analysis)
- [Validation and Error Handling](#validation-and-error-handling)
- [Excel and SQL Sources](#excel-and-sql-sources)
- [S3 Integration](#s3-integration)
- [Command Line](#command-line)

## Installation

```bash
pip install forklift-etl                      # core: pyarrow, boto3, jsonschema, ...
pip install "forklift-etl[excel]"             # Excel input (openpyxl, xlrd)
pip install "forklift-etl[sql]"               # SQL input (pyodbc; needs the unixODBC library)
pip install "forklift-etl[pandas,polars]"     # only for DataFrameReader.as_pandas() / as_polars()
pip install "forklift-etl[all]"               # all optional packages
```

Forklift processes all data with PyArrow. pandas and polars are optional **output** formats: they are
imported only when you call `as_pandas()` / `as_polars()`, and nothing in the import or schema
generation code uses them.

## Data Import Workflows

### Basic CSV Import

```python
import forklift

# Simple CSV import to Parquet
results = forklift.import_csv("sales_data.csv", "./output/")

print(results.output_files)   # ['./output/data.parquet']
```

The output directory receives `data.parquet` (accepted rows), `bad_rows.parquet` (only when rows were
rejected), `manifest.json`, `metadata.json` and `output_data_metadata.json` (column statistics). Files
with those names from an earlier run are removed when a run starts.

### Import with Schema Validation

```python
import forklift

# Import with schema validation
results = forklift.import_csv(
    input_path="sales_data.csv",
    output_path="./output/",
    schema_file="sales_schema.json"
)

# Check results
print(f"Total rows processed: {results.total_rows}")
print(f"Valid rows: {results.valid_rows}")
print(f"Invalid rows: {results.invalid_rows}")
print(f"Rows cut to the header width: {results.truncated_rows}")
if results.bad_rows_file:
    print(f"Rejected rows: {results.bad_rows_file}")
```

What a schema does during `import_csv`:

- **Types are applied.** Each column is converted to the type the schema declares (`x-csv.parquetTypeMapping` first, then the JSON `type`/`format`). A column declared `string` keeps its text exactly, so `00123` stays `00123`. Columns that are not in the schema keep Arrow's type inference when the file is read locally and are strings when it is read from S3.
- **Values that do not convert go to `bad_rows.parquet`.** The whole row is rejected, and the file stores every column as a string in the shape of the input, so you can see what the original text was.
- **`required` is matched by column name**, and an empty string counts as missing. A required column that is not in the file at all raises `ValueError` before any row is read.
- **Nulls** can be configured with `x-csv.nulls`.
- **The `x-...` extensions run as well** (`x-transformations`, `x-columnMapping`, `x-calculatedColumns`, `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, per-property constraints such as `minimum` and `pattern`, `x-rowHash`, ...): see [Applying Schema Extensions](#applying-schema-extensions). Pass `apply_schema_extensions=False` to ignore them.

### Applying Schema Extensions

For CSV files, `import_csv` (and `forklift ingest --input-kind csv`) runs the schema's `x-...` extensions on every batch. Excel, SQL and fixed-width imports apply none of them, and `x-pii` is documentation only: no masking is applied. The pages in [docs/schemas](../schemas/README.md) describe each extension; this is the summary of what is applied.

| Extension | Keys that are applied | Effect |
|---|---|---|
| `x-csv` | `nulls` (`global`, `perColumn`), `parquetTypeMapping` | null markers become NULL; Parquet types |
| `x-transformations` | `column_transformations.<column>.<step>`, with steps such as `string_cleaning`, `money_conversion`, `numeric_cleaning`, `regex_replace`, `string_replace`, `string_trimming`, `string_padding`, `html_xml_cleaning`, `datetime` and the `*_formatting` steps; a step only runs with `"enabled": true` (without the key the step is skipped and the import warns) | cleans the text of a column before its type is applied |
| `x-special-type` (on a property) | `ssn`, `zip-5`, `zip-9`, `zip-permissive`, `phone`, `email`, `ipv4`, `ipv6`, `ip`, `mac-address` | formats the value; an invalid value becomes NULL (counted as `INVALID_SPECIAL_VALUE:<column>`) and the row is kept |
| `x-columnMapping` | `explicitMappings`, `namingConvention`, `caseSensitive`, `allowUnmapped`, `dropUnmapped` | renames columns; `allowUnmapped: false` (like `dropUnmapped: true`) drops the unmapped ones |
| `x-calculatedColumns` | `constants`, `expressions`, `calculated` (each with `name`, `dataType` and a `value` or `expression`), `failOnError`, `addMetadata`, `validateDependencies` | appends columns |
| `x-dataQuality` | `fieldSpecificRules` (`min`, `max`, `pattern`), `fieldQualityRules.<column>.parameters` (`min_length`, `max_length`, `pattern`, `min_value`, `max_value`), `enabled` | report only: findings are counted, no row is dropped |
| `x-validation` | `fieldValidations.<column>` (`required`, `unique`, `range`, `stringValidation`, `enumValidation`, `dateValidation`), `uniquenessHandling.strategy`, `badRowsHandling.maxBadRowsPercent` / `failOnExceedThreshold` | rejects rows |
| `x-primaryKey`, `x-uniqueConstraints` | `columns`, `type`, `enforceUniqueness`, `allowNulls`; `name`, `columns` | rejects duplicate rows (and rows with a NULL primary key); the first row of a key wins |
| per-property constraints | `minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique` | rejects rows (other keywords such as `exclusiveMinimum` or `multipleOf` are not enforced) |
| `x-constraintHandling` | `errorMode` | what happens to a violation: `bad_rows` (default), `fail_fast`, `fail_complete` |
| `x-rowHash` | see [X_ROW_HASH_DOCUMENTATION](../schemas/X_ROW_HASH_DOCUMENTATION.md) | appends hash and metadata columns, last |

Each batch passes through the stages in this order:

```text
header names   x-rowHash input hash and row numbers (only if x-rowHash asks for them; the hash covers the raw text)
               x-csv null markers -> NULL
               x-transformations, then the automatic x-special-type formatting
               type conversion (properties, x-csv.parquetTypeMapping)    rows that do not convert -> bad rows
               required                                                  rows with a missing value -> bad rows
output names   x-columnMapping          renames columns
               x-calculatedColumns      appends columns
               x-dataQuality            findings only
               x-validation             rows -> bad rows
               x-primaryKey, x-uniqueConstraints, property constraints, x-constraintHandling   rows -> bad rows
               x-rowHash                appends the hash and metadata columns
```

**Names.** `properties`, `required`, `x-csv` and `x-transformations` use the column names exactly as they are in the file header, because they run before anything is renamed. Every stage from `x-columnMapping` on uses the *output* names (a header name that was renamed is accepted there too). A `properties` entry declared under the *new* name of a renamed column is not applied to it; the import adds a warning.

**Rejected rows.** `bad_rows.parquet` has all-string columns named like the input file's columns, with the values as the stage saw them (after transformations and type conversion). When the schema configures `x-validation` or any constraint, it gets an extra last column `_rejection_reason`: the reason is `CODE` or `CODE:column` (for example `UNIQUE_VIOLATION:id`, `VALIDATION_ERROR:age`, `NULL_VIOLATION:id`, `RANGE_VIOLATION:age`), several joined by `; `, and never contains a cell value. Rows rejected by type conversion, by `required` or (with `excess_column_mode="reject"`) for excess fields carry `type_conversion_failed`, `required_value_missing` or `too_many_fields` in that column. Without `x-validation` and constraints the file has no reason column.

**Results.** `ProcessingResults` reports what happened in three fields: `schema_extensions` (the extensions that were applied), `validation_summary` (counts per `CODE` or `CODE:column`; never values) and `warnings`. The CLI prints them (`Schema extensions applied: ...`, `Findings by the schema extensions:`, warnings on stderr) and `metadata.json` records them.

**Violation handling.** `x-constraintHandling.errorMode` is case-insensitive and any other value raises `ValueError`. `bad_rows` (default) rejects the violating rows, and for a duplicate key the first row wins. `fail_fast` raises `ValueError` at the first violation, `fail_complete` checks everything and raises at the end; in both no output file is left behind. `x-validation.badRowsHandling.maxBadRowsPercent` (default 10) with `failOnExceedThreshold` (default true) raises `BadRowsThresholdExceededError` (a `RuntimeError`) as soon as more than that percentage of the rows that reached `x-validation` so far was rejected by it, so small test files need a higher value.

**Columns the file lacks.** `x-primaryKey` and `x-uniqueConstraints` naming a column that is not in the file raise `ValueError` before anything is written. For `x-validation` and `x-dataQuality`, a name that is neither in the file nor in `properties` (a typo) raises `ValueError`, while a name declared in `properties` that this file lacks only adds a warning and the rules for that column (its per-property constraints included) are skipped, so a wide standard can be used with narrower files. A calculated column whose listed `dependencies` include a column the file lacks is left out with a warning when `properties` declares that column (and so is every calculated column that depends on it); a dependency that nothing declares raises `ValueError` before anything is written. List the columns an expression uses in `dependencies`: that is what lets the import check them. A calculated column or a hash column that would replace an existing column raises `ValueError`, and header names starting with `__forklift_` are reserved when `x-rowHash` or `x-transformations` is used. Calculated-column expressions use the safe expression syntax (`x if cond else y`, `coalesce`, `isnull`, `length`, `year`, `today`, ...), not SQL `CASE WHEN`; a date or timestamp `dataType` accepts ISO text such as `"2024-08-26"`.

**Warnings instead of errors.** Content that no processor reads does not stop the import; it is added to `results.warnings` (and logged). That covers `x-pii`, the `x-transformations` blocks other than `column_transformations` (`stringCleaning`, `moneyType`, ...; write `x-transformations.column_transformations.<column>.<step>` instead), `x-calculatedColumns.indexColumns` / `partitionColumns` / `options`, `x-columnMapping.standardizationRules`, `x-constraintHandling` keys other than `errorMode`, `x-validation` `crossFieldValidations` / `globalValidations`, `x-uniqueConstraints` `condition`, and more. `forklift.processors.schema_extensions.unsupported_extension_keys(schema)` lists everything it recognises.

**Switching it off.** `import_csv(..., apply_schema_extensions=False)`, `ImportConfig(apply_schema_extensions=False)` or `forklift ingest --no-schema-extensions` ignore all of the above; column types, null markers and `required` still apply.

A worked example. `people.csv` has a padded name, a duplicate id and a bad age:

```text
id,name,age
1,  ann ,34
2,bob,70
2,carol,41
3,dave,x
4,erin,29
```

`people_schema.json` title-cases and trims the names, adds a constant and a calculated column, and makes `id` the primary key:

```json
{
  "type": "object",
  "properties": {
    "id":   {"type": "integer"},
    "name": {"type": "string"},
    "age":  {"type": "integer"}
  },
  "x-transformations": {
    "column_transformations": {
      "name": {"string_cleaning": {"enabled": true, "strip_whitespace": true, "case_transform": "title"}}
    }
  },
  "x-calculatedColumns": {
    "constants": [{"name": "source", "value": "people.csv", "dataType": "string"}],
    "expressions": [
      {"name": "age_band", "expression": "'senior' if age >= 65 else 'adult'", "dataType": "string"}
    ]
  },
  "x-primaryKey": {"columns": ["id"]}
}
```

```python
import pyarrow.parquet as pq

import forklift

results = forklift.import_csv("people.csv", "out", schema_file="people_schema.json")

print(results.total_rows, results.valid_rows, results.invalid_rows)
print(results.schema_extensions)
print(results.validation_summary)
print(results.warnings)

for row in pq.read_table("out/data.parquet").to_pylist():
    print(row)
for row in pq.read_table(results.bad_rows_file).to_pylist():
    print(row)
```

```text
5 3 2
['x-transformations', 'x-calculatedColumns', 'x-primaryKey/x-uniqueConstraints/constraints']
{'UNIQUE_VIOLATION:id': 1}
[]
{'id': 1, 'name': 'Ann', 'age': 34, 'source': 'people.csv', 'age_band': 'adult'}
{'id': 2, 'name': 'Bob', 'age': 70, 'source': 'people.csv', 'age_band': 'senior'}
{'id': 4, 'name': 'Erin', 'age': 29, 'source': 'people.csv', 'age_band': 'adult'}
{'id': '3', 'name': 'Dave', 'age': 'x', '_rejection_reason': 'type_conversion_failed'}
{'id': '2', 'name': 'Carol', 'age': '41', '_rejection_reason': 'UNIQUE_VIOLATION:id'}
```

The names were cleaned before the types were applied (the rejected rows show `Dave` and `Carol`, not `dave` and `carol`), the first row with `id` 2 won, and `age` `x` failed type conversion. The same run from the command line prints `Schema extensions applied: x-transformations, x-calculatedColumns, x-primaryKey/x-uniqueConstraints/constraints` and, under `Findings by the schema extensions:`, `UNIQUE_VIOLATION:id: 1`.

### Header, Comment and Footer Handling

```python
import forklift

results = forklift.import_csv(
    input_path="export.csv",
    output_path="./output/",
    header_mode="auto",              # "present" (default), "absent" or "auto"; enum members work too
    header_search_rows=20,           # a header must show up within this many rows, else ValueError
    comment_rows=[r"^#"],            # regexes; only rows above the header are checked
    footer_detection={"column_index": 0, "patterns": [r"^Total"]},
)
```

- With `header_mode="absent"` the schema's property names become the columns; without a schema they are `col_1`, `col_2`, ...
- `comment_rows=None` (the default) treats only a row that is a single `#...` cell as a comment, so `# Generated 2025-01-01` is skipped while `#,name,amount` is a header. `comment_rows=[]` turns comment detection off.
- Completely empty lines are skipped everywhere. Data rows are never treated as comments.
- An empty file produces no output file; a file with only a header produces an empty `data.parquet` that carries the column names (and schema types).

### Value Statistics in the Output Metadata

`output_data_metadata.json` contains counts, null statistics, distinct counts, string lengths and the mean/standard deviation/variance of numeric columns. Statistics that copy cell values (`top_values`, numeric/temporal `min_value`/`max_value`, `median`, `mode`, `quantiles`) can expose personal data, so they are only written when you ask for them:

```python
results = forklift.import_csv(
    "sales_data.csv", "./output/", include_value_statistics=True
)
```

The metadata records base file names (no directories) as provenance. `distinct_count_is_lower_bound` is `true` when a column has more distinct values than the tracking limit (the ratios are then `null`), and `quantiles_are_estimated` is `true` when the quantiles come from a sample.

### What `import_csv` Does Not Do

`import_csv` has no `preprocessors` argument (the CLI's `--pre` only prints a warning). Fixed-width import (`import_fwf`) raises `NotImplementedError`. The schema extensions are CSV-only (`import_excel` and `import_sql` ignore them), `x-pii` is not acted on (no masking), and the options that [Applying Schema Extensions](#applying-schema-extensions) lists as warnings are not implemented.

## Schema Generation

### Basic Schema Generation

```python
import forklift

# Generate schema from CSV (the whole file is analysed by default)
schema = forklift.generate_schema_from_csv("customer_data.csv")

# Pretty print the schema
import json
print(json.dumps(schema, indent=2))
```

Types are inferred from the text of the sampled values: `00123` stays a string, `NA` is not null, `2024-01-31` becomes `date32`, ISO timestamps become `timestamp`. Only local paths and `s3://` URIs are accepted (`http://`, `ftp://`, ... raise `ValueError`).

### Schema Generation with Analysis Options

```python
import forklift

# Generate schema with limited row analysis for large files
schema = forklift.generate_schema_from_csv(
    "large_dataset.csv",
    nrows=10000  # Analyze the first 10,000 rows; None (the default) = whole file
)

# Generate with primary key inference
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    infer_primary_key_from_metadata=True
)

# Manually specify primary key
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    user_specified_primary_key=["customer_id"]
)
```

`nrows` must be a positive integer or `None`; `0`, negative numbers and non-integers raise `ValueError`. A primary key is only inferred for a column that is 100% unique (and non-null) in the sample.

### Schema Generation with All Output Options

```python
import forklift

# Generate schema to stdout (default)
schema = forklift.generate_schema_from_csv("customer_data.csv")
print(schema)

# Generate schema and save to file
forklift.generate_and_save_schema(
    input_path="products.csv",
    output_path="products_schema.json",
    file_type="csv"
)

# Generate schema and copy to clipboard (requires pyperclip)
forklift.generate_and_copy_schema(
    input_path="products.csv",
    file_type="csv"
)

# Generate with all metadata options: any SchemaGenerationConfig field can be passed
forklift.generate_and_save_schema(
    "data.csv",
    "detailed_schema.json",
    "csv",
    nrows=5000,                           # Analyze first 5000 rows
    include_sample_data=True,             # x-sample rows (opt-in, copies cell values)
    infer_primary_key_from_metadata=True, # Auto-infer primary key
    enum_threshold=0.1,                   # Suggest enums for low-cardinality columns
    uniqueness_threshold=0.95,            # Flag highly unique fields
    top_n_values=10,                      # Top/bottom values (needs include_value_statistics)
    include_value_statistics=True,        # Also write top/bottom values, min/max, quantiles
)
```

### Privacy of the Generated Schema

By default the generated `x-metadata` contains **no raw cell values**: it has row/null counts, distinct counts, uniqueness ratios, type information, mean/standard deviation/variance, outlier counts, string-length statistics and `enum_suggestions` that say a column *looks* categorical (`is_enum_candidate`, `confidence`, `distinct_count`, ...). Set `include_value_statistics=True` (CLI: `--include-value-stats`) to add `top_values`, `bottom_values`, `suggested_enum_values`, `min_value`, `max_value`, `median`, `range` and `quantiles` (keys like `quantile_25` and `quantile_99_5`). Sample rows (`x-sample`) are a separate opt-in (`include_sample_data`). `x-generation.source_file` holds the file name only.

### Using a Generated Schema for an Import

A generated schema can be passed straight to `import_csv`, but two things now matter because `import_csv` applies the schema's extensions. The suggested `x-transformations.column_transformations` steps are all written with `"enabled": false`, so they change nothing until you enable them. An inferred `x-primaryKey` is enforced: a later file with a duplicate or NULL key has those rows rejected (`UNIQUE_VIOLATION:<column>`, `NULL_VIOLATION:<column>`). The keys the generator adds for documentation only (`x-transformations.version`, `global_settings` and `transformation_types`, `x-primaryKey.inference_metadata`) are not read; the import reports them in `results.warnings`.

### CLI Schema Generation to Different Outputs

```bash
# Generate to stdout (default)
forklift generate-schema data.csv --file-type csv

# Generate and save to file
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Generate and copy to clipboard
forklift generate-schema data.csv --file-type csv --output clipboard

# Generate with all options
forklift generate-schema data.csv \
  --file-type csv \
  --nrows 5000 \
  --include-sample \
  --infer-primary-key \
  --enum-threshold 0.1 \
  --uniqueness-threshold 0.95 \
  --top-n-values 15 \
  --include-value-stats \
  --output file \
  --output-path detailed_schema.json

# Also write the column metadata to a separate file
forklift generate-schema data.csv --file-type csv --metadata-output data_metadata.json

# Excel schema generation (--sheet takes a sheet name)
forklift generate-schema financial_data.xlsx \
  --file-type excel \
  --sheet "Summary" \
  --output file \
  --output-path excel_schema.json

# Parquet schema generation
forklift generate-schema existing_data.parquet \
  --file-type parquet \
  --output clipboard
```

`--nrows` defaults to the whole file.

## Data Reading and Analysis

The `read_*` functions run the same pipeline as the `import_*` functions into a temporary directory and return a `DataFrameReader`. Call `as_pyarrow()`, `as_pandas()` or `as_polars()` on it.

### Reading for Quick Analysis

```python
import forklift

# Read CSV into a pandas DataFrame (needs the pandas extra)
df = forklift.read_csv("sales_data.csv").as_pandas()
print(df.head())
print(df.info())

# Read with specific encoding
df = forklift.read_csv("legacy_data.csv", encoding="latin-1").as_pandas()

# Read with custom delimiter
df = forklift.read_csv("pipe_delimited.txt", delimiter="|").as_pandas()
```

Only the accepted rows are returned. Rows that went to `bad_rows.parquet` (values that did not convert, empty required columns, rows rejected by the schema extensions, ...) are not in the DataFrame; call `forklift.import_csv(...)` and read `results.bad_rows_file` if you need them. `read_csv` applies the schema extensions like `import_csv` does, so the DataFrame has the renamed and calculated columns (with the `people_schema.json` example: `id`, `name`, `age`, `source`, `age_band` and 3 rows); pass `apply_schema_extensions=False` to get the file as it is. A file that has a header but no data rows gives an empty DataFrame with the header's columns.

### Cleaning Up

A `DataFrameReader` owns the temporary Parquet files it was built from. Use it as a context manager (or call `close()`) to delete them as soon as you are done; otherwise they are removed when the interpreter exits.

```python
import forklift

with forklift.read_csv("sales_data.csv", schema_file="sales_schema.json") as reader:
    table = reader.as_pyarrow()
    df = reader.as_pandas()
# the temporary files are gone here; reader.as_pandas() would now raise ValueError
```

Convert before closing: a polars `LazyFrame` (`as_polars(lazy=True)`) reads the files when you `collect()` it, so it stops working once the reader is closed. The files are deliberately not removed when the reader is garbage collected, because a lazy frame may outlive it.

### Reading Excel Files

```python
import forklift

# Read a specific sheet (name or 0-based index)
df = forklift.read_excel("quarterly_report.xlsx", sheet="Q1").as_pandas()
```

To skip metadata rows above the header, describe the sheet in an Excel schema (see [Excel and SQL Sources](#excel-and-sql-sources)) and pass `schema_file=`.

### Reading Fixed-Width Files

`forklift.read_fwf()` and `forklift.import_fwf()` raise `NotImplementedError`: fixed-width import is not wired into the engine yet. `forklift ingest --input-kind fwf` exits with status 2 for the same reason. The parsing classes (`forklift.inputs.fwf`, `forklift.schema.fwf`) can be used directly; see [X_FWF_DOCUMENTATION](../schemas/X_FWF_DOCUMENTATION.md).

### Loading Data into Different DataFrame Libraries

```python
import forklift
import polars as pl

# Read and convert to Pandas DataFrame
df_pandas = forklift.read_csv("sales_data.csv").as_pandas()
print(df_pandas.head())

# Read and convert to Polars DataFrame
df_polars = forklift.read_csv("sales_data.csv").as_polars()
print(df_polars.head())

# Read and convert to Polars LazyFrame for lazy evaluation
lf_polars = forklift.read_csv("large_dataset.csv").as_polars(lazy=True)
result = lf_polars.filter(pl.col("amount") > 100).collect()

# Read and convert to PyArrow Table (no extra package needed)
table_arrow = forklift.read_csv("sales_data.csv").as_pyarrow()
print(table_arrow.schema)
```

### DataFrame Conversion Examples by Format

#### CSV to Different Formats

```python
import forklift

# With schema validation during reading
reader = forklift.read_csv("customer_data.csv", schema_file="customer_schema.json")

# Convert to pandas for analysis
df_pandas = reader.as_pandas()
print(f"Pandas DataFrame: {df_pandas.shape}")
print(df_pandas.dtypes)

# Convert to polars for performance
df_polars = reader.as_polars()
print(f"Polars DataFrame: {df_polars.shape}")
print(df_polars.dtypes)

# Convert to pyarrow for columnar operations
table_arrow = reader.as_pyarrow()
print(f"PyArrow Table: {table_arrow.num_rows} rows, {table_arrow.num_columns} columns")
```

#### Excel to Different Formats

```python
import forklift
import polars as pl

# Excel with sheet specification
reader = forklift.read_excel("financial_data.xlsx", sheet="Q4_Results")

# Convert to pandas (common for Excel analysis)
df = reader.as_pandas()
print(df.describe())

# Convert to polars for faster processing
df_polars = reader.as_polars()
aggregated = df_polars.group_by("category").agg([
    pl.col("amount").sum().alias("total_amount"),
    pl.col("amount").mean().alias("avg_amount")
])
```

### Lazy Processing with Polars

```python
import forklift
import polars as pl

# Read large file with lazy evaluation
reader = forklift.read_csv("very_large_file.csv")
lazy_frame = reader.as_polars(lazy=True)

# Chain operations without loading full dataset
result = (
    lazy_frame
    .filter(pl.col("status") == "active")
    .with_columns([
        pl.col("amount").cast(pl.Float64),
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    ])
    .group_by("category")
    .agg([
        pl.col("amount").sum().alias("total"),
        pl.col("amount").count().alias("count")
    ])
    .sort("total", descending=True)
    .collect()  # Execute the lazy operations
)

print(result)
reader.close()  # delete the temporary files once the lazy query has been collected
```

### Working with Spark (via PyArrow)

```python
import forklift
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("ForkliftData").getOrCreate()

# Read data through Forklift and convert to PyArrow
with forklift.read_csv("large_dataset.csv", schema_file="schema.json") as reader:
    arrow_table = reader.as_pyarrow()

# Convert PyArrow table to Spark DataFrame (arrow_table.to_pandas() needs pandas)
spark_df = spark.createDataFrame(arrow_table.to_pandas())

print(f"Spark DataFrame with {spark_df.count()} rows")
spark_df.show(5)

# Process with Spark
result = spark_df.groupBy("category").agg({"amount": "sum", "id": "count"})
result.show()
```

### Advanced DataFrame Integration

#### Custom Processing Pipeline

```python
import forklift
import polars as pl

def process_sales_data(file_path: str, schema_path: str):
    """Process sales data with validation and return clean dataframe."""

    # Read with validation
    with forklift.read_csv(file_path, schema_file=schema_path) as reader:
        # Convert to polars for efficient processing
        df = reader.as_polars()

    # Clean and transform data (columns declared as string in the schema stay strings)
    cleaned_df = (
        df
        .with_columns([
            # Clean currency columns
            pl.col("amount").str.replace_all(r"[$,]", "").cast(pl.Float64),
            # Standardize dates
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
            # Add calculated columns
            (pl.col("amount") * pl.col("tax_rate")).alias("tax_amount")
        ])
        .filter(pl.col("amount") > 0)  # Remove invalid amounts
        .sort("date")
    )

    return cleaned_df

# Use the pipeline
sales_df = process_sales_data("sales.csv", "sales_schema.json")
print(f"Processed {sales_df.height} valid sales records")
```

#### Memory-Efficient Large File Processing

```python
import forklift
import polars as pl

def process_large_file_efficiently(file_path: str):
    """Process large files using lazy evaluation."""

    # Read with forklift validation, convert to lazy polars
    reader = forklift.read_csv(file_path)
    lazy_df = reader.as_polars(lazy=True)

    # Define processing pipeline (not executed yet)
    pipeline = (
        lazy_df
        .filter(pl.col("status").is_in(["active", "pending"]))
        .with_columns([
            pl.col("created_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("amount").cast(pl.Float64)
        ])
        .group_by([pl.col("created_date").dt.year(), "category"])
        .agg([
            pl.col("amount").sum().alias("total_amount"),
            pl.col("id").count().alias("record_count")
        ])
    )

    # Execute only when needed, then delete the temporary files
    try:
        return pipeline.collect()
    finally:
        reader.close()

# Process without loading the full file into memory
summary = process_large_file_efficiently("huge_dataset.csv")
print(summary)
```

### Chaining with Data Analysis Libraries

```python
import forklift
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

# Read and process data
df = forklift.read_csv("sales_data.csv").as_polars()

# Quick polars analysis
summary = df.group_by("region").agg([
    pl.col("sales").sum().alias("total_sales"),
    pl.col("sales").mean().alias("avg_sales")
])

# Convert to pandas for visualization
pandas_df = summary.to_pandas()

# Create visualization
plt.figure(figsize=(10, 6))
sns.barplot(data=pandas_df, x="region", y="total_sales")
plt.title("Sales by Region")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## Validation and Error Handling

### Basic Validation

```python
import forklift
import pyarrow.parquet as pq

# Import with validation: bad rows are set aside, processing continues
results = forklift.import_csv(
    input_path="customer_data.csv",
    output_path="./output/",
    schema_file="customer_schema.json"
)

# Check for validation issues
if results.invalid_rows > 0:
    print(f"Found {results.invalid_rows} invalid rows")
    bad_rows = pq.read_table(results.bad_rows_file)   # every column is a string
    print(bad_rows.to_pandas().head())                # or bad_rows.to_pylist()
```

A row is rejected when

- a value cannot be converted to the type the schema declares for its column (for example `"Yes"` in a `boolean` column, or `"abc"` in an `integer` column),
- a column listed under `required` is null or an empty string,
- it has more fields than the header and `excess_column_mode` is `REJECT`, or
- a schema extension rejects it: `x-validation`, `x-primaryKey` / `x-uniqueConstraints` (duplicate or NULL key) or a per-property constraint (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique`). See [Applying Schema Extensions](#applying-schema-extensions).

When the schema configures `x-validation` or a constraint, `bad_rows.parquet` has a last column `_rejection_reason` that says which rule rejected each row (`type_conversion_failed`, `required_value_missing`, `too_many_fields`, `UNIQUE_VIOLATION:id`, ...); the reason never contains the cell value. `results.validation_summary` counts the findings per reason.

The run itself only stops (an exception is raised and `data.parquet` / `bad_rows.parquet` are not left behind) for problems with the input as a whole: a file that cannot be decoded with the configured `encoding`, no header found, a required column that is missing from the file, an unreadable schema, an extension that is configured incorrectly or names a column that does not exist (`ValueError`, before any output is written), `x-constraintHandling.errorMode` `fail_fast` / `fail_complete` with a violation, `x-validation` rejecting more rows than `maxBadRowsPercent`, or an I/O error. `max_validation_errors` is reserved and not enforced, so there is no "stop after N bad rows" mode.

### Excess Column Handling

Forklift provides three strategies for data rows that have more fields than the header (or, with `header_mode="absent"`, than the schema's columns). Short rows are padded with empty strings in all modes.

```python
import forklift

results = forklift.import_csv(
    input_path="data_with_extra_columns.csv",
    output_path="./output/",
    excess_column_mode="truncate",   # or "reject" / "passthrough" (enum members work too)
)
```

#### TRUNCATE Mode (Default)
Extra fields are removed and the row is kept. This is **positional truncation to the header width**, not column filtering by name. The number of affected rows is reported as `results.truncated_rows` (and logged as a warning).

**Example**:
- CSV header: `Name,Age,City`
- A data row: `Ann,41,Paris,France,555-1234`
- Result: `Ann,41,Paris`; `truncated_rows` is increased by one

The width that matters is the one of the header in the file, not the number of properties in the schema. A schema that lists fewer (or more) columns than the file does not add or remove columns; it only supplies types and `required` rules for the columns it names, matched by name.

#### REJECT Mode
Rows with extra fields are written to `bad_rows.parquet` (cut to the header width; the extra fields are not kept) and counted in `invalid_rows`.

```python
results = forklift.import_csv(
    input_path="strict_format.csv",
    output_path="./output/",
    schema_file="exact_schema.json",
    excess_column_mode="reject",
)
print(results.invalid_rows, results.bad_rows_file)
```

#### PASSTHROUGH Mode
All fields are kept, and the extra ones are named `col_4`, `col_5`, ... after their position. Because the output schema is fixed by the first batch that is written, a row that is *wider than anything seen so far* is only accepted before the first batch is written; afterwards `import_csv` raises a `ValueError` that names the data row. Use it for files whose widest row comes first, otherwise prefer `TRUNCATE` or `REJECT`.

**Example with PASSTHROUGH**:
- Input header: `Name,Age,City`; the first data row has `Ann,41,Paris,France,555-1234`
- Output columns: `Name,Age,City,col_4,col_5`

## Excel and SQL Sources

`import_excel` and `import_sql` do not apply the schema extensions that `import_csv` runs (`x-transformations`, `x-columnMapping`, `x-calculatedColumns`, `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, constraints, `x-rowHash`), and they write no `bad_rows.parquet`.

### Excel Import

```python
import forklift

# Every sheet becomes <workbook name>_<sheet name>.parquet
results = forklift.import_excel("financial_data.xlsx", "./output/")

# Import one sheet (name or 0-based index)
results = forklift.import_excel("financial_data.xlsx", "./output/", sheet="Q4_Results")
```

- `.xlsx` is read with openpyxl in read-only streaming mode and legacy `.xls` with xlrd (`pip install "forklift-etl[excel]"`). The workbook must be a local file; the output location may be on S3.
- `.xlsx` archives are checked before they are opened, and sheets are read with caps: `ExcelInputConfig` has `max_uncompressed_bytes` (1 GiB), `max_compression_ratio` (200), `max_rows` (1,048,576) and `max_cells` (10,000,000). Exceeding one raises `ValueError`. To change a limit, use `ExcelInputHandler` directly (`ExcelInputHandler(ExcelInputConfig(max_rows=...)).process_sheets(path)` yields `(sheet_name, pyarrow.Table)`; `get_sheet_info(path)` lists the sheets without reading them).
- A sheet name or index that does not exist raises `ValueError`.
- Cell values keep their Excel type; a column that mixes types is written as text. Only empty and whitespace-only cells are null by default (`NA` stays text unless the schema lists it under `x-excel.nulls`); formula cells without a cached value are null.
- Excel schemas describe sheets, header and data rows (`header.row` is 0-based, `dataStartRow` / `dataEndRow` are 1-based sheet rows, both inclusive) and column mappings. A single sheet can be written as `"x-excel": {"sheet": "Sales Data", "header": {"row": 3}}`. See the [importers readme](../../src/forklift/engine/importers/forklift.engine.importers.readme.md).

### SQL Import

```python
import forklift

results = forklift.import_sql(
    connection_string="Driver={ODBC Driver 18 for SQL Server};Server=db;Database=crm;Uid=reader;Pwd=...",
    output_path="./output/",
    schema_file="crm_tables.json",       # x-sql.tables lists the tables to export
)
```

- Install `forklift-etl[sql]` and the ODBC driver for your database. The schema file names the tables explicitly (`"x-sql": {"tables": [{"select": {"schema": "dbo", "name": "orders"}, "outputName": "orders"}]}`); a `select` with only a `pattern` is rejected, and names must look like identifiers.
- Names are checked against the database catalog and always quoted; the connection is requested read-only; the connection string is redacted (`Pwd=***`) in `metadata.json` and in error messages.
- A table that fails is aborted without leaving a partial file; the other tables still run and a `ProcessingError` is raised at the end (the partial results are on `error.results`). Pass `continue_on_error=True` to get the results back instead; failed tables are in `results.errors` and under `failed_tables` in `metadata.json`.
- The output location may be on S3 (`s3://bucket/prefix/`).

## S3 Integration

Any input or output location can be an `s3://bucket/key` URI; credentials come from the standard boto3 credential chain.

```python
import forklift

results = forklift.import_csv(
    input_path="s3://my-bucket/raw/sales.csv",
    output_path="s3://my-bucket/curated/sales/",
    schema_file="s3://my-bucket/schemas/sales_schema.json",
)

schema = forklift.generate_schema_from_csv("s3://my-bucket/raw/sales.csv", nrows=1000)
```

- **Pass S3 locations as strings.** `pathlib.Path("s3://bucket/key")` turns into `s3:/bucket/key`. Forklift recognises that form for inputs (`is_s3_path()` and the I/O handlers accept path-like objects), and raises `ValueError` for an output location that arrives collapsed, but strings are the safe choice.
- Keys may contain `?` and `#`; the key is everything after the bucket.
- Writers are all-or-nothing: if a run fails, the pending upload is aborted and nothing is published. Stale `data.parquet` / `bad_rows.parquet` objects at the output prefix are deleted when a run starts.
- Text is read with line endings preserved, so CRLF files and line breaks inside quoted fields behave as they do locally.
- Schema generation streams CSV samples from S3 and stops reading once `nrows` rows have been read; Parquet and Excel need random access, so they are first copied to a temporary local file.
- Excel workbooks are read from local files only; `import_excel` and `import_sql` can write to S3 (and accept `s3_client=` for a pre-configured client).

## Command Line

```bash
# CSV with a schema; --include-value-stats adds value-bearing statistics to the output metadata
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json
forklift ingest s3://bucket/data.csv --dest s3://bucket/out/ --input-kind csv --include-value-stats

# Excel: --sheet takes a sheet name (or a 0-based index when no sheet has that name); default is all sheets
forklift ingest book.xlsx --dest ./output/ --input-kind excel --sheet "Q4_Results"

# Header handling
forklift ingest data.csv --dest ./output/ --input-kind csv --header-mode auto

# Ignore the schema's x-... extensions (types, null markers and `required` still apply); CSV only
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json --no-schema-extensions
```

For a CSV import the summary also lists the schema extensions that were applied, the findings they counted (`Findings by the schema extensions:`, one `CODE:column: count` line each) and, on stderr, the warnings (see [Applying Schema Extensions](#applying-schema-extensions)).

Exit codes: `0` success, `1` processing failed (or the run reported errors), `2` usage errors and input kinds that are not implemented (`--input-kind fwf`). `--encoding-priority` accepts several encodings but only the first one is used; the engine does not fall back to the others.
