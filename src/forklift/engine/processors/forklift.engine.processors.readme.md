# Forklift Engine Processors

## Overview

The **Processors** module is a core component of the Forklift data processing engine that handles the actual transformation and processing of data after it has been imported. Processors are responsible for taking raw data and converting it into clean, validated output formats with comprehensive metadata and error handling.

## Role in the Forklift Ecosystem

Forklift follows a modular architecture with distinct responsibilities:

1. **Importers** (`/importers/`) - Handle reading data from various sources (CSV, Excel, SQL, etc.)
2. **Processors** (`/processors/`) - Transform and validate the imported data 
3. **Config** (`/config/`) - Manage configuration and settings
4. **Core Engine** (`forklift_core.py`) - Orchestrates the entire workflow

### Processing Pipeline

```
Raw Data → Importer → Processor → Validated Output
                         ↓
                    Metadata & Manifests
```

Processors sit between the raw imported data and the final output, providing:
- **Schema validation**, constraint checking and (CSV) the schema's `x-...` extensions through `extensions.py`
- **Batch processing** for memory-efficient handling of large datasets
- **Header detection** and column mapping
- **Error handling** with separate good/bad data streams
- **Output generation** in multiple formats (Parquet, JSON metadata, manifests)

## Processor Components

### Core Interfaces

#### `base.py`
Legacy abstract base class for processors. Contains a minimal interface definition that has been superseded by `base_processor.py`.

**Key Components:**
- `BaseProcessor` (ABC) - Abstract interface requiring `process()` method implementation

#### `base_processor.py` 
The primary abstract base class that defines the processor interface used throughout Forklift.

**Key Components:**
- `BaseProcessor` (ABC) - Main abstract base class for all data processors
- `process()` method signature - Takes `ImportConfig` and returns `ProcessingResults`

**Usage Pattern:**
```python
class MyProcessor(BaseProcessor):
    def process(self, config: ImportConfig) -> ProcessingResults:
        # Implementation here
        pass
```

### Main Processors

#### `csv_processor.py`
The primary processor implementation for CSV data processing. This is the most comprehensive processor in the system.

**Key Features:**
- **Streaming Processing** - Uses PyArrow for memory-efficient batch processing
- **S3 Integration** - Supports both local files and S3 input/output
- **Header Detection** - Automatic detection of header rows
- **Schema Validation** - Validates data against JSON schemas
- **Schema Extensions** - Runs the schema's `x-...` extensions through the `ExtensionPipeline` (see `extensions.py` below)
- **Error Separation** - Splits valid and invalid data into separate output streams
- **Metadata Generation** - Creates comprehensive metadata about processed data
- **Manifest Creation** - Generates file manifests for output tracking

**Main Workflow:**
1. Initialize components (schema processor, header detector, batch processor)
2. Load schema from file (if provided)
3. Detect header row location and column names
4. Build the schema extension pipeline (`extensions.py`) from the schema and the header; a misconfigured extension raises `ValueError` here, before any output is written (skipped when `ImportConfig.apply_schema_extensions` is false)
5. Process data in streaming batches: pre stage (null markers, `x-transformations`), type conversion, `required` check, post stage (mapping, calculated columns, validation, constraints, row hash)
6. Write valid/invalid data to separate Parquet files
7. Finalize the pipeline (`errorMode: fail_complete` raises here) and generate metadata and manifest files

**Key Methods:**
- `process()` - Main processing orchestration method
- `_detect_header_row()` - Determines header location
- `_validate_batch()` - Validates data against schema (required columns are looked up by name;
  null or, for text columns, empty values reject the row)
- `_build_extension_pipeline()` - Builds the `ExtensionPipeline` (`None` when `apply_schema_extensions` is false or there is no schema); copies its warnings and the names of the active extensions into the results
- `_create_s3_manifest()` / `_create_s3_metadata()` - Output file generation

**Outputs and failure behaviour:**
- `data.parquet` holds accepted rows (with the columns the schema extensions renamed, added or hashed);
  `bad_rows.parquet` holds rejected rows (failed type conversion, missing required value, excess fields in
  REJECT mode, and the rows `x-validation`, keys and constraints reject) as strings, with the input's column
  names. When the pipeline has `x-validation` or a constraint (`ExtensionPipeline.rejects_rows`) the file gets
  a last column `_rejection_reason` for every row: `type_conversion_failed`, `too_many_fields`,
  `required_value_missing` or `CODE` / `CODE:column` (several joined by `; `, cut at 200 characters, never a
  cell value).
  `ProcessingResults.bad_rows_file` names it; it is also still listed in `output_files`.
- Outputs of an earlier run (those two file names) are removed when a run starts.
- On any error the partial `data.parquet`/`bad_rows.parquet` is discarded (local file removed,
  S3 upload not completed), the error is recorded in `results.errors` (Arrow messages are
  stripped of row content) and re-raised.
- A header without data rows produces an empty `data.parquet` carrying the schema.
- `manifest.json` (file names and sizes) and `metadata.json` (processing summary, including
  `truncated_rows`, and `schema_extensions`, `validation_summary` and `warnings` from the schema
  extension pipeline) are written next to the data. `output_data_metadata.json` (column statistics
  from `OutputMetadataCollector`, taken from the final output rows) is written when `create_metadata`
  is on and at least one row was accepted. It contains no cell values unless `ImportConfig.include_value_statistics=True`, and its
  provenance (`input_path`, `schema_file`, output files) is recorded as base names. The data files
  are finished before it is written, so a failure to write it is logged and appended to
  `results.errors` without raising.

### Specialized Processing Components

#### `batch_processor.py`
Handles the core batch processing logic for streaming large datasets efficiently.

**Key Features:**
- **PyArrow Integration** - Creates streaming RecordBatch readers
- **Memory Management** - Processes data in configurable batch sizes
- **Column Mismatch Handling** - Deals with rows having different column counts
- **Footer Detection** - Stops processing when footers are detected
- **S3 Streaming** - Fallback processing for S3 inputs
- **Data Corruption Detection** - Identifies and handles corrupted data

**Typing and null handling:** schema columns are read as raw strings and converted per batch
by `type_conversion.ColumnConverter`, on both the Arrow path and the S3/fallback row path, so
both produce the same Parquet schema (`00123` stays `00123` for a `string` column). A row whose
value cannot be converted is passed to the reject handler instead of aborting the stream.
Columns outside the schema keep Arrow inference on the local path (strings on the row path); if
Arrow's column-count check forces the fallback reader mid-file, rows already delivered are
skipped and the remaining batches are cast to the schema established so far.

**Hooks for the schema extensions:** `BatchProcessor(..., pre_convert=hook)` calls `hook(batch)` on every
raw (all-text) batch before the schema types are applied (the pipeline's pre stage: null markers,
`x-transformations`, hidden row-id / input-hash columns; it must keep the row count). When the reject handler
is called, `last_reject_reason` says why (`type_conversion_failed` or `too_many_fields`); the CSV processor
stores it in the `_rejection_reason` column.

**Key Methods:**
- `create_batch_reader()` - Creates PyArrow streaming reader for local files
- `create_s3_batch_reader()` - Unified interface for both local and S3 files
- `_handle_column_mismatch_reader()` - Handles inconsistent column counts
- `_convert_rows_to_batch()` - Converts row data to PyArrow RecordBatch
- `_create_filtered_file()` - Creates temporary files with footers removed

**Excess Column Handling Modes:**
- `REJECT` - Rows with extra columns go to `bad_rows.parquet` (cut to the header width)
- `TRUNCATE` - Remove excess columns from rows; the count is `results.truncated_rows`
- `PASSTHROUGH` - Keep all columns, extending the schema until the first batch is written;
  a wider row after that raises a `ValueError` naming the data row number

Blank lines are skipped on every path.

#### `type_conversion.py`
`ColumnConverter` applies the schema's column types and null markers (`x-csv.nulls`) to each
batch and splits off rows that cannot be converted. `parse_arrow_type()` reads
`x-csv.parquetTypeMapping` type names (`int32`, `decimal128(10,2)`, `timestamp[us]`, ...); a mapping
entry wins over the JSON `type`/`format`. Nested types and anything text cannot be converted to stay
`string`. `to_string_batch()` produces the all-string shape of `bad_rows.parquet`.
`ColumnConverter.mark_nulls(batch)` applies only the null markers (it is `convert`'s first step), so the
pipeline's pre stage can let `x-transformations` see NULL where the file said `NA`, `-` or `0.00`.

#### `extensions.py`
`ExtensionPipeline` turns the `x-...` extensions of the schema into the processors of `forklift.processors`
and runs them on every batch of a CSV import; `build_extension_pipeline(schema, header_names, ...)` creates it
(or returns `None` when the schema asks for nothing and there is nothing to warn about). Import it from
`forklift.engine.processors.extensions`.

Order per batch (`pre_convert` runs on the raw text, `post_convert` on the typed rows that passed `required`):

| Stage | Names | Step |
|---|---|---|
| PRE | header | hidden row-id / input-hash columns (only if `x-rowHash` asks); `x-csv` null markers; `x-transformations` + automatic `x-special-type` formatting |
| engine | header | type conversion (`properties`, `x-csv.parquetTypeMapping`), `required` |
| POST | output | `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality` (findings only), `x-validation` (drops rows), constraints (`x-primaryKey`, `x-uniqueConstraints`, per-property constraints, `x-constraintHandling.errorMode`; drop rows), `x-rowHash` (appends columns, last) |

`post_convert` returns a `PostStageResult` (`kept`, `rejected`, `reasons`): `rejected` has the rows in the
shape they had when they entered the post stage, so `bad_rows.parquet` keeps the input's column names.
`finalize()` lets `errorMode: fail_complete` raise after the last batch. Attributes: `warnings` (set when
building), `summary` (counts of non-valid results per `CODE` / `CODE:column`, at most 200 distinct keys and
then `OTHER`), `applied` (names of the active extensions), `rejects_rows` (whether `bad_rows.parquet` gets
`_rejection_reason`). `describe()` is what goes into `metadata.json`.

Building checks the references before anything is written: `x-primaryKey` / `x-uniqueConstraints` naming a
column the file lacks, or `x-validation` / `x-dataQuality` naming a column that is neither in the file nor
in `properties`, raise `ValueError`; a column declared in `properties` that this file lacks only adds a
warning and the rules for it (`x-validation`, `x-dataQuality`, per-property constraints) are left out. Calculated or hash columns that would replace an existing
column raise `ValueError`, as do header names starting with `__forklift_` when `x-rowHash` or
`x-transformations` is used. Content that no processor reads is returned as warnings
(`schema_extensions.unsupported_extension_keys`). Only CSV imports build a pipeline: `import_excel`,
`import_sql` and `import_fwf` do not.

#### `text_utils.py`
`read_encoding()` (reads plain UTF-8 as `utf-8-sig` so a byte order mark never sticks to the first
column name) and `sanitize_arrow_error()` (removes row content from Arrow error messages before they
are logged or stored in `results.errors`).

#### `header_detector.py`
Specialized component for detecting and extracting header information from CSV files.

**Key Features:**
- **Multiple Detection Modes** - AUTO, PRESENT, ABSENT
- **Comment Row Handling** - Skips rows matching comment patterns
- **Footer Detection** - Stops processing when footers are found
- **S3 Support** - Works with both local files and S3 objects
- **Pattern Matching** - Uses regex patterns for flexible detection

**Header Modes:**
- `PRESENT` - Header expected at first non-comment row
- `ABSENT` - No header present, use schema names or generate `col_1`..`col_N`
- `AUTO` - Automatically detect header by analyzing content patterns

A `ValueError` is raised if no header is found within `header_search_rows` rows. Only an empty
(or blank/comment-only) file yields `(-1, [])`. A UTF-8 byte order mark is ignored. With
`comment_rows=None` a row that is a single `#...` cell is a comment, while `#,name,amount` is a
header; `comment_rows=[]` disables comment detection.

**Key Methods:**
- `detect_header_row()` - Main header detection orchestration
- `_find_first_data_row()` - Locates first non-comment data row
- `_auto_detect_header()` - Analyzes multiple rows to identify header
- `_looks_like_header()` - Determines if a row appears to be a header
- `should_stop_for_footer()` - Footer detection logic

#### `schema_processor.py`
Manages schema loading, conversion, and validation operations.

**Key Features:**
- **JSON Schema Support** - Loads and parses JSON schema files
- **PyArrow Conversion** - Converts JSON schemas to PyArrow schemas
- **S3 Schema Loading** - Supports schemas stored in S3
- **Metadata Configuration** - Extracts metadata generation settings from schema
- **Row Hash Configuration** - Handles primary key and hash configuration

**Key Methods:**
- `load_schema()` - Loads schema from file (local or S3)
- `_json_schema_to_pyarrow()` - Converts JSON schema to PyArrow format
- `_json_type_to_pyarrow()` - Maps JSON types to PyArrow data types
- `get_column_names_from_schema()` - Extracts column names from schema
- `get_metadata_config()` - Gets metadata generation configuration

**Schema extensions read here:**
- `x-rowHash` - exposed through `get_row_hash_config()` / `has_row_hash_config()` (nothing in the engine calls them; the row hash is built from the schema dictionary by the extension pipeline)
- `x-metadata-generation` - Metadata collection settings

All other `x-...` processing (transformations, mapping, calculated columns, validation, keys and constraints) is built from `schema_dict` by `extensions.py`.

### Empty/Placeholder Files

#### `batch_converter.py`
Currently empty - likely intended for future batch conversion functionality.

#### `validator.py`
Currently empty - likely intended for future advanced validation logic.

## Configuration Integration

Processors work closely with the configuration system (`ImportConfig`) to:
- **Processing Parameters** - Batch size, encoding, delimiters
- **Validation Settings** - Schema file paths, validation modes
- **Output Configuration** - File paths, compression, metadata generation
- **Error Handling** - How to handle validation failures and malformed data

## Error Handling and Logging

The processors implement comprehensive error handling:
- **Graceful Degradation** - Continue processing when possible
- **Error Separation** - Invalid data written to separate bad rows file
- **Detailed Reporting** - Processing statistics and error details in results
- **Resource Cleanup** - Proper cleanup of temporary files and connections

## Performance Considerations

- **Streaming Architecture** - Memory-efficient processing of large files
- **Batch Processing** - Configurable batch sizes for optimal performance
- **PyArrow Integration** - High-performance columnar processing
- **S3 Optimization** - Efficient streaming for cloud storage
- **Lazy Loading** - Components initialized only when needed

## Future Extensions

The processor architecture is designed to be extensible:
- New processor types can be added by implementing `BaseProcessor`
- Additional validation logic can be added to `validator.py`
- Batch conversion utilities can be implemented in `batch_converter.py`
- New output formats can be supported by extending existing processors
