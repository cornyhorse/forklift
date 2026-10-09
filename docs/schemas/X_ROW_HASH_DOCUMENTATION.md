# x-rowHash Documentation

## Overview
The `x-rowHash` extension provides comprehensive row-level hash generation and metadata column capabilities for change detection, data integrity verification, and audit trail creation. This feature enables tracking of data lineage, detecting changes between processing runs, and adding processing metadata to output files.

> **Breaking change - hash encoding version 2 (default).** Row hashes are now computed from an
> *injective* encoding of the row (column names, type tags, length-prefixed values, an explicit NULL
> marker). Hashes produced by earlier releases (the `||`-joined text encoding, now called
> `hashVersion` 1) will **not** match the new values. To keep verifying or comparing against hashes
> you stored earlier, set `"legacyEncoding": true` (or `"hashVersion": 1`); new data should use the
> default. Never compare hashes of different versions. `md5` and `sha1` now require
> `"allowWeakHash": true`. See [Hash encoding versions](#hash-encoding-versions).

## Schema Structure
```json
{
  "x-rowHash": {
    "description": "Configuration for generating row-level hash columns and metadata for change detection and data integrity",
    "enabled": false,
    "columnName": "row_hash",
    "algorithm": "sha256",
    "allowWeakHash": false,
    "legacyEncoding": false,
    "includeColumns": null,
    "excludeColumns": [],
    "nullValue": "NULL",
    "separator": "||",
    "inputHashEnabled": false,
    "inputHashColumnName": "_input_hash",
    "sourceUriEnabled": false,
    "sourceUriColumnName": "_source_uri",
    "ingestedAtEnabled": false,
    "ingestedAtColumnName": "_ingested_at_utc",
    "rowNumberEnabled": false,
    "sourceRowNumberColumnName": "_rownum_in_source_file",
    "processingRowNumberColumnName": "_rownum",
    "description_detail": "Row hash and metadata columns disabled by default. When enabled, can generate: output row hash (SHA256 default), input row hash, source URI, ingestion timestamp, and row numbers. Supports MD5, SHA1, SHA256, SHA384, SHA512 algorithms."
  }
}
```

## Configuration Properties

### Row Hash Generation

#### `enabled`
- **Type**: Boolean
- **Description**: Enable or disable row hash generation
- **Default**: `false`
- **Implementation**: When `true`, generates hash column for each row

#### `columnName`
- **Type**: String
- **Description**: Name of the generated hash column
- **Default**: `"row_hash"`
- **Implementation**: Column added to output with computed hash values

#### `algorithm`
- **Type**: String
- **Description**: Cryptographic algorithm for hash generation
- **Values**: `"md5"`, `"sha1"`, `"sha256"`, `"sha384"`, `"sha512"`
- **Default**: `"sha256"`
- **Recommendation**: Use SHA256 or higher for security and collision resistance
- **Weak algorithms**: `md5` and `sha1` are only accepted together with `"allowWeakHash": true`; without it the configuration is rejected

#### `allowWeakHash`
- **Type**: Boolean
- **Description**: Explicit opt-in that permits `md5` / `sha1`
- **Default**: `false`

#### `legacyEncoding` / `hashVersion`
- **Type**: Boolean / integer (`1` or `2`)
- **Description**: `legacyEncoding: true` (equivalently `hashVersion: 1`) reproduces the pre-2.0 preimage byte for byte so previously stored hashes can be verified. The default is `hashVersion` 2.
- **Default**: `false` / `2`
- **Recorded in output**: the hash column's Arrow field carries the metadata keys `forklift.row_hash.version` and `forklift.row_hash.algorithm` (they are written to the Parquet schema), and the processor exposes `config.hash_version` and `get_hash_info()`.

#### `includeColumns`
- **Type**: Array of strings or null
- **Description**: Specific columns to include in hash calculation
- **Default**: `null` (include all columns)
- **Implementation**: When specified, only listed columns are used for hash

#### `excludeColumns`
- **Type**: Array of strings
- **Description**: Columns to exclude from hash calculation
- **Default**: `[]`
- **Implementation**: Specified columns are not included in hash computation
- **Use Cases**: Exclude timestamp columns, processing metadata, or frequently changing fields

#### `nullValue`
- **Type**: String
- **Description**: String representation for null values in hash calculation
- **Default**: `"NULL"`
- **Implementation**: Null values converted to this string before hashing. **Only used with `legacyEncoding`** - hash version 2 has an explicit NULL marker that cannot collide with any value.

#### `separator`
- **Type**: String
- **Description**: Separator between field values in hash input
- **Default**: `"||"`
- **Implementation**: Fields concatenated with this separator before hashing. **Only used with `legacyEncoding`** - hash version 2 length-prefixes every field instead.

### Input Hash Tracking

#### `inputHashEnabled`
- **Type**: Boolean
- **Description**: Generate hash of original input row before transformations
- **Default**: `false`
- **Use Cases**: Track changes made during processing, detect transformation impacts
- **Implementation**: The hash covers *every* column of the input batch (in its schema order, under its own name), with the same algorithm and encoding version as the row hash. The processor needs that batch: pass `input_batch` (it must have exactly the rows of the batch being processed) or, when rows were dropped since, a precomputed `input_hash` array (see [Pipelines that drop rows](#pipelines-that-drop-rows-and-rename-columns)). Enabling it and supplying neither raises `ValueError` (the column is never silently left out).

#### `inputHashColumnName`
- **Type**: String
- **Description**: Column name for input hash
- **Default**: `"_input_hash"`

### Source Metadata

#### `sourceUriEnabled`
- **Type**: Boolean
- **Description**: Add column with source file/database URI
- **Default**: `false`
- **Use Cases**: Data lineage tracking, source identification

#### `sourceUriColumnName`
- **Type**: String
- **Description**: Column name for source URI
- **Default**: `"_source_uri"`

### Processing Metadata

#### `ingestedAtEnabled`
- **Type**: Boolean
- **Description**: Add timestamp column showing when data was processed
- **Default**: `false`
- **Use Cases**: Audit trails, processing time tracking

#### `ingestedAtColumnName`
- **Type**: String
- **Description**: Column name for ingestion timestamp
- **Default**: `"_ingested_at_utc"`
- **Format**: UTC timestamp in ISO 8601 format

### Row Numbering

#### `rowNumberEnabled`
- **Type**: Boolean
- **Description**: Add row number columns
- **Default**: `false`

#### `sourceRowNumberColumnName`
- **Type**: String
- **Description**: Column name for the source row number: the **1-based position of the row among the data rows of the source** (the header line is not a data row)
- **Default**: `"_rownum_in_source_file"`
- **Implementation**: Without further input the processor counts the rows it receives, starting at `source_row_offset` + 1 (`set_source_context(uri, source_row_offset)`; the offset is 0 unless you want to count e.g. the header line). That equals the position in the source only while no row was dropped before the processor; otherwise pass `source_row_numbers` to `process_batch` (see [Pipelines that drop rows](#pipelines-that-drop-rows-and-rename-columns)). Supplied positions are written as given; `source_row_offset` is not added to them.

#### `processingRowNumberColumnName`
- **Type**: String
- **Description**: Column name for processing order row number: the 1-based count of the rows this processor has emitted
- **Default**: `"_rownum"`
- **Implementation**: Sequential numbering of the rows the processor receives, continuing across batches (an empty batch consumes no numbers); it is independent of `source_row_numbers`. Both row-number columns restart at 1 whenever a new source is started (`set_source_context`)

## Implementation Details

### Hash Calculation Process
1. **Column Selection**: Determine which columns to include based on configuration
2. **Value Preparation**: Encode every value (see below); NULL gets its own marker
3. **Concatenation**: Build one byte string per row from the encoded fields
4. **Hash Generation**: Apply specified algorithm to that byte string
5. **Encoding**: Convert hash to hexadecimal string representation

### Hash encoding versions

#### Version 2 (default)
For every hashed column, in schema order, the preimage contains

```
len(name) | name | type-tag | len(payload) | payload        (NULL: len(name) | name | 0x00)
```

with 8-byte big-endian lengths, preceded by a fixed header and the column count. Type tags: `s` string,
`b` binary, `i` integer (decimal text, any width), `o` boolean, `f` float (IEEE-754 bytes, one NaN, no
negative zero), `d` decimal, `t` date/time/timestamp/duration (storage integer + Arrow type) and `x`
for nested types (canonical JSON). Consequences:
- `("Ann", "Lee||X", "111")` and `("Ann||Lee", "X", "111")` no longer collide
- NULL and the string `"NULL"` differ; empty bytes and NULL differ
- the integer `1` and the string `"1"` differ
- the same values under different column names (or in another column order) differ

#### Version 1 (`legacyEncoding: true`)
The original format, kept so stored hashes can still be verified:
```
column1_value||column2_value||NULL||column4_value
```
Known weaknesses: values containing the separator collide, `"NULL"` equals NULL, empty binary values
hash as NULL, column names and types are not part of the preimage.

#### Failure behaviour
The processor fails closed: it never returns a batch without a requested column. It raises
`ValueError` when

- hashing or adding a metadata column fails, or a metadata column (`row_hash`, `_input_hash`,
  `_source_uri`, ...) already exists in the batch;
- `inputHashEnabled` is on but neither `input_batch` nor `input_hash` was given, or they do not match
  the rows of the batch;
- `sourceUriEnabled` / `ingestedAtEnabled` is on but `set_source_context` was not called;
- `enabled` is on but no column is left to hash (`includeColumns` names no column of the batch, or
  `excludeColumns` removes all of them). `includeColumns` entries that are not in the batch are
  otherwise ignored.

`ProcessorPipeline` passes the batch as it entered the pipeline to processors that accept an
`input_batch` argument.

### Pipelines that drop rows and rename columns

The input hash and the source row number describe the row *as it entered the pipeline*. When the
processor runs last in a per-batch pipeline in which rows were dropped (type conversion, validation,
uniqueness) and columns were renamed or added, the caller computes both up front and carries them
along, aligned with the surviving rows:

```python
processor = create_row_hash_processor_from_schema(config)       # the inner x-rowHash dict
processor.set_source_context("file:///data/in.csv")

input_hash = processor.compute_input_hash(raw_batch)             # before any row is dropped
positions = pa.array(range(first, first + raw_batch.num_rows), pa.int64())  # 1-based, per source
...                                                              # drop rows: filter both the same way
out, _ = processor.process_batch(
    final_batch,
    input_hash=input_hash.filter(keep_mask),
    source_row_numbers=positions.filter(keep_mask),
)
```

- `compute_input_hash(input_batch) -> pa.Array`: one hash per row (string array); exactly the value
  `process_batch` stores in the input-hash column for that row when given the same batch as
  `input_batch` (same algorithm, encoding version and column selection: all columns of the batch).
  It needs no source context and changes no processor state.
- `process_batch(batch, input_batch=None, *, input_hash=None, source_row_numbers=None)`:
  - `input_hash`: precomputed string array with one non-null hash per row of `batch` (a length
    mismatch raises `ValueError`). It is used instead of computing from `input_batch`, which may
    then be omitted or have a different length.
  - `source_row_numbers`: integer array (stored as `int64`) with one non-null value >= 1 per row of
    `batch`: the 1-based position of each row among the data rows of the source. It is used for the
    source row number column instead of the internal counter. The processing sequence column keeps
    counting the rows the processor receives.
  - Both arguments may be `pa.Array` or `pa.ChunkedArray`, and are validated even if the matching
    option is off (then they are ignored).
- Existing calls (`process_batch(batch)`, `process_batch(batch, input_batch=...)`,
  `set_source_context`) behave as before.
- Empty batches: `process_batch` on a batch with 0 rows returns the same columns, types and field
  metadata (hash version and algorithm) as for a non-empty batch, so an empty output can be given its
  schema by processing an empty batch; `get_output_schema(input_schema)` returns that schema without
  a batch. A batch with 0 rows consumes no row numbers.
- `row_hash_output_columns(config_dict)` (in `forklift.processors.row_hash_factory`) returns the
  names of the columns the processor adds for an `x-rowHash` dictionary, in order (`columnName`,
  `inputHashColumnName`, `sourceUriColumnName`, `ingestedAtColumnName`, then
  `sourceRowNumberColumnName` and `processingRowNumberColumnName`, each only when its option is on;
  an empty list when the factory would create no processor). The processor's `output_columns()` gives
  the same list. Use it to compute the final output schema and to detect name collisions with the data
  columns before processing; invalid configuration raises the same `ValueError` as the factory.
- The row hash covers every column of the batch it receives (except its own metadata columns and
  `excludeColumns`); a caller that carries helper columns in the batch has to remove them first or
  list them in `excludeColumns`.

#### Migrating
Hashes are only comparable within one version. When switching an existing change-detection pipeline
to version 2, recompute the stored baseline (or keep `legacyEncoding: true` until you can).

### Change Detection Workflow
1. **Initial Load**: Generate hashes for all rows
2. **Subsequent Loads**: Generate hashes for new data
3. **Comparison**: Compare hashes to detect:
   - New records (hash not in previous dataset)
   - Changed records (same key, different hash)
   - Unchanged records (same hash)
   - Deleted records (hash missing from new dataset)

### Performance Considerations
- **Hash Algorithm Speed**: MD5 fastest, SHA512 slowest
- **Column Selection**: Fewer columns = faster processing
- **Memory Usage**: Hash comparison requires storing previous hashes
- **String Concatenation**: Large text fields increase processing time

## Use Cases

### Change Data Capture (CDC)
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "sha256",
    "excludeColumns": ["last_modified", "_ingested_at_utc"],
    "ingestedAtEnabled": true
  }
}
```

### Data Quality Monitoring
```json
{
  "x-rowHash": {
    "enabled": true,
    "inputHashEnabled": true,
    "algorithm": "sha256",
    "sourceUriEnabled": true,
    "rowNumberEnabled": true
  }
}
```

### Audit Trail Creation
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "sha512",
    "ingestedAtEnabled": true,
    "sourceUriEnabled": true,
    "rowNumberEnabled": true,
    "excludeColumns": ["processing_timestamp"]
  }
}
```

### Performance-Optimized Configuration
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "md5",
    "allowWeakHash": true,
    "includeColumns": ["id", "name", "status", "amount"],
    "ingestedAtEnabled": false,
    "rowNumberEnabled": false
  }
}
```

## Integration Examples

### With Primary Key Validation
```json
{
  "x-primaryKey": {
    "columns": ["customer_id"],
    "enforceUniqueness": true
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["customer_id"],
    "description": "Hash excludes primary key to focus on data changes"
  }
}
```

### With PII Masking
```json
{
  "x-pii": {
    "fields": {
      "ssn": {"isPII": true, "category": "direct_identifier"}
    }
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["ssn"],
    "description": "Hash excludes PII fields to avoid privacy issues"
  }
}
```

### With Calculated Columns
```json
{
  "x-calculatedColumns": {
    "constants": [
      {"name": "batch_id", "value": "batch_001"}
    ]
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["batch_id", "_ingested_at_utc"],
    "ingestedAtEnabled": true,
    "description": "Hash excludes processing metadata"
  }
}
```

## Output Example

With full metadata enabled:

| customer_id | name | email | row_hash | _input_hash | _source_uri | _ingested_at_utc | _rownum_in_source_file | _rownum |
|-------------|------|-------|----------|-------------|-------------|------------------|------------------------|---------|
| 1 | John Doe | john@example.com | a1b2c3d4... | x9y8z7w6... | file:///data/customers.csv | 2024-08-26T10:30:00Z | 1 | 1 |
| 3 | Jim Roe | jim@example.com | e5f6g7h8... | v5u4t3s2... | file:///data/customers.csv | 2024-08-26T10:30:00Z | 3 | 2 |

The second source row (customer 2) was dropped before the hash step: `_rownum_in_source_file` is the
position among the source's data rows (1, 3) while `_rownum` counts the rows the processor emitted
(1, 2). Without `source_row_numbers` the two columns would both read 1, 2.

## Best Practices

### Algorithm Selection
- **MD5**: Fast, suitable for change detection in trusted environments (requires `allowWeakHash: true`)
- **SHA1**: Deprecated for security, avoid for new implementations (requires `allowWeakHash: true`)
- **SHA256**: Good balance of security and performance, recommended default
- **SHA384/SHA512**: Maximum security, use for sensitive data or compliance requirements

### Column Selection Strategy
1. **Include Business Data**: Focus on columns that represent actual data changes
2. **Exclude Metadata**: Remove processing timestamps, batch IDs, etc.
3. **Exclude Volatile Fields**: Remove frequently changing non-business fields
4. **Include Key Fields**: Consider including primary/foreign keys for context

### Change Detection Workflow
1. **Baseline Creation**: Generate hashes for initial dataset
2. **Hash Storage**: Store hashes in metadata database or comparison files
3. **Incremental Processing**: Compare new hashes against stored baseline
4. **Action Planning**: Define actions for new, changed, and deleted records

### Performance Optimization
1. **Algorithm Choice**: Use fastest algorithm that meets security requirements
2. **Column Minimization**: Include only necessary columns in hash
3. **Batch Processing**: Process hashes in batches for large datasets
4. **Parallel Processing**: Leverage multiple cores for hash generation

### Security Considerations
1. **Algorithm Security**: Use SHA256 or higher for production systems
2. **Salt Usage**: Consider adding salt for additional security (custom implementation)
3. **Hash Storage**: Protect stored hashes if they could reveal sensitive information
4. **Access Control**: Limit access to hash comparison results

## Troubleshooting

### Common Issues
1. **Hash Instability**: Different hash values for same logical data
   - **Cause**: Inconsistent null handling, floating-point precision
   - **Solution**: Standardize null representation, round floating-point values

2. **Performance Issues**: Slow hash generation
   - **Cause**: Large text fields, complex algorithm, too many columns
   - **Solution**: Optimize column selection, use faster algorithm

3. **Memory Usage**: High memory consumption during processing
   - **Cause**: Storing too many hashes for comparison
   - **Solution**: Implement streaming comparison, process in batches

### Debugging Tips
1. **Hash Verification**: Compare hash inputs to identify differences
2. **Performance Profiling**: Measure hash generation time per row
3. **Column Impact Analysis**: Test hash generation with different column sets
4. **Algorithm Comparison**: Benchmark different algorithms with actual data
