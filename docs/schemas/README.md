# Forklift Extended JSON Schema (x-attributes) Documentation

## Overview
This directory contains comprehensive documentation for Forklift's extended JSON schema attributes (x-attributes) that provide powerful data processing, validation, transformation, and quality management capabilities beyond standard JSON schema functionality.

## What the engine applies today

The extensions below are the schema vocabulary. **The CSV engine** (`import_csv`, `read_csv`, `forklift ingest --input-kind csv`) applies most of them to every batch it reads; the table says which part of the schema does what. Excel, SQL and fixed-width imports apply none of them (`import_excel` reads `x-excel`, `import_sql` reads `x-sql`; fixed-width import is not wired into the engine).

| Schema part | What `import_csv` does with it | Rows that fail go to `bad_rows.parquet` with reason |
| --- | --- | --- |
| Column types: `properties.<col>.type` / `format`, overridden by `x-csv.parquetTypeMapping` | Converts the (cleaned) text to the type | `type_conversion_failed` |
| `required` | A null or empty-string value rejects the row | `required_value_missing` |
| `x-csv.nulls` (`global`, `perColumn`) | Marker text becomes NULL, before `x-transformations` and again with the type conversion | - |
| `x-special-type` (`ssn`, `zip-5`/`zip-9`/`zip-permissive`, `phone`, `email`, `ipv4`/`ipv6`/`ip`, `mac-address`) | Formats and validates the text before the types are applied; an invalid value becomes NULL | - (counted as `INVALID_SPECIAL_VALUE:<col>`) |
| `x-transformations.column_transformations` | Per-column cleaning of the text, before the types are applied | - |
| `x-columnMapping` | Renames columns (`explicitMappings`, `namingConvention`, ...) | - |
| `x-calculatedColumns` | Appends constant and expression columns | - |
| `x-dataQuality` | Reports findings only (`fieldSpecificRules`, `fieldQualityRules[col].parameters`); no row is dropped | - (counted in `validation_summary`) |
| `x-validation` | `fieldValidations` (`required`, `unique`, `range`, `stringValidation`, `enumValidation`, `dateValidation`) | `VALIDATION_ERROR:<col>` |
| `x-primaryKey`, `x-uniqueConstraints` | Key columns must be unique (first row wins) and, for the primary key, not NULL | `UNIQUE_VIOLATION:<col>`, `NULL_VIOLATION:<col>` |
| Per-property `minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique` | Checked on typed values; NULL passes. Other JSON Schema keywords (`exclusiveMinimum`, `multipleOf`, `format`, ...) are not enforced | `RANGE_VIOLATION`, `LENGTH_VIOLATION`, `PATTERN_VIOLATION`, `ENUM_VIOLATION`, `UNIQUE_VIOLATION` (each `:<col>`) |
| `x-constraintHandling.errorMode` | `bad_rows` (default), `fail_fast` or `fail_complete` for the constraints above | - |
| `x-rowHash` | Appends the row hash and metadata columns (last) | - |
| `x-metadata-generation` | `enabled`, `enum_detection.uniqueness_threshold`, `statistics.categorical.top_n_values`, `statistics.numeric.quantiles` configure `output_data_metadata.json` (computed from the final data) | - |
| `x-pii` | **Not read**: no masking or hashing is applied; the import warns (`x-pii is documentation only: no masking is applied`) | - |

Read settings such as delimiter, encoding, header handling and batch size come from the keyword arguments of `import_csv` (`ImportConfig`), not from `x-csv`.

### Order of the stages

For every batch, `import_csv` runs:

```
PRE    names = the file's header names
         hidden row-number / input-hash columns          (only if x-rowHash asks for them)
         x-csv null markers -> NULL
         x-transformations, then the automatic x-special-type formatting
engine type conversion (properties types, x-csv.parquetTypeMapping)   -> bad_rows: type_conversion_failed
engine `required` check                                               -> bad_rows: required_value_missing
POST   names = the OUTPUT names (after x-columnMapping)
         x-columnMapping       renames columns
         x-calculatedColumns   appends columns
         x-dataQuality         report only
         x-validation          rejects rows
         constraints           x-primaryKey, x-uniqueConstraints, per-property constraints
                               (errorMode from x-constraintHandling)   rejects rows
         x-rowHash             appends hash and metadata columns (last)
```

**Names.** `properties`, `required`, `x-csv` and `x-transformations` use the column names *as in the file header* (they run before any rename). Every stage after `x-columnMapping` uses the *output* names; a header name that was renamed is also accepted by `x-validation`, `x-dataQuality`, the keys and the per-property constraints (it is resolved to the output name). `x-calculatedColumns` expressions and `x-rowHash.includeColumns` / `excludeColumns` see output names only. A `properties` entry declared under a rename's *new* name does not apply to the renamed column; the import warns about it.

### What you get back

- **`bad_rows.parquet`** has all-string columns in the shape (names) of the input file, with the text the input file had (not the cleaned or converted values). When `x-validation` or any constraint is configured it has an extra last column `_rejection_reason`: `CODE` or `CODE:column` (for example `UNIQUE_VIOLATION:id`, `VALIDATION_ERROR:age`; the column is the *output* name), several joined by `; `. In that case the rows rejected earlier carry the reasons `type_conversion_failed`, `too_many_fields` (with `excess_column_mode="reject"`) or `required_value_missing` in the same column. A reason never contains cell values.
- **`ProcessingResults`** has `warnings` (list of text), `validation_summary` (count per `CODE` / `CODE:column`, never values) and `schema_extensions` (names of the extensions that were applied). They are also written to `metadata.json`. The CLI prints `Schema extensions applied: ...`, `Findings by the schema extensions:` and the warnings (on stderr).
- **Warnings, not errors**, for schema content that no processor reads (the list is `unsupported_extension_keys()` in `forklift.processors.schema_extensions`): `x-pii`, `x-transformations` blocks other than `column_transformations`, `x-calculatedColumns.options` / `indexColumns` / `partitionColumns`, `x-columnMapping.standardizationRules` and the other unknown keys, `x-constraintHandling` keys other than `errorMode`, `x-validation.crossFieldValidations` / `globalValidations` / most `badRowsHandling` keys / `onViolation`, `x-dataQuality` blocks other than `fieldSpecificRules` / `fieldQualityRules`, `x-uniqueConstraints` `condition` / `ignoreNulls: false` / `caseSensitive: false`. Each feature page lists what it ignores.
- **Errors** (the import stops and leaves no output files): an extension that is configured incorrectly, a key column that is not in the file (`x-primaryKey`, `x-uniqueConstraints`), a rule for a column that is neither in the file nor in `properties` (a typo), a calculated or row-hash column that would overwrite an existing column, a header name starting with `__forklift_` when `x-rowHash` or `x-transformations` is used, `fail_fast` / `fail_complete` violations. (One more stop: more rejected rows than `x-validation.badRowsHandling.maxBadRowsPercent` allows; it discards the data file but keeps `bad_rows.parquet` and names it in the error, see [x-validation](./X_VALIDATION_DOCUMENTATION.md).) A rule for a column that is declared in `properties` but absent from this file only warns and is left out, so a wide standard can be used with narrower files.

### Switching it off

`ImportConfig(apply_schema_extensions=False)`, `import_csv(..., apply_schema_extensions=False)` or the CLI flag `--no-schema-extensions` skips every extension in the table except the column types, the null markers and `required`. The default is on.

### Example

`people.csv`:

```
id,name,age,salary
1,"  ann   LEE ","30","$55,000.00"
2,bob ray,200,"$61,000.00"
2,dup dan,41,"$70,000.00"
3,cy poe,52,"$48,500.00"
```

`schema.json`:

```json
{
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "age": {"type": "integer", "maximum": 150},
    "salary": {"type": "number"}
  },
  "required": ["id", "name"],
  "x-transformations": {
    "column_transformations": {
      "name": {"string_cleaning": {"enabled": true, "strip_whitespace": true, "collapse_whitespace": true, "case_transform": "title"}},
      "salary": {"money_conversion": {"enabled": true}}
    }
  },
  "x-primaryKey": {"columns": ["id"]},
  "x-calculatedColumns": {"constants": [{"name": "source", "value": "people.csv", "dataType": "string"}]}
}
```

`forklift ingest people.csv --dest out --input-kind csv --schema schema.json` prints (among the usual lines)

```
Schema extensions applied: x-transformations, x-calculatedColumns, x-primaryKey/x-uniqueConstraints/constraints
Findings by the schema extensions:
  RANGE_VIOLATION:age: 1
```

and writes

| `data.parquet` | id | name | age | salary | source |
| --- | --- | --- | --- | --- | --- |
| | 1 | Ann Lee | 30 | 55000.0 | people.csv |
| | 2 | Dup Dan | 41 | 70000.0 | people.csv |
| | 3 | Cy Poe | 52 | 48500.0 | people.csv |

| `bad_rows.parquet` | id | name | age | salary | _rejection_reason |
| --- | --- | --- | --- | --- | --- |
| | 2 | Bob Ray | 200 | 61000 | RANGE_VIOLATION:age |

The first row with id 2 was rejected by the `maximum` of `age`; it does not claim the key, so the later row with id 2 is kept. With `--no-schema-extensions` nothing cleans the text, `$55,000.00` is not a number for the type conversion, and every row goes to `bad_rows.parquet` (types and `required` still apply).

Statistics that copy cell values into metadata (top/bottom values, min/max, quantiles, enum value lists, samples) are never produced by a schema setting: they need `include_value_statistics=True` / `--include-value-stats`, because those values can be personal data.

## Complete Feature Set

### Core Data Integrity Features

#### [x-primaryKey](./X_PRIMARY_KEY_DOCUMENTATION.md)
Primary key configuration and constraint enforcement.

**Key Features:**
- Single and composite primary key support
- Uniqueness enforcement (the first row of a key wins, later rows go to `bad_rows.parquet`) or `fail_fast` / `fail_complete` via `x-constraintHandling`
- NULL keys are rejected unless `allowNulls` is true
- Rejected rows carry `UNIQUE_VIOLATION:<col>` / `NULL_VIOLATION:<col>` as reason

#### [x-uniqueConstraints](./X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md)
Additional unique constraints beyond primary keys for complex business rules.

**Key Features:**
- Single and multi-column unique constraints (case-sensitive)
- A key with a NULL part is never compared
- Violations are counted in `validation_summary` (`UNIQUE_VIOLATION:<col>`); `condition`, `ignoreNulls: false` and `caseSensitive: false` are not implemented (warning)

#### [x-constraintHandling](./X_CONSTRAINT_HANDLING_DOCUMENTATION.md)
What happens to rows that break a key or a per-property constraint.

**Key Features:**
- `errorMode`: `bad_rows` (default), `fail_fast`, `fail_complete`
- Applies to `x-primaryKey`, `x-uniqueConstraints` and the per-property constraints (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique`)
- Other keys (`primaryKeyViolations`, `badRowsOutput`, `validationOptions`, ...) are not implemented (warning)

### Data Type and Validation Features

#### [x-special-type](./X_SPECIAL_TYPE_DOCUMENTATION.md)
Specialized data type handling for common structured formats.

**Supported Types:**
- **Personal Identifiers:** SSN, phone numbers, email addresses
- **Geographic:** ZIP codes (5-digit, 9-digit, permissive), IP addresses (IPv4, IPv6, auto-detect)
- **Network:** MAC addresses with format standardization
- **Validation & Normalization:** `import_csv` formats the column automatically before the types are applied; an invalid value becomes NULL

#### [x-validation](./X_VALIDATION_DOCUMENTATION.md)
Field rules that reject rows, with a threshold for how many rejections are tolerated.

**Key Features:**
- Per field: `required`, `unique`, `range`, `stringValidation`, `enumValidation`, `dateValidation`
- `uniquenessHandling.strategy`: `first_wins`, `last_wins`, `fail_on_duplicate`, `mark_all_duplicates`
- `badRowsHandling.maxBadRowsPercent` (default 10) and `failOnExceedThreshold` (default true) abort the import when too many rows fail; `thresholdMode` (`end_of_file` by default, or `early`) says whether the whole input is checked before the verdict
- Rejected rows carry `VALIDATION_ERROR:<col>` as reason

#### [x-transformations](./X_TRANSFORMATIONS_DOCUMENTATION.md)
Advanced data transformation and standardization capabilities.

**Transformation steps** (configured per column under `x-transformations.column_transformations`):
- **String Cleaning** (`string_cleaning`): Unicode normalization, whitespace handling, quote standardization, case conversion with exceptions
- **Numeric Cleaning** (`numeric_cleaning`): Thousands separators, decimal handling, NaN processing
- **Money Type** (`money_conversion`): Currency symbol removal, parentheses negative notation
- **DateTime Parsing** (`datetime`): Multiple format support, fuzzy parsing, timezone handling
- **Format Transformations:** SSN, ZIP, phone, email, IP and MAC address formatting
- **HTML/XML Cleaning** (`html_xml_cleaning`): Tag stripping and entity decoding (text extraction, not a sanitizer)
- **Text replacement, padding, trimming** (`regex_replace`, `string_replace`, `string_padding`, `string_trimming`)

### Data Enhancement Features

#### [x-calculatedColumns](./X_CALCULATED_COLUMNS_DOCUMENTATION.md)
Dynamic column generation including constants, expressions, and computed fields.

**Column Types:**
- **Constants:** Static values for data lineage and versioning
- **Expressions:** Python-like safe expressions (`x if cond else y`, `coalesce`, `length`, `year`, ...), not SQL
- **Calculated Fields:** `calculated[]` entries are expressions too (the old `function` key is an alias of `expression`); there are no pre-built functions beyond the expression functions
- `partitionColumns` is recorded only, `indexColumns` and `options` are not read (warning)

#### [x-rowHash](./X_ROW_HASH_DOCUMENTATION.md)
Row-level hash generation and metadata for change detection and data integrity.

**Features:**
- **Multiple Hash Algorithms:** MD5, SHA1 (need `allowWeakHash`), SHA256, SHA384, SHA512
- **Input/Output Hashing:** Track changes made during processing
- **Metadata Columns:** Source URI, ingestion timestamps, row numbering
- **Change Detection:** Support for CDC and data quality monitoring

### Privacy and Security Features

#### [x-pii](./X_PII_DOCUMENTATION.md)
PII marking vocabulary. **Documentation only**: no code reads it, nothing is masked or hashed, and `import_csv` warns that `x-pii` is not applied.

**PII Categories:**
- **Direct Identifiers:** Names, SSNs, emails requiring strong protection
- **Quasi-Identifiers:** Birth dates, ZIP codes that can identify when combined
- **Sensitive Information:** Financial data, medical information requiring protection
- **System Identifiers:** Technical IDs with linking capability

**Masking Methods** (vocabulary, not implemented):
- **Hash Masking:** Cryptographic hashing for complete anonymization
- **Generalization:** Broader categories preserving utility
- **Range/Category:** Ranges for sensitive numeric data
- **Redaction:** Complete or partial value removal

### File Format Specific Features

#### [x-csv](./X_CSV_DOCUMENTATION.md)
Advanced CSV processing with robust parsing and type mapping.

**Features:**
- **Encoding Detection:** Multi-encoding support with priority ordering
- **Delimiter:** A single character (`auto` passes schema validation, but no automatic detection is performed)
- **Header/Footer Handling:** Flexible header detection and footer exclusion
- **Null Handling:** Global and per-column null value configuration
- **Type Mapping:** Explicit Parquet type mapping with inference fallback

#### [x-fwf](./X_FWF_DOCUMENTATION.md)
Fixed-width file processing with precise field positioning.

**Features:**
- **Field Definition:** Exact positioning with start/length specifications
- **Alignment & Padding:** Left/right/center alignment with custom padding
- **Type Conversion:** Direct mapping to Parquet types
- **Conditional Processing:** Record-type based field definitions
- **Legacy Support:** Mainframe and legacy system compatibility

### Data Quality and Analysis Features

#### [x-metadata-generation](./X_METADATA_GENERATION_DOCUMENTATION.md)
Automatic metadata analysis and statistics generation.

**Analysis Types:**
- **Enum Detection:** Automatic categorical data identification
- **Statistical Analysis:** Numeric mean / standard deviation / variance and outlier counts; min/max, median and quantiles only with `include_value_statistics`
- **String Analysis:** Length statistics (`min_length` / `max_length`) and character-class counts
- **Performance Optimization:** Streaming collection with a distinct-value tracking cap and seeded reservoir sampling for quantiles (the output metadata flags `distinct_count_is_lower_bound` and `quantiles_are_estimated`)

#### [x-dataQuality](./X_DATA_QUALITY_DOCUMENTATION.md)
Per-column quality findings. **Report only**: no row is dropped; the findings are counted in `validation_summary`.

**What is checked** (numeric columns for the value rules, text columns for the length and pattern rules):
- `fieldSpecificRules.<col>`: `min`, `max`, `pattern`
- `fieldQualityRules.<col>.parameters`: `min_length`, `max_length`, `pattern`, `min_value`, `max_value`
- `qualityThresholds`, `crossFieldValidation`, `statisticalChecks`, `reporting` and the quality dimensions are not implemented (warning). Rules that drop rows belong to [x-validation](./X_VALIDATION_DOCUMENTATION.md)

### Data Integration Features

#### [x-columnMapping](./X_COLUMN_MAPPING_DOCUMENTATION.md)
Advanced column name mapping and standardization.

**Supported keys:**
- `explicitMappings`: source name -> output name
- `namingConvention`: `snake_case`, `camelCase`, `PascalCase`, `lowercase`, `UPPERCASE`
- `caseSensitive`, `allowUnmapped`, `dropUnmapped`
- `globalMappings`, `tableMappings`, `patternMappings`, `standardization` and `validation` are not implemented (warning)

## How the extensions work together

The extensions run in the fixed order shown under [Order of the stages](#order-of-the-stages), so they combine like this:

| If you use ... | ... then ... |
| --- | --- |
| `x-csv.nulls` with `x-transformations` | the markers are applied to the text of the file first, so a transformation sees NULL where the file said `NA` |
| `x-transformations` with `x-special-type` | the explicit steps of a column run first, the automatic `x-special-type` formatting last |
| `x-transformations`, `x-special-type` with `required` | `required` is checked after them: a value that became NULL rejects the row (`required_value_missing`) |
| `x-columnMapping` with any later extension | the later extension uses the new (output) names; `properties`, `required`, `x-csv` and `x-transformations` keep the header names |
| `x-calculatedColumns` with `x-validation`, `x-dataQuality`, keys, `x-rowHash` | the calculated columns exist by then and can be validated, used as keys and hashed |
| `x-validation` with `x-primaryKey` / `x-uniqueConstraints` | `x-validation` runs first; the constraints only see the rows it kept, so a row rejected by `x-validation` is not rejected again and does not claim its key |
| `x-rowHash` | runs last: the hash covers the final columns (except `excludeColumns`); the input hash covers the raw text of the file row |

## Common Use Case Scenarios

### Enterprise Data Integration
```json
{
  "x-columnMapping": {"explicitMappings": {"emp_id": "employee_id"}},
  "x-transformations": {"column_transformations": {"emp_name": {"string_cleaning": {"enabled": true, "strip_whitespace": true}}}},
  "x-primaryKey": {"columns": ["employee_id"], "enforceUniqueness": true},
  "x-dataQuality": {"fieldSpecificRules": {"age": {"min": 0, "max": 150}}}
}
```

### Financial Data Processing
```json
{
  "properties": {
    "ssn": {"type": "string", "x-special-type": "ssn"},
    "amount": {"type": "number"}
  },
  "x-pii": {"fields": {"ssn": {"isPII": true, "category": "direct_identifier"}}},
  "x-transformations": {"column_transformations": {"amount": {"money_conversion": {"enabled": true, "currency_symbols": ["$"]}}}},
  "x-constraintHandling": {"errorMode": "fail_fast"},
  "x-primaryKey": {"columns": ["ssn"]}
}
```
(`x-pii` only documents the column: nothing is masked and the import warns about it.)

### Legacy System Migration
```json
{
  "x-columnMapping": {"explicitMappings": {"CUST_NO": "customer_id"}, "namingConvention": "snake_case"},
  "x-calculatedColumns": {"constants": [{"name": "migration_batch", "value": "2024_Q3", "dataType": "string"}]},
  "x-rowHash": {"enabled": true, "sourceUriEnabled": true}
}
```

### Data Quality Monitoring
```json
{
  "x-metadata-generation": {"enabled": true, "enum_detection": {"uniqueness_threshold": 0.1}},
  "x-dataQuality": {"fieldSpecificRules": {"salary": {"min": 0}}},
  "x-rowHash": {"enabled": true, "algorithm": "sha256"},
  "x-constraintHandling": {"errorMode": "bad_rows"}
}
```

## Getting Started

1. **Start Simple**: Begin with the column types, `required` and a key (`x-primaryKey`)
2. **Add Data Quality**: Add `x-dataQuality` findings and `x-validation` rules; read `validation_summary` and `bad_rows.parquet`
3. **Enhance Processing**: Add `x-transformations` and `x-special-type` for data standardization
4. **Advanced Features**: Add `x-columnMapping`, `x-calculatedColumns` and `x-rowHash` as needed (`x-pii` only documents columns)
5. **File-Specific**: Use `x-csv` (null markers, types) for CSV sources; the other extensions apply to CSV imports only
6. **Check the warnings**: `results.warnings` lists every part of the schema that was ignored

## Performance Considerations

- **Memory Usage**: Key and uniqueness checks (`x-primaryKey`, `x-uniqueConstraints`, `x-unique`, `unique` in `x-validation`) remember every distinct key seen so far, so memory grows with the number of distinct keys. The violations kept in memory are bounded (`ConstraintConfig.max_retained_violations`, 1000); the counts in `validation_summary` stay exact
- **Processing Speed**: Each extension adds work per batch (transformations, calculated columns and the validators iterate the values in Python); enable only what you need
- **Metadata statistics**: collected in a streaming way with a cap on tracked distinct values and reservoir sampling for quantiles (see the metadata generation page)

## Best Practices

1. **Feature Selection**: Enable only the features you need to minimize overhead
2. **Configuration Testing**: Test configurations with representative data samples
3. **Error Handling**: Choose `x-constraintHandling.errorMode` on purpose (`bad_rows` keeps going, `fail_fast` / `fail_complete` stop the import and leave no output) and set `x-validation.badRowsHandling.maxBadRowsPercent` for your data
4. **Documentation**: Document business rules and transformation logic
5. **Monitoring**: Set up monitoring for data quality metrics and processing performance
6. **Version Control**: Track changes to schema configurations over time

## Support and Troubleshooting

Each feature documentation includes:
- Detailed configuration examples
- Common use cases and patterns
- Integration guidance with other features
- Performance optimization tips
- Troubleshooting common issues
- Best practices and recommendations

For complex scenarios involving multiple features, refer to the integration examples in each feature's documentation and the table "How the extensions work together" above to understand feature interactions.
