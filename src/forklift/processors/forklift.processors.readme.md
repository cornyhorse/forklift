# Forklift Processors Package

## Overview

`forklift.processors` holds the batch processors (`BaseProcessor.process_batch(batch) -> (batch,
results)`) that validate, transform and enrich PyArrow `RecordBatch`es. Sub-packages have their own
readmes (`transformations`, `calculated_columns`, `schema_validator`, `data_validation`). This readme covers
the modules at the top of the package and `schema_extensions.py`, which builds processors from the `x-*`
extensions of a schema dictionary.

## Use by `import_csv`

`forklift.import_csv()` (and `forklift ingest --input-kind csv`) builds these processors from the schema and
runs them on every batch; the pipeline lives in `forklift.engine.processors.extensions`
(`ExtensionPipeline`). `import_excel`, `import_sql` and `import_fwf` do not use it, and you can run any of the
processors on your own PyArrow batches as well. The order per batch:

```
PRE   (header names)  hidden row-id / input-hash columns (only if x-rowHash asks for them)
                      x-csv null markers -> NULL
                      SchemaBasedTransformer: x-transformations, then the automatic x-special-type formatting
engine                type conversion, then the required check
POST  (output names)  ColumnMapper               x-columnMapping
                      CalculatedColumnsProcessor x-calculatedColumns
                      DataQualityProcessor       x-dataQuality (findings only)
                      DataValidationProcessor    x-validation (drops rows)
                      ConstraintValidator        x-primaryKey, x-uniqueConstraints, per-property constraints,
                                                 x-constraintHandling.errorMode (drops rows)
                      RowHashProcessor           x-rowHash (appends columns, last)
```

Rows dropped by `DataValidationProcessor` and `ConstraintValidator` are written by the engine to
`bad_rows.parquet`, with a `_rejection_reason` built from each result's `error_code` and `column_name`
(`UNIQUE_VIOLATION:id`, `VALIDATION_ERROR:age`); the processors themselves write no files. The engine adds
the counts of all non-valid results to `ProcessingResults.validation_summary` and the output of
`unsupported_extension_keys` to `ProcessingResults.warnings`.
`ImportConfig(apply_schema_extensions=False)` leaves all of this out.

## Schema extension loaders (`schema_extensions.py`)

| Extension | Loader | Processor |
|---|---|---|
| `x-columnMapping` | `build_column_mapper(schema)` | `ColumnMapper` |
| `x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`, per-property constraints | `build_constraint_validator(schema, *, resolve_column)` | `ConstraintValidator` |
| `x-validation` | `build_data_validator(schema, *, resolve_column)` | `DataValidationProcessor` |
| `x-dataQuality` | `build_quality_processor(schema, *, resolve_column)` | `DataQualityProcessor` |

(`x-transformations`, `x-calculatedColumns` and `x-rowHash` have their own factories.)

Helpers: `referenced_columns(schema)` lists the columns each extension refers to (as written in the
schema), `unsupported_extension_keys(schema)` lists the extension content that no processor applies.

Rules shared by all loaders:

- A loader returns `None` when the extension is absent or has nothing to apply (for example
  `enforceUniqueness: false` with `allowNulls: true`, or an empty `fieldValidations`).
- Invalid configuration (wrong type or shape, impossible values, invalid or unsafe regular expressions,
  `min` greater than `max`, unknown enum values, ...) raises `ValueError("x-<extension>...: <problem>")`
  that names the offending key. Messages never contain data values.
- Keys that no processor reads do **not** raise - the shipped standard files contain such keys. They are
  reported, one human-readable line each, by `unsupported_extension_keys(schema)`; empty blocks,
  blocks switched off with `enabled: false` and values that equal what happens anyway
  (`onViolation: "bad_rows"`) are not reported.
- Naming: the schema `properties` use the file **header** names, the processors run on the **output** names
  (after the column mapping). `resolve_column(name) -> output name` (default: identity) is applied to every
  column name in the extension; names that already are output names must be returned unchanged. For the
  mapping use `mapper.output_names(header_names)`.

### `x-columnMapping`

```json
{"x-columnMapping": {
  "explicitMappings": {"FirstName": "first_name"},
  "namingConvention": "snake_case",
  "caseSensitive": true,
  "allowUnmapped": true,
  "dropUnmapped": false
}}
```

`namingConvention`: `snake_case`, `camelCase`, `PascalCase`, `lowercase` or `UPPERCASE`. Columns without an
explicit mapping are dropped when `dropUnmapped` is true **or** `allowUnmapped` is false (an identity mapping
such as `"A": "A"` counts as mapped). Not supported (ignored, reported): `standardizationRules` and the
keys of the old documentation (`globalMappings`, `tableMappings`, `patternMappings`, `standardization`,
`validation`).

`ColumnMapper.output_names(names) -> {input name: output name or None}` computes, without data, exactly the
renaming `process_batch` applies (explicit mappings, naming convention, custom transform, dropping) and
raises the same `ValueError` for two columns that would get the same output name.

### `x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`

```json
{"x-primaryKey": {"columns": ["id"], "type": "single", "enforceUniqueness": true, "allowNulls": false},
 "x-uniqueConstraints": [{"name": "u_pair", "columns": ["a", "b"], "description": "..."}],
 "x-constraintHandling": {"errorMode": "bad_rows"}}
```

- `x-primaryKey.columns` (required, non-empty list of distinct names) must be unique - a single name, or a
  tuple for a composite key - unless `enforceUniqueness` is false (default true); the key columns must not be
  NULL unless `allowNulls` is true (default false). A NULL in any column of a key rejects the row. `type`
  is optional (`single`/`composite`) and must agree with the number of columns.
- `x-uniqueConstraints[].columns` is a unique constraint (a tuple when it has several columns). `name` is
  informational but must be unique. Rows with a NULL in a unique key are not compared (SQL semantics).
- The same key defined more than once (also in another column order, or as a per-property `x-unique`) is
  checked once.
- `x-constraintHandling.errorMode` is `bad_rows` (default; violating rows are removed from the batch),
  `fail_fast` (the first violation raises) or `fail_complete` (rows are kept, `finalize()` raises). It applies
  to all constraints of the validator. Anything else (the documentation's `ignore` and `transform` included)
  raises `ValueError`.
- The per-property keywords `minimum`, `maximum`, `enum`, `pattern` (unanchored search), `minLength`,
  `maxLength` and `x-unique` of `properties` are checked as well; their names are header names and go through
  `resolve_column`. NULL passes the value constraints.
- Not supported (ignored, reported): `x-uniqueConstraints[].condition`, `ignoreNulls: false` and
  `caseSensitive: false`; `x-constraintHandling` keys other than `errorMode` (`primaryKeyViolations`,
  `uniqueConstraintViolations`, `notNullViolations`, `badRowsOutput`, `validationOptions`).

Every violation is a `ValidationResult` with `row_index` (position in the batch passed in),
`error_code` (`UNIQUE_VIOLATION`, `NULL_VIOLATION`, `RANGE_VIOLATION`, `ENUM_VIOLATION`,
`PATTERN_VIOLATION`, `LENGTH_VIOLATION`) and `column_name` (the first column of the constraint). Violations
are retained up to `ConstraintConfig.max_retained_violations` (1000), `violation_count` is exact, and cell
values are not kept (`include_values=False`).

### `x-validation`

See the `data_validation` readme: `fieldValidations` (`required`, `unique`, `range`, `stringValidation`,
`enumValidation`, `dateValidation`), `uniquenessHandling.strategy` and
`badRowsHandling.maxBadRowsPercent`/`failOnExceedThreshold`. The processor drops rejected rows, does not write
files and does not accumulate them. Ignored and reported: `badRowsHandling.outputPath`, `fileFormat`,
`includeOriginalRow`, `includeValidationErrors` (and `enabled: false`), `fieldValidations.<f>.onViolation`
(other than `bad_rows`), `crossFieldValidations`, `globalValidations`.

### `x-dataQuality`

Report only: the processor returns the batch unchanged and reports violations as `ValidationResult`s with
`row_index`, `error_code` and `column_name`; its messages contain no cell values. Two shapes are read, per
column:

```json
{"x-dataQuality": {
  "fieldSpecificRules": {"age": {"min": 0, "max": 150}, "email": {"pattern": "^[^@]+@[^@]+$"}},
  "fieldQualityRules": {"code": {"parameters": {"min_length": 8, "max_length": 8, "pattern": "^[A-Z]+", "min_value": 0, "max_value": 9}}}
}}
```

`fieldSpecificRules` (the standard's shape): `min` -> `min_value`, `max` -> `max_value`, `pattern`.
`fieldQualityRules[col].parameters` (the documentation's shape): `min_length`, `max_length`, `pattern`,
`min_value`, `max_value`, used as they are. `enabled: false` switches the extension off. Not supported
(ignored, reported): the blocks `completeness`, `uniqueness`, `consistency`, `accuracy`,
`qualityThresholds`, `crossFieldValidation`, `statisticalChecks`, `reporting`, and per column `dataType`,
`required`, `standardizeFormat`, `rules`, `severity` and the other `parameters`.

`DataQualityProcessor(rules, allow_unsafe_regex=False, *, include_values=True)` keeps cell values in its
pattern and range messages for backward compatibility; the loader builds it with `include_values=False`.

### Other extensions

`unsupported_extension_keys` also reports: `x-pii` (documentation only, no masking is applied),
`x-transformations` keys other than `column_transformations` (with the matching
`column_transformations.<column>.<step>` as a hint) and `x-calculatedColumns.options` / `indexColumns` (not
read) and `partitionColumns` (recorded only).
