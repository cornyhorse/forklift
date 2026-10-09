# x-constraintHandling Documentation

## Overview
The `x-constraintHandling` extension chooses what happens when a constraint is violated during processing. It covers three kinds of constraints, all checked by the same validator: the primary key (`x-primaryKey`), the unique constraints (`x-uniqueConstraints`, per-property `x-unique`) and the per-property value constraints (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`). The only setting that is read is `errorMode`.

## Schema Structure
```json
{
  "x-constraintHandling": {
    "description": "Configuration for handling constraint violations and data quality issues",
    "errorMode": "bad_rows"
  }
}
```

## Configuration Properties

### `errorMode` (optional)
- **Type**: String (case-insensitive)
- **Description**: How violating rows are handled
- **Values**:
  - `"bad_rows"`: Route violating rows to `bad_rows.parquet` and keep going
  - `"fail_fast"`: Stop with a `ValueError` at the first violation
  - `"fail_complete"`: Check the whole file, then raise one `ValueError` that reports the number of violations
- **Default**: `"bad_rows"`
- Any other value (the old documentation's `"ignore"` and `"transform"` included) raises a `ValueError` that names the valid values, before any output is written
- With `fail_fast` and `fail_complete` no output file is left behind (`data.parquet` and `bad_rows.parquet` are removed)

### `description` (optional)
- **Type**: String
- **Description**: Free text, ignored

### Not implemented (ignored with a warning)

Every other key is ignored and reported in `results.warnings` (`x-constraintHandling.<key> is not supported and is ignored`). They were part of earlier versions of this page; no processor reads them:

| Key | What happens instead |
| --- | --- |
| `primaryKeyViolations` (`duplicates`, `nulls`), `uniqueConstraintViolations`, `notNullViolations` | `errorMode` applies to all constraints. A value equal to the `errorMode` (for example `"bad_rows"` with `errorMode: "bad_rows"`) is not reported; `keep_first` / `keep_last` / `generate_id` / `deduplicate` / `fill_default` and the like do not exist (the first row of a key is always the one kept) |
| `badRowsOutput` | Rejected rows always go to `bad_rows.parquet` with the reason column described below; there is no format, row limit or summary option |
| `validationOptions` (`continueOnError`, `collectAllErrors`, `maxErrorsPerRow`) | A row lists every reason it was rejected for (joined by `; `); the number of violations kept in memory is bounded by `ConstraintConfig.max_retained_violations` (1000) and the counts stay exact |

## In `import_csv`

### Constraints and reasons

| Constraint | Source | Reason (`_rejection_reason`, `validation_summary`) |
| --- | --- | --- |
| Duplicate key | `x-primaryKey`, `x-uniqueConstraints`, `x-unique` | `UNIQUE_VIOLATION:<first key column>` |
| NULL key | `x-primaryKey` (unless `allowNulls`) | `NULL_VIOLATION:<column>` |
| Value too small / large | `minimum` / `maximum` of a property | `RANGE_VIOLATION:<column>` |
| Text too short / long | `minLength` / `maxLength` | `LENGTH_VIOLATION:<column>` |
| No match | `pattern` (unanchored search; anchor with `^...$`) | `PATTERN_VIOLATION:<column>` |
| Not allowed | `enum` | `ENUM_VIOLATION:<column>` |

Notes:
- The per-property keywords are matched by header name, like the rest of `properties`; their reasons use the output name. NULL passes the value constraints (use `required` for that). Other JSON Schema keywords (`exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`, `format`, `const`, ...) are not enforced
- A violating row in `bad_rows.parquet` is shown as the input file had it; the reason never contains the value
- A row that breaks several constraints lists all of them, joined by `; `
- The stage runs after `x-validation`, so the constraints only see the rows that passed it. A row that violates anything does not claim its unique keys
- With `bad_rows` (the default) the first row of a key stays and the later rows with that key are rejected
- `x-constraintHandling` alone (without a key or constraint) checks nothing, but an invalid `errorMode` is still an error

### Example

```
id,name,age
1,Ann,30
2,Bob,41
2,Bo,22
3,Di,60
```

with `{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "age": {"type": "integer"}}, "x-primaryKey": {"columns": ["id"]}, "x-constraintHandling": {"errorMode": "bad_rows"}}`:

| `data.parquet` | id | name | age |
| --- | --- | --- | --- |
| | 1 | Ann | 30 |
| | 2 | Bob | 41 |
| | 3 | Di | 60 |

| `bad_rows.parquet` | id | name | age | _rejection_reason |
| --- | --- | --- | --- | --- |
| | 2 | Bo | 22 | UNIQUE_VIOLATION:id |

With `"errorMode": "fail_fast"` the import raises `ValueError: Constraint 'id_unique' violated (row 2); 1 violation(s) in the batch` (the row is a 0-based position in its batch) and leaves no output; with `"fail_complete"` it raises `ValueError: Constraint validation failed with 1 violations` after the last row.

## Bad Rows Output Structure

`bad_rows.parquet` has all-string columns in the shape (names, order) of the input file and, when a constraint or `x-validation` is configured, a last column `_rejection_reason`:

| id | name | age | _rejection_reason |
| --- | --- | --- | --- |
| 2 | Bo | 22 | UNIQUE_VIOLATION:id |

There is no row number, timestamp or source file in it. Rows rejected by the type conversion or the `required` check are in the same file, with the reasons `type_conversion_failed` and `required_value_missing` (and `too_many_fields` with `excess_column_mode="reject"`).

## Implementation Details

### Constraint Validation Pipeline
1. **Key and NULL checks**: `x-primaryKey` (NULL and duplicate keys), `x-uniqueConstraints`, `x-unique`
2. **Value checks**: `minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum` on the typed values
3. **Routing**: `bad_rows` removes the violating rows from the batch; `fail_fast` raises at the first violation; `fail_complete` keeps every row and raises at the end of the file

### Memory Management
- The set of keys seen so far is kept for the whole import (one set per distinct constraint)
- The violations kept in memory are bounded (the first 1000); the total count is exact
- No cell values are stored in a violation

### Error Reporting
- `results.validation_summary`: count per `CODE:column`
- `results.invalid_rows`: number of rejected rows
- `bad_rows.parquet`: the rows, with the reasons
- The CLI prints `Findings by the schema extensions:` with the counts

## Usage Examples

### Strict Validation (Fail Fast)
```json
{
  "x-constraintHandling": {
    "errorMode": "fail_fast"
  }
}
```

### Check Everything, Then Fail
```json
{
  "x-constraintHandling": {
    "errorMode": "fail_complete"
  }
}
```

### Default: Collect Bad Rows
```json
{
  "x-constraintHandling": {
    "errorMode": "bad_rows"
  }
}
```

## Integration with Other Features

### Primary Key Integration
- Works with `x-primaryKey` configuration
- Enforces primary key constraints defined in schema (duplicates and NULLs follow the same `errorMode`)

### Unique Constraints Integration
- Works with `x-uniqueConstraints` definitions and per-property `x-unique`
- One mode for all constraints; there is no per-constraint handling

### x-validation
- `x-validation` does not use `errorMode`: its rejected rows always go to `bad_rows.parquet`, and its own threshold (`badRowsHandling.maxBadRowsPercent`) stops the import when too many rows fail, see [x-validation](./X_VALIDATION_DOCUMENTATION.md)

### Metadata Integration
- The counts are in `results.validation_summary` and in `metadata.json` (`validation_summary`)

## Performance Considerations

1. **Memory Usage**: Constraint tracking requires memory proportional to the number of distinct keys
2. **Processing Speed**: Validation adds overhead, especially for patterns
3. **Bad Rows Storage**: Large numbers of violations create a large `bad_rows.parquet`; use `fail_fast` / `fail_complete` or `x-validation`'s threshold to stop early

## Best Practices

1. **Start with `bad_rows`**: look at `bad_rows.parquet` and `validation_summary` before deciding to fail
2. **Fail where bad data must not load**: use `fail_fast` (stops quickly) or `fail_complete` (reports the full count)
3. **Monitor Bad Rows**: alert on high `invalid_rows` counts
4. **Review Patterns**: analyse the reasons to improve data sources
5. **Test Configurations**: validate constraint handling with sample data
