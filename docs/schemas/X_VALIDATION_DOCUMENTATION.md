# x-validation Documentation

## Overview
The `x-validation` extension defines field rules that **reject rows**: a missing value, a duplicate, a number outside a range, text that breaks a length or pattern rule, a value outside a list, or a date outside a period. `import_csv` checks every batch, writes the rejected rows to `bad_rows.parquet` with the reason `VALIDATION_ERROR:<column>`, and stops the import when more than a configured share of the rows is rejected.

It is the extension to use for business rules that drop rows. `x-dataQuality` only reports, and `x-primaryKey`, `x-uniqueConstraints` and the per-property constraints (`minimum`, `pattern`, `enum`, ...) are handled by `x-constraintHandling`.

## Schema Structure
This is the shape used by the shipped standard `schema-standards/20250826-csv.json` (excerpt):

```json
{
  "x-validation": {
    "description": "Comprehensive field validation rules with bad row handling for data quality enforcement",
    "badRowsHandling": {
      "enabled": true,
      "maxBadRowsPercent": 10.0,
      "failOnExceedThreshold": true
    },
    "uniquenessHandling": {
      "strategy": "first_wins",
      "options": ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
    },
    "fieldValidations": {
      "id": {
        "required": true,
        "unique": true,
        "range": {"min": 1, "max": 999999999, "inclusive": true},
        "onViolation": {"required": "bad_rows", "unique": "bad_rows", "range": "bad_rows"}
      },
      "age": {
        "required": false,
        "unique": false,
        "range": {"min": 0, "max": 150, "inclusive": true},
        "onViolation": {"range": "bad_rows"}
      },
      "birth_date": {
        "required": false,
        "unique": false,
        "dateValidation": {
          "minDate": "1900-01-01",
          "maxDate": "2100-12-31",
          "format": ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
        },
        "onViolation": {"dateValidation": "bad_rows"}
      },
      "category": {
        "required": true,
        "unique": false,
        "enumValidation": {"allowedValues": ["A", "B", "C"], "caseSensitive": true},
        "onViolation": {"required": "bad_rows", "enumValidation": "bad_rows"}
      }
    }
  }
}
```

## Configuration Properties

### `enabled`

Top-level `enabled: false` turns the whole block off (default `true`).

### `badRowsHandling`

#### `maxBadRowsPercent`
- **Type**: Number from 0 to 100 (anything else raises a `ValueError`)
- **Default**: `10`
- **Description**: The import is aborted when the rows rejected by `x-validation` exceed this percentage of the rows that reached `x-validation`. The comparison uses "greater than": exactly 10% is still accepted, 10.1% is not. When it is made depends on [`thresholdMode`](#thresholdmode)
- **Failure**: `BadRowsThresholdExceededError` (a `RuntimeError`). `data.parquet` is not kept, but `bad_rows.parquet` is: it is finished and readable, holds every row rejected so far (all of them with `end_of_file`; only those rejected before the import stopped with `early`) as the input file had them, with the `_rejection_reason` column, and its path is in the message and in `error.bad_rows_file`. The message gives the counts and, with the default `thresholdMode`, the findings by rule, for example: `Bad rows (44.0%) exceed threshold (10.0%): 44 of 100 rows that were validated by x-validation were rejected. Findings by rule: VALIDATION_ERROR:age x34, VALIDATION_ERROR:name x15. The whole input was checked and the import was aborted, so no data file was kept. To keep the data of the accepted rows as well (the rejected rows are in bad_rows.parquet either way) set x-validation.badRowsHandling.failOnExceedThreshold to false; ... All rows rejected by the checks are in out/bad_rows.parquet (the _rejection_reason column says why).`
- Rows rejected before `x-validation` (type conversion, `required`) are in neither the numerator nor the denominator; rows rejected after it (constraints) are in the denominator only

#### `thresholdMode`
- **Type**: `"end_of_file"` or `"early"` (anything else raises a `ValueError` that lists the choices)
- **Default**: `"end_of_file"`
- **`end_of_file`**: the whole input is checked first and the share of rejected rows is compared with `maxBadRowsPercent` once, at the end. The verdict is the same whatever the order of the rows and the `batch_size`, and the error can say what was wrong across the whole file. The price is that a hopeless input is read to the end before the import fails
- **`early`**: the share is compared after every batch, against the rows seen so far, and the import stops at the first batch that goes over the limit. Faster for hopeless input, but a bad start (3 bad rows in the first 10 of a file that is 3% bad overall) fails an import that would end under the limit, so the result depends on where the bad rows are and on `batch_size`

#### `failOnExceedThreshold`
- **Type**: Boolean
- **Default**: `true`
- **Description**: With `false` the threshold is not enforced: the rows are rejected and the import finishes

#### Other `badRowsHandling` keys
`enabled: true` is accepted. `enabled: false`, `outputPath`, `fileFormat`, `includeOriginalRow`, `includeValidationErrors` and any other key are **not implemented**: rejected rows are always removed and written by the import to `bad_rows.parquet` (in the shape of the input file, with `_rejection_reason`). Each one is reported in `results.warnings`.

### `uniquenessHandling`

#### `strategy`
- **Type**: String
- **Default**: `first_wins`
- **Description**: What happens to the rows of a key that a `unique` field rule sees more than once
- **Values**:
  - `first_wins`: the first valid row of a key is kept, later rows are rejected
  - `fail_on_duplicate`: the same result as `first_wins` (the duplicates are rejected, the import does not fail)
  - `last_wins`: within a batch the last valid row of a key is kept and the earlier ones are rejected; rows already written by earlier batches cannot be taken back, so a later duplicate of such a key is rejected
  - `mark_all_duplicates`: every row of a key that occurs more than once is rejected, the first one included (also when the key was seen in an earlier batch)
- Any other value raises a `ValueError`. `options` (the list of the values above) is accepted and ignored

A row claims its keys only when it passes all the rules, so a row rejected for another reason never makes a later valid row a duplicate. NULL and blank values are not keys.

### `fieldValidations`
- **Type**: Object with column names as keys (the output names, after `x-columnMapping`; a renamed header name is accepted)
- **Description**: One entry of rules per column. An entry that checks nothing (`"required": false, "unique": false`, empty blocks) is ignored

| Rule | Keys | Rejects the row when |
| --- | --- | --- |
| `required` (default `false`) | | the value is NULL, empty or only whitespace |
| `unique` (default `false`) | | another row has the same value (see `uniquenessHandling`) |
| `range` | `min`, `max` (either may be left out), `inclusive` (default `true`) | the value is below `min` or above `max`; with `inclusive: false` the bounds themselves are rejected too |
| `stringValidation` | `minLength`, `maxLength`, `pattern`, `allowEmpty` (default `true`) | the text is shorter / longer, does not match `pattern`, or is empty with `allowEmpty: false` |
| `enumValidation` | `allowedValues` (required, non-empty), `caseSensitive` (default `true`) | the value is not in the list |
| `dateValidation` | `minDate`, `maxDate`, `format` (one strptime pattern or a list) | the value is not a date in one of the `format` patterns (default `%Y-%m-%d`), or is before `minDate` / after `maxDate` |
| `onViolation` | | accepted when every value is `bad_rows` (what happens anyway); any other value is **not implemented** and warns |

Details:
- **NULL** skips every rule except `required`. Empty and whitespace-only text are values: the length, pattern, enum, range and date rules apply to them. In a `string` column an empty cell is the empty string, not NULL, unless `x-csv.nulls` lists `""`
- **Range**: compares exact decimals; numbers, numeric text (`" 7 "`) and dates are accepted as values and as bounds (ISO text such as `"2020-01-01"` for dates); text that is not a number or date is rejected. `min` greater than `max`, or equal with `inclusive: false`, raises a `ValueError`
- **Strings**: `pattern` is an unanchored search, as in JSON Schema (anchor with `^...$`). A number is checked as its text. An invalid pattern, or one with nested unbounded quantifiers such as `(a+)+`, raises a `ValueError`
- **Dates**: a typed date column is compared as a date; text is parsed with the `format` patterns and rejected when none fits. `minDate` / `maxDate` are ISO dates (or in a `format` pattern) and `minDate` after `maxDate` raises a `ValueError`
- A violation never puts the cell value in the reason

### Not implemented (ignored with a warning)
`crossFieldValidations`, `globalValidations`, per-field `dataType` and any unknown key, in addition to the `badRowsHandling` keys and `onViolation` values named above. They are listed in `results.warnings` (`x-validation.<key> is not supported and is ignored`); the other rules still run. The shipped standard no longer contains them.

## In `import_csv`

- **Stage**: after `x-columnMapping`, `x-calculatedColumns` and `x-dataQuality`, before the constraints (`x-primaryKey`, `x-uniqueConstraints`, per-property constraints) and `x-rowHash`. The rules see typed values and the output names. The constraints only see the rows that passed `x-validation`, so a row rejected here is not rejected again there and does not claim its keys
- **Reason**: `VALIDATION_ERROR:<column>` in `_rejection_reason` and `validation_summary`; a row that breaks rules on several columns lists each, joined by `; `. Several rules on one column give the reason once
- **`required` of the schema vs. `required` here**: the schema's `required` is checked first by the engine (NULL or empty string, reason `required_value_missing`); `x-validation`'s `required` also rejects whitespace-only text
- **Missing columns**: a field that is in neither the file nor `properties` (a typo) raises a `ValueError` before any output is written; a field that `properties` declares but this file lacks is left out with a warning (`x-validation rules for column(s) 'score' are not checked: the columns are not in the input`), so a wide standard works with narrower files
- **Invalid rules** (bad ranges, patterns, strategy, types) raise a `ValueError` naming the key, before any output is written
- **Switch off**: `apply_schema_extensions=False` / `--no-schema-extensions`

### Example

```
id,name,age,category,born
1,Ann,30,A,1990-05-01
2,Bob,200,B,1985-03-04
3,,41,C,1980-01-01
4,Di-9,52,a,1700-01-01
5,Ed,19,A,1999-12-31
5,Eve,20,B,2000-01-01
6,Fay,25,B,
7,Gus,26,A,2001-01-01
```

with

```json
{
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "category": {"type": "string"},
    "born": {"type": "string", "format": "date"}
  },
  "x-validation": {
    "badRowsHandling": {"maxBadRowsPercent": 50},
    "uniquenessHandling": {"strategy": "first_wins"},
    "fieldValidations": {
      "id": {"required": true, "unique": true, "range": {"min": 1, "max": 999999999}},
      "name": {"required": true, "stringValidation": {"maxLength": 20, "pattern": "^[A-Za-z\\s.-]+$", "allowEmpty": false}},
      "age": {"range": {"min": 0, "max": 150}},
      "category": {"enumValidation": {"allowedValues": ["A", "B", "C"], "caseSensitive": true}},
      "born": {"dateValidation": {"minDate": "1900-01-01", "maxDate": "2100-12-31", "format": ["%Y-%m-%d"]}}
    }
  }
}
```

`data.parquet` has the rows 1, 5 (Ed), 6 and 7. `bad_rows.parquet` (all text, plus the reason):

| id | name | age | category | born | _rejection_reason |
| --- | --- | --- | --- | --- | --- |
| 2 | Bob | 200 | B | 1985-03-04 | VALIDATION_ERROR:age |
| 3 | | 41 | C | 1980-01-01 | VALIDATION_ERROR:name |
| 4 | Di-9 | 52 | a | 1700-01-01 | VALIDATION_ERROR:name; VALIDATION_ERROR:category; VALIDATION_ERROR:born |
| 5 | Eve | 20 | B | 2000-01-01 | VALIDATION_ERROR:id |

`results.validation_summary` is `{'VALIDATION_ERROR:age': 1, 'VALIDATION_ERROR:name': 2, 'VALIDATION_ERROR:category': 1, 'VALIDATION_ERROR:born': 1, 'VALIDATION_ERROR:id': 1}`. Four of eight rows are rejected (50%), which is not more than the 50 allowed. With the default `maxBadRowsPercent` of 10 the same import stops with `BadRowsThresholdExceededError: Bad rows (50.0%) exceed threshold (10.0%)` and writes nothing.

## Implementation Details

### Validation Process
1. **Rule loading**: `fieldValidations`, the strategy and the threshold are read and checked when the import starts
2. **Per row**: each rule of each field is evaluated; the reasons of a row are collected
3. **Uniqueness**: applied to the rows without other errors, according to the strategy
4. **Rejection**: rows with a reason are removed from the batch and returned to the import, which writes them to `bad_rows.parquet`
5. **Threshold**: after the batch, the running share of rejected rows is compared with `maxBadRowsPercent`

### Memory
- The processor only counts the rejected rows (it does not keep them), so the memory used for rejected rows is constant
- `unique` fields keep the values seen so far (one set per field)

## Usage Examples

### Required Columns and Ranges
```json
{
  "x-validation": {
    "fieldValidations": {
      "customer_id": {"required": true, "unique": true},
      "order_total": {"required": true, "range": {"min": 0}},
      "discount_percent": {"range": {"min": 0, "max": 100}}
    }
  }
}
```

### Codes and Lists
```json
{
  "x-validation": {
    "fieldValidations": {
      "status": {"enumValidation": {"allowedValues": ["active", "inactive", "pending"], "caseSensitive": false}},
      "country": {"stringValidation": {"minLength": 2, "maxLength": 2, "pattern": "^[A-Z]{2}$"}}
    }
  }
}
```

### Dates in Several Formats
```json
{
  "x-validation": {
    "fieldValidations": {
      "signup_date": {
        "dateValidation": {"minDate": "2000-01-01", "maxDate": "2100-12-31", "format": ["%Y-%m-%d", "%m/%d/%Y"]}
      }
    }
  }
}
```

### Strict Threshold
```json
{
  "x-validation": {
    "badRowsHandling": {"maxBadRowsPercent": 1, "failOnExceedThreshold": true},
    "fieldValidations": {"email": {"required": true, "unique": true}}
  }
}
```

### Keep Only the Last Row of a Key
```json
{
  "x-validation": {
    "uniquenessHandling": {"strategy": "last_wins"},
    "fieldValidations": {"account_id": {"unique": true}}
  }
}
```
`last_wins` only compares the rows of one batch: a duplicate in a later batch is rejected and the earlier row stays, so use a `batch_size` that holds the whole file if the last row must win everywhere.

## Integration with Other Features

- **x-dataQuality**: reports the same kind of findings without rejecting rows; `x-validation` runs after it
- **x-primaryKey / x-uniqueConstraints / per-property constraints**: run after `x-validation`, with `x-constraintHandling.errorMode`; `x-validation` itself has no `fail_fast` mode, only the threshold
- **x-transformations / x-special-type**: run first, so the rules see the cleaned text (a value that became NULL passes every rule except `required`)
- **x-columnMapping**: use the new names (or the header names) in `fieldValidations`
- **x-calculatedColumns**: calculated columns exist by then and can be validated

## Best Practices

1. **Set the threshold on purpose**: the default of 10% stops the import for a worse file; raise it for exploratory runs and lower it for strict feeds
2. **Choose when the threshold is judged**: the default (`thresholdMode: end_of_file`) checks the whole input before judging it and explains the failure; use `early` only to stop a hopeless input quickly, and then mind that small batches judge a file by its first rows
3. **Anchor patterns**: `pattern` is a search; use `^...$` for whole-value matches
4. **Use `unique` for a business key you want to keep one row of**; use `x-primaryKey` when a NULL key must be rejected too or when `errorMode` should be able to stop the import
5. **Read the warnings**: `results.warnings` shows every rule or key that was ignored
