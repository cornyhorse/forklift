# x-dataQuality Documentation

## Overview
The `x-dataQuality` extension describes per-column quality checks that **only report**. `import_csv` runs them on every batch, counts the findings per code and column in `results.validation_summary` (and prints them with the CLI), and keeps all rows: nothing is dropped, changed or written to `bad_rows.parquet`. Rules that must reject rows belong to [x-validation](./X_VALIDATION_DOCUMENTATION.md), the primary key and the per-property constraints.

## Schema Structure
```json
{
  "x-dataQuality": {
    "description": "Data quality validation and metrics configuration",
    "enabled": true,
    "fieldSpecificRules": {
      "age": {"min": 0, "max": 150},
      "email_address": {"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}
    },
    "fieldQualityRules": {
      "product_sku": {
        "parameters": {"min_length": 8, "max_length": 8, "pattern": "^[A-Z]{2}\\d{6}$"}
      },
      "order_amount": {
        "parameters": {"min_value": 0.01, "max_value": 100000}
      }
    }
  }
}
```

## Configuration Properties

### `enabled`
- **Type**: Boolean
- **Default**: `true`
- **Description**: `false` switches the whole extension off (nothing is checked and nothing is reported)

### `fieldSpecificRules` (the shape of the shipped standard)
- **Type**: Object with column names as keys
- **Description**: Value and pattern checks per column

| Key | Meaning | Applies to | Finding |
| --- | --- | --- | --- |
| `min` | smallest accepted number | integer, floating point and decimal columns | `MIN_VALUE_VIOLATION` |
| `max` | largest accepted number | integer, floating point and decimal columns | `MAX_VALUE_VIOLATION` |
| `pattern` | regular expression, an unanchored search (anchor with `^...$`) | text columns | `PATTERN_VIOLATION` |

### `fieldQualityRules` (the shape of the earlier documentation)
- **Type**: Object with column names as keys; only `parameters` is read
- **Description**: The same checks, with the processor's own names

| `parameters` key | Meaning | Applies to | Finding |
| --- | --- | --- | --- |
| `min_length` | shortest accepted text | text columns | `MIN_LENGTH_VIOLATION` |
| `max_length` | longest accepted text | text columns | `MAX_LENGTH_VIOLATION` |
| `pattern` | regular expression (unanchored search) | text columns | `PATTERN_VIOLATION` |
| `min_value` | smallest accepted number | numeric columns | `MIN_VALUE_VIOLATION` |
| `max_value` | largest accepted number | numeric columns | `MAX_VALUE_VIOLATION` |

Both shapes may be used together; for one column the rules are merged, and two different values for the same check (for example `max` and `max_value`) raise a `ValueError`.

### Rules of the checks
- NULL values are skipped; a text rule on a number column (or a number rule on a text column) is skipped, not an error. Decimals are compared exactly
- `min` greater than `max`, a rule value of the wrong type, an invalid or unsafe regular expression (for example nested quantifiers like `(a+)+`) raise a `ValueError` naming the key, before any output is written
- A finding is counted as `CODE:column` in `validation_summary`, one per offending value, so a value that breaks two rules counts twice. No finding contains the cell value
- Column names are the output names (after `x-columnMapping`; a renamed header name is accepted). A column that is neither in the file nor in `properties` (a typo) raises a `ValueError`; a column declared in `properties` but absent from this file only warns (`x-dataQuality rules for column(s) 'age' are not checked: the columns are not in the input`), so a wide standard can be used with narrower files
- `required: false` and `standardizeFormat: false` inside a `fieldSpecificRules` entry are accepted and do nothing

### Not implemented (ignored with a warning)

Each of these is reported in `results.warnings` (`x-dataQuality.<key> is not supported and is ignored`); the extension works without it. Earlier versions of this page described them:

| Key | Status |
| --- | --- |
| `qualityThresholds` (completeness, validity, uniqueness; actions `warn`, `fail`, `flag`) | not implemented; to stop an import on too many bad rows use `x-validation.badRowsHandling.maxBadRowsPercent` |
| `fieldQualityRules.<col>.rules` (`not_null`, `email_format`, `domain_valid`, ...) and `severity` | not implemented; express the check as a `parameters` rule, an `x-validation` rule or a constraint |
| `fieldQualityRules.<col>.parameters` other than the five above (`min_date`, `max_date`, `blocked_domains`, `decimal_places`, `lookup_table`, ...) | not implemented |
| `crossFieldValidation`, `statisticalChecks`, `reporting` | not implemented |
| `completeness`, `uniqueness`, `consistency`, `accuracy` blocks | not implemented |
| per column `dataType`, `required: true`, `standardizeFormat: true` | not implemented |

## In `import_csv`

Stage: after `x-columnMapping` and `x-calculatedColumns`, before `x-validation`, so the checks see the typed values, the output names and the calculated columns, but also the rows that `x-validation` or a constraint will reject afterwards.

### Example

```
id,name,age,email,code
1,Ann,30,ann@example.com,AB123456
2,Bob,200,bob-at-example.com,AB1
3,,41,cy@example.com,ZZ999999
```

with

```json
{
  "type": "object",
  "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "age": {"type": "integer"}, "email": {"type": "string"}, "code": {"type": "string"}},
  "x-dataQuality": {
    "fieldSpecificRules": {"age": {"min": 0, "max": 150}, "email": {"pattern": "^[^@]+@[^@]+$"}},
    "fieldQualityRules": {"code": {"parameters": {"min_length": 8, "max_length": 8, "pattern": "^AB"}}}
  }
}
```

all three rows are written to `data.parquet` (there is no `bad_rows.parquet`) and

```
results.schema_extensions   ['x-dataQuality']
results.validation_summary  {'MAX_VALUE_VIOLATION:age': 1, 'PATTERN_VIOLATION:email': 1,
                             'MIN_LENGTH_VIOLATION:code': 1, 'PATTERN_VIOLATION:code': 1}
```

The CLI prints the same counts under `Findings by the schema extensions:`.

## Implementation Details

### Quality Assessment Pipeline
1. **Rule loading**: the rules are read once, checked and compiled (patterns) when the import starts
2. **Field-level checks**: each batch is checked column by column
3. **Counting**: every finding is counted under `CODE:column` (at most 200 distinct keys are tracked, the rest is counted as `OTHER`)

### Memory Management
- **Streaming Validation**: the data is processed batch by batch; only the counters are kept

## Usage Examples

### E-commerce Data Quality
```json
{
  "x-dataQuality": {
    "fieldSpecificRules": {
      "order_amount": {"min": 0.01, "max": 100000}
    },
    "fieldQualityRules": {
      "customer_email": {
        "parameters": {"pattern": "^[^@\\s]+@[^@\\s]+\\.[a-z]{2,}$", "max_length": 254}
      },
      "product_sku": {
        "parameters": {"pattern": "^[A-Z]{2}\\d{6}$", "min_length": 8, "max_length": 8}
      }
    }
  }
}
```

### Financial Data Quality
```json
{
  "x-dataQuality": {
    "fieldQualityRules": {
      "account_number": {
        "parameters": {"pattern": "^\\d{10,12}$"}
      },
      "transaction_amount": {
        "parameters": {"min_value": -1000000, "max_value": 1000000}
      }
    }
  }
}
```

### HR Data Quality
```json
{
  "x-dataQuality": {
    "fieldQualityRules": {
      "employee_id": {
        "parameters": {"pattern": "^EMP\\d{6}$"}
      },
      "salary": {
        "parameters": {"min_value": 20000, "max_value": 500000}
      }
    }
  }
}
```

## Integration with Other Features

### With Constraint Handling and x-validation
`x-dataQuality` never rejects rows. To reject the same rows, repeat the rule in `x-validation` (`range`, `stringValidation`) or as a per-property constraint (`minimum`, `maxLength`, `pattern`); `x-constraintHandling.errorMode` applies to the constraints only.

### With Transformations
Transformations run first, so the rules see the cleaned text and the converted types:
```json
{
  "x-transformations": {
    "column_transformations": {
      "customer_name": {"string_cleaning": {"enabled": true, "strip_whitespace": true}}
    }
  },
  "x-dataQuality": {
    "fieldQualityRules": {
      "customer_name": {"parameters": {"min_length": 2, "max_length": 100}}
    }
  }
}
```

### With Metadata Generation
`x-metadata-generation` describes the final data in `output_data_metadata.json`; `x-dataQuality` findings are in `validation_summary` (and `metadata.json`). They are independent.

## Best Practices

### Rule Design
1. **Start Simple**: Begin with a few rules on the columns that matter and add more gradually
2. **Business Focus**: Align rules with actual business requirements
3. **Anchor patterns**: a pattern is a search; write `^...$` when the whole value must match
4. **Maintainability**: Keep the rules easy to understand and maintain

### Reading the Findings
1. **Compare runs**: watch the counts in `validation_summary` over time
2. **Promote to enforcement**: when a finding must keep a row out, move the rule to `x-validation`
3. **Check the warnings**: `results.warnings` lists every part of the extension that was ignored
