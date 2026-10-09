# x-csv Documentation

## Overview
The `x-csv` extension provides comprehensive CSV file processing configuration with advanced parsing options, encoding detection, delimiter handling, header scanning, and Parquet type mapping. This feature enables robust processing of CSV files with varying formats and quality issues.

## What is applied and what is only validated

`CsvSchemaImporter` validates the whole extension (a schema needs `$schema` 2020-12, an `$id` under `https://github.com/cornyhorse/forklift/schema-standards/`, a `title`, `type: object` and an `x-csv` object; every problem found is reported in one `SchemaValidationError`). `forklift.import_csv()` reads only these parts of it:

- `parquetTypeMapping` (together with each property's JSON `type` / `format`): the column types of the output. Columns declared `string` keep their text exactly (`00123` stays `00123`); a value that does not convert sends its row to `bad_rows.parquet`
- `nulls` (`global` and `perColumn`): text values that become NULL
- the schema's `required` list (matched by column name; null and empty strings reject the row)

The rest of the schema is read by the other extensions, which `import_csv` also applies (see [README.md](./README.md#what-the-engine-applies-today) for the table): `x-transformations`, `x-special-type`, `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality`, `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`, the per-property constraints and `x-rowHash`. The order per batch is:

1. read the text of the rows with the `ImportConfig` settings (delimiter, encoding, header, footer, ...); hidden row-number / input-hash columns for `x-rowHash` are added
2. **`x-csv.nulls`**: the marker text becomes NULL (header names are used for `perColumn`). This early step only matters when `x-transformations` or `x-special-type` is used; otherwise the markers are applied in step 4
3. `x-transformations` and the automatic `x-special-type` formatting (header names)
4. **type conversion** with `parquetTypeMapping` / the property types (the null markers are applied once more here); unconvertible rows go to `bad_rows.parquet`
5. **`required`** check; failing rows go to `bad_rows.parquet`
6. `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality`, `x-validation`, the constraints, `x-rowHash` (output names)

`ImportConfig(apply_schema_extensions=False)` / `--no-schema-extensions` skips every extension except steps 2, 4 and 5. Excel, SQL and fixed-width imports do not apply them.

Everything else here (`encodingPriority`, `delimiter`, `quotechar`, `escapechar`, `multiline`, `header`, `footer`, `case`) documents the file layout and is validated, but the engine takes its read settings from `ImportConfig` (`encoding`, `delimiter`, `quote_char`, `escape_char`, `header_mode`, `comment_rows`, `footer_detection`, ...). In particular there is no encoding fallback chain and no delimiter auto-detection in the engine; the CLI's `--encoding-priority` accepts a list but only its first entry is used.

Property definitions may declare `type` as a string, a nullable array (`["integer", "null"]`) or a nullable `anyOf` / `oneOf` union; `required` must list names of defined properties. Validation errors name the field, for example `Invalid type 'foo' for field 'age'` or `required[1] refers to unknown property 'x'`.

## Schema Structure
```json
{
  "x-csv": {
    "encodingPriority": ["utf-8-sig", "utf-8", "latin-1"],
    "delimiter": "auto",
    "quotechar": "\"",
    "escapechar": "\\",
    "multiline": true,
    "header": { 
      "mode": "stability_scan", 
      "keywords": ["id", "name", "age", "salary"] 
    },
    "footer": { 
      "mode": "regex", 
      "pattern": "^(total|summary)\\b" 
    },
    "nulls": {
      "global": ["", "NA", "N/A", "-", "NULL"],
      "perColumn": {
        "salary": ["", "0.00"],
        "tags": ["", "[]"],
        "metadata": ["", "{}"]
      }
    },
    "case": { 
      "standardizeNames": "postgres", 
      "dedupeNames": "suffix" 
    },
    "parquetTypeMapping": {
      "id": "int64",
      "name": "string",
      "age": "int32",
      "salary": "double",
      "is_active": "bool",
      "birth_date": "date32",
      "created_timestamp": "timestamp[us]"
    }
  }
}
```

## Configuration Properties

### Encoding Detection

#### `encodingPriority`
- **Type**: Array of strings
- **Description**: Priority order of the encodings the file may use
- **Default**: `["utf-8-sig", "utf-8", "latin-1"]` in generated schemas
- **Validation**: Any text encoding Python's `codecs` module knows is accepted (`utf-8`, `iso-8859-1`, `cp1250`, `utf-16`, `cp037`, ...); an unknown or non-text codec is a validation error
- **Implementation**: The importer exposes the list (`get_encoding_priority()`); the engine itself uses the single `encoding` of `ImportConfig` and does not retry other encodings. A file that cannot be decoded raises a `ValueError` asking for the right encoding
- **Common Encodings**:
  - `"utf-8-sig"`: UTF-8 with BOM (Byte Order Mark)
  - `"utf-8"`: Standard UTF-8
  - `"latin-1"`: ISO-8859-1 (Western European)
  - `"cp1252"`: Windows-1252 (Windows Western)
  - `"ascii"`: ASCII encoding

### Delimiter and Format Detection

#### `delimiter`
- **Type**: String or "auto"
- **Description**: CSV field delimiter character. Must be `"auto"` or exactly one character
- **Values**:
  - `"auto"`: Accepted by validation; no automatic detection is performed by the engine (set `ImportConfig.delimiter`)
  - `","`: Comma (standard CSV)
  - `";"`: Semicolon (European CSV)
  - `"\t"`: Tab (TSV files)
  - `"|"`: Pipe delimiter
  - Custom single character
- **Default**: `","` for `ImportConfig`; `get_delimiter()` returns `","` when the key is absent

#### `quotechar`
- **Type**: String
- **Description**: Character used to quote fields containing delimiters
- **Default**: `"\""`
- **Implementation**: Fields containing delimiters are wrapped in quote characters

#### `escapechar`
- **Type**: String
- **Description**: Character used to escape special characters within fields
- **Default**: `"\\"`
- **Implementation**: Used to escape quote characters within quoted fields

#### `multiline`
- **Type**: Boolean
- **Description**: Allow fields to span multiple lines
- **Default**: `true`
- **Implementation**: Handles quoted fields containing newlines

### Header Detection and Processing

#### `header.mode`
- **Type**: String
- **Description**: Strategy for detecting header row
- **Values** (anything else is a validation error):
  - `"present"`: The first row that is not blank or a comment is the header
  - `"absent"`: File has no header row (columns come from the schema, or are `col_1`, `col_2`, ...)
  - `"auto"`: Pick the row (within `header_search_rows`) that looks most like a header
  - `"stability_scan"`: Scan for stable header patterns using `keywords` (requires a non-empty `keywords` list)
- **Engine equivalent**: `ImportConfig.header_mode` (`PRESENT`, `ABSENT`, `AUTO`; strings are accepted). A header must be found within `header_search_rows` rows or a `ValueError` is raised. With `comment_rows=None` only a single-cell `#...` row above the header counts as a comment (`#,name,amount` is a header); `comment_rows=[]` disables comments

#### `header.keywords`
- **Type**: Array of strings
- **Description**: Expected column names to identify header row
- **Use**: Helps locate header in files with variable leading content
- **Example**: `["id", "name", "age", "salary"]`

#### `header.skipRows`
- **Type**: Integer
- **Description**: Number of rows to skip before looking for header. Not read by the importer or the engine (use `comment_rows` / `header_search_rows`)
- **Default**: `0`

### Footer Detection and Handling

#### `footer.mode`
- **Type**: String
- **Description**: Strategy for detecting footer content
- **Values** (anything else is a validation error):
  - `"regex"`: Use regular expression to identify footer rows (requires `pattern`, which must compile)
  - `"blank_line"`: A blank line ends the data
- **Engine equivalent**: `ImportConfig.footer_detection`, for example `{"stop_on_blank": True}` or `{"column_index": 0, "patterns": ["^Total"]}`

#### `footer.pattern`
- **Type**: String
- **Description**: Regular expression pattern to identify footer rows
- **Example**: `"^(total|summary|grand total)\\b"`
- **Implementation**: Rows matching pattern are excluded from data processing

#### `footer.skipLastRows`
- **Type**: Integer
- **Description**: Not read by the importer or the engine

### Null Value Handling

#### `nulls.global`
- **Type**: Array of strings
- **Description**: Global null value representations
- **Default**: none. `get_null_values()` returns `[""]` when the key is absent, and without an `x-csv.nulls` block the engine only applies Arrow's own null markers to non-string columns (string columns keep every text value, including the empty string)
- **Implementation**: These string values are converted to null across all columns. In `import_csv` they are applied to the text of the file **before** `x-transformations` (so a transformation, the `required` check and the rules see NULL, not `NA`) and once more together with the type conversion (a transformation that produces a marker text yields NULL). The generated schemas suggest `["", "NA", "N/A", "-", "NULL", "null"]`; note that the schema sampler itself does not treat `NA` as null (it can be a real value)

#### `nulls.perColumn`
- **Type**: Object
- **Description**: Column-specific null value representations
- **Implementation**: Overrides (replaces, it does not extend) the global list for those columns
- **Use Cases**:
  - Financial data: `"0.00"` as null for optional amounts
  - JSON fields: `"{}"` as null for empty objects
  - Arrays: `"[]"` as null for empty arrays

### Column Name Standardization

#### `case.standardizeNames`
- **Type**: String
- **Description**: Column name standardization strategy
- **Values** (anything else is a validation error):
  - `"postgres"`: PostgreSQL naming (lowercase ASCII, underscores, accents transliterated, at most 63 characters)
  - `"snake_case"`: Snake case formatting (`User ID` -> `user_id`, `customerName` -> `customer_name`)
  - `"camelCase"`: Camel case formatting (`user_id` -> `userId`)
- **Where it applies**: `CsvSchemaImporter.standardize_column_names()`; `import_csv` does not rename columns from `case`. To rename columns in the import use `x-columnMapping` (`explicitMappings`, `namingConvention`)

#### `case.dedupeNames`
- **Type**: String
- **Description**: Strategy for handling duplicate column names
- **Values**:
  - `"suffix"`: Add numeric suffix (name_1, name_2)
  - `"prefix"`: Add numeric prefix (1_name, 2_name)
  - `"error"`: Raise error on duplicates
- **Note**: It is applied together with `standardizeNames`; without `standardizeNames` the importer returns the names unchanged. With `postgres`, suffixed names stay within the 63-character limit

### Parquet Type Mapping

#### `parquetTypeMapping`
- **Type**: Object
- **Description**: Explicit mapping from CSV columns to Parquet data types
- **Purpose**: Override automatic type inference with specific types
- **Keys**: must be properties of the schema (`Parquet type mapping for unknown field` is a validation error)
- **Supported Types** (the grammar is strict: units, precision, scale and nesting are parsed, a malformed string is rejected rather than prefix-matched):
  - **Numeric**: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
  - **Floating**: `float32`, `double`
  - **Decimal**: `decimal128(precision,scale)` (precision 1-38), `decimal256(precision,scale)` (1-76)
  - **Boolean**: `bool`
  - **String**: `string`, `large_string`
  - **Temporal**: `date32`, `date64`, `time32[s|ms]`, `time64[us|ns]`, `timestamp[s|ms|us|ns]`, `timestamp[us, tz=UTC]`, `duration[s|ms|us|ns]`
  - **Complex**: `list<type>`, `large_list<type>`, `struct`, `dictionary<values=type, indices=int type>`
  - **Binary**: `binary`, `large_binary`
- **In `import_csv`**: scalar types, `decimal128/256`, `timestamp`, `duration` and `dictionary` are built from the CSV text. Nested types (`list<...>`, `struct`) cannot be parsed from a CSV cell, so such a column stays `string`. Timestamps without a time zone also accept values with a `Z` or UTC offset (the UTC wall time is kept)

## Advanced Features

### Automatic Type Inference
When Parquet types are not explicitly specified, the system automatically infers types:

- **Schema generation** (`generate_schema_from_csv`) reads every sampled value as text and applies fixed rules: integers must match `-?(0|[1-9]\d*)` (so `02134` stays a string), then plain decimal numbers, `true`/`false` booleans (any case), `YYYY-MM-DD` dates (`date32`), `YYYY-MM-DD[T ]HH:MM[:SS[.f]]` timestamps (with or without a UTC offset), otherwise string. Empty values and `NULL`, `null`, `N/A`, `n/a`, `#N/A`, `NaN`, `nan` count as missing; `NA` does not.
- **`import_csv`** uses the schema's types for the columns it lists; other columns keep Arrow's inference when the file is read locally and are strings when it is read from S3.

### Error Recovery
- **Malformed Rows**: Rows with fewer fields are padded; rows with more fields follow `ImportConfig.excess_column_mode` (`TRUNCATE` to the header width and count them in `truncated_rows`, `REJECT` to `bad_rows.parquet`, or `PASSTHROUGH` into extra `col_N` columns)
- **Encoding Errors**: A file that is not valid in the configured encoding raises a `ValueError` that names the byte offset and asks for the right `encoding`; there is no retry with other encodings
- **Type Conversion Errors**: The whole row goes to `bad_rows.parquet` (all-string columns in the shape of the header; when `x-validation` or a constraint is configured the file has a `_rejection_reason` column and these rows carry `type_conversion_failed`)
- **Blank lines** are skipped

### Performance Optimization
- **Streaming Processing**: Process large files without loading entirely into memory (`ImportConfig.batch_size` is an upper bound on rows per written batch)
- **Fast path**: Local files are read with Arrow's streaming CSV reader; S3 inputs and files with mismatching row widths are read row by row

## Usage Examples

### Basic CSV Processing
```json
{
  "x-csv": {
    "delimiter": ",",
    "header": { "mode": "present" },
    "nulls": { "global": ["", "NULL"] }
  }
}
```

### European CSV Format
```json
{
  "x-csv": {
    "encodingPriority": ["utf-8", "latin-1"],
    "delimiter": ";",
    "quotechar": "\"",
    "nulls": { "global": ["", "NULL", "N/A"] },
    "case": { "standardizeNames": "snake_case" }
  }
}
```

### Complex CSV with Headers and Footers
```json
{
  "x-csv": {
    "delimiter": "auto",
    "header": {
      "mode": "stability_scan",
      "keywords": ["customer_id", "order_date", "amount"],
      "skipRows": 2
    },
    "footer": {
      "mode": "regex",
      "pattern": "^(TOTAL|SUMMARY|Grand Total)\\b"
    },
    "nulls": {
      "global": ["", "N/A", "-"],
      "perColumn": {
        "amount": ["", "0.00", "N/A"],
        "notes": ["", "None", "N/A"]
      }
    }
  }
}
```

### Financial Data Processing
```json
{
  "x-csv": {
    "delimiter": ",",
    "parquetTypeMapping": {
      "account_id": "string",
      "balance": "decimal128(15,2)",
      "transaction_date": "date32",
      "timestamp": "timestamp[us]",
      "is_active": "bool",
      "tags": "list<string>"
    },
    "nulls": {
      "perColumn": {
        "balance": ["", "0.00", "NULL"],
        "notes": ["", "N/A", "None"]
      }
    }
  }
}
```

### Multi-Language CSV
```json
{
  "x-csv": {
    "encodingPriority": ["utf-8-sig", "utf-8", "utf-16", "latin-1"],
    "delimiter": "auto",
    "case": {
      "standardizeNames": "postgres",
      "dedupeNames": "suffix"
    },
    "header": {
      "mode": "auto"
    }
  }
}
```

## Integration with Other Features

### With Transformations
The null markers are applied first, then the per-column transformations (see [x-transformations](./X_TRANSFORMATIONS_DOCUMENTATION.md)):
```json
{
  "x-csv": {
    "delimiter": ",",
    "nulls": { "global": ["", "NULL"] }
  },
  "x-transformations": {
    "column_transformations": {
      "name": {
        "string_cleaning": { "enabled": true, "strip_whitespace": true, "collapse_whitespace": true }
      },
      "amount": {
        "numeric_cleaning": { "enabled": true, "thousands_separator": ",", "decimal_separator": "." }
      }
    }
  }
}
```

### With Constraint Handling
```json
{
  "x-csv": {
    "delimiter": ",",
    "header": { "mode": "present" }
  },
  "x-primaryKey": {"columns": ["id"]},
  "x-constraintHandling": {
    "errorMode": "bad_rows"
  }
}
```
Rows that break the key go to `bad_rows.parquet` with `_rejection_reason` (see [x-constraintHandling](./X_CONSTRAINT_HANDLING_DOCUMENTATION.md)).

### With Special Types
```json
{
  "properties": {
    "ssn": {
      "type": "string",
      "x-special-type": "ssn"
    },
    "email": {
      "type": "string", 
      "x-special-type": "email"
    }
  },
  "x-csv": {
    "delimiter": ",",
    "parquetTypeMapping": {
      "ssn": "string",
      "email": "string"
    }
  }
}
```

## Best Practices

### File Format Detection
1. **Be explicit**: Set `ImportConfig.delimiter` and `encoding`; the engine does not detect them
2. **Encoding Priority**: List the encodings the file may use, most likely first (documentation for readers of the schema)
3. **Header Keywords**: Provide expected column names for robust header detection
4. **Test with Samples**: Validate configuration with representative file samples

### Type Mapping Strategy
1. **Explicit Mapping**: Specify types for critical columns
2. **Precision Control**: Use decimal types for financial data
3. **Memory Optimization**: Choose appropriate integer sizes
4. **Future Compatibility**: Consider schema evolution needs

### Error Handling
1. **Null Value Coverage**: Define comprehensive null representations
2. **Bad Rows Configuration**: Set up bad rows output for malformed data
3. **Validation Rules**: Combine with constraint handling for data quality
4. **Monitoring**: Track parsing success rates and common errors

### Performance Tuning
1. **Batch Size**: `batch_size` bounds the rows per written batch
2. **Type Inference**: Declare types in the schema; schema generation can sample with `nrows`
3. **Local vs. S3**: Local files take the faster Arrow path

## Common Issues and Solutions

### Encoding Problems
- **Issue**: Garbled characters in output, or a `ValueError` about bytes that are not valid for the encoding
- **Solution**: Set `encoding` to the one the file was written with (for example `latin-1` or `cp1252`); a UTF-8 byte order mark is ignored

### Delimiter Detection Failures
- **Issue**: Fields not properly separated
- **Solution**: Set `delimiter` explicitly, check for unusual delimiters

### Header Detection Issues
- **Issue**: Wrong row used as header, or `No header row found`
- **Solution**: Use `header_mode="auto"`, raise `header_search_rows`, or adjust `comment_rows`

### Type Conversion Errors
- **Issue**: Data doesn't convert to expected types
- **Solution**: Check null representations, use string type as fallback

### Memory Issues
- **Issue**: Out of memory errors with large files
- **Solution**: Reduce `batch_size`; processing is streaming by default
