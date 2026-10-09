# x-fwf Documentation

## Overview
The `x-fwf` extension provides comprehensive Fixed Width File processing configuration with precise field positioning, alignment, padding, and data type validation for Parquet output. This feature enables robust processing of mainframe exports, legacy system outputs, and structured fixed-width data files.

## Schema Structure
```json
{
  "x-fwf": {
    "encoding": "utf-8",
    "trim": { "lstrip": false, "rstrip": true },
    "fields": [
      { "name": "id", "start": 1, "length": 10, "align": "right", "pad": " ", "parquetType": "int64" },
      { "name": "name", "start": 11, "length": 30, "align": "left", "pad": " ", "parquetType": "string" },
      { "name": "salary", "start": 44, "length": 12, "align": "right", "pad": " ", "parquetType": "double" }
    ],
    "nulls": {
      "global": ["", "NULL", "N/A"],
      "perColumn": {
        "salary": ["", "          "],
        "tags": ["", "[]"]
      }
    },
    "footer": { "mode": "regex", "pattern": "^(TOTAL|SUMMARY)\\b" }
  }
}
```

## Runtime Status and Behaviour

Fixed-width parsing is implemented by `FwfSchemaImporter` (schema validation) and `FwfInputHandler` (`forklift.inputs.fwf`, reading). It is **not wired into the engine yet**: `forklift.import_fwf()` and `forklift.read_fwf()` raise `NotImplementedError`, and `forklift ingest --input-kind fwf` exits with status 2. Use the handler directly:

```python
from pathlib import Path
from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.fwf_utils import create_fwf_config_from_schema

handler = FwfInputHandler(create_fwf_config_from_schema(Path("fwf_schema.json")))
table = handler.create_arrow_table(Path("mainframe_export.txt"))   # pyarrow.Table

print(handler.errors)          # values that could not be converted (line number + field name only)
print(handler.rejected_lines)  # lines dropped: no matching conditional schema / required field blank
```

- **Nothing is dropped silently.** A value that does not match its declared type becomes null and is listed in `handler.errors` (`{"line_number", "field", "error": "invalid_value", "type"}`). A line that is dropped is listed in `handler.rejected_lines` with `"no_matching_schema"` (conditional schemas) or `"required_missing"` (a `required` field is blank). The lists contain line numbers and field names, never the data.
- **Conversion is strict.** An `int*` field must hold an integer that fits the width (`12A` or `70000` in an `int16` is invalid, not `0`); a `bool` field accepts `true/false/1/0/yes/no/y/n/t/f` (any case) and anything else is invalid, not `False`. Blank values are null without being an error.
- A UTF-8 byte order mark on the first line is ignored (it would shift every field of the first record by one).
- The layout is validated when the handler is created (`ValueError` for empty names, `start` < 1, `length` < 1, duplicate names, overlapping fields, unknown types, ...).
- With conditional schemas the flag column is populated for every record, even when the matched variant does not list it among its own fields. A column declared in several variants must have compatible types: `FwfSchemaImporter` unifies them to the wider type (`int32` and `double` give `double`) and rejects combinations that cannot be unified (a `ConditionalSchemaError` / schema validation error).
- Lines are read with `\r\n` / `\n` stripped; `encoding: "auto"` detects the encoding (verified against the whole file).
- The Parquet types understood by the FWF converter are the ones in the table of the [x-csv documentation](./X_CSV_DOCUMENTATION.md#parquet-type-mapping); types are parsed strictly when the schema is validated (an unknown unit or malformed `decimal128(...)` is a schema error).

## Configuration Properties

### File-Level Settings

#### `encoding`
- **Type**: String
- **Description**: Character encoding of the fixed-width file
- **Default**: `"utf-8"`
- **Common Values**: any text encoding Python's `codecs` module knows: `"utf-8"`, `"ascii"`, `"latin-1"`, `"cp1252"`, `"cp037"` (EBCDIC), `"utf-16"` ... An unknown name is a schema validation error

#### `trim`
Global trimming configuration applied to all fields.

##### `lstrip`
- **Type**: Boolean
- **Description**: Validated as a boolean, but not used when reading: see `rstrip`
- **Default**: `false`

##### `rstrip`
- **Type**: Boolean
- **Description**: Global switch for whitespace trimming. `create_fwf_config_from_schema()` reads it into `FwfInputConfig.trim_whitespace`; when it is true every field is stripped of leading and trailing whitespace unless the field sets `"trim": false`
- **Default**: `true`

### Field Definitions

#### `fields`
- **Type**: Array of field objects
- **Description**: Complete specification of each field in the fixed-width record
- **Required**: At least one field must be defined

#### Field Object Properties

##### `name` (required)
- **Type**: String
- **Description**: Column name for the extracted field
- **Implementation**: Used as column name in output Parquet file

##### `start` (required)
- **Type**: Integer
- **Description**: Starting position of field (1-based indexing)
- **Implementation**: Character position where field begins in the record

##### `length` (required)
- **Type**: Integer
- **Description**: Length of field in characters
- **Implementation**: Number of characters to extract for this field

##### `align` (optional)
- **Type**: String
- **Description**: Field alignment within its allocated space
- **Values**:
  - `"left"`: Left-aligned with padding on right
  - `"right"`: Right-aligned with padding on left
  - `"center"`: Center-aligned with padding on both sides
- **Default**: `"left"`

##### `pad` (optional)
- **Type**: String
- **Description**: Padding character used for field alignment
- **Default**: `" "` (space)
- **Common Values**: `" "` (space), `"0"` (zero), `"*"` (asterisk)

##### `required` (optional)
- **Type**: Boolean
- **Description**: A blank value rejects the whole line (listed in `handler.rejected_lines`)
- **Default**: `false`

##### `trim` (optional)
- **Type**: Boolean
- **Description**: Strip whitespace from this field (also needs the global `trim.rstrip`)
- **Default**: `true`

##### `parquetType` (optional)
- **Type**: String
- **Description**: Target Parquet data type for the field
- **Implementation**: Overrides automatic type inference
- **Supported Types**: Same as x-csv parquetTypeMapping

### Null Value Handling

#### `nulls.global`
- **Type**: Array of strings
- **Description**: Global representations of null values
- **Default**: `["", "NULL", "N/A"]`
- **FWF-Specific**: Often includes space-filled strings

#### `nulls.perColumn`
- **Type**: Object
- **Description**: Field-specific null value representations
- **FWF-Specific**: Common to have space-padded nulls per field width

### Footer Handling

#### `footer.mode`
- **Type**: String
- **Description**: Strategy for detecting footer records
- **Values**: Same as x-csv footer handling

#### `footer.pattern`
- **Type**: String
- **Description**: Regular expression to identify footer records
- **Example**: `"^(TOTAL|SUMMARY)\\b"`

## Field Positioning Examples

### Basic Field Layout
```
Position: 1234567890123456789012345678901234567890
Record:   0001      John Doe                 00075000.00
Fields:   [id ]     [name                  ] [salary    ]
          1-4       11-40                    41-52
```

### Configuration for Above Layout
```json
{
  "fields": [
    { "name": "id", "start": 1, "length": 4, "align": "right", "pad": "0", "parquetType": "int32" },
    { "name": "name", "start": 11, "length": 30, "align": "left", "pad": " ", "parquetType": "string" },
    { "name": "salary", "start": 41, "length": 12, "align": "right", "pad": " ", "parquetType": "double" }
  ]
}
```

### Complex Record Layout
```
Position: 1234567890123456789012345678901234567890123456789012345678901234567890
Record:   A0001DOE          JOHN     M19850615001234567890123456NY10001    Y
Fields:   [T][id][last_name ][first ][M][dob    ][ssn          ][state][active]
```

### Configuration for Complex Layout
```json
{
  "fields": [
    { "name": "record_type", "start": 1, "length": 1, "parquetType": "string" },
    { "name": "id", "start": 2, "length": 4, "align": "right", "pad": "0", "parquetType": "int32" },
    { "name": "last_name", "start": 6, "length": 10, "align": "left", "pad": " ", "parquetType": "string" },
    { "name": "first_name", "start": 16, "length": 8, "align": "left", "pad": " ", "parquetType": "string" },
    { "name": "middle_initial", "start": 24, "length": 1, "parquetType": "string" },
    { "name": "birth_date", "start": 25, "length": 8, "parquetType": "date32" },
    { "name": "ssn", "start": 33, "length": 15, "align": "left", "pad": " ", "parquetType": "string" },
    { "name": "state", "start": 48, "length": 2, "parquetType": "string" },
    { "name": "zip", "start": 50, "length": 5, "parquetType": "string" },
    { "name": "is_active", "start": 59, "length": 1, "parquetType": "bool" }
  ]
}
```

## Advanced Features

### Conditional Field Processing
Some FWF files have different layouts depending on a record-type flag. Use `conditionalSchemas` (instead of `fields`) with a `flagColumn` and one schema per flag value; a line whose flag matches none of them is rejected (`no_matching_schema`):

```json
{
  "x-fwf": {
    "conditionalSchemas": {
      "flagColumn": { "name": "record_type", "start": 1, "length": 1, "parquetType": "string" },
      "schemas": [
        {
          "flagValue": "A",
          "description": "Customer record",
          "fields": [
            { "name": "customer_id", "start": 2, "length": 10 },
            { "name": "customer_name", "start": 12, "length": 30 }
          ]
        },
        {
          "flagValue": "B",
          "description": "Order record",
          "fields": [
            { "name": "order_id", "start": 2, "length": 10 },
            { "name": "order_date", "start": 12, "length": 8 },
            { "name": "amount", "start": 20, "length": 12, "parquetType": "double" }
          ]
        }
      ]
    }
  }
}
```

The output has one column for every field of every variant (plus the flag column); fields that a record's variant does not have are null.

### Not supported
Packed decimal (COMP-3) and raw binary fields are not implemented: there is no `dataFormat`, `precision` or `scale` field property. Fields are read as text and converted to the declared `parquetType`. EBCDIC text files can be read by naming a Python codec such as `cp037` in `encoding`.

## Integration with Other Features

### With Transformations
```json
{
  "x-fwf": {
    "fields": [
      { "name": "name", "start": 1, "length": 30, "align": "left", "pad": " " }
    ]
  },
  "x-transformations": {
    "column_transformations": {
      "name": {
        "string_cleaning": { "enabled": true, "case_transform": "title" }
      }
    }
  }
}
```

### With PII Handling
```json
{
  "x-fwf": {
    "fields": [
      { "name": "ssn", "start": 20, "length": 11, "parquetType": "string" }
    ]
  },
  "x-pii": {
    "fields": {
      "ssn": {
        "isPII": true,
        "category": "direct_identifier",
        "fieldPosition": { "start": 20, "length": 11 }
      }
    }
  }
}
```

### With Special Types
```json
{
  "properties": {
    "ssn": {
      "type": "string",
      "x-special-type": "ssn"
    },
    "zip_code": {
      "type": "string",
      "x-special-type": "zip-5"
    }
  },
  "x-fwf": {
    "fields": [
      { "name": "ssn", "start": 25, "length": 11, "parquetType": "string" },
      { "name": "zip_code", "start": 50, "length": 5, "parquetType": "string" }
    ]
  }
}
```

## Performance Considerations

### Memory Usage
- **Record Length**: Longer records require more memory per row
- **Field Count**: More fields increase processing overhead
- **String Operations**: Trimming and padding operations add CPU cost

### Processing Speed
- **Simple Fields**: Numeric fields with no transformations process fastest
- **String Processing**: Text fields with cleaning operations are slower
- **Type Conversion**: Complex type conversions add overhead

### Optimization Strategies
1. **Minimize Field Definitions**: Only define needed fields
2. **Efficient Data Types**: Use appropriate numeric types
3. **Reduce String Operations**: Limit unnecessary trimming/padding
4. **Batch Processing**: Process multiple records together

## Common Use Cases

### Mainframe Data Export
```json
{
  "x-fwf": {
    "encoding": "cp037",
    "fields": [
      { "name": "account_num", "start": 1, "length": 12, "align": "right", "pad": "0" },
      { "name": "balance", "start": 13, "length": 15, "align": "right", "pad": " ", "parquetType": "double" },
      { "name": "status", "start": 28, "length": 1 }
    ]
  }
}
```

### Legacy System Report
```json
{
  "x-fwf": {
    "encoding": "ascii",
    "trim": { "lstrip": false, "rstrip": true },
    "fields": [
      { "name": "emp_id", "start": 1, "length": 6, "align": "right", "pad": "0" },
      { "name": "last_name", "start": 8, "length": 20, "align": "left", "pad": " " },
      { "name": "first_name", "start": 29, "length": 15, "align": "left", "pad": " " },
      { "name": "hire_date", "start": 45, "length": 8, "parquetType": "date32" },
      { "name": "salary", "start": 54, "length": 10, "align": "right", "pad": " " }
    ],
    "footer": { "mode": "regex", "pattern": "^EMPLOYEE COUNT:" }
  }
}
```

### Financial Data File
```json
{
  "x-fwf": {
    "encoding": "utf-8",
    "fields": [
      { "name": "transaction_id", "start": 1, "length": 16, "parquetType": "string" },
      { "name": "account_id", "start": 17, "length": 12, "align": "right", "pad": "0" },
      { "name": "amount", "start": 29, "length": 15, "align": "right", "pad": " ", "parquetType": "decimal128(13,2)" },
      { "name": "currency", "start": 44, "length": 3, "parquetType": "string" },
      { "name": "trans_date", "start": 47, "length": 8, "parquetType": "date32" },
      { "name": "trans_time", "start": 55, "length": 6, "parquetType": "string" }
    ],
    "nulls": {
      "perColumn": {
        "amount": ["", "               "],
        "currency": ["", "   "]
      }
    }
  }
}
```

## Best Practices

### Field Definition
1. **Accurate Positioning**: Verify start positions and lengths with source documentation
2. **Appropriate Types**: Choose Parquet types that match data precision needs
3. **Null Handling**: Define space-filled nulls for each field width
4. **Documentation**: Document field meanings and business rules

### Data Quality
1. **Validation**: Combine with constraint handling for data validation
2. **Error Tracking**: Monitor `handler.errors` and `handler.rejected_lines`
3. **Sample Testing**: Test with representative data samples
4. **Format Verification**: Verify field formats match expectations

### Performance
1. **Essential Fields**: Only extract needed fields to reduce processing time
2. **Type Optimization**: Use smallest appropriate numeric types
3. **Check the error lists**: inspect `handler.errors` and `handler.rejected_lines` after reading

### Maintenance
1. **Version Control**: Track changes to field definitions
2. **Documentation**: Maintain mapping documentation
3. **Testing**: Create test cases for different record formats
4. **Monitoring**: Track processing success rates and performance metrics
