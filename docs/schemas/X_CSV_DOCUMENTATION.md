# x-csv Documentation

## Overview
The `x-csv` extension provides comprehensive CSV file processing configuration with advanced parsing options, encoding detection, delimiter handling, header scanning, and Parquet type mapping. This feature enables robust processing of CSV files with varying formats and quality issues.

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
- **Description**: Priority order for encoding detection attempts
- **Default**: `["utf-8-sig", "utf-8", "latin-1"]`
- **Implementation**: Tries encodings in order until successful parsing
- **Common Encodings**:
  - `"utf-8-sig"`: UTF-8 with BOM (Byte Order Mark)
  - `"utf-8"`: Standard UTF-8
  - `"latin-1"`: ISO-8859-1 (Western European)
  - `"cp1252"`: Windows-1252 (Windows Western)
  - `"ascii"`: ASCII encoding

### Delimiter and Format Detection

#### `delimiter`
- **Type**: String or "auto"
- **Description**: CSV field delimiter character
- **Values**: 
  - `"auto"`: Automatic delimiter detection
  - `","`: Comma (standard CSV)
  - `";"`: Semicolon (European CSV)
  - `"\t"`: Tab (TSV files)
  - `"|"`: Pipe delimiter
  - Custom single character
- **Default**: `"auto"`

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
- **Values**:
  - `"stability_scan"`: Scan for stable header patterns using keywords
  - `"first_row"`: Always use first row as header
  - `"no_header"`: File has no header row
  - `"auto_detect"`: Automatically determine if header exists
  - `"skip_to_keywords"`: Skip rows until keywords found

#### `header.keywords`
- **Type**: Array of strings
- **Description**: Expected column names to identify header row
- **Use**: Helps locate header in files with variable leading content
- **Example**: `["id", "name", "age", "salary"]`

#### `header.skipRows`
- **Type**: Integer
- **Description**: Number of rows to skip before looking for header
- **Default**: `0`

### Footer Detection and Handling

#### `footer.mode`
- **Type**: String
- **Description**: Strategy for detecting footer content
- **Values**:
  - `"regex"`: Use regular expression to identify footer rows
  - `"row_count"`: Skip last N rows
  - `"keyword"`: Look for specific footer keywords
  - `"none"`: No footer processing

#### `footer.pattern`
- **Type**: String
- **Description**: Regular expression pattern to identify footer rows
- **Example**: `"^(total|summary|grand total)\\b"`
- **Implementation**: Rows matching pattern are excluded from data processing

#### `footer.skipLastRows`
- **Type**: Integer
- **Description**: Number of rows to skip from end of file
- **Use**: When footer has fixed number of rows

### Null Value Handling

#### `nulls.global`
- **Type**: Array of strings
- **Description**: Global null value representations
- **Default**: `["", "NA", "N/A", "-", "NULL"]`
- **Implementation**: These string values are converted to null across all columns

#### `nulls.perColumn`
- **Type**: Object
- **Description**: Column-specific null value representations
- **Implementation**: Overrides global null handling for specific columns
- **Use Cases**:
  - Financial data: `"0.00"` as null for optional amounts
  - JSON fields: `"{}"` as null for empty objects
  - Arrays: `"[]"` as null for empty arrays

### Column Name Standardization

#### `case.standardizeNames`
- **Type**: String
- **Description**: Column name standardization strategy
- **Values**:
  - `"postgres"`: PostgreSQL naming (lowercase, underscores)
  - `"snake_case"`: Snake case formatting
  - `"camelCase"`: Camel case formatting
  - `"PascalCase"`: Pascal case formatting
  - `"none"`: No standardization

#### `case.dedupeNames`
- **Type**: String
- **Description**: Strategy for handling duplicate column names
- **Values**:
  - `"suffix"`: Add numeric suffix (name_1, name_2)
  - `"prefix"`: Add numeric prefix (1_name, 2_name)
  - `"error"`: Raise error on duplicates

### Parquet Type Mapping

#### `parquetTypeMapping`
- **Type**: Object
- **Description**: Explicit mapping from CSV columns to Parquet data types
- **Purpose**: Override automatic type inference with specific types
- **Supported Types**:
  - **Numeric**: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
  - **Floating**: `float32`, `double`
  - **Decimal**: `decimal128(precision,scale)`
  - **Boolean**: `bool`
  - **String**: `string`
  - **Temporal**: `date32`, `timestamp[us]`, `duration[s]`
  - **Complex**: `list<type>`, `struct`, `dictionary<values=type, indices=type>`
  - **Binary**: `binary`

## Advanced Features

### Automatic Type Inference
When Parquet types are not explicitly specified, the system automatically infers types:

1. **Integer Detection**: Identifies numeric columns with whole numbers
2. **Float Detection**: Identifies decimal numbers
3. **Boolean Detection**: Recognizes true/false, yes/no, 1/0 patterns
4. **Date Detection**: Identifies common date formats
5. **String Fallback**: Default type for unrecognized patterns

### Error Recovery
- **Malformed Rows**: Handle rows with wrong number of fields
- **Encoding Errors**: Retry with different encodings
- **Type Conversion Errors**: Route problematic values to bad rows
- **Quote Mismatch**: Attempt to repair unbalanced quotes

### Performance Optimization
- **Streaming Processing**: Process large files without loading entirely into memory
- **Chunk Processing**: Process files in configurable chunks
- **Parallel Parsing**: Utilize multiple cores for parsing operations
- **Memory Management**: Configurable memory limits for large files

## Usage Examples

### Basic CSV Processing
```json
{
  "x-csv": {
    "delimiter": ",",
    "header": { "mode": "first_row" },
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
      "mode": "auto_detect"
    }
  }
}
```

## Integration with Other Features

### With Transformations
```json
{
  "x-csv": {
    "delimiter": ",",
    "nulls": { "global": ["", "NULL"] }
  },
  "x-transformations": {
    "stringCleaning": {
      "stripWhitespace": true,
      "collapseWhitespace": true
    },
    "numericCleaning": {
      "thousandsSeparator": ",",
      "decimalSeparator": "."
    }
  }
}
```

### With Constraint Handling
```json
{
  "x-csv": {
    "delimiter": ",",
    "header": { "mode": "first_row" }
  },
  "x-constraintHandling": {
    "errorMode": "bad_rows",
    "badRowsOutput": {
      "enabled": true,
      "includeOriginalData": true
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
1. **Use Auto-Detection**: Start with `"auto"` for delimiter detection
2. **Encoding Priority**: Order encodings from most to least likely
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
1. **Chunk Size**: Optimize for available memory
2. **Parallel Processing**: Enable for large files
3. **Type Inference**: Limit inference to improve performance
4. **Memory Limits**: Set appropriate limits for large datasets

## Common Issues and Solutions

### Encoding Problems
- **Issue**: Garbled characters in output
- **Solution**: Add more encodings to `encodingPriority`, check source file encoding

### Delimiter Detection Failures
- **Issue**: Fields not properly separated
- **Solution**: Specify delimiter explicitly, check for unusual delimiters

### Header Detection Issues
- **Issue**: Wrong row used as header
- **Solution**: Use `stability_scan` with keywords, adjust `skipRows`

### Type Conversion Errors
- **Issue**: Data doesn't convert to expected types
- **Solution**: Check null representations, use string type as fallback

### Memory Issues
- **Issue**: Out of memory errors with large files
- **Solution**: Reduce chunk size, enable streaming processing
