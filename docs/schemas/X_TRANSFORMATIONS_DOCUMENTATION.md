# x-transformations Documentation

## Overview
The `x-transformations` extension provides comprehensive data transformation and standardization capabilities for processing files with advanced data cleaning, normalization, and formatting options. This feature supports file-type-specific transformations and field-level customization.

## Schema Structure
```json
{
  "x-transformations": {
    "description": "Data transformation and standardization configuration",
    "stringCleaning": {
      "normalizeQuotes": true,
      "normalizeDashes": true,
      "normalizeSpaces": true,
      "collapseWhitespace": true,
      "stripWhitespace": true,
      "removeZeroWidth": true,
      "removeControlChars": true,
      "preserveNewlines": false,
      "unicodeNormalize": "NFKC"
    },
    "caseTransformation": {
      "caseTransform": "title",
      "fixCaseIssues": true,
      "titleCaseExceptions": ["of", "the", "and", "or", "but"],
      "customCaseMappings": {
        "ca": "CA",
        "ny": "NY",
        "usa": "USA"
      }
    },
    "numericCleaning": {
      "thousandsSeparator": ",",
      "decimalSeparator": ".",
      "allowNaN": true,
      "nanValues": ["", "N/A", "NULL"],
      "stripWhitespace": true
    },
    "moneyType": {
      "currencySymbols": ["$", "€", "£"],
      "thousandsSeparator": ",",
      "decimalSeparator": ".",
      "parenthesesNegative": true
    },
    "dateTimeParsing": {
      "mode": "specify_formats",
      "allowFuzzy": false,
      "targetType": "date",
      "formats": ["%Y-%m-%d", "%m/%d/%Y"]
    }
  }
}
```

## Transformation Categories

### String Cleaning
Comprehensive text normalization and cleaning options.

#### `normalizeQuotes`
- **Type**: Boolean
- **Description**: Converts various quote characters to standard ASCII quotes
- **Implementation**: Replaces smart quotes, backticks, and Unicode quotes with " and '
- **Default**: `true`

#### `normalizeDashes`
- **Type**: Boolean  
- **Description**: Standardizes various dash and hyphen characters
- **Implementation**: Converts em-dashes, en-dashes, and Unicode hyphens to standard ASCII hyphen (-)
- **Default**: `true`

#### `normalizeSpaces`
- **Type**: Boolean
- **Description**: Converts various space characters to standard ASCII space
- **Implementation**: Replaces non-breaking spaces, tabs, and Unicode spaces with standard space
- **Default**: `true`

#### `collapseWhitespace`
- **Type**: Boolean
- **Description**: Collapses multiple consecutive whitespace characters to single spaces
- **Implementation**: Replaces sequences of whitespace with single space character
- **Default**: `true`

#### `stripWhitespace`
- **Type**: Boolean
- **Description**: Removes leading and trailing whitespace
- **Implementation**: Applies trim() operation to string values
- **Default**: `true`

#### `removeZeroWidth`
- **Type**: Boolean
- **Description**: Removes zero-width Unicode characters
- **Implementation**: Strips zero-width spaces, joiners, and non-joiners
- **Default**: `true`

#### `removeControlChars`
- **Type**: Boolean
- **Description**: Removes ASCII control characters (except newlines/tabs if preserved)
- **Implementation**: Filters out characters in range 0x00-0x1F and 0x7F-0x9F
- **Default**: `true`

#### `preserveNewlines`
- **Type**: Boolean
- **Description**: Whether to preserve newline characters during cleaning
- **Implementation**: Excludes \n and \r from control character removal when true
- **Default**: `false`

#### `unicodeNormalize`
- **Type**: String
- **Description**: Unicode normalization form to apply
- **Values**: `"NFC"`, `"NFD"`, `"NFKC"`, `"NFKD"`, `null`
- **Implementation**: Applies specified Unicode normalization
- **Default**: `"NFKC"`

### Case Transformation
Advanced case normalization with business logic support.

#### `caseTransform`
- **Type**: String
- **Description**: Primary case transformation to apply
- **Values**:
  - `"upper"`: Convert to uppercase
  - `"lower"`: Convert to lowercase  
  - `"title"`: Convert to title case
  - `"sentence"`: Sentence case (first letter capitalized)
  - `"none"`: No transformation
- **Default**: `"none"`

#### `fixCaseIssues`
- **Type**: Boolean
- **Description**: Automatically fix common case problems
- **Implementation**: Corrects all-caps words, improper capitalization
- **Default**: `true`

#### `titleCaseExceptions`
- **Type**: Array of strings
- **Description**: Words to keep lowercase in title case transformation
- **Default**: `["of", "the", "and", "or", "but", "in", "on", "at", "to", "for", "with"]`

#### `customCaseMappings`
- **Type**: Object
- **Description**: Custom word-specific case mappings
- **Implementation**: Exact word replacements after case transformation
- **Example**: `{"ca": "CA", "ny": "NY", "usa": "USA"}`

#### `caseMappingMode`
- **Type**: String
- **Description**: How to apply custom case mappings
- **Values**:
  - `"exact"`: Exact word match
  - `"partial"`: Substring match
  - `"regex"`: Regular expression match

### Numeric Cleaning
Standardization of numeric data formats.

#### `thousandsSeparator`
- **Type**: String
- **Description**: Expected thousands separator character
- **Implementation**: Removes specified character from numeric strings
- **Default**: `","`

#### `decimalSeparator`
- **Type**: String
- **Description**: Expected decimal separator character
- **Implementation**: Converts to standard decimal point if different
- **Default**: `"."`

#### `allowNaN`
- **Type**: Boolean
- **Description**: Whether to allow NaN values in numeric fields
- **Default**: `true`

#### `nanValues`
- **Type**: Array of strings
- **Description**: String values to treat as NaN/null
- **Default**: `["", "N/A", "NULL", "null", "NaN", "nan"]`

#### `stripWhitespace`
- **Type**: Boolean
- **Description**: Remove whitespace from numeric strings before parsing
- **Default**: `true`

### Money Type Cleaning
Specialized handling for currency and monetary values.

#### `currencySymbols`
- **Type**: Array of strings
- **Description**: Currency symbols to remove during parsing
- **Default**: `["$", "€", "£", "¥", "₹", "₽", "¢"]`

#### `thousandsSeparator`
- **Type**: String
- **Description**: Thousands separator in monetary values
- **Default**: `","`

#### `decimalSeparator`
- **Type**: String
- **Description**: Decimal separator in monetary values
- **Default**: `"."`

#### `parenthesesNegative`
- **Type**: Boolean
- **Description**: Whether parentheses indicate negative values
- **Implementation**: Converts "(100.00)" to "-100.00"
- **Default**: `true`

### DateTime Parsing
Advanced date and time parsing with format detection.

#### `mode`
- **Type**: String
- **Description**: DateTime parsing strategy
- **Values**:
  - `"specify_formats"`: Use only specified formats
  - `"auto_detect"`: Automatically detect formats
  - `"fuzzy"`: Allow fuzzy parsing
  - `"sql_aware"`: SQL-specific datetime handling
- **Default**: `"specify_formats"`

#### `allowFuzzy`
- **Type**: Boolean
- **Description**: Enable fuzzy date parsing for ambiguous formats
- **Default**: `false`

#### `fromEpoch`
- **Type**: Boolean
- **Description**: Parse numeric values as epoch timestamps
- **Default**: `false`

#### `targetType`
- **Type**: String
- **Description**: Target data type for parsed dates
- **Values**: `"date"`, `"datetime"`, `"timestamp"`
- **Default**: `"date"`

#### `formats`
- **Type**: Array of strings
- **Description**: Specific date formats to try when mode is "specify_formats"
- **Format**: Python strftime format codes
- **Example**: `["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]`

## File-Type Specific Extensions

### Fixed-Width File (FWF) Specific
```json
{
  "stringCleaning": {
    "fwfSpecific": {
      "respectFieldBoundaries": true,
      "trimPaddingChars": true,
      "paddingChar": " ",
      "preserveLeadingZeros": true
    }
  },
  "numericCleaning": {
    "fwfSpecific": {
      "paddedWithZeros": true,
      "signedFormat": "leading",
      "impliedDecimal": false,
      "decimalPlaces": 2
    }
  }
}
```

### SQL Database Specific
```json
{
  "stringCleaning": {
    "sqlSpecific": {
      "handleNullStrings": true,
      "emptyStringToNull": false,
      "trimVarcharPadding": true
    }
  },
  "numericCleaning": {
    "sqlSpecific": {
      "handleSqlNull": true,
      "preservePrecision": true,
      "scaleHandling": "preserve"
    }
  }
}
```

## Field-Specific Transformations

You can apply different transformations to specific fields:

```json
{
  "x-transformations": {
    "fieldSpecific": {
      "customer_name": {
        "transformations": ["stringCleaning", "caseTransformation"],
        "caseTransform": "title"
      },
      "salary": {
        "transformations": ["moneyType", "numericCleaning"]
      },
      "birth_date": {
        "transformations": ["dateTimeParsing"],
        "dateTimeConfig": {
          "formats": ["%Y-%m-%d", "%m/%d/%Y"]
        }
      }
    }
  }
}
```

## Implementation Details

### Processing Pipeline
1. **Field Classification**: Determine data type and required transformations
2. **Pre-processing**: Apply file-type specific rules
3. **Core Transformations**: Execute primary transformation categories
4. **Field-Specific Rules**: Apply custom field transformations
5. **Validation**: Verify transformation results
6. **Error Handling**: Route transformation failures appropriately

### Performance Optimization
- **Lazy Evaluation**: Only apply transformations when necessary
- **Caching**: Cache compiled regex patterns and normalization tables
- **Vectorization**: Process multiple records simultaneously when possible
- **Memory Management**: Stream processing for large datasets

### Error Handling
- **Transformation Failures**: Route to bad rows when transformations fail
- **Partial Success**: Handle cases where some transformations succeed
- **Rollback**: Option to preserve original values on transformation failure

## Usage Examples

### Basic String Cleaning
```json
{
  "x-transformations": {
    "stringCleaning": {
      "normalizeQuotes": true,
      "collapseWhitespace": true,
      "stripWhitespace": true,
      "unicodeNormalize": "NFKC"
    }
  }
}
```

### Business Name Standardization
```json
{
  "x-transformations": {
    "caseTransformation": {
      "caseTransform": "title",
      "titleCaseExceptions": ["of", "the", "and", "LLC", "Inc"],
      "customCaseMappings": {
        "llc": "LLC",
        "inc": "Inc",
        "corp": "Corp"
      }
    }
  }
}
```

### Financial Data Processing
```json
{
  "x-transformations": {
    "moneyType": {
      "currencySymbols": ["$"],
      "thousandsSeparator": ",",
      "parenthesesNegative": true
    },
    "numericCleaning": {
      "allowNaN": false,
      "nanValues": ["", "N/A", "--"]
    }
  }
}
```

### Multi-Format Date Handling
```json
{
  "x-transformations": {
    "dateTimeParsing": {
      "mode": "specify_formats",
      "formats": [
        "%Y-%m-%d",
        "%m/%d/%Y", 
        "%d/%m/%Y",
        "%Y%m%d"
      ],
      "targetType": "date"
    }
  }
}
```

## Best Practices

1. **Start Conservative**: Begin with basic transformations and add complexity as needed
2. **Test with Real Data**: Validate transformation rules with actual data samples
3. **Document Business Rules**: Clearly document why specific transformations are applied
4. **Monitor Transformation Success**: Track rates of successful transformations
5. **Field-Specific Rules**: Use field-specific configurations for complex requirements
6. **Performance Testing**: Benchmark transformation performance with large datasets
7. **Preserve Originals**: Consider keeping original values for audit trails
