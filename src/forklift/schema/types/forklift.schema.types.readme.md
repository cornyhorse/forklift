# forklift.schema.types

## Overview

The `forklift.schema.types` subpackage provides comprehensive data type detection, conversion, and transformation capabilities. It serves as the foundation for intelligent schema generation by analyzing data patterns and converting between different type systems (PyArrow, JSON Schema, database types).

## Key Components

### DataTypeConverter
The core type conversion engine that handles mapping between different type systems:
- **PyArrow to JSON Schema**: Converts PyArrow data types to JSON Schema type definitions with appropriate formats (`date32` -> `{"type": "string", "format": "date"}`, `timestamp` -> `format: date-time`, decimals -> `number`)
- **PyArrow to `parquetType` strings**: `arrow_to_parquet_type_string()` is lossless: decimal precision/scale, timestamp unit and time zone, duration unit and list/dictionary nesting are kept (`decimal128(10,2)`, `timestamp[us, tz=UTC]`)
- **Strict `parquetType` grammar**: `parse_parquet_type()` / `is_valid_parquet_type()` parse the strings the CSV, Excel, SQL and FWF schema importers accept: `int8`..`int64`, `uint8`..`uint64`, `float32`, `double`, `bool`, `string`, `binary`, `date32`, `date64`, `time32[s|ms]`, `time64[us|ns]`, `timestamp[unit]` / `timestamp[unit, tz=ZONE]`, `duration[unit]`, `decimal128(p,s)` / `decimal256(p,s)`, `list<T>`, `dictionary<values=T, indices=INT>` and `struct`. Units are `s|ms|us|ns`; precision, scale and nesting depth are range-checked and a malformed string is invalid rather than prefix-matched
- **Unification**: `unify_parquet_types()` returns the narrowest type that holds several declarations of one field (used for conditional FWF variants), or `None` when they cannot be combined
- **Complex Type Handling**: Processes nested structures, arrays, and object types

### SpecialTypeDetector
Advanced pattern recognition system for detecting domain-specific data types:
- **Email Detection**: Identifies email addresses using pattern matching
- **Phone Number Recognition**: Detects US-style phone numbers with separators (`(123) 456-7890`, `123-456-7890`)
- **SSN Detection**: Recognizes `123-45-6789`
- **ZIP codes**: `12345-6789` from content; a plain 5-digit ZIP is only recognised through the column name
- **IP and MAC addresses**: IPv4/IPv6 and the usual MAC notations
- Column names are matched on whole word tokens (`client_ip`, `zipCode` match; `tip`, `description` do not)

## Type Detection Capabilities

### Primitive Type Analysis
- **Numeric Types**: Integer vs. floating-point detection with precision analysis
- **Boolean Recognition**: Identifies boolean data in various formats (true/false, 1/0, yes/no)
- **String Classification**: Determines string subtypes and format patterns
- **Null Handling**: Analyzes null patterns and nullable field identification
- **Mixed Type Resolution**: Handles columns with multiple data types

### Temporal Type Detection
- **Date Formats**: Recognizes various date formats (ISO 8601, localized formats, custom patterns)
- **DateTime Processing**: Handles timezone-aware and naive datetime formats
- **Time Analysis**: Identifies time-only values and time format patterns
- **Epoch Detection**: Recognizes Unix timestamps and epoch-based time values
- **Relative Dates**: Detects relative date expressions and time intervals

### Complex Type Analysis
- **Array Detection**: Identifies list and array structures in data
- **Object Recognition**: Detects nested object structures and JSON-like data
- **Enum Identification**: Recognizes categorical data and suggests enum constraints
- **Hierarchical Data**: Analyzes parent-child relationships and tree structures
- **Composite Keys**: Identifies multi-field identifier patterns

## Pattern Recognition

### Format Pattern Detection
- **Regular Expressions**: Generates regex patterns for string validation
- **Length Constraints**: Determines appropriate min/max length constraints
- **Character Set Analysis**: Identifies allowed character sets and encoding requirements
- **Case Sensitivity**: Analyzes case patterns and normalization requirements
- **Whitespace Handling**: Detects whitespace significance and trimming needs

### Data Quality Patterns
- **Consistency Analysis**: Identifies format consistency across data samples
- **Completeness Assessment**: Analyzes missing data patterns and requirements
- **Uniqueness Detection**: Determines field uniqueness and identifier potential
- **Range Analysis**: Calculates appropriate numeric and date ranges
- **Outlier Detection**: Identifies anomalous values and data quality issues

## Transformation Support

### Type Coercion Rules
- **Safe Conversions**: Defines safe type conversion paths without data loss
- **Lossy Conversions**: Handles conversions that may result in precision loss
- **Validation Rules**: Creates validation constraints for converted data
- **Default Values**: Suggests appropriate default values for missing data
- **Error Handling**: Defines strategies for handling conversion failures

### Format Standardization
- **Normalization Rules**: Creates rules for data format standardization
- **Cleaning Operations**: Suggests data cleaning transformations
- **Validation Templates**: Generates validation rule templates
- **Business Rules**: Supports domain-specific business rule implementation
- **Custom Transformations**: Framework for implementing custom type transformations

## Usage Patterns

### Basic Type Conversion
```python
from forklift.schema.types import DataTypeConverter
import pyarrow as pa

converter = DataTypeConverter()
arrow_type = pa.string()
json_schema_type = converter.arrow_to_json_schema_type(arrow_type)
# Returns: {"type": "string"}
```

### Special Type Detection
```python
from forklift.schema.types import SpecialTypeDetector

sample_data = ["john@example.com", "jane@company.org"]
special_type = SpecialTypeDetector.detect_special_type("contact", sample_data)
# Returns: "email"
config = SpecialTypeDetector.get_transformation_config(special_type)
# Returns the default email formatting options (normalize_case, validate_format, ...)
```

### Pattern Analysis
```python
# Detect numeric patterns in string data
sample_values = ["$1,234.50", "(67.89)", "100.00"]
patterns = converter.detect_numeric_patterns(sample_values)
# Returns: {"has_thousands_separator": True, "has_decimal_separator": True,
#           "has_currency_symbols": True, "has_parentheses_negative": True}
```

## Integration Points

### Internal Dependencies
- `forklift.schema.processors.*` - Schema processing and generation
- `forklift.schema.utils.*` - Utility functions and helpers
- `forklift.utils.transformations.*` - Data transformation engine

### External Dependencies
- **PyArrow**: Core type system and data processing
- Standard library `re` and `ipaddress` for pattern matching (no pandas or NumPy)

## Type System Mapping

### JSON Schema Formats
- `"date"` - ISO 8601 date format
- `"date-time"` - ISO 8601 datetime format
- `"time"` - ISO 8601 time format
- `"email"` - RFC 5322 email format
- `"uri"` - RFC 3986 URI format
- `"uuid"` - RFC 4122 UUID format

### Custom Format Extensions
These are the `x-special-type` markers a property can carry (see the special type documentation). `import_csv` formats and validates a column that carries one of them before its type is applied: a value that is not valid becomes NULL and is counted as `INVALID_SPECIAL_VALUE:<column>` in `ProcessingResults.validation_summary`.

- `"ssn"` - Social Security Number format
- `"phone"` - Phone number format
- `"email"` - Email address
- `"zip-permissive"`, `"zip-5"`, `"zip-9"` - ZIP code formats
- `"ipv4"`, `"ipv6"`, `"ip"` - IP addresses
- `"mac-address"` - MAC address

## Performance Considerations

- **Sampling Strategy**: Analysis runs on the sampled rows (`nrows`); pattern detection looks at the first values only
- **Pattern Caching**: Compiled regex patterns are cached at class level
