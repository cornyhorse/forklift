# forklift.schema.types

## Overview

The `forklift.schema.types` subpackage provides comprehensive data type detection, conversion, and transformation capabilities. It serves as the foundation for intelligent schema generation by analyzing data patterns and converting between different type systems (PyArrow, JSON Schema, database types).

## Key Components

### DataTypeConverter
The core type conversion engine that handles mapping between different type systems:
- **PyArrow to JSON Schema**: Converts PyArrow data types to JSON Schema type definitions with appropriate formats
- **Type Enhancement**: Adds format constraints, patterns, and validation rules based on data analysis
- **Complex Type Handling**: Processes nested structures, arrays, and object types
- **Precision Mapping**: Maintains type precision information across conversions
- **Format Detection**: Identifies specific formats like dates, times, emails, and URIs

### SpecialTypeDetector
Advanced pattern recognition system for detecting domain-specific data types:
- **Email Detection**: Identifies email addresses using pattern matching and validation
- **Phone Number Recognition**: Detects various phone number formats and international patterns
- **SSN/Tax ID Detection**: Recognizes social security numbers and tax identification formats
- **Geographic Data**: Identifies ZIP codes, postal codes, and geographic coordinates
- **Financial Data**: Detects currency amounts, account numbers, and financial identifiers
- **Custom Pattern Support**: Extensible framework for adding domain-specific type detection

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

detector = SpecialTypeDetector()
sample_data = ["john@example.com", "jane@company.org"]
is_email = detector.is_email_field(sample_data)
# Returns: True
```

### Pattern Analysis
```python
# Detect numeric patterns in string data
sample_values = ["123.45", "67.89", "100.00"]
patterns = converter.detect_numeric_patterns(sample_values)
# Returns: {"is_currency": True, "decimal_places": 2}
```

## Integration Points

### Internal Dependencies
- `forklift.schema.processors.*` - Schema processing and generation
- `forklift.schema.utils.*` - Utility functions and helpers
- `forklift.transformations.*` - Data transformation engine

### External Dependencies
- **PyArrow**: Core type system and data processing
- **Pandas**: Data analysis and type inference
- **NumPy**: Numerical computing and array operations
- **regex**: Advanced pattern matching capabilities

## Type System Mapping

### JSON Schema Formats
- `"date"` - ISO 8601 date format
- `"date-time"` - ISO 8601 datetime format
- `"time"` - ISO 8601 time format
- `"email"` - RFC 5322 email format
- `"uri"` - RFC 3986 URI format
- `"uuid"` - RFC 4122 UUID format

### Custom Format Extensions
- `"ssn"` - Social Security Number format
- `"phone"` - Phone number format
- `"zip-code"` - ZIP/postal code format
- `"currency"` - Monetary amount format
- `"percentage"` - Percentage value format

## Performance Considerations

- **Sampling Strategy**: Efficient analysis of large datasets through statistical sampling
- **Pattern Caching**: Caches compiled regex patterns for performance
- **Lazy Evaluation**: Defers expensive type analysis until needed
- **Memory Optimization**: Minimizes memory usage during type detection
- **Parallel Processing**: Supports parallel analysis of independent columns
