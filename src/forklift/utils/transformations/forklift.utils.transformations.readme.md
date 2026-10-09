# Forklift Data Transformations

## Overview

The `forklift.utils.transformations` module provides a comprehensive suite of data transformation utilities for the Forklift data processing ecosystem. This module serves as the central hub for all data cleaning, formatting, validation, and standardization operations, integrating seamlessly with Forklift's PyArrow-based processing pipeline.

## Role in Forklift Ecosystem

The transformations module is a core component that enables Forklift to:

- **Clean and Standardize Data**: Apply consistent formatting and cleaning rules across datasets
- **Validate Data Quality**: Ensure data meets specified requirements and constraints
- **Transform Data Types**: Convert between different data types and formats
- **Support Schema Compliance**: Transform data to match target schema requirements
- **Enable Data Integration**: Standardize data from disparate sources for unified processing

## Architecture

The transformation system follows a modular, configuration-driven architecture:
- **Base Classes**: Common interfaces and patterns for all transformers
- **Specialized Transformers**: Domain-specific transformation capabilities
- **Configuration System**: Type-safe configuration objects for all transformations
- **Factory Pattern**: Dynamic creation of transformers based on configuration
- **PyArrow Integration**: Efficient columnar processing with type safety

## Core Module Files

### `base.py`
**Data Transformation Infrastructure**

Provides the main `DataTransformer` class that orchestrates all transformation operations:
- **Unified Interface**: Single entry point for all transformation types
- **Configuration Management**: Handles complex transformation configurations
- **Pipeline Coordination**: Manages execution order and dependencies
- **Performance Optimization**: Efficient batch processing of transformations
- **Integration Point**: Primary interface used by Forklift processors

### `configs.py`
**Configuration Definitions**

Defines typed configuration objects for all transformation types:
- **Type Safety**: Compile-time validation of configuration parameters
- **Documentation**: Self-documenting configuration options with defaults
- **Extensibility**: Easy addition of new configuration types
- **Validation**: Built-in validation for configuration parameters
- **Standardization**: Consistent configuration patterns across all transformers

### `factory.py`
**Transformer Factory**

Implements the factory pattern for creating appropriate transformers:
- **Dynamic Creation**: Creates transformers based on configuration type
- **Registration System**: Allows registration of new transformer types
- **Configuration Mapping**: Maps configuration objects to transformer classes
- **Dependency Injection**: Manages transformer dependencies and initialization
- **Error Handling**: Graceful handling of unknown or invalid configurations

## Specialized Transformers

### `string_transformations.py`
**String Processing**

Comprehensive string cleaning, formatting, and case transformation capabilities:
- **Text Cleaning**: Remove unwanted characters, normalize whitespace
- **Case Transformations**: Convert between different case formats
- **Regex Operations**: Pattern-based find/replace operations
- **String Padding**: Add padding to achieve consistent string lengths
- **Unicode Normalization**: Handle international characters properly

### `datetime_transformations.py`
**Temporal Data Processing**

DateTime parsing, formatting, and timezone conversion capabilities:
- **Date Parsing**: Convert string dates to standardized formats
- **Timezone Conversion**: Handle timezone-aware datetime operations
- **Format Standardization**: Ensure consistent datetime representations
- **Validation**: Verify datetime values meet specified constraints
- **Integration**: Works with the date_parser module for robust parsing

### `numeric_transformations.py`
**Numeric Data Processing**

Numeric cleaning, formatting, and validation operations:
- **Data Type Conversion**: Convert between numeric types safely
- **Range Validation**: Ensure numeric values fall within specified ranges
- **Precision Control**: Manage decimal precision and rounding
- **Currency Formatting**: Handle monetary values and currency symbols
- **Statistical Operations**: Basic statistical transformations and aggregations

### `html_xml_transformations.py`
**Markup Processing (text extraction)**

Specialized handling for HTML and XML content:
- **Tag Removal**: Tags are stripped first with the standard library's `html.parser` tokenizer
  (quoted `>` in attributes, comments, doctypes and processing instructions are handled like a
  browser would); `<script>`/`<style>` content is dropped; a `<` that does not start a tag
  (`a < b`) stays text; a tag or comment still open at the end of the value is dropped
- **Entity Decoding**: Entities are decoded *after* tag removal, exactly once, and the decoded text
  is never re-interpreted as markup (`a &lt; b and c &gt; d` keeps all its words)
- **CDATA**: `<![CDATA[...]]>` content is kept as literal text

> **This is text extraction, not a security sanitizer.** The result is plain text that can still
> contain `<`, `>` and `&` (for example `5 &lt; 6` becomes `5 < 6`). Escape it for the target
> context (HTML, SQL, shell) before using it anywhere that interprets markup.

### `format_transformations.py`
**Legacy Format Support**

Provides backward compatibility and legacy format transformation support:
- **Legacy Interface**: Maintains compatibility with older Forklift versions
- **Format Bridging**: Bridges between old and new transformation APIs
- **Migration Support**: Helps migrate from legacy transformation patterns
- **Deprecation Management**: Manages deprecated transformation methods

## Sub-Modules

### `format/`
**Specialized Format Transformers**

A dedicated sub-module providing formatters for specific data types:
- **Email Formatting**: Email address validation and normalization
- **Phone Number Formatting**: Phone number standardization across formats
- **Postal Code Formatting**: ZIP and postal code formatting
- **Network Address Formatting**: IP and MAC address formatting
- **SSN Formatting**: Social Security Number formatting with privacy options

*See [Format Transformations README](format/forklift.utils.transformations.format.readme.md) for detailed documentation.*

## Usage Examples

### Basic String Transformation
```python
from forklift.utils.transformations.base import DataTransformer
from forklift.utils.transformations.configs import StringCleaningConfig

config = StringCleaningConfig(
    strip_whitespace=True,
    normalize_case="lower",
    remove_punctuation=True
)

transformer = DataTransformer()
result = transformer.apply_string_cleaning(column_data, config)
```

### DateTime Processing
```python
from forklift.utils.transformations.configs import DateTimeTransformConfig

config = DateTimeTransformConfig(
    input_format="%m/%d/%Y",
    output_format="iso",
    timezone="UTC"
)

result = transformer.apply_datetime_transformation(column_data, config)
```

### Multiple Transformations
```python
# Configure multiple transformations for different columns
transformations = {
    'email_column': EmailConfig(normalize_case=True),
    'phone_column': PhoneNumberConfig(format_style="standard"),
    'date_column': DateTimeTransformConfig(output_format="iso")
}

# Apply all transformations
results = transformer.apply_transformations(data_table, transformations)
```

## Integration with Forklift

The transformations module integrates throughout the Forklift ecosystem:

1. **Data Processors**: Core component of data processing pipelines
2. **Schema Validation**: Ensures data conforms to target schemas
3. **Input Readers**: Transforms data during the reading process
4. **Output Writers**: Formats data for output systems
5. **Validation Framework**: Validates transformed data quality

## Performance Considerations

- **Vectorized Operations**: Leverages PyArrow for efficient columnar processing
- **Memory Management**: Optimized memory usage for large datasets
- **Lazy Evaluation**: Defers expensive operations until necessary
- **Batch Processing**: Processes data in optimized chunks
- **Type Safety**: Minimal runtime type checking overhead

## Configuration Management

All transformations use a consistent configuration system:
- **Type Safety**: Strongly typed configuration objects
- **Validation**: Built-in parameter validation
- **Documentation**: Self-documenting with clear defaults
- **Composition**: Configurations can be composed and reused
- **Serialization**: Configurations can be serialized for persistence

## Behaviour Notes

- **Arrow types are preserved**: every string transformer accepts `string` and `large_string`
  and returns the input column's type (also for all-null results), so transformations chain.
- **No pandas**: transformers work on Arrow data; nulls are `None`, never `NaN`.
- **Unknown options fail**: `create_transformation_from_config` raises `ValueError` listing the
  valid keys instead of silently dropping a misspelled option such as `zeropad`.
- **Numeric/money separators**: setting only `decimal_separator=","` implies
  `thousands_separator="."` (and vice versa); both set to the same value raises `ValueError`.
  Integer targets (`int8`..`uint64`) only accept integral values (`"3.9"` becomes NULL) and
  produce exactly the requested Arrow type; NaN/Infinity text and overflow become NULL.
- **Regular expressions**: `regex_replace` patterns are compiled when the configuration is
  created (bad patterns raise `ValueError`). They run on stdlib `re`, which has no timeout: only
  use patterns from trusted schemas.
- **Lossy defaults**: `unicode_normalize="NFKC"` and `ascii_only=True` are lossy; set
  `unicode_normalize=None` to keep the original characters.
- **Mojibake repair** (`fix_encoding_errors`): text with typical cp1252/latin-1 mojibake markers is
  re-decoded as UTF-8 only if that round trip succeeds; otherwise it is left unchanged.
- **Datetime**: `timezone` is validated when the configuration is created (IANA names via
  `zoneinfo`, falling back to `pytz` if installed); unparseable cells become NULL, anything else
  raises. `dayfirst` (default `True`) resolves dates such as `03-04-2024`.

## Error Handling

Robust error handling throughout the transformation pipeline:
- **Graceful Degradation**: Continues processing when possible
- **Detailed Logging**: Comprehensive logging for debugging
- **Validation Feedback**: Clear error messages for configuration issues
- **Recovery Strategies**: Multiple approaches for handling edge cases
- **Fail-Fast Options**: Configurable strict vs. permissive behavior
