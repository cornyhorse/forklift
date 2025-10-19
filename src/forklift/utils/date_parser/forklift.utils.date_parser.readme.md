# Forklift Date Parser

## Overview

The `forklift.utils.date_parser` module provides comprehensive date and datetime parsing capabilities for the Forklift data processing ecosystem. It offers flexible, robust parsing of date values from various string formats and legacy date representations, with intelligent format detection and standardized output.

## Role in Forklift Ecosystem

The date parser is a foundational utility that enables Forklift to:

- **Handle Diverse Date Formats**: Parse dates from multiple input formats automatically
- **Standardize Temporal Data**: Convert various date representations to consistent formats
- **Support Legacy Systems**: Handle epoch timestamps and non-standard date formats
- **Enable Schema Compliance**: Ensure date fields match expected schema requirements
- **Power DateTime Transformations**: Provide the parsing foundation for datetime transformations

## Key Features

- **Intelligent Format Detection**: Automatically detects and parses common date formats
- **Multiple Output Formats**: Supports ISO dates, datetime objects, and custom formats
- **Epoch Timestamp Handling**: Converts Unix timestamps and other epoch formats
- **Flexible Configuration**: Customizable format lists and parsing behaviors
- **Performance Optimized**: Efficient parsing for large datasets
- **Error Handling**: Graceful handling of invalid dates with configurable fallbacks

## Module Files

### `core.py`
**Public API Interface**

Provides the main public functions that maintain backward compatibility with the original date_parser interface:
- **`parse_date()`**: Validates if a value can be parsed as a date
- **`coerce_date()`**: Converts values to ISO date format (YYYY-MM-DD)
- **`coerce_datetime()`**: Converts values to ISO datetime format
- **Backward Compatibility**: Maintains compatibility with legacy Forklift code
- **Simple Interface**: Easy-to-use functions for common date parsing tasks

### `parsing.py`
**Core Parsing Logic**

Contains the detailed parsing implementation and format detection:
- **Format Detection**: Intelligent detection of date formats from input strings
- **Multiple Format Support**: Handles dozens of common date format patterns
- **Validation Logic**: Robust validation of parsed date values
- **Custom Format Support**: Allows specification of custom date formats
- **Performance Optimization**: Efficient parsing algorithms for batch processing

### `constants.py`
**Format Definitions**

Defines standard date format patterns and constants:
- **Format Collections**: Predefined sets of common date formats
- **Pattern Definitions**: Regular expressions for date format detection
- **Standard Formats**: ISO and other standardized date format specifications
- **Locale Support**: Format patterns for different regional conventions
- **Extensibility**: Easy addition of new format patterns

### `epoch.py`
**Epoch Timestamp Handling**

Specialized handling for Unix timestamps and epoch-based dates:
- **Unix Timestamps**: Converts seconds/milliseconds since epoch
- **Multiple Epoch Types**: Supports various epoch starting points
- **Precision Handling**: Manages different timestamp precisions
- **Timezone Awareness**: Handles timezone conversions for epoch values
- **Legacy Support**: Compatible with older timestamp formats

### `format_utils.py`
**Format Utilities**

Helper functions for date format manipulation and validation:
- **Format Conversion**: Converts between different format specification systems
- **Validation Helpers**: Utilities for validating date format strings
- **Pattern Matching**: Advanced pattern matching for complex date formats
- **Format Normalization**: Standardizes format specifications
- **Error Handling**: Utilities for graceful format error handling

## Usage Examples

### Basic Date Parsing
```python
from forklift.utils.date_parser import parse_date, coerce_date

# Check if a value can be parsed as a date
is_valid = parse_date("2023-12-25")  # Returns True
is_valid = parse_date("invalid")     # Returns False

# Convert to ISO date format
iso_date = coerce_date("12/25/2023")  # Returns "2023-12-25"
iso_date = coerce_date("Dec 25, 2023")  # Returns "2023-12-25"
```

### Custom Format Specification
```python
from forklift.utils.date_parser import coerce_date

# Specify custom formats
formats = ["%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"]
result = coerce_date("December 25, 2023", formats=formats)
```

### Datetime Parsing
```python
from forklift.utils.date_parser import coerce_datetime

# Convert to ISO datetime format
datetime_str = coerce_datetime("2023-12-25 14:30:00")
# Returns "2023-12-25T14:30:00"
```

### Epoch Timestamp Handling
```python
from forklift.utils.date_parser.epoch import convert_epoch

# Convert Unix timestamp
date_str = convert_epoch(1703520000)  # Returns ISO date
```

## Integration with Forklift

The date parser integrates throughout the Forklift ecosystem:

1. **Data Readers**: Used by CSV, Excel, and other readers to parse date columns
2. **Schema Validation**: Ensures date fields conform to expected formats
3. **Transformations**: Powers the datetime transformation pipeline
4. **Processors**: Used by main data processors for temporal data handling
5. **Output Writers**: Ensures consistent date formats in output data

## Supported Formats

The date parser automatically detects and handles:

- **ISO Formats**: 2023-12-25, 2023-12-25T14:30:00
- **US Formats**: 12/25/2023, 12-25-2023
- **European Formats**: 25/12/2023, 25.12.2023
- **Text Formats**: December 25, 2023, Dec 25 2023
- **Epoch Timestamps**: Unix seconds, milliseconds
- **Custom Formats**: User-specified strptime patterns

## Performance Considerations

- **Format Caching**: Caches successful format patterns for repeated use
- **Early Termination**: Stops parsing attempts once a valid format is found
- **Batch Optimization**: Optimized for processing large datasets
- **Memory Efficiency**: Minimal memory overhead for format detection
- **Error Fast-Fail**: Quick rejection of obviously invalid date strings

## Configuration Options

- **Format Lists**: Specify custom lists of acceptable date formats
- **Validation Levels**: Configure strict vs. lenient parsing behavior
- **Default Formats**: Set preferred formats for ambiguous dates
- **Error Handling**: Configure behavior for unparseable dates
- **Timezone Handling**: Specify timezone conversion preferences

## Error Handling

The date parser provides robust error handling:
- **Graceful Degradation**: Returns None or default values for invalid dates
- **Detailed Logging**: Logs parsing failures for debugging
- **Validation Modes**: Configurable strict vs. permissive parsing
- **Format Feedback**: Reports which formats were attempted
- **Recovery Strategies**: Multiple fallback approaches for edge cases
