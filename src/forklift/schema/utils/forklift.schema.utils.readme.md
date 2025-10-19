# forklift.schema.utils

## Overview

The `forklift.schema.utils` subpackage provides essential utility functions and helper classes that support schema generation, formatting, and validation operations across the Forklift framework. It includes formatting utilities, error handling, and common helper functions used throughout the schema processing pipeline.

## Key Components

### SchemaFormatter
A comprehensive formatting utility that handles the presentation and structure of generated schemas:
- **Base Schema Creation**: Generates foundational schema structures with proper JSON Schema compliance
- **Metadata Integration**: Adds generation timestamps, version information, and processing metadata
- **Format Standardization**: Ensures consistent formatting across different schema outputs
- **Extension Handling**: Manages custom schema extensions and x-prefixed properties
- **Output Formatting**: Provides multiple output formats (JSON, YAML, pretty-printed JSON)

### SchemaValidationError
Custom exception class for schema-related validation errors:
- **Detailed Error Messages**: Provides comprehensive error descriptions with context
- **Error Categorization**: Classifies errors by type (structure, data, configuration)
- **Validation Context**: Includes information about where validation failures occurred
- **Recovery Suggestions**: Offers actionable suggestions for resolving validation issues
- **Error Aggregation**: Supports collecting multiple validation errors in a single exception

## Utility Functions

### Schema Structure Helpers
- **Base Schema Templates**: Pre-configured schema templates for different file types
- **Property Merging**: Intelligent merging of schema properties from multiple sources
- **Reference Resolution**: Handles JSON Schema $ref references and definitions
- **Schema Normalization**: Standardizes schema structures for consistent processing
- **Validation Rule Integration**: Incorporates validation rules into schema definitions

### Formatting Operations
- **JSON Schema Compliance**: Ensures generated schemas conform to JSON Schema specifications
- **Pretty Printing**: Formats schemas for human readability
- **Compact Serialization**: Generates minimal schema representations for production use
- **YAML Export**: Converts JSON schemas to YAML format for configuration files
- **Documentation Generation**: Creates human-readable schema documentation

### Validation Utilities
- **Schema Structure Validation**: Validates schema compliance with JSON Schema standards
- **Cross-Reference Checking**: Verifies internal schema references and dependencies
- **Constraint Validation**: Validates constraint definitions and logical consistency
- **Format Verification**: Checks format strings and pattern validity
- **Extension Validation**: Validates custom schema extensions and x-properties

## Error Handling

### Exception Hierarchy
- **SchemaValidationError**: Base exception for schema validation issues
- **StructureError**: Schema structure and format violations
- **DataCompatibilityError**: Data-schema compatibility issues
- **ConfigurationError**: Invalid configuration parameters
- **ProcessingError**: Runtime processing failures

### Error Context
- **Location Information**: Precise location of validation failures within schemas
- **Suggested Fixes**: Actionable recommendations for resolving issues
- **Related Errors**: Links to related validation problems
- **Severity Levels**: Classification of error severity (warning, error, critical)
- **Recovery Strategies**: Automated and manual recovery options

## Schema Enhancement

### Metadata Addition
- **Generation Timestamps**: Adds creation and modification timestamps
- **Version Information**: Tracks schema version and compatibility information
- **Processing Statistics**: Includes analysis statistics and confidence metrics
- **Source Attribution**: Records data source information and processing history
- **Quality Metrics**: Embeds data quality assessment results

### Extension Support
- **File Format Extensions**: Handles x-csv, x-excel, x-parquet extensions
- **Transformation Extensions**: Manages x-transformations configuration
- **Custom Extensions**: Framework for adding domain-specific extensions
- **Validation Extensions**: Custom validation rule definitions
- **Processing Hints**: Performance and processing optimization hints

## Output Formatting

### JSON Schema Output
- **Draft-07 Compliance**: Ensures compatibility with JSON Schema Draft-07
- **Proper Escaping**: Handles special characters and unicode in schemas
- **Indentation Control**: Configurable indentation for readability
- **Property Ordering**: Consistent property ordering for diff-friendly output
- **Minification**: Compact output for production environments

### Alternative Formats
- **YAML Export**: Human-readable YAML schema representations
- **Markdown Documentation**: Generated documentation from schemas
- **HTML Reports**: Interactive schema documentation with examples
- **CSV Summaries**: Tabular summaries of schema properties
- **XML Schema**: Conversion to XML Schema format when needed

## Usage Patterns

### Basic Schema Formatting
```python
from forklift.schema.utils import SchemaFormatter

formatter = SchemaFormatter()
base_schema = formatter.create_base_schema("csv")
formatted_schema = formatter.add_generation_metadata(schema, config)
```

### Error Handling
```python
from forklift.schema.utils import SchemaValidationError

try:
    # Schema processing operation
    process_schema(schema)
except SchemaValidationError as e:
    print(f"Validation failed: {e.message}")
    print(f"Suggestions: {e.suggestions}")
```

### Output Formatting
```python
# Pretty-print JSON schema
formatted_json = formatter.format_json(schema, indent=2)

# Export to YAML
yaml_output = formatter.to_yaml(schema)

# Generate documentation
docs = formatter.generate_documentation(schema)
```

## Integration Points

### Internal Dependencies
- `forklift.schema.generator.*` - Core schema generation
- `forklift.schema.processors.*` - Schema processing components
- `forklift.schema.types.*` - Type system integration

### External Dependencies
- **JSON Schema**: Validation against JSON Schema specifications
- **PyYAML**: YAML format support
- **Jinja2**: Template engine for documentation generation
- **Markdown**: Documentation formatting support

## Configuration Options

### Formatting Preferences
- **Indentation Style**: Spaces vs tabs, indentation width
- **Property Ordering**: Alphabetical, logical, or custom ordering
- **Comment Inclusion**: Whether to include explanatory comments
- **Example Data**: Inclusion of example values in schemas
- **Validation Strictness**: Level of validation rule enforcement

### Output Options
- **Format Selection**: JSON, YAML, or custom format selection
- **Compression**: Schema minification and compression options
- **Encoding**: Character encoding preferences
- **Line Endings**: Platform-specific line ending handling
- **Metadata Verbosity**: Level of metadata inclusion

## Performance Features

- **Lazy Formatting**: Deferred formatting operations for large schemas
- **Template Caching**: Cached templates for repeated operations
- **Streaming Output**: Memory-efficient output for large schemas
- **Parallel Processing**: Multi-threaded formatting for complex schemas
- **Memory Optimization**: Minimal memory footprint during processing
