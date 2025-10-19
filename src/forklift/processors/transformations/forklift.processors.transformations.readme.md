# Forklift Processors - Transformations Package

## Overview

The `forklift.processors.transformations` package provides powerful data transformation capabilities for the Forklift data processing framework. This package is a core component of the Forklift processor ecosystem, enabling users to apply standardization, cleaning, formatting, and custom transformations to their data during the ETL process.

## Package Architecture

This package follows a modular design pattern with clear separation of concerns:

- **Base Processors**: Built on the `BaseProcessor` interface for consistent integration with the Forklift pipeline
- **Configuration-Driven**: Supports both programmatic configuration and schema-driven automatic transformations
- **PyArrow Integration**: Leverages PyArrow for high-performance columnar data processing
- **Extensible Design**: Easy to add new transformation types and functions

## Core Components

### 1. `__init__.py` - Package Interface
The package entry point that provides a clean API by re-exporting all essential components:

- **Processors**: `ColumnTransformer`, `SchemaBasedTransformer`
- **Common Functions**: `trim_whitespace`, `uppercase`, `lowercase`
- **Factory Functions**: Various `apply_*` functions for creating transformation pipelines
- **Configuration Classes**: All transformation configuration objects from utils
- **Utilities**: `DataTransformer` and configuration creation utilities

### 2. `column_transformer.py` - Basic Column Transformations

**Purpose**: Provides programmatic column-level transformations with explicit configuration.

**Key Features**:
- Maps column names to lists of transformation functions
- Applies transformations in sequence for each configured column
- Handles transformation errors gracefully with detailed validation results
- Works with PyArrow RecordBatch data structures

**Usage Example**:
```python
transformer = ColumnTransformer({
    'name': [trim_whitespace, uppercase],
    'phone': [apply_phone_formatting()]
})
```

### 3. `schema_transformer.py` - Schema-Driven Transformations

**Purpose**: Automatically applies transformations based on JSON schema configurations and special type markers.

**Key Features**:
- **Auto-Detection**: Recognizes `x-special-type` fields in schemas and applies appropriate transformations
- **Schema Extensions**: Processes `x-transformations` section for explicit transformation configuration
- **Special Types Supported**:
  - `ssn` - Social Security Number formatting
  - `zip-permissive`, `zip-5`, `zip-9` - ZIP code formatting
  - `phone` - Phone number formatting
  - `email` - Email address normalization
  - `ipv4`, `ipv6`, `ip` - IP address formatting
  - `mac-address` - MAC address formatting

**Configuration Format**:
```json
{
  "x-transformations": {
    "column_transformations": {
      "column_name": {
        "transformation_type": {
          "enabled": true,
          "config_option": "value"
        }
      }
    }
  }
}
```

### 4. `common.py` - Basic String Operations

**Purpose**: Provides fundamental string transformation functions using PyArrow compute functions.

**Available Functions**:
- `trim_whitespace(column)` - Removes leading/trailing whitespace
- `uppercase(column)` - Converts strings to uppercase
- `lowercase(column)` - Converts strings to lowercase

**Implementation**: Uses PyArrow compute functions (`pc.utf8_trim_whitespace`, `pc.utf8_upper`, etc.) for optimal performance.

### 5. `factories.py` - Advanced Transformation Factories

**Purpose**: Creates complex transformation functions with configuration objects.

**Available Factory Functions**:

#### Financial Data
- `apply_money_conversion()` - Handles currency symbols, thousands separators, parentheses for negatives

#### Numeric Processing
- `apply_numeric_cleaning()` - Standardizes numeric formats, handles separators, type conversion

#### Text Processing
- `apply_regex_replace()` - Pattern-based text replacement with regex support
- `apply_string_replace()` - Simple string replacement operations
- `apply_html_xml_cleaning()` - Removes HTML/XML tags and decodes entities
- `apply_string_padding()` - Pads strings to specified width
- `apply_string_trimming()` - Advanced trimming with configurable characters and sides

**Factory Pattern Benefits**:
- Encapsulates complex configuration
- Creates reusable transformation functions
- Provides consistent parameter validation
- Enables easy composition of transformation pipelines

## Integration with Forklift Framework

### Processor Pipeline Integration
Transformation processors integrate seamlessly with the Forklift processor pipeline:

1. **Input**: Receives PyArrow RecordBatch from previous processors
2. **Processing**: Applies configured transformations column by column
3. **Output**: Returns transformed RecordBatch and validation results
4. **Error Handling**: Provides detailed error reporting for debugging

### Schema Integration
The package integrates with Forklift's schema validation system:

- Reads transformation rules from JSON schemas
- Automatically applies transformations based on field types
- Validates transformation results against schema constraints
- Provides error messages that integrate with schema validation reports

### Configuration Management
Supports multiple configuration approaches:

- **Programmatic**: Direct instantiation with transformation dictionaries
- **Schema-Based**: Automatic configuration from JSON schema extensions
- **Factory-Based**: Configuration objects for complex transformations
- **Hybrid**: Mix of programmatic and schema-driven approaches

## Usage Patterns

### 1. Simple Column Transformations
```python
from forklift.processors.transformations import ColumnTransformer, trim_whitespace, uppercase

transformer = ColumnTransformer({
    'customer_name': [trim_whitespace, uppercase],
    'email': [trim_whitespace, lowercase]
})
```

### 2. Schema-Driven Processing
```python
from forklift.processors.transformations import SchemaBasedTransformer

# Schema with x-transformations extension
schema = {
    "properties": {
        "ssn": {"type": "string", "x-special-type": "ssn"},
        "zip_code": {"type": "string", "x-special-type": "zip-5"}
    },
    "x-transformations": {
        "column_transformations": {
            "description": {
                "string_cleaning": {"enabled": true, "strip_html": true}
            }
        }
    }
}

transformer = SchemaBasedTransformer(schema)
```

### 3. Advanced Factory Usage
```python
from forklift.processors.transformations import (
    ColumnTransformer, apply_money_conversion, apply_regex_replace
)

transformer = ColumnTransformer({
    'salary': [apply_money_conversion(currency_symbols=['$', '€'])],
    'product_code': [apply_regex_replace(r'[^A-Z0-9]', '', flags=re.IGNORECASE)]
})
```

## Performance Considerations

- **PyArrow Optimization**: All transformations use PyArrow compute functions for vectorized operations
- **Memory Efficiency**: Transformations operate on columnar data without unnecessary copying
- **Batch Processing**: Designed to work efficiently with large RecordBatch objects
- **Error Isolation**: Transformation errors don't halt entire batch processing

## Extension Points

### Adding Custom Transformations
1. Create transformation functions that accept `pa.Array` and return `pa.Array`
2. Add factory functions in `factories.py` for complex configurations
3. Register new transformation types in `create_transformation_from_config`
4. Add special type handling in `SchemaBasedTransformer._add_special_type_transformations`

### Configuration Extensions
- Extend configuration classes in `utils.transformations`
- Add new schema extension patterns
- Create domain-specific transformation factories

## Error Handling

The package provides comprehensive error handling:

- **Validation Results**: Each processor returns `ValidationResult` objects
- **Error Codes**: Standardized error codes for different failure types
- **Column-Level Errors**: Errors are isolated to specific columns
- **Graceful Degradation**: Processing continues even when some transformations fail

## Dependencies

- **PyArrow**: Core data structure and compute functions
- **Forklift Utils**: Transformation utilities and configuration classes
- **Forklift Base**: `BaseProcessor` interface and `ValidationResult`

This package serves as the primary transformation engine for the Forklift data processing framework, providing flexible, performant, and extensible data transformation capabilities for ETL workflows.
