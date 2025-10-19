# Forklift Calculated Columns Processor

The **Calculated Columns** package is a core component of the Forklift data processing pipeline that enables dynamic field generation and computation during data transformation. This processor extends PyArrow RecordBatches with calculated fields based on expressions, constants, and business logic.

## Package Context within Forklift

Forklift is a comprehensive data processing tool that provides high-performance data import, intelligent schema generation, and robust validation with PyArrow streaming. The calculated columns processor fits into this ecosystem as one of several specialized processors that transform data during the pipeline execution.

### Position in the Processing Pipeline

```
Data Source → Schema Validation → [Calculated Columns] → Quality Validation → Output
```

The calculated columns processor typically runs after initial schema validation but before final quality checks, allowing:
- Addition of derived fields needed for validation rules
- Business logic implementation during data ingestion
- Partition key generation for optimized storage
- Data quality metrics calculation

### Integration with Other Processors

- **Base Processor**: Inherits from `BaseProcessor` providing standardized batch processing interface
- **Schema Validator**: Works with validated schemas to ensure type safety
- **Constraint Validator**: Calculated fields can be used in constraint validation
- **Quality Processor**: Derived metrics feed into data quality assessments
- **Pipeline**: Orchestrated through the processor pipeline for complex workflows

## Core Functionality

### Supported Column Types

1. **Expression Columns**: Dynamic calculations using Python expressions
2. **Constant Columns**: Static values added to all rows (useful for partitioning)
3. **Calculated Columns**: Generic container supporting both expression and constant logic

### Expression Capabilities

- **Arithmetic Operations**: `+`, `-`, `*`, `/`, `%`, `**`
- **String Operations**: Concatenation, case conversion, substring extraction
- **Date/Time Operations**: Date arithmetic, formatting, component extraction
- **Conditional Logic**: If-then-else, case statements, null handling
- **Mathematical Functions**: Round, abs, sqrt, trigonometric functions
- **Type Conversions**: String, integer, float, boolean conversions
- **Comparison Operations**: Equality, inequality, greater/less than
- **Logical Operations**: AND, OR, NOT
- **Utility Functions**: Min, max, sum, average, coalesce

### Built-in Functions Library

The processor includes 40+ built-in functions covering:
- Mathematical operations (abs, round, sqrt, sin, cos, log)
- String manipulation (concat, upper, lower, trim, substring)
- Date/time functions (now, today, year, month, day)
- Conditional logic (if_then_else, coalesce, nullif)
- Type conversions (to_string, to_int, to_float)
- Null handling with automatic propagation in arithmetic operations

## Python Files Documentation

### 1. `__init__.py`
**Purpose**: Package initialization and public API definition

**Key Exports**:
- `CalculatedColumnsProcessor`: Main processor class
- `CalculatedColumnsConfig`: Configuration container
- `CalculatedColumn`, `ConstantColumn`, `ExpressionColumn`: Data models
- `ExpressionEvaluator`: Expression evaluation engine
- `get_available_functions`, `get_constants`: Function discovery utilities

**Role**: Provides clean public interface for the package while maintaining backward compatibility.

### 2. `models.py`
**Purpose**: Data models and configuration classes

**Key Classes**:
- `CalculatedColumn`: Generic column configuration with expression and data type
- `ConstantColumn`: Static value column with automatic type inference
- `ExpressionColumn`: Expression-based column with dependency tracking
- `CalculatedColumnsConfig`: Processor configuration with validation settings

**Features**:
- Automatic data type inference with PyArrow integration
- Dependency tracking for proper column ordering
- Backward compatibility support for different configuration styles
- Default value handling with sentinel objects

### 3. `evaluator.py`
**Purpose**: Expression evaluation engine

**Key Classes**:
- `ExpressionEvaluator`: Core expression evaluation logic

**Key Features**:
- Safe expression evaluation with restricted builtins
- Automatic null propagation in arithmetic operations
- Function library integration
- Expression validation against sample data
- Row-by-row evaluation with context building
- Error handling with configurable fail-on-error behavior

**Security**: Uses restricted evaluation context to prevent code injection while allowing mathematical and business logic expressions.

### 4. `functions.py`
**Purpose**: Built-in function library for expressions

**Function Categories**:
- **Arithmetic**: Basic math operations with null handling
- **Mathematical**: Advanced math functions (sqrt, log, trigonometric)
- **String**: Text manipulation and formatting
- **Conditional**: Logic and branching operations
- **Date/Time**: Date arithmetic and component extraction
- **Type Conversion**: Safe type casting
- **Comparison**: Equality and ordering operations
- **Logical**: Boolean operations
- **Utility**: Aggregation and utility functions

**Constants**: Provides mathematical constants (PI, E) and logical constants (TRUE, FALSE, NULL).

### 5. `processor.py`
**Purpose**: Main processor implementation

**Key Classes**:
- `CalculatedColumnsProcessor`: Primary processor implementing BaseProcessor interface

**Key Features**:
- Batch processing with PyArrow RecordBatch input/output
- Dependency resolution and topological sorting
- Circular dependency detection
- Column addition to batches while preserving schema
- Validation result generation
- Error handling with partial success support
- Metadata generation for processing audit trails

**Processing Flow**:
1. Validate configuration and dependencies
2. Sort columns by dependency order
3. Process each column in sequence
4. Add calculated columns to batch
5. Return enhanced batch with validation results

## Usage Examples

### Basic Expression Column
```python
from forklift.processors.calculated_columns import (
    CalculatedColumnsProcessor,
    CalculatedColumnsConfig,
    ExpressionColumn
)

config = CalculatedColumnsConfig(
    expressions=[
        ExpressionColumn(
            name="full_name",
            expression="first_name + ' ' + last_name",
            dependencies=["first_name", "last_name"]
        )
    ]
)

processor = CalculatedColumnsProcessor(config)
result_batch, validation_results = processor.process_batch(batch)
```

### Constant Column for Partitioning
```python
config = CalculatedColumnsConfig(
    constants=[
        ConstantColumn(name="data_source", value="customer_data"),
        ConstantColumn(name="load_date", value="2024-10-19")
    ],
    partition_columns=["data_source", "load_date"]
)
```

### Complex Business Logic
```python
config = CalculatedColumnsConfig(
    expressions=[
        ExpressionColumn(
            name="risk_score",
            expression="if_then_else(age < 25, credit_score * 0.8, credit_score * 1.2)",
            dependencies=["age", "credit_score"],
            data_type=pa.float64()
        )
    ]
)
```

## Configuration Options

- `fail_on_error`: Stop processing on first error (default: True)
- `add_metadata`: Include processing metadata in validation results
- `validate_dependencies`: Check for circular dependencies at initialization
- `partition_columns`: Specify columns for partitioned output optimization

## Error Handling

The processor supports flexible error handling:
- **Fail-fast mode**: Stop on first error
- **Partial success mode**: Continue processing, populate failed columns with null
- **Validation results**: Detailed error reporting with error codes and context

## Performance Considerations

- **Dependency Optimization**: Automatic topological sorting minimizes recalculation
- **Batch Processing**: Efficient PyArrow operations on entire columns
- **Memory Management**: Streaming-friendly design for large datasets
- **Type Safety**: Early type validation prevents runtime errors

## Integration Points

### Schema-Driven Configuration
The package integrates with Forklift's schema system to support:
- Schema-based processor configuration
- Type validation against schema definitions
- Automatic dependency resolution from schema metadata

### Pipeline Integration
- Compatible with all Forklift data sources (CSV, Excel, FWF, SQL)
- Works with S3 streaming for cloud-native processing
- Integrates with quality validation and constraint checking

This calculated columns processor provides a powerful, flexible foundation for data transformation within the Forklift ecosystem, enabling complex business logic while maintaining high performance and type safety.
