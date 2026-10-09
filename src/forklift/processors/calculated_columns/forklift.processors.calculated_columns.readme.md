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

### Expression Language and Safety

Expressions are **not** passed to Python's `eval`. Each expression is parsed once with `ast`,
validated against a whitelist and interpreted node by node (the compiled form is cached).

Allowed syntax: literals (`'text'`, `42`, `1.5`, `True`, `None`), column names, the constants
`PI`, `E`, `TRUE`, `FALSE`, `NULL`, the operators `+ - * / // % **`, comparisons
(`== != < <= > >= in not in is None`), `and` / `or` / `not`, `a if cond else b`, and calls to the
built-in functions (positional or keyword arguments). Anything else - attribute access
(`x.__class__`), subscripts, lambdas, comprehensions, f-strings, `*args`, `:=`, `__dunder__`
names, calls to anything but a bare built-in function name - is rejected with a `ValueError`
when the processor is created (even with `fail_on_error=False`).

Limits (see `limits.py`): 4096 characters / 512 AST nodes per expression, 64 call arguments,
`|exponent| <= 10000` and `2**65536` for `**`/`power()`, 1,000,000 characters for strings produced by
repetition, concatenation or `replace()` (plus the size of the inputs, so large cell values keep
working). `'text' % x` string formatting is not supported.

Name resolution: constants (`PI`, `NULL`, ...) first, then columns, so a column called `sum` or
`day` can be used as a value while `sum(...)` / `day(...)` still call the function.

**Null semantics (SQL-like)**: arithmetic (`+ - * / // % **`, unary `-`) and ordering comparisons
(`< <= > >=`) with a NULL operand give NULL, whatever the column names are called. `==` / `!=`,
`and` / `or` / `not` and `is None` keep Python semantics; functions handle NULL as documented
(`coalesce`, `isnull`, ...).

**Constants** (`ConstantColumn`) are never turned into expression source: the value is carried on the
`CalculatedColumn` (`constant_value`) and returned as-is, so quotes, backslashes and dates are safe.
`ConstantColumn.to_calculated_column().expression` is only a `repr`-based rendering of the value.

**Errors**: with `fail_on_error=True` (default) a failing column raises `ValueError`; the processor
never returns the unmodified batch together with an error result. With `fail_on_error=False` a row
that fails becomes NULL. A calculated column named like an input column (or another calculated
column) raises `ValueError`. Error messages never contain cell values.

**`now()` / `today()`** return one snapshot per `process_batch` call (every row and every column of
a batch sees the same timestamp).

### Built-in Functions Library

The processor includes 40+ built-in functions covering:
- Mathematical operations (abs, round, sqrt, sin, cos, log)
- String manipulation (concat, upper, lower, trim, substring)
- Date/time functions (now, today, year, month, day)
- Conditional logic (if_then_else, coalesce, nullif)
- Type conversions (to_string, to_int, to_float)
- Null handling with automatic propagation in arithmetic operations

Behaviour worth knowing:
- `round(x, n)` is Python's `round`: ties go to the even neighbour (banker's rounding), e.g.
  `round(0.5) == 0`, `round(1.5) == 2`, `round(2.5) == 2`. Use `floor`/`ceil` for other behaviour.
- `to_bool` parses strings: `true/t/yes/y/1/on` -> True, `false/f/no/n/0/off` -> False, blank -> NULL,
  anything else is an error. Numbers use Python truthiness.
- `substring(x, start, length=None)` is 0-based like a Python slice; NULL `x` or `start` gives NULL.
- `left(x, n)` / `right(x, n)` return `''` for `n <= 0`.
- `sum`, `min`, `max`, `avg` skip NULLs and return NULL when every argument is NULL.
- `power` / `multiply` / `replace` / `concat` refuse results above the size limits.

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
- Safe expression evaluation with an AST whitelist (no `eval`), parsed once per expression
- Automatic null propagation in arithmetic operations
- Function library integration
- Expression validation against sample data
- Row-by-row evaluation with context building
- Error handling with configurable fail-on-error behavior

**Security**: Expressions are interpreted from a validated AST whitelist (`safe_eval.py`, limits in `limits.py`); `eval` is not used anywhere.

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
