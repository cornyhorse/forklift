# Forklift Engine

## Overview

The **Forklift Engine** is a high-performance data processing framework designed for streaming import, validation, and transformation of various data formats. Built around Apache PyArrow for memory-efficient processing, Forklift provides a unified interface for importing data from CSV, Excel, and SQL sources with comprehensive validation, error handling, and metadata generation.

## Core Purpose

Forklift Engine serves as the central orchestration layer for data import operations, providing:

- **Streaming Data Processing**: Memory-efficient handling of large datasets using PyArrow streaming
- **Multi-Format Support**: Unified API for CSV, Excel, and SQL data sources
- **Schema Validation**: Comprehensive data validation against JSON schemas
- **Error Handling**: Separation of valid and invalid data with detailed error reporting
- **Cloud Integration**: Native support for S3 input/output with streaming capabilities
- **Metadata Generation**: Automatic creation of manifests, metadata, and processing reports

## Architecture

The Forklift Engine follows a modular architecture with distinct responsibilities:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   forklift_core │───▶│   Importers     │───▶│   Processors    │
│   (Orchestrator)│    │  (Data Sources) │    │ (Transformation)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Config       │    │   Excel/SQL     │    │   Validation    │
│  (Settings)     │    │   Importers     │    │   & Output      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Core Components

#### 1. **forklift_core.py** - The Engine Orchestrator

The `ForkliftCore` class serves as the central orchestration engine that:

- **Coordinates Processing Workflow**: Manages the end-to-end data import pipeline
- **Provides Unified API**: Offers consistent interface across different data formats
- **Handles Configuration**: Integrates with the configuration system for flexible processing
- **Manages Resources**: Ensures proper initialization and cleanup of processing components

**Key Responsibilities:**
```python
class ForkliftCore:
    def __init__(self, config: ImportConfig)
    def process_csv(self) -> ProcessingResults
```

The core engine delegates format-specific processing to specialized components while maintaining consistent error handling and result reporting across all data sources.

#### 2. **Public API Functions**

The engine exposes high-level functions for different data formats:

- **`import_csv()`**: Streaming CSV processing with PyArrow
- **`import_excel()`**: Multi-sheet Excel file processing
- **`import_sql()`**: Database import with ODBC connectivity
- **`import_fwf()`**: Fixed-width file processing (planned)

Each function provides a simplified interface while supporting advanced configuration through keyword arguments.

## Processing Pipeline

### 1. **Initialization Phase**
```
Input Configuration → Validation → Component Setup → Resource Allocation
```

- Configuration validation and setup
- Schema loading and parsing (if provided)
- Processor component initialization
- Output directory preparation

### 2. **Data Import Phase**
```
Source Detection → Format-Specific Import → Streaming Setup → Header Detection
```

- **CSV**: PyArrow streaming reader with configurable batching
- **Excel**: Multi-sheet processing with workbook optimization
- **SQL**: ODBC streaming with batch fetching

### 3. **Processing Phase**
```
Batch Processing → Schema Validation → Error Separation → Output Generation
```

- Stream data in configurable batches for memory efficiency
- Apply schema validation to each batch
- Separate valid and invalid data into different output streams
- Generate Parquet files with compression

### 4. **Finalization Phase**
```
Metadata Generation → Manifest Creation → Resource Cleanup → Results Reporting
```

- Create comprehensive metadata about processed data
- Generate file manifests for output tracking
- Clean up temporary resources and connections
- Return detailed processing results

## Key Features

### Streaming Architecture

Forklift uses PyArrow's streaming capabilities to process large datasets efficiently:

```python
# Memory-efficient processing of large files
results = import_csv(
    input_path="large_dataset.csv",
    output_path="output/",
    batch_size=50000  # Process in 50K row batches
)
```

### Schema-Driven Validation

Comprehensive validation against JSON schemas with flexible error handling:

```python
# Schema validation with error separation
results = import_csv(
    input_path="data.csv",
    output_path="output/",
    schema_file="validation_schema.json",
    max_validation_errors=1000  # Stop after 1000 errors
)

# Access validation results
print(f"Valid rows: {results.valid_rows}")
print(f"Invalid rows: {results.invalid_rows}")
print(f"Error files: {results.bad_rows_file}")
```

### Multi-Format Support

Unified interface across different data sources:

```python
# CSV processing
csv_results = import_csv("data.csv", "output/")

# Excel processing with multi-sheet support
excel_results = import_excel("workbook.xlsx", "output/")

# SQL database import
sql_results = import_sql(
    connection_string="DRIVER={SQL Server};SERVER=localhost;...",
    output_path="output/",
    schema_file="sql_schema.json"
)
```

### Cloud Integration

Native S3 support for input and output operations:

```python
# S3 to S3 processing
results = import_csv(
    input_path="s3://input-bucket/data.csv",
    output_path="s3://output-bucket/processed/",
    schema_file="s3://config-bucket/schema.json"
)
```

### Advanced Configuration

Flexible configuration system supporting various processing modes:

```python
# Advanced CSV configuration
results = import_csv(
    input_path="complex.csv",
    output_path="output/",
    delimiter="|",
    encoding="latin-1",
    header_mode=HeaderMode.AUTO,
    excess_column_mode=ExcessColumnMode.REJECT,
    footer_detection={"patterns": ["^Total:", "^Summary:"]},
    compression="gzip"
)
```

## Configuration System

The engine uses a comprehensive configuration system through `ImportConfig`:

### Core Settings
- **File Paths**: Input/output locations (local or S3)
- **Processing Options**: Batch sizes, encoding, delimiters
- **Validation Settings**: Schema files, error thresholds
- **Output Configuration**: Compression, metadata generation

### Header Detection
- **PRESENT**: Headers expected at specified location
- **ABSENT**: No headers, use schema or generate names
- **AUTO**: Automatic header detection using content analysis

### Error Handling
- **ExcessColumnMode**: TRUNCATE, REJECT, or PASSTHROUGH extra columns
- **Validation Limits**: Configurable error thresholds
- **Bad Data Separation**: Invalid rows written to separate files

## Output Generation

### Primary Outputs
- **Parquet Files**: Compressed columnar data files
- **Metadata JSON**: Processing statistics and configuration
- **Manifest Files**: List of generated output files

### Error Outputs
- **Bad Rows Files**: Invalid data in JSON format
- **Error Reports**: Detailed validation failure information
- **Processing Logs**: Execution statistics and timing

## Performance Optimization

### Memory Management
- **Streaming Processing**: Process data in configurable batches
- **PyArrow Integration**: Efficient columnar data handling
- **Resource Cleanup**: Automatic cleanup of temporary resources

### Scalability Features
- **Batch Processing**: Configurable batch sizes for optimal memory usage
- **S3 Streaming**: Efficient cloud storage integration
- **Lazy Loading**: Components initialized only when needed

### Performance Tuning
```python
# Optimize for large files
results = import_csv(
    input_path="huge_dataset.csv",
    output_path="output/",
    batch_size=100000,      # Larger batches for throughput
    validate_schema=False,   # Skip validation for trusted data
    compression="snappy"     # Fast compression
)
```

## Error Handling and Resilience

### Graceful Error Recovery
- **Partial Processing**: Continue processing despite individual row failures
- **Error Separation**: Invalid data preserved for investigation
- **Detailed Reporting**: Comprehensive error information in results

### Exception Management
```python
try:
    results = import_csv("data.csv", "output/")
    if results.invalid_rows > 0:
        print(f"Processing completed with {results.invalid_rows} invalid rows")
        # Invalid data available in results.bad_rows_file
except ProcessingError as e:
    print(f"Processing failed: {e}")
```

## Integration Examples

### Basic Usage
```python
from forklift.engine import import_csv, import_excel, import_sql

# Simple CSV import
results = import_csv("data.csv", "output/")

# Excel with schema validation
results = import_excel(
    input_path="workbook.xlsx",
    output_path="output/",
    schema_file="excel_schema.json"
)

# SQL database import
results = import_sql(
    connection_string="postgresql://user:pass@localhost:5432/db",
    output_path="output/",
    schema_file="sql_schema.json"
)
```

### Advanced Configuration
```python
from forklift.engine import ForkliftCore, ImportConfig, HeaderMode

# Custom configuration
config = ImportConfig(
    input_path="complex_data.csv",
    output_path="s3://my-bucket/processed/",
    schema_file="validation_schema.json",
    header_mode=HeaderMode.AUTO,
    batch_size=25000,
    create_manifest=True,
    max_validation_errors=500
)

# Direct engine usage
engine = ForkliftCore(config)
results = engine.process_csv()
```

## Extension Points

The Forklift Engine is designed for extensibility:

### Custom Processors
- Implement `BaseProcessor` interface for new data sources
- Add custom validation logic through processor extensions
- Integrate with existing streaming architecture

### Configuration Extensions
- Extend `ImportConfig` for format-specific options
- Add custom validation rules through schema extensions
- Implement custom error handling strategies

### Output Format Support
- Add new output formats through processor extensions
- Implement custom metadata generation
- Support additional compression algorithms

## Dependencies and Requirements

### Core Dependencies
- **PyArrow**: High-performance columnar processing
- **Pandas**: Data manipulation and analysis
- **boto3**: AWS S3 integration
- **pyodbc**: Database connectivity (for SQL import)

### Optional Dependencies
- **openpyxl/xlrd**: Excel file processing
- **fastparquet**: Alternative Parquet engine
- **s3fs**: Enhanced S3 filesystem operations

## Future Roadmap

### Planned Enhancements
- **Fixed-Width File Support**: Complete implementation of `import_fwf()`
- **Additional Formats**: JSON, XML, and other structured data formats
- **Advanced Transformations**: Built-in data transformation capabilities
- **Performance Optimizations**: Further streaming and parallel processing improvements

### Integration Opportunities
- **Data Catalog Integration**: Automatic schema registry updates
- **Monitoring Integration**: Enhanced observability and metrics
- **Workflow Orchestration**: Integration with data pipeline frameworks
