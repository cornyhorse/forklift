# forklift.schema.generator

## Overview

The `forklift.schema.generator` subpackage is responsible for automatically generating JSON schemas from various data file formats. It provides a complete pipeline for schema inference, validation, and generation with support for CSV, Excel, and Parquet files.

## Key Components

### SchemaGenerator
The main orchestrator class that coordinates the entire schema generation process. It integrates multiple components to:
- Read sample data from various file formats
- Infer data types and structures
- Generate JSON schema properties
- Add file-format specific extensions
- Include transformation configurations
- Generate metadata and validation rules

### DataTypeInferrer
Handles the core data type inference logic for different file formats:
- **CSV Support**: Custom delimiters, encodings, and sampling strategies
- **Excel Support**: Sheet selection, row limits, and format detection
- **Parquet Support**: Efficient columnar format processing with built-in schema information
- **S3 Integration**: Seamless handling of cloud-stored files

### SchemaValidator
Provides comprehensive validation capabilities for:
- **Schema Structure**: Validates JSON schema compliance and required fields
- **Data Compatibility**: Ensures data matches schema definitions
- **Transformation Config**: Validates transformation rule configurations
- **Field Requirements**: Checks required field constraints and null value handling

## Configuration Options

The subpackage supports extensive configuration through `SchemaGenerationConfig`:

### Input Configuration
- File paths (local or S3)
- File type specification (CSV/Excel/Parquet)
- Encoding and delimiter settings
- Sheet selection for Excel files

### Processing Configuration
- Sample size for analysis (nrows parameter)
- Enum detection thresholds
- Uniqueness analysis parameters
- Quantile calculations for numeric data

### Output Configuration
- Multiple output targets (stdout, file, clipboard)
- Sample data inclusion options
- Metadata generation controls
- Primary key inference settings

## File Format Support

### CSV Files
- Automatic delimiter detection
- Custom encoding support
- Header row handling
- Null value interpretation
- Large file sampling strategies

### Excel Files
- Multi-sheet support
- Format detection (.xlsx, .xls)
- Cell type inference
- Date/time format handling
- Formula value extraction

### Parquet Files
- Schema preservation
- Efficient column sampling
- Metadata extraction
- Type system mapping
- Compression handling

## Generated Schema Features

### Core Schema Elements
- JSON Schema Draft-07 compliance
- Property definitions with type constraints
- Required field specifications
- Format validations

### File-Specific Extensions
- **x-csv**: CSV-specific parsing configurations
- **x-excel**: Excel sheet and format settings
- **x-transformations**: Data transformation rules
- **x-primaryKey**: Primary key configurations

### Analysis Metadata
- Column statistics and distributions
- Data quality metrics
- Type inference confidence scores
- Sample data representations

## Usage Patterns

### Basic Schema Generation
```python
from forklift.schema.generator import SchemaGenerator, SchemaGenerationConfig, FileType

config = SchemaGenerationConfig(
    input_path="data.csv",
    file_type=FileType.CSV,
    nrows=1000
)
generator = SchemaGenerator(config)
schema = generator.generate_schema()
```

### Advanced Configuration
```python
config = SchemaGenerationConfig(
    input_path="s3://bucket/data.xlsx",
    file_type=FileType.EXCEL,
    sheet_name="Sheet1",
    include_sample_data=True,
    generate_metadata=True,
    enum_threshold=0.1,
    uniqueness_threshold=0.95
)
```

## Integration Points

### Internal Dependencies
- `forklift.schema.processors.*` - Specialized schema processing
- `forklift.schema.types.*` - Type detection and transformation
- `forklift.schema.utils.*` - Formatting and helper utilities
- `forklift.io` - Unified I/O operations

### External Dependencies
- **PyArrow**: High-performance data processing
- **Pandas**: Data manipulation and analysis
- **pyperclip**: Clipboard integration (optional)

## Error Handling

The subpackage provides robust error handling for:
- File access and format issues
- Schema validation failures
- Data type inference conflicts
- Configuration validation errors
- Memory and performance constraints

## Performance Considerations

- **Sampling Strategy**: Configurable row limits for large files
- **Memory Management**: Efficient PyArrow-based processing
- **S3 Optimization**: Streaming and partial reads for cloud files
- **Type Inference**: Optimized algorithms for fast analysis
