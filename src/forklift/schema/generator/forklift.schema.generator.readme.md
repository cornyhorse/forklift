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
Handles the core data type inference logic for different file formats (PyArrow and openpyxl only, no pandas):
- **CSV Support**: Custom delimiters and encodings. The file (local or S3) is streamed with `pyarrow.csv.open_csv` as all-string columns and reading stops after `nrows` rows; Forklift's own inference then assigns the types, so `nrows=None` and a large `nrows` agree and identifiers with leading zeros (`02134`) stay strings. Integers must match `^-?(0|[1-9]\d*)$`; booleans are `true`/`false`; `YYYY-MM-DD` dates and `YYYY-MM-DD[T ]HH:MM[:SS[.f]]` timestamps are detected; null tokens are the empty string, `NULL`, `null`, `N/A`, `n/a`, `#N/A`, `NaN` and `nan` (`NA` is a value, not a null)
- **Excel Support**: `openpyxl.load_workbook(read_only=True, data_only=True)`, reading at most `nrows + 1` rows of the chosen sheet; cells keep their Excel types and the workbook is closed afterwards. Legacy `.xls` is not supported
- **Parquet Support**: Only the row groups needed for `nrows` rows are read; the file's own types are kept
- **S3 Integration**: Objects are opened as binary streams through `UnifiedIOHandler.open_for_read(path, encoding="binary")`
- **Input guard**: only local paths and `s3://` URIs are accepted. URL-like inputs (`http://`, `https://`, `ftp://`, `file://` ...) raise `ValueError`

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
- Sample size for analysis (`nrows`; `None` analyses the whole file; `0`, negative numbers, booleans and non-integers raise `ValueError`). The `SchemaGenerationConfig` default is 1000 rows, the `generate_schema_from_csv` and `generate_schema_from_parquet` API functions and the CLI default to `None`
- Enum detection thresholds
- Uniqueness analysis parameters
- Quantile calculations for numeric data (each quantile must be within 0..1, otherwise `ValueError`)

### Output Configuration
- Multiple output targets (stdout, file, clipboard)
- Sample data inclusion options (`include_sample_data`, off by default)
- Metadata generation controls, including `include_value_statistics` (off by default): top/bottom values, enum value lists, min/max/median and quantiles copy raw cell values into the schema and are only produced when enabled
- Primary key inference settings

## File Format Support

### CSV Files
- Configurable delimiter (comma by default; there is no auto-detection) and encoding (an unknown encoding raises `ValueError`)
- The first row is the header; empty or duplicate names are made unique (`Unnamed: 3`, `a.1`)
- Null value interpretation (see above)
- Streaming sampling: reading stops after `nrows` rows

### Excel Files
- One sheet per run (`sheet_name`: a name or, through the Python API, a 0-based integer index; default the first sheet). The CLI passes `--sheet` as text, so it is looked up as a sheet name
- `.xlsx` only: a legacy `.xls` workbook raises `ValueError` (convert it, or import it with `import_excel`)
- Cell type inference from the Excel cell types; a column that mixes types becomes a string column
- Date/time values keep their type
- Cached formula values are used

### Parquet Files
- Schema preservation
- Efficient column sampling
- Metadata extraction
- Type system mapping
- Compression handling

## Generated Schema Features

### Core Schema Elements
- JSON Schema Draft 2020-12 (`$schema`, `$id`, `title`, `type: object`)
- Property definitions with type constraints
- Required field specifications
- Format validations

### File-Specific Extensions
- **x-csv**: CSV layout that was analysed (encoding priority, delimiter, null markers, `dataTypes`)
- **x-excel**: Excel sheet settings (`sheet` is a single name or index, `header`, `skipRows`, `nulls`); `ExcelSchemaImporter` accepts both this `sheet` form and a `sheets` list
- **x-transformations**: Suggested cleaning steps per column. Every step is written with `"enabled": false`, so `import_csv` changes nothing until you enable one; the block's `version`, `global_settings` and `transformation_types` keys are documentation only and `import_csv` reports them in `results.warnings`
- **x-primaryKey**: Primary key configurations (inferred only for columns that are 100% unique in the sample). `import_csv` enforces it: later files with a duplicate or NULL key get those rows rejected. Its `inference_metadata` key is not read (reported as a warning)

### Analysis Metadata
- Column statistics (counts, null statistics, distinct counts, string lengths; value statistics only with `include_value_statistics=True`)
- Data quality metrics
- The source file is recorded by file name only, never by absolute path
- Sample data representations (only with `include_sample_data=True`)

## Usage Patterns

### Basic Schema Generation
```python
from forklift.schema import SchemaGenerator, SchemaGenerationConfig, FileType

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
    uniqueness_threshold=0.95,
    include_value_statistics=False,  # default; True adds top/bottom values, min/max, quantiles
)
schema = SchemaGenerator(config).generate_schema()
```

## Integration Points

### Internal Dependencies
- `forklift.schema.processors.*` - Specialized schema processing
- `forklift.schema.types.*` - Type detection and transformation
- `forklift.schema.utils.*` - Formatting and helper utilities
- `forklift.io` - Unified I/O operations

### External Dependencies
- **PyArrow**: High-performance data processing
- **openpyxl**: Excel sampling
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
- **S3 Optimization**: CSV objects are streamed and closed once `nrows` rows are read; Parquet and Excel need random access, so the object is first copied to a temporary local file
- **Type Inference**: Optimized algorithms for fast analysis
