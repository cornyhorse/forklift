# Forklift API Reference

Complete API documentation for all Forklift functions and classes.

## Table of Contents

- [Import Functions](#import-functions)
- [Reader Functions](#reader-functions)
- [Schema Generation Functions](#schema-generation-functions)
- [Configuration Classes](#configuration-classes)
- [Exception Classes](#exception-classes)

## Import Functions

These functions perform ETL operations, reading data and writing to Parquet files.

### `import_csv()`

Import CSV data to Parquet with validation and processing.

```python
def import_csv(
    source: Union[str, Path],
    destination: Union[str, Path],
    schema_path: Optional[Union[str, Path]] = None,
    config: Optional[ImportConfig] = None,
    preprocessors: Optional[List[str]] = None,
    constraint_config: Optional[ConstraintConfig] = None
) -> ImportResult
```

**Parameters:**
- `source`: Path to CSV file (local or S3 URI)
- `destination`: Output directory path (local or S3 URI)
- `schema_path`: Optional path to JSON schema file
- `config`: Import configuration options
- `preprocessors`: List of preprocessor names to apply
- `constraint_config`: Constraint validation configuration

**Returns:** `ImportResult` object with processing statistics

**Example:**
```python
import forklift

result = forklift.import_csv(
    source="data.csv",
    destination="./output/",
    schema_path="schema.json"
)
print(f"Processed {result.total_rows} rows")
```

### `import_excel()`

Import Excel data to Parquet with validation and processing.

```python
def import_excel(
    source: Union[str, Path],
    destination: Union[str, Path],
    sheet_name: Optional[Union[str, int]] = None,
    schema_path: Optional[Union[str, Path]] = None,
    config: Optional[ImportConfig] = None,
    preprocessors: Optional[List[str]] = None
) -> ImportResult
```

**Parameters:**
- `source`: Path to Excel file (local or S3 URI)
- `destination`: Output directory path (local or S3 URI)
- `sheet_name`: Sheet name or index to process
- `schema_path`: Optional path to JSON schema file
- `config`: Import configuration options
- `preprocessors`: List of preprocessor names to apply

**Returns:** `ImportResult` object with processing statistics

### `import_fwf()`

Import Fixed-Width File data to Parquet with validation and processing.

```python
def import_fwf(
    source: Union[str, Path],
    destination: Union[str, Path],
    schema_path: Union[str, Path],
    config: Optional[ImportConfig] = None,
    preprocessors: Optional[List[str]] = None
) -> ImportResult
```

**Parameters:**
- `source`: Path to FWF file (local or S3 URI)
- `destination`: Output directory path (local or S3 URI)
- `schema_path`: Path to JSON schema file with FWF field definitions
- `config`: Import configuration options
- `preprocessors`: List of preprocessor names to apply

**Returns:** `ImportResult` object with processing statistics

### `import_sql()`

Import SQL query results to Parquet with validation and processing.

```python
def import_sql(
    query: str,
    connection_string: str,
    destination: Union[str, Path],
    schema_path: Optional[Union[str, Path]] = None,
    config: Optional[ImportConfig] = None,
    preprocessors: Optional[List[str]] = None
) -> ImportResult
```

**Parameters:**
- `query`: SQL query to execute
- `connection_string`: Database connection string
- `destination`: Output directory path (local or S3 URI)
- `schema_path`: Optional path to JSON schema file
- `config`: Import configuration options
- `preprocessors`: List of preprocessor names to apply

**Returns:** `ImportResult` object with processing statistics

## Reader Functions

These functions read data into pandas DataFrames for analysis.

### `read_csv()`

Read CSV file into pandas DataFrame.

```python
def read_csv(
    source: Union[str, Path],
    encoding: str = "utf-8",
    delimiter: str = ",",
    header_mode: Union[str, HeaderMode] = HeaderMode.PRESENT,
    nrows: Optional[int] = None,
    **kwargs
) -> pd.DataFrame
```

**Parameters:**
- `source`: Path to CSV file (local or S3 URI)
- `encoding`: File encoding (default: "utf-8")
- `delimiter`: Field delimiter (default: ",")
- `header_mode`: Header handling mode
- `nrows`: Maximum number of rows to read
- `**kwargs`: Additional pandas.read_csv arguments

**Returns:** pandas DataFrame

### `read_excel()`

Read Excel file into pandas DataFrame.

```python
def read_excel(
    source: Union[str, Path],
    sheet_name: Optional[Union[str, int]] = None,
    skip_rows: int = 0,
    nrows: Optional[int] = None,
    **kwargs
) -> pd.DataFrame
```

**Parameters:**
- `source`: Path to Excel file (local or S3 URI)
- `sheet_name`: Sheet name or index to read
- `skip_rows`: Number of rows to skip at beginning
- `nrows`: Maximum number of rows to read
- `**kwargs`: Additional pandas.read_excel arguments

**Returns:** pandas DataFrame

### `read_fwf()`

Read Fixed-Width File into pandas DataFrame.

```python
def read_fwf(
    source: Union[str, Path],
    schema_path: Optional[Union[str, Path]] = None,
    field_specs: Optional[List[Dict]] = None,
    encoding: str = "utf-8",
    **kwargs
) -> pd.DataFrame
```

**Parameters:**
- `source`: Path to FWF file (local or S3 URI)
- `schema_path`: Path to JSON schema with field definitions
- `field_specs`: Inline field specifications
- `encoding`: File encoding (default: "utf-8")
- `**kwargs`: Additional processing arguments

**Returns:** pandas DataFrame

### `read_sql()`

Read SQL query results into pandas DataFrame.

```python
def read_sql(
    query: str,
    connection_string: str,
    **kwargs
) -> pd.DataFrame
```

**Parameters:**
- `query`: SQL query to execute
- `connection_string`: Database connection string
- `**kwargs`: Additional pandas.read_sql arguments

**Returns:** pandas DataFrame

### `DataFrameReader`

Unified reader class for multiple formats.

```python
class DataFrameReader:
    @staticmethod
    def read(
        source: Union[str, Path],
        file_type: str,
        **kwargs
    ) -> pd.DataFrame
```

**Parameters:**
- `source`: Path to data file
- `file_type`: Type of file ("csv", "excel", "fwf", "parquet")
- `**kwargs`: Format-specific arguments

**Returns:** pandas DataFrame

## Schema Generation Functions

### `generate_schema_from_csv()`

Generate JSON schema from CSV file analysis.

```python
def generate_schema_from_csv(
    input_path: Union[str, Path],
    nrows: Optional[int] = None,
    delimiter: str = ",",
    encoding: str = "utf-8",
    include_sample_data: bool = False,
    infer_primary_key_from_metadata: bool = False,
    user_specified_primary_key: Optional[List[str]] = None
) -> Dict[str, Any]
```

**Parameters:**
- `input_path`: Path to CSV file (local or S3 URI)
- `nrows`: Number of rows to analyze (None = entire file)
- `delimiter`: CSV field delimiter
- `encoding`: File encoding
- `include_sample_data`: Include sample data in schema
- `infer_primary_key_from_metadata`: Infer primary key automatically
- `user_specified_primary_key`: Manually specify primary key columns

**Returns:** Dictionary containing generated schema

### `generate_schema_from_excel()`

Generate JSON schema from Excel file analysis.

```python
def generate_schema_from_excel(
    input_path: Union[str, Path],
    sheet_name: Optional[Union[str, int]] = None,
    nrows: Optional[int] = None,
    skip_rows: int = 0,
    include_sample_data: bool = False,
    infer_primary_key_from_metadata: bool = False,
    user_specified_primary_key: Optional[List[str]] = None
) -> Dict[str, Any]
```

**Parameters:**
- `input_path`: Path to Excel file (local or S3 URI)
- `sheet_name`: Sheet name or index to analyze
- `nrows`: Number of rows to analyze (None = entire sheet)
- `skip_rows`: Number of rows to skip at beginning
- `include_sample_data`: Include sample data in schema
- `infer_primary_key_from_metadata`: Infer primary key automatically
- `user_specified_primary_key`: Manually specify primary key columns

**Returns:** Dictionary containing generated schema

### `generate_schema_from_parquet()`

Generate JSON schema from Parquet file analysis.

```python
def generate_schema_from_parquet(
    input_path: Union[str, Path],
    include_sample_data: bool = False
) -> Dict[str, Any]
```

**Parameters:**
- `input_path`: Path to Parquet file (local or S3 URI)
- `include_sample_data`: Include sample data in schema

**Returns:** Dictionary containing generated schema

### `generate_and_save_schema()`

Generate schema and save to file.

```python
def generate_and_save_schema(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    file_type: str,
    **kwargs
) -> None
```

**Parameters:**
- `input_path`: Path to input data file
- `output_path`: Path for output schema file
- `file_type`: Type of input file ("csv", "excel", "parquet")
- `**kwargs`: Additional generation arguments

### `generate_and_copy_schema()`

Generate schema and copy to clipboard.

```python
def generate_and_copy_schema(
    input_path: Union[str, Path],
    file_type: str,
    **kwargs
) -> None
```

**Parameters:**
- `input_path`: Path to input data file
- `file_type`: Type of input file ("csv", "excel", "parquet")
- `**kwargs`: Additional generation arguments

**Requires:** `pyperclip` package for clipboard functionality

## Configuration Classes

### `ImportConfig`

Configuration for import operations.

```python
class ImportConfig:
    def __init__(
        self,
        batch_size: int = 10000,
        memory_limit_mb: int = 1024,
        error_handling_mode: ErrorHandlingMode = ErrorHandlingMode.LOG,
        bad_rows_path: Optional[str] = None,
        use_streaming: bool = True,
        output_format: str = "parquet",
        compression: str = "snappy"
    )
```

**Attributes:**
- `batch_size`: Number of rows to process per batch
- `memory_limit_mb`: Memory limit in megabytes
- `error_handling_mode`: How to handle validation errors
- `bad_rows_path`: Path for bad rows output
- `use_streaming`: Enable streaming for large files
- `output_format`: Output file format
- `compression`: Compression algorithm

### `HeaderMode`

Enum for header handling modes.

```python
class HeaderMode(Enum):
    PRESENT = "present"    # File has header row
    ABSENT = "absent"      # File has no header
    AUTO = "auto"          # Auto-detect header presence
```

### `ErrorHandlingMode`

Enum for error handling strategies.

```python
class ErrorHandlingMode(Enum):
    FAIL_FAST = "fail_fast"        # Stop on first error
    FAIL_COMPLETE = "fail_complete" # Collect all errors, then fail
    LOG = "log"                     # Log errors and continue
    BAD_ROWS = "bad_rows"          # Save bad rows to separate file
```

### `ConstraintConfig`

Configuration for constraint validation.

```python
class ConstraintConfig:
    def __init__(
        self,
        enforce_primary_key: bool = True,
        enforce_unique_constraints: bool = True,
        enforce_not_null: bool = True,
        error_mode: ErrorHandlingMode = ErrorHandlingMode.LOG
    )
```

**Attributes:**
- `enforce_primary_key`: Validate primary key constraints
- `enforce_unique_constraints`: Validate unique constraints
- `enforce_not_null`: Validate not-null constraints
- `error_mode`: How to handle constraint violations

## Result Classes

### `ImportResult`

Result object returned by import functions.

```python
class ImportResult:
    total_rows: int           # Total rows processed
    valid_rows: int           # Number of valid rows
    invalid_rows: int         # Number of invalid rows
    output_files: List[str]   # List of output file paths
    processing_time: float    # Processing time in seconds
    errors: List[str]         # List of error messages
    metadata: Dict[str, Any]  # Additional metadata
```

## Exception Classes

### `ForkliftError`

Base exception class for all Forklift errors.

```python
class ForkliftError(Exception):
    """Base exception for Forklift operations."""
    pass
```

### `ValidationError`

Exception raised for data validation errors.

```python
class ValidationError(ForkliftError):
    """Exception raised for data validation errors."""
    
    def __init__(self, message: str, row_number: Optional[int] = None):
        self.row_number = row_number
        super().__init__(message)
```

### `SchemaError`

Exception raised for schema-related errors.

```python
class SchemaError(ForkliftError):
    """Exception raised for schema validation or generation errors."""
    pass
```

### `ConfigurationError`

Exception raised for configuration errors.

```python
class ConfigurationError(ForkliftError):
    """Exception raised for invalid configuration."""
    pass
```

## Usage Examples

### Complete Import Workflow

```python
import forklift
from forklift.engine.forklift_core import ImportConfig, ErrorHandlingMode
from forklift.processors.constraint_validator import ConstraintConfig

# Configure import
config = ImportConfig(
    batch_size=50000,
    memory_limit_mb=2048,
    error_handling_mode=ErrorHandlingMode.BAD_ROWS,
    bad_rows_path="./errors/"
)

# Configure validation
constraint_config = ConstraintConfig(
    enforce_primary_key=True,
    enforce_unique_constraints=True,
    enforce_not_null=True
)

# Perform import
result = forklift.import_csv(
    source="large_dataset.csv",
    destination="./output/",
    schema_path="schema.json",
    config=config,
    constraint_config=constraint_config,
    preprocessors=["string_cleaning", "date_standardization"]
)

# Check results
print(f"Import completed:")
print(f"  Total rows: {result.total_rows}")
print(f"  Valid rows: {result.valid_rows}")
print(f"  Invalid rows: {result.invalid_rows}")
print(f"  Processing time: {result.processing_time:.2f}s")
print(f"  Output files: {result.output_files}")
```

### Schema Generation with Options

```python
import forklift

# Generate comprehensive schema
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    nrows=10000,  # Analyze first 10k rows
    include_sample_data=False,  # Privacy-safe
    infer_primary_key_from_metadata=True
)

# Save with metadata
forklift.generate_and_save_schema(
    input_path="customer_data.csv",
    output_path="customer_schema.json",
    file_type="csv",
    include_sample_data=False,
    infer_primary_key_from_metadata=True
)
```

This API reference provides complete documentation for all public Forklift functions and classes. For usage examples and workflows, see the [Usage Guide](USAGE.md).
